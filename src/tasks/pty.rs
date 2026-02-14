use std::{os::fd::OwnedFd, task::Poll};

use anyhow::Result;
use rustix::fs::{fcntl_getfl, fcntl_setfl};
use tokio::io::{AsyncRead, unix::AsyncFd};

pub struct Pty(AsyncFd<OwnedFd>);

pub type PtyChild = OwnedFd;

pub fn create_pty_pair() -> Result<(Pty, PtyChild)> {
    use rustix::{
        fs::{Mode, OFlags, open},
        io::{FdFlags, fcntl_getfd, fcntl_setfd},
        pty::{OpenptFlags, grantpt, openpt, ptsname, unlockpt},
    };

    let pty = openpt(OpenptFlags::RDWR | OpenptFlags::NOCTTY)?;
    let mut flags = fcntl_getfd(&pty)?;
    flags |= FdFlags::CLOEXEC;
    fcntl_setfd(&pty, flags)?;
    // enable non-blocking
    let mut flags = fcntl_getfl(&pty)?;
    flags |= OFlags::NONBLOCK;
    fcntl_setfl(&pty, flags)?;
    grantpt(&pty)?;
    unlockpt(&pty)?;

    let child_name = ptsname(&pty, Vec::new())?;
    let child = open(&child_name, OFlags::RDWR | OFlags::NOCTTY, Mode::empty())?;

    let pty = Pty(AsyncFd::new(pty)?);
    Ok((pty, child))
}

// TODO: maybe make Pty splitable into reader and writer parts

impl AsyncRead for Pty {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            let mut guard = match self.0.poll_read_ready(cx) {
                Poll::Ready(g) => g?,
                Poll::Pending => return Poll::Pending,
            };
            // Safety: unfilled_mut() requires for no de-initialization but read will never do that
            let b = unsafe { buf.unfilled_mut() };
            match guard.try_io(|inner| rustix::io::read(inner.get_ref(), b).map_err(Into::into)) {
                Ok(Ok((read_bytes, _))) => {
                    let read_bytes = read_bytes.len();
                    // Safety: we are sure that read_bytes is the number of bytes we just initialized
                    unsafe {
                        buf.assume_init(read_bytes);
                    }
                    buf.advance(read_bytes);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                Err(_) => continue,
            }
        }
    }
}

// TODO: implement tokio async write for Pty
