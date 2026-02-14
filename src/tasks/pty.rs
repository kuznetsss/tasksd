use std::{os::fd::OwnedFd, task::Poll};

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite, unix::AsyncFd};

#[derive(Debug)]
pub struct Pty(AsyncFd<OwnedFd>);

pub type PtyChild = OwnedFd;

pub fn create_pty_pair() -> Result<(Pty, PtyChild)> {
    use rustix::{
        fs::{Mode, OFlags, fcntl_getfl, fcntl_setfl, open},
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

impl AsyncWrite for Pty {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            let mut guard = match self.0.poll_write_ready(cx) {
                Poll::Ready(guard) => guard?,
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|inner| rustix::io::write(inner.get_ref(), buf).map_err(Into::into))
            {
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let mut guard = match self.0.poll_write_ready(cx) {
                Poll::Ready(guard) => guard?,
                Poll::Pending => return Poll::Pending,
            };
            match guard.try_io(|_| Ok(())) {
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create() {
        let (pty, child) = create_pty_pair().unwrap();
        assert!(rustix::termios::isatty(pty.0));
        assert!(rustix::termios::isatty(child));
    }

    #[tokio::test]
    async fn write_to_child_read_from_pty() {
        let msg = "test message";
        let (pty, child) = create_pty_pair().unwrap();
        rustix::io::write(&child, msg.as_bytes()).unwrap();
        let mut buf = [0u8; 32];
        let read_len = rustix::io::read(pty.0, &mut buf).unwrap();
        assert_eq!(msg, String::from_utf8_lossy(&buf[..read_len]));
    }

    #[tokio::test]
    async fn write_to_pty_read_from_child() {
        let msg = "test message\n"; // child will return data on reading only after '\n'
        let (pty, child) = create_pty_pair().unwrap();
        rustix::io::write(&pty.0, msg.as_bytes()).unwrap();
        let mut buf = [0u8; 32];
        let read_len = rustix::io::read(&child, &mut buf).unwrap();
        assert_eq!(msg, String::from_utf8_lossy(&buf[..read_len]));
    }

    #[tokio::test]
    async fn pty_closed() {
        let (pty, child) = create_pty_pair().unwrap();
        drop(pty);
        rustix::io::write(&child, "test".as_bytes()).unwrap_err();
        let mut buf = [0u8; 32];
        assert_eq!(rustix::io::read(&child, &mut buf).unwrap(), 0);
    }

    #[tokio::test]
    async fn child_closed() {
        let (pty, child) = create_pty_pair().unwrap();
        drop(child);
        rustix::io::write(&pty.0, "test".as_bytes()).unwrap_err();
        let mut buf = [0u8; 32];
        assert_eq!(rustix::io::read(&pty.0, &mut buf).unwrap(), 0);
    }
}
