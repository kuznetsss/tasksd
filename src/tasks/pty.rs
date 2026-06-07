use std::{os::fd::OwnedFd, task::Poll, time::Duration};

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncWrite, unix::AsyncFd};
use tracing::warn;

#[derive(Debug)]
pub struct Pty(AsyncFd<OwnedFd>);

#[derive(Debug)]
pub struct PtyReadPart(AsyncFd<OwnedFd>);
#[derive(Debug)]
pub struct PtyWritePart(AsyncFd<OwnedFd>);

pub type PtyChild = OwnedFd;

pub fn create_pty_pair() -> Result<(Pty, PtyChild)> {
    // Opening multiple ptys in parallel may fail with unknown error (-6) on macos.
    // Retrying in such case
    const MAX_ATTEMPTS: usize = 5;
    const INTERVAL_BETWEEN_ATTEMPTS: Duration = Duration::from_millis(50);

    for i in 1..=MAX_ATTEMPTS {
        match create_pty_pair_impl() {
            Ok(r) => {
                return Ok(r);
            }
            Err(e)
                if e.root_cause()
                    .downcast_ref::<rustix::io::Errno>()
                    .is_some_and(|e| e.raw_os_error() == -6) =>
            {
                warn!("Attempt {i} of {MAX_ATTEMPTS} to create pty failed: {e}.");
                if i != MAX_ATTEMPTS {
                    warn!("Will try to create pty again after {INTERVAL_BETWEEN_ATTEMPTS:?}");
                    std::thread::sleep(INTERVAL_BETWEEN_ATTEMPTS);
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    anyhow::bail!("Couldn't create pty pair after {} attempts", MAX_ATTEMPTS)
}

fn create_pty_pair_impl() -> Result<(Pty, PtyChild)> {
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

    // Disable echo so writes to the master aren't reflected back as output
    let mut termios = rustix::termios::tcgetattr(&pty)?;
    termios.local_modes &= !rustix::termios::LocalModes::ECHO;
    rustix::termios::tcsetattr(&pty, rustix::termios::OptionalActions::Now, &termios)?;

    let pty = Pty(AsyncFd::new(pty)?);
    Ok((pty, child))
}

impl Pty {
    pub fn into_split(self) -> Result<(PtyReadPart, PtyWritePart)> {
        let pty_clone = self.0.get_ref().try_clone()?;
        let pty_read = PtyReadPart(AsyncFd::new(pty_clone)?);
        Ok((pty_read, PtyWritePart(self.0)))
    }
}

impl AsyncRead for PtyReadPart {
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

impl AsyncWrite for PtyWritePart {
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
    use std::{
        io::ErrorKind,
        mem::MaybeUninit,
        pin::{Pin, pin},
        task::Context,
        time::Duration,
    };

    use super::*;
    use rustix::{
        fs::{OFlags, fcntl_getfl, fcntl_setfl},
        path::Arg,
    };
    use tokio::io::{AsyncRead, ReadBuf};

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

    #[tokio::test]
    async fn pty_async_read_pending() {
        let (pty, _child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [MaybeUninit::uninit(); 8];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut buf);
        match pty.as_mut().poll_read(&mut cx, &mut read_buf) {
            Poll::Pending => (),
            Poll::Ready(r) => panic!("Unexpected Ready: {r:?}"),
        }
    }

    async fn read(pty: &mut Pin<&mut PtyReadPart>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) {
        let mut attempt = 0;
        const MAX_ATTEMPTS: i32 = 10;
        while attempt < MAX_ATTEMPTS {
            match pty.as_mut().poll_read(cx, buf) {
                Poll::Pending => tokio::time::sleep(Duration::from_millis(5)).await,
                Poll::Ready(r) => {
                    r.unwrap();
                    break;
                }
            }
            attempt += 1;
        }
        assert!(attempt <= MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn pty_async_read_ready() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [MaybeUninit::uninit(); 8];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut buf);
        let msg = "test\n";
        rustix::io::write(&child, msg.as_bytes()).unwrap();
        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), msg.len() + 1); // pty adds '\r'
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "test\r\n");
        assert_eq!(read_buf.initialized().len(), read_buf.filled().len());

        rustix::io::write(&child, "second".as_bytes()).unwrap();
        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), read_buf.capacity());
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "test\r\nse");
        assert_eq!(read_buf.initialized().len(), read_buf.filled().len());
    }

    #[tokio::test]
    async fn pty_async_read_0_bytes() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [MaybeUninit::uninit(); 8];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut buf);
        let msg = "test\n";
        rustix::io::write(&child, msg.as_bytes()).unwrap();
        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), msg.len() + 1); // pty adds '\r'
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "test\r\n");
        assert_eq!(read_buf.initialized().len(), read_buf.filled().len());

        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), msg.len() + 1);
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "test\r\n");
        assert_eq!(read_buf.initialized().len(), read_buf.filled().len());
    }

    #[tokio::test]
    async fn pty_async_read_child_closed() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [MaybeUninit::uninit(); 8];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut buf);
        drop(child);

        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), 0);
        assert_eq!(read_buf.initialized().len(), 0);

        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.filled().len(), 0);
        assert_eq!(read_buf.initialized().len(), 0);
    }

    #[tokio::test]
    async fn pty_async_read_into_full_buffer() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [0; 8];
        let mut read_buf = tokio::io::ReadBuf::new(&mut buf);
        assert_eq!(read_buf.initialized().len(), read_buf.capacity());
        read_buf.advance(read_buf.capacity());
        assert_eq!(read_buf.filled().len(), read_buf.capacity());

        rustix::io::write(&child, "test\n".as_bytes()).unwrap();
        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.initialized().len(), read_buf.capacity());
        assert_eq!(read_buf.filled().len(), read_buf.capacity());
        assert!(read_buf.filled().iter().all(|&e| e == 0));
    }

    #[tokio::test]
    async fn pty_async_read_more_data_than_buffer_size() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [0; 8];
        let mut read_buf = tokio::io::ReadBuf::new(&mut buf);

        rustix::io::write(&child, "some long line\n".as_bytes()).unwrap();

        read(&mut pty, &mut cx, &mut read_buf).await;
        assert_eq!(read_buf.initialized().len(), read_buf.capacity());
        assert_eq!(read_buf.filled().len(), read_buf.capacity());
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "some lon");

        read_buf.clear();
        match pty.as_mut().poll_read(&mut cx, &mut read_buf) {
            Poll::Pending => panic!("Expected Ready"),
            Poll::Ready(r) => r.unwrap(),
        }
        assert_eq!(read_buf.initialized().len(), read_buf.capacity());
        assert_eq!(read_buf.filled().len(), read_buf.capacity());
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), "g line\r\n");
    }

    #[tokio::test]
    async fn pty_async_read_multiple_lines() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty, _) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut buf = [MaybeUninit::uninit(); 64];
        let mut read_buf = tokio::io::ReadBuf::uninit(&mut buf);

        rustix::io::write(&child, "one\n".as_bytes()).unwrap();
        rustix::io::write(&child, "two\n".as_bytes()).unwrap();
        rustix::io::write(&child, "three\n".as_bytes()).unwrap();

        read(&mut pty, &mut cx, &mut read_buf).await;
        let expected = "one\r\ntwo\r\nthree\r\n";
        assert_eq!(read_buf.initialized().len(), expected.len());
        assert_eq!(read_buf.filled().len(), expected.len());
        assert_eq!(String::from_utf8_lossy(read_buf.filled()), expected);
    }

    async fn write(pty: &mut Pin<&mut PtyWritePart>, cx: &mut Context<'_>, buf: &str) -> usize {
        const MAX_ATTEMPTS: i32 = 10;
        for _ in 0..MAX_ATTEMPTS {
            match pty.as_mut().poll_write(cx, buf.as_bytes()) {
                Poll::Pending => tokio::time::sleep(Duration::from_millis(5)).await,
                Poll::Ready(r) => {
                    return r.unwrap();
                }
            }
        }
        panic!("Write didn't happen after {MAX_ATTEMPTS} attempts");
    }

    #[tokio::test]
    async fn pty_async_write() {
        let (pty, child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let msg = "test\n";
        assert_eq!(write(&mut pty, &mut cx, msg).await, msg.len());
        let mut buf = [0; 64];
        let read_size = rustix::io::read(&child, &mut buf).unwrap();
        assert_eq!(read_size, msg.len());
        assert_eq!(String::from_utf8_lossy(&buf[..read_size]), msg);
    }

    #[tokio::test]
    async fn pty_no_echo() {
        let (pty, child) = create_pty_pair().unwrap();
        let (pty_read, pty_write) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty_write = pin!(pty_write);
        let msg = "test\n";

        write(&mut pty_write, &mut cx, msg).await;

        let mut buf = [0u8; 64];
        let read_size = rustix::io::read(&child, &mut buf).unwrap();
        assert_eq!(read_size, msg.len());
        assert_eq!(String::from_utf8_lossy(&buf[..read_size]), msg);

        let response = "ack\n";
        assert_eq!(
            rustix::io::write(&child, response.as_bytes()).unwrap(),
            response.len()
        );

        // when echo is enabled read side will also get written data
        let mut pty_read = pin!(pty_read);
        let mut buf = [MaybeUninit::uninit(); 64];
        let mut read_buf = ReadBuf::uninit(&mut buf);

        for _ in 0..10 {
            match pty_read.as_mut().poll_read(&mut cx, &mut read_buf) {
                Poll::Pending => tokio::time::sleep(Duration::from_millis(5)).await,
                Poll::Ready(r) => {
                    r.unwrap();
                    break;
                }
            }
        }
        assert_eq!(read_buf.filled().len(), response.len() + 1);
        assert_eq!(read_buf.filled().as_str().unwrap(), "ack\r\n");
    }

    fn set_non_blocking(fd: &OwnedFd) {
        let mut flags = fcntl_getfl(fd).unwrap();
        flags |= OFlags::NONBLOCK;
        fcntl_setfl(fd, flags).unwrap();
    }

    #[tokio::test]
    async fn pty_async_write_full_buffer() {
        let (pty, child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        set_non_blocking(&child);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);

        let msg = "test\n";
        let mut written_bytes = 0;
        written_bytes += write(&mut pty, &mut cx, msg).await;
        loop {
            match pty.as_mut().poll_write(&mut cx, msg.as_bytes()) {
                Poll::Pending => {
                    break;
                }
                Poll::Ready(r) => {
                    written_bytes += r.unwrap();
                }
            }
            assert!(written_bytes < 1024 * 1024);
        }
        let mut buf = vec![0u8; written_bytes];
        let mut read_bytes = 0;
        let mut i = 0;
        while i < 100000 {
            match rustix::io::read(&child, &mut buf) {
                Ok(read_size) => {
                    assert_eq!(read_size, msg.len());
                    assert_eq!(String::from_utf8_lossy(&buf[..read_size]), msg);
                    read_bytes += read_size;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => panic!("Unexpected error: {e}"),
            }
            i += 1;
        }
        assert_eq!(read_bytes, written_bytes);
    }

    #[tokio::test]
    async fn pty_async_write_child_dropped() {
        let (pty, child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        drop(child);
        let mut i = 0;
        const MAX_ATTEMPTS: u32 = 1024 * 1024;
        loop {
            match pty.as_mut().poll_write(&mut cx, "test\n".as_bytes()) {
                Poll::Pending => tokio::time::sleep(Duration::from_millis(5)).await,
                Poll::Ready(r) => {
                    r.unwrap_err();
                    break;
                }
            };
            i += 1;
            assert!(i < MAX_ATTEMPTS);
        }
    }

    #[tokio::test]
    async fn pty_async_write_zero_bytes() {
        let (pty, child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        set_non_blocking(&child);
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        assert_eq!(write(&mut pty, &mut cx, "").await, 0);
        let mut buf = [0; 64];
        assert_eq!(
            rustix::io::read(&child, &mut buf).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn pty_async_flush() {
        let (pty, _child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        let mut attempt = 0;
        const MAX_ATTEMPTS: i32 = 10;
        loop {
            match pty.as_mut().poll_flush(&mut cx) {
                Poll::Ready(Ok(())) => break,
                Poll::Ready(Err(e)) => panic!("Unexpected error: {e}"),
                Poll::Pending => tokio::time::sleep(Duration::from_millis(5)).await,
            }
            attempt += 1;
            assert!(attempt <= MAX_ATTEMPTS);
        }
    }

    #[tokio::test]
    async fn pty_async_shutdown() {
        let (pty, _child) = create_pty_pair().unwrap();
        let (_, pty) = pty.into_split().unwrap();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pty = pin!(pty);
        match pty.as_mut().poll_shutdown(&mut cx) {
            Poll::Ready(Ok(())) => (),
            Poll::Ready(Err(e)) => panic!("Unexpected error: {e}"),
            Poll::Pending => panic!("Expected Ready, got Pending"),
        }
    }
}
