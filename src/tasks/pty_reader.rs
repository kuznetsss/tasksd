use std::{pin::Pin, task::Poll};

use tokio::io::AsyncRead;

use crate::tasks::pty::{PtyChild, PtyReadPart};

pub(in crate::tasks) type PtyReaderFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// On macos pty buffer is dropped once pty child is closed.
/// [`PtyReader`] fixes this problem by holding a copy of child pty
/// and draining pty's buffer when the child process has finished
pub(in crate::tasks) struct PtyReader {
    pty_read_part: PtyReadPart,
    child_process_exit_future: PtyReaderFuture,
    child_process_exited: bool,
    _child: PtyChild,
}

impl PtyReader {
    pub(in crate::tasks) fn new(
        read_part: PtyReadPart,
        _child: PtyChild,
        child_process_exit_future: PtyReaderFuture,
    ) -> Self {
        Self {
            pty_read_part: read_part,
            child_process_exit_future,
            child_process_exited: false,
            _child,
        }
    }
}

impl AsyncRead for PtyReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut this = self.as_mut();
        if this.child_process_exited {
            return Poll::Ready(try_read(Pin::new(&mut this.pty_read_part), buf));
        }
        match Pin::new(&mut this.pty_read_part).poll_read(cx, buf) {
            Poll::Ready(r) => return Poll::Ready(r),
            Poll::Pending => {}
        };
        match this.child_process_exit_future.as_mut().poll(cx) {
            Poll::Ready(_) => {
                this.child_process_exited = true;
                // The child is reaped, so no more output can ever arrive. The poll_read above
                // returned Pending and armed a readiness waker — but if the buffer is already
                // empty that waker will never fire (nothing left to make the fd readable), so
                // waiting would hang. Read straight from the kernel instead: it returns any
                // bytes still buffered (the readiness cache may not reflect them yet) or
                // WouldBlock, which we treat as EOF.
                Poll::Ready(try_read(Pin::new(&mut this.pty_read_part), buf))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn try_read(
    mut pty_read_part: Pin<&mut PtyReadPart>,
    buf: &mut tokio::io::ReadBuf<'_>,
) -> std::io::Result<()> {
    match pty_read_part.try_read(buf) {
        Ok(_) => Ok(()),
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::Notify;

    use super::*;

    use crate::tasks::pty::create_pty_pair;
    use std::assert_matches;
    use std::{mem::MaybeUninit, sync::Arc};

    struct PtyReaderTestContext {
        reader: PtyReader,
        child: PtyChild,
        child_process_exit: Arc<Notify>,
    }

    impl PtyReaderTestContext {
        fn new() -> Self {
            let (pty, child) = create_pty_pair().unwrap();
            let (read, _) = pty.into_split().unwrap();
            let child_process_exit = Arc::new(Notify::new());
            let reader = PtyReader::new(
                read,
                child.try_clone().unwrap(),
                Box::pin({
                    let child_process_exit = child_process_exit.clone();
                    async move {
                        child_process_exit.notified().await;
                    }
                }),
            );
            Self {
                reader,
                child,
                child_process_exit,
            }
        }

        fn simulate_child_process_exit(&self) {
            self.child_process_exit.notify_one();
        }
    }

    struct PtyReaderTestBuffer {
        raw: Vec<MaybeUninit<u8>>,
    }
    impl PtyReaderTestBuffer {
        fn new(size: usize) -> Self {
            Self {
                raw: vec![MaybeUninit::new(0); size],
            }
        }

        fn buffer(&mut self) -> tokio::io::ReadBuf<'_> {
            tokio::io::ReadBuf::uninit(&mut self.raw)
        }
    }

    #[tokio::test]
    async fn pty_reader_poll_has_data() {
        let mut ctx = PtyReaderTestContext::new();
        let msg = "some message";
        assert_eq!(
            rustix::io::write(&ctx.child, msg.as_bytes()).unwrap(),
            msg.len()
        );
        tokio::task::yield_now().await;
        let mut task_context = std::task::Context::from_waker(std::task::Waker::noop());
        let mut buffer = PtyReaderTestBuffer::new(msg.len());
        let mut read_buf = buffer.buffer();
        let poll_result = Pin::new(&mut ctx.reader).poll_read(&mut task_context, &mut read_buf);
        assert_matches!(poll_result, Poll::Ready(Ok(())));
        assert_eq!(read_buf.filled(), msg.as_bytes());
    }

    #[tokio::test]
    async fn pty_reader_poll_child_process_exited() {
        let mut ctx = PtyReaderTestContext::new();
        ctx.simulate_child_process_exit();
        let msg = "some message";
        assert_eq!(
            rustix::io::write(&ctx.child, msg.as_bytes()).unwrap(),
            msg.len()
        );
        // No yield_now() here: without it the reactor never runs, so poll_read()
        // returns Pending even though the bytes are buffered.
        let mut task_context = std::task::Context::from_waker(std::task::Waker::noop());
        let mut buffer = PtyReaderTestBuffer::new(msg.len());
        let mut read_buf = buffer.buffer();
        let poll_result = Pin::new(&mut ctx.reader).poll_read(&mut task_context, &mut read_buf);
        assert_matches!(poll_result, Poll::Ready(Ok(())));
        assert_eq!(read_buf.filled(), msg.as_bytes());
    }

    #[tokio::test]
    async fn pty_reader_poll_no_data_yet() {
        let mut ctx = PtyReaderTestContext::new();
        let mut task_context = std::task::Context::from_waker(std::task::Waker::noop());
        let mut buffer = PtyReaderTestBuffer::new(16);
        let mut read_buf = buffer.buffer();
        let poll_result = Pin::new(&mut ctx.reader).poll_read(&mut task_context, &mut read_buf);
        assert_matches!(poll_result, Poll::Pending);
    }

    #[tokio::test]
    async fn pty_reader_poll_to_drain_data_left() {
        let mut ctx = PtyReaderTestContext::new();
        ctx.simulate_child_process_exit();
        let mut task_context = std::task::Context::from_waker(std::task::Waker::noop());
        let msg = "some message";
        let mut buffer = PtyReaderTestBuffer::new(msg.len());
        let mut read_buf = buffer.buffer();

        let poll_result = Pin::new(&mut ctx.reader).poll_read(&mut task_context, &mut read_buf);
        assert_matches!(poll_result, Poll::Ready(Ok(())));
        assert!(read_buf.filled().is_empty());

        assert_eq!(
            rustix::io::write(&ctx.child, msg.as_bytes()).unwrap(),
            msg.len()
        );
        let poll_result = Pin::new(&mut ctx.reader).poll_read(&mut task_context, &mut read_buf);
        assert_matches!(poll_result, Poll::Ready(Ok(())));
        assert_eq!(read_buf.filled(), msg.as_bytes());
    }

    #[tokio::test]
    async fn try_read_maps_ok_to_ok() {
        let (pty, child) = create_pty_pair().unwrap();
        let (mut read, _write) = pty.into_split().unwrap();

        const MSG: &str = "some message";
        assert_eq!(
            rustix::io::write(&child, MSG.as_bytes()).unwrap(),
            MSG.len()
        );

        let mut buffer = PtyReaderTestBuffer::new(MSG.len());
        let mut read_buf = buffer.buffer();
        try_read(Pin::new(&mut read), &mut read_buf).unwrap();
        assert_eq!(read_buf.filled(), MSG.as_bytes());
    }

    #[tokio::test]
    async fn try_read_maps_would_block_error_to_ok() {
        let (pty, _child) = create_pty_pair().unwrap();
        let (mut read, _write) = pty.into_split().unwrap();

        let mut buf_raw = [MaybeUninit::new(0); 16];
        let mut buf = tokio::io::ReadBuf::uninit(&mut buf_raw);
        try_read(Pin::new(&mut read), &mut buf).unwrap();
        assert!(buf.filled().is_empty());
    }
}
