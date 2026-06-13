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
