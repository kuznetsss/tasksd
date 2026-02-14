use std::{env::current_dir, os::fd::AsRawFd, path::PathBuf};

use anyhow::Result;
use tokio::process::{Child, Command};

use crate::tasks::pty::{Pty, create_pty_pair};

struct Task {
    executable: String,
    args: Vec<String>,
    working_dir: PathBuf,
    child: Child,
    pty: Pty,
}

impl Task {
    fn new(executable: String, args: Vec<String>, working_dir: Option<PathBuf>) -> Result<Self> {
        let working_dir = working_dir.unwrap_or(current_dir()?);
        let (pty, child_fd) = create_pty_pair()?;
        let child = unsafe {
            Command::new(&executable)
                .args(&args)
                .stdin(child_fd.try_clone()?)
                .stdout(child_fd.try_clone()?)
                .stderr(child_fd.try_clone()?)
                .current_dir(&working_dir)
                .pre_exec(move || {
                    rustix::process::setsid()?;
                    rustix::process::ioctl_tiocsctty(&child_fd)?;
                    Ok(())
                })
                .spawn()?
        };
        Ok(Self {
            executable,
            args,
            working_dir,
            child,
            pty,
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn try_task() {
        let msg = "hello from pty";
        let mut t = Task::new("echo".to_string(), vec![msg.to_string()], None).unwrap();

        let mut buf = String::new();
        t.pty.read_to_string(&mut buf).await.unwrap();
        assert_eq!(buf, format!("{msg}\r\n"));

        t.child.wait().await.unwrap();
    }
}
