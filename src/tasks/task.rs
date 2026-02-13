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
    use std::{fs::File, io::Read};

    use super::*;

    #[tokio::test]
    async fn try_task() {
        let mut t =
            Task::new("echo".to_string(), vec!["hello from pty".to_string()], None).unwrap();

        let mut buf = [0u8; 4096];
        let mut f = File::from(t.pty);
        let n = f.read(&mut buf).unwrap();
        let output = String::from_utf8_lossy(&buf[..n]);
        println!("{output}");
        assert!(output.contains("hello from pty"), "got: {output}");
        t.child.wait().await.unwrap();
    }
}
