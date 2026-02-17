use std::{env::current_dir, path::PathBuf};

use anyhow::Result;
use tokio::{
    io::BufReader,
    process::{Child, Command},
};

use crate::tasks::pty::{PtyReadPart, PtyWritePart, create_pty_pair};

struct Task {
    executable: String,
    args: Vec<String>,
    working_dir: PathBuf,
    child: Child,
    stdin: PtyWritePart,
    stdout: BufReader<PtyReadPart>,
}

impl Task {
    fn new(executable: String, args: Vec<String>, working_dir: Option<PathBuf>) -> Result<Self> {
        let working_dir = working_dir.unwrap_or(current_dir()?);
        let (pty, child_in_out_pty) = create_pty_pair()?;
        let (stdout, stdin) = pty.into_split()?;
        let child = unsafe {
            Command::new(&executable)
                .args(&args)
                .stdin(child_in_out_pty.try_clone()?)
                .stdout(child_in_out_pty.try_clone()?)
                .stderr(child_in_out_pty.try_clone()?)
                .current_dir(&working_dir)
                .pre_exec(move || {
                    rustix::process::setsid()?;
                    rustix::process::ioctl_tiocsctty(&child_in_out_pty)?;
                    Ok(())
                })
                .spawn()?
        };
        Ok(Self {
            executable,
            args,
            working_dir,
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    use super::*;

    #[tokio::test]
    async fn try_task() {
        let msg = "hello from pty";
        let mut t = Task::new("echo".to_string(), vec![msg.to_string()], None).unwrap();

        let mut buf = String::new();
        t.stdout.read_line(&mut buf).await.unwrap();
        assert_eq!(buf, format!("{msg}\r\n"));

        t.child.wait().await.unwrap();
    }
}
