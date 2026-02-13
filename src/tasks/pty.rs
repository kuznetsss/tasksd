use std::{os::fd::OwnedFd, process::Stdio};

use anyhow::Result;

// TODO: wrap OwnedFd into tokio AsyncFd
pub type Pty = OwnedFd;

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
    grantpt(&pty)?;
    unlockpt(&pty)?;
    // TODO: set non blocking for pty

    let child_name = ptsname(&pty, Vec::new())?;
    let child = open(&child_name, OFlags::RDWR | OFlags::NOCTTY, Mode::empty())?;

    Ok((pty, child))
}

// TODO: maybe make Pty splitable into reader and writer parts
// TODO: implement async read and async write
