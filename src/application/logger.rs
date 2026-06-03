use std::{io::IsTerminal, path::PathBuf};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Default, Debug)]
pub struct Guard {
    inner: Option<tracing_appender::non_blocking::WorkerGuard>,
}

pub fn setup_logger(
    log_file: Option<&PathBuf>,
    log_to_console: bool,
) -> Result<Guard, std::io::Error> {
    let mut guard = Guard::default();
    let registry = tracing_subscriber::registry();

    let file_layer = log_file
        .map(|log_file| -> Result<_, std::io::Error> {
            let file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(log_file)?;
            let (appender, g) = tracing_appender::non_blocking(file);
            guard.inner = Some(g);
            Ok(tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(appender))
        })
        .transpose()?;

    let console_layer = log_to_console.then(|| {
        tracing_subscriber::fmt::layer()
            .with_ansi(std::io::stdout().is_terminal())
            .with_writer(std::io::stdout)
    });

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    registry
        .with(env_filter)
        .with(console_layer)
        .with(file_layer)
        .init();
    Ok(guard)
}

#[cfg(test)]
mod tests {
    use core::assert_matches;
    use std::{io::Read, str::FromStr};

    use super::*;

    #[test]
    fn setup_logger_log_to_console() {
        let guard = setup_logger(None, true).unwrap();
        assert!(guard.inner.is_none());
    }

    #[test]
    fn setup_logger_non_existing_file() {
        let path = std::path::PathBuf::from_str("/proc/non_existing").unwrap();
        let err = setup_logger(Some(&path), false).unwrap_err();
        assert_matches!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn setup_logger_log_to_file() {
        let msg = "some log";
        let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
        let guard = setup_logger(Some(&tmp_file.path().into()), false).unwrap();
        tracing::info!("{msg}");
        drop(guard);
        let mut buffer = String::new();
        tmp_file.reopen().unwrap();
        tmp_file.read_to_string(&mut buffer).unwrap();
        assert!(dbg!(buffer).contains(msg));
    }
}
