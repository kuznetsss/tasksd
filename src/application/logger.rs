use std::{io::IsTerminal, path::PathBuf};

use tracing::{Subscriber, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Default, Debug)]
pub struct Guard {
    inner: Option<tracing_appender::non_blocking::WorkerGuard>,
}

pub fn setup_logger(
    log_file: Option<&PathBuf>,
    log_to_console: bool,
) -> Result<Guard, std::io::Error> {
    let (subscriber, guard) = build_subscriber(log_file, log_to_console)?;
    subscriber.init();
    Ok(guard)
}

fn build_subscriber(
    log_file: Option<&PathBuf>,
    log_to_console: bool,
) -> Result<(impl Subscriber, Guard), std::io::Error> {
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

    let subscriber = registry
        .with(env_filter)
        .with(console_layer)
        .with(file_layer);
    Ok((subscriber, guard))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use core::assert_matches;
    use std::{io::Read, str::FromStr};

    use super::*;

    #[test]
    fn build_subscriber_log_to_console() {
        let (_, guard) = build_subscriber(None, true).unwrap();
        assert!(guard.inner.is_none());
    }

    #[test]
    fn build_subscriber_non_existing_file() {
        let path = std::path::PathBuf::from_str("/proc/non_existing").unwrap();
        let err = build_subscriber(Some(&path), false);
        assert!(err.is_err());
        let err = unsafe { err.unwrap_err_unchecked() };
        assert_matches!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn build_subscriber_log_to_file() {
        let msg = "some log";
        let mut tmp_file = tempfile::NamedTempFile::new().unwrap();
        let (subscriber, guard) = build_subscriber(Some(&tmp_file.path().into()), false).unwrap();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("{msg}");
        });
        drop(guard);
        let mut buffer = String::new();
        tmp_file.reopen().unwrap();
        tmp_file.read_to_string(&mut buffer).unwrap();
        assert!(dbg!(buffer).contains(msg));
    }
}
