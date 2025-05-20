use printer::Printer;
use restart::RestartableMachine;
use rotate::Rotate;
use std::{io, path::Path};
use storage::Store;
use string_cache::StringCache;
use tape::{InstructionSet, TapeMachine, TapeMachineLogger};
use tracing_subscriber::{Registry, layer::SubscriberExt, util::SubscriberInitExt};

pub mod printer;
pub mod restart;
pub mod rotate;
pub mod storage;
pub mod string_cache;
pub mod tape;

pub fn install_logger<W>(out: W, with_stderr: Option<bool>)
where
    W: io::Write + Send + 'static,
{
    do_installer_logger(out_logger(out), with_stderr);
}

pub fn install_rotate_logger<P: AsRef<Path>>(
    path: P,
    max_len: u64,
    with_stderr: Option<bool>,
) -> io::Result<()> {
    let rotate = rotate_logger(path.as_ref(), max_len)?;
    do_installer_logger(rotate, with_stderr);
    Ok(())
}

fn do_installer_logger<T>(logger: TapeMachineLogger<T>, with_stderr: Option<bool>)
where
    T: TapeMachine<InstructionSet>,
{
    let registry = Registry::default();
    #[cfg(feature = "env-filter")]
    let (filter, registry) = {
        let filter = std::env::var("RUST_LOG").unwrap_or("warn".to_string());
        let registry = registry.with(tracing_subscriber::EnvFilter::from(&filter));
        (filter, registry)
    };
    #[cfg(not(feature = "env-filter"))]
    let filter: Option<()> = None;

    let registry = registry.with(logger);
    let init = match with_stderr {
        Some(color) => registry
            .with(printer_logger(io::stderr(), color))
            .try_init(),
        None => registry.try_init(),
    };

    match init {
        Ok(()) => tracing::debug!(?filter, ?with_stderr, "Logger initialized"),
        Err(e) => {
            tracing::warn!(%e, "Trying to initialize logger twice");
            tracing::debug!(?e);
        }
    }
}

pub fn out_logger<W>(out: W) -> TapeMachineLogger<impl TapeMachine<InstructionSet>>
where
    W: io::Write + Send + 'static,
{
    TapeMachineLogger::new(StringCache::new(Store::new(out)))
}

pub fn rotate_logger(
    path: &Path,
    max_len: u64,
) -> io::Result<TapeMachineLogger<impl TapeMachine<InstructionSet>>> {
    Ok(TapeMachineLogger::new(RestartableMachine::new(
        StringCache::new(Rotate::new(path, max_len)?),
    )))
}

pub fn printer_logger<W>(out: W, color: bool) -> TapeMachineLogger<impl TapeMachine<InstructionSet>>
where
    W: io::Write + Send + 'static,
{
    TapeMachineLogger::new(Printer::new(out, color))
}
