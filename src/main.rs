pub mod printer;
pub mod tape;

fn main() {
    tape::install(printer::Printer::new(std::io::stdout()));
    // tracing_subscriber::fmt::init();

    tracing::info!("Installed logger");

    recurse(5, 2);
}

fn recurse(level: i32, out: i32) {
    if level == 0 {
        tracing::info!("last");
        return;
    }

    let _span = tracing::info_span!("recursing", level).entered();
    let next = level - 1;
    _span.record("level", "before");
    tracing::info!(next, "enter");
    recurse(next, 0);
    _span.record("level", "new");
    _span.record("level", "new2");
    tracing::info!("got back");

    let next_out = out - 1;
    if next_out > 0 {
        tracing::info!(next_out, "going once more");
        recurse(level, next_out);
    }
}
