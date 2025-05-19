use std::fs::File;
pub mod printer;
pub mod rotate;
pub mod storage;
pub mod tape;

fn main() {
    if let Some(read) = std::env::args().nth(1) {
        storage::Load::new(File::open(read).unwrap())
            .forward(&mut printer::Printer::new(std::io::stdout()))
            .unwrap();
        return;
    }

    tape::install(rotate::Rotate::new("out.log", 64 * 1024 * 1024).unwrap());
    tracing::info!("Installed logger");
    for i in 0.. {
        tracing::info!(i, "Spamming logs");
        recurse(i % 10, 2);
    }
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
