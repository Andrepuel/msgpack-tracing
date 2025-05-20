use std::fs::File;

fn main() {
    let mut rotate: Option<u64> = None;
    for arg in std::env::args().skip(1) {
        match arg.parse() {
            Ok(0) => {
                rotate = None;
            }
            Ok(max_len) => {
                rotate = Some(max_len);
            }
            Err(_) => {
                install_logger(&arg, rotate);
                tracing::info!("Installed logger");
                for i in 0.. {
                    tracing::info!(i, "Spamming logs");
                    recurse(i % 10, 2);
                }
            }
        }
    }
}

fn install_logger(path: &str, rotate: Option<u64>) {
    match rotate {
        Some(max_len) => msgpack_tracing::install_rotate_logger(path, max_len, Some(true)).unwrap(),
        None => {
            msgpack_tracing::install_logger(File::create(path).unwrap(), Some(true));
        }
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
    tracing::info!(level=?LevelDebug(level), next, "enter");
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

#[derive(Debug)]
struct LevelDebug(#[expect(unused)] i32);
