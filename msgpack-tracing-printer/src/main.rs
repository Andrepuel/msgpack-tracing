use msgpack_tracing::{
    printer::Printer, storage::Load, string_cache::StringUncache, tape::TapeMachine,
};
use std::{fs::File, io};

fn main() {
    let mut color = atty::is(atty::Stream::Stdout);

    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--color" | "-c" => color = true,
            "--no-color" => color = false,
            path => {
                if let Err(e) = print_log(path, color) {
                    eprintln!("Error loading {path}: {e}");
                    eprintln!("{e:?}");
                }
            }
        }
    }
}

fn print_log(path: &str, color: bool) -> io::Result<()> {
    let mut printer = StringUncache::new(Printer::new(std::io::stdout(), color));
    let mut load = Load::new(File::open(path)?);

    loop {
        let instruction = match load.fetch_one_cached() {
            Ok(Some(instruction)) => instruction,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error loading instruction: {e}");
                eprintln!("{e:?}");
                eprintln!("Skipping to next Restart instruction");
                load.restart();
                continue;
            }
        };

        printer.handle(instruction);
    }

    Ok(())
}
