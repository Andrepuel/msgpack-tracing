use crate::{
    storage::Store,
    string_cache::{CacheInstruction, CacheInstructionSet},
    tape::{Instruction, InstructionSet, TapeMachine},
};
use std::{
    fs::File,
    io::{self, Seek},
    path::{Path, PathBuf},
    time::Duration,
};

pub struct Rotate {
    file: Option<File>,
    path: PathBuf,
    path1: Option<PathBuf>,
    max_len: u64,
}
impl Rotate {
    pub fn new<P: AsRef<Path>>(path: P, max_len: u64) -> io::Result<Self> {
        let file = File::options().append(true).create(true).open(&path)?;
        let path1 = path
            .as_ref()
            .to_str()
            .map(|str| PathBuf::from(format!("{str}.1")));

        Ok(Self {
            file: Some(file),
            path: path.as_ref().to_owned(),
            path1,
            max_len,
        })
    }

    pub fn file_mut(&mut self) -> io::Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "file closed"))
    }

    pub fn do_needs_restart(&mut self) -> io::Result<bool> {
        let max_len = self.max_len;
        let file = self.file_mut()?;

        if file.stream_position()? <= max_len {
            return Ok(false);
        }

        std::thread::sleep(Duration::from_secs(1));
        self.file = None;

        if let Some(path1) = self.path1.as_ref() {
            std::fs::rename(&self.path, path1)?;
        }
        self.file = Some(File::create(&self.path)?);

        Ok(true)
    }
}
impl TapeMachine<CacheInstructionSet> for Rotate {
    fn needs_restart(&mut self) -> bool {
        self.do_needs_restart().unwrap_or_default()
    }

    fn handle(&mut self, instruction: CacheInstruction) {
        let Ok(file) = self.file_mut() else {
            return;
        };

        let _ = Store::do_handle_cached(file, instruction);
    }
}
impl TapeMachine<InstructionSet> for Rotate {
    fn needs_restart(&mut self) -> bool {
        self.do_needs_restart().unwrap_or_default()
    }

    fn handle(&mut self, instruction: Instruction) {
        let Ok(file) = self.file_mut() else {
            return;
        };

        let _ = Store::do_handle(file, instruction);
    }
}
