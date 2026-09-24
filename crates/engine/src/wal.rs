//! Minimal append-only write-ahead log. Every record is length-prefixed
//! (u32 LE) so a reader can walk the file without a separate index, and
//! every write is followed by `sync_data` so a crash after `append`
//! returns never loses an acknowledged record — durability over speed,
//! which is the right default for a WAL. Batched/async fsync is a
//! deliberate later optimization, not an oversight.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal { writer: BufWriter::new(file) })
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<()> {
        let len = record.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(record)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}