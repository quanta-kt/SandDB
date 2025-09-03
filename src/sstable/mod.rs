const MAGIC: u32 = 0xFAA7BEEF;
const VERSION: u8 = 1;

const OS_PAGE_SIZE: usize = 4096; // 4 KiB

// TODO: Make this configurable
const DEFAULT_PAGE_SIZE: usize = OS_PAGE_SIZE;

pub mod reader;
pub mod writer;

use std::path::{Path, PathBuf};

pub use writer::SSTableWriter;

#[derive(Debug, Clone)]
pub struct ChunkDesc {
    pub index: usize,
    pub pos: u64,
    pub min_key: String,
    pub max_key: String,
}

fn sst_filename(id: u64) -> String {
    format!("sstable_{id:016}.sst")
}

fn sst_file_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(sst_filename(id))
}
