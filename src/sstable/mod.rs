const MAGIC: u32 = 0xFAA7BEEF;
const VERSION: u8 = 1;

const OS_PAGE_SIZE: usize = 4096; // 4 KiB

/// Ideal size an SST chunk shuold be.
///
/// This is not a hard limit. A chunk with a single key that is larger than this target size can
/// for example make the actual chunk size exceed this size.
///
/// TODO: Make this configurable
const CHUNK_SIZE_TARGET: usize = OS_PAGE_SIZE;

pub mod reader;
pub mod writer;

use std::path::{Path, PathBuf};

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
