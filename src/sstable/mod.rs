const MAGIC: u32 = 0xFAA7BEEF;
const VERSION: u8 = 1;

const OS_PAGE_SIZE: usize = 4096; // 4 KiB

// TODO: Make this configurable
const DEFAULT_PAGE_SIZE: usize = OS_PAGE_SIZE;

pub mod reader;
pub mod writer;

pub use writer::SSTableWriter;

#[derive(Debug, Clone)]
pub struct ChunkDesc {
    pub index: usize,
    pub pos: u64,
    pub min_key: String,
    pub max_key: String,
}
