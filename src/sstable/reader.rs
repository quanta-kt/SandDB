use crate::{datastructure::lru::LruCache, io_ext::ReadExt};
use std::{
    cell::RefCell,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    ops::RangeBounds,
    path::PathBuf,
};

use super::{ChunkDesc, sst_file_path};

pub trait SSTableReader {
    type ChunkIterator: Iterator<Item = Vec<(String, Vec<u8>)>> + 'static;

    fn list_chunks(&self, sst_id: u64) -> Vec<ChunkDesc>;

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> Option<Vec<(String, Vec<u8>)>>;

    fn chunk_iterator(&self, sst_id: u64) -> Self::ChunkIterator;

    fn get_candidate_chunks_for_key(&self, sst_id: u64, key: &str) -> Vec<ChunkDesc> {
        let chunks = self.list_chunks(sst_id);
        chunks
            .into_iter()
            .filter(move |chunk| chunk.min_key.as_str() <= key && chunk.max_key.as_str() >= key)
            .collect()
    }

    fn get_candidate_chunks_for_range<Range: RangeBounds<str>>(
        &self,
        sst_id: u64,
        range: Range,
    ) -> Vec<ChunkDesc> {
        let chunks = self.list_chunks(sst_id);
        chunks
            .into_iter()
            .filter(move |chunk| range.contains(&chunk.min_key) || range.contains(&chunk.max_key))
            .collect()
    }
}

pub struct FsSSTReader {
    directory: PathBuf,
}

impl FsSSTReader {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    pub fn cached(self) -> CachedSSTableReader<Self> {
        CachedSSTableReader::new(self)
    }
}

impl SSTableReader for FsSSTReader {
    type ChunkIterator = SSTChunkIterator;

    fn list_chunks(&self, sst_id: u64) -> Vec<ChunkDesc> {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        RawSSTableReader::open(sstable_path).unwrap().list_chunks()
    }

    fn chunk_iterator(&self, sst_id: u64) -> Self::ChunkIterator {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        SSTChunkIterator::open(sstable_path).unwrap()
    }

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> Option<Vec<(String, Vec<u8>)>> {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        RawSSTableReader::open(sstable_path)
            .unwrap()
            .read_chunk_at_index(chunk_index)
    }
}

pub struct CachedSSTableReader<S: SSTableReader> {
    chunk_desc_cache: RefCell<LruCache<String, Vec<ChunkDesc>>>,
    chunk_cache: RefCell<LruCache<(u64, usize), Vec<(String, Vec<u8>)>>>,
    source: S,
}

impl<S: SSTableReader> CachedSSTableReader<S> {
    pub fn new(source: S) -> Self {
        Self {
            chunk_desc_cache: RefCell::new(LruCache::new(512)),
            chunk_cache: RefCell::new(LruCache::new(1024)),
            source,
        }
    }
}

impl<S: SSTableReader> SSTableReader for CachedSSTableReader<S> {
    type ChunkIterator = S::ChunkIterator;

    fn list_chunks(&self, sst_id: u64) -> Vec<ChunkDesc> {
        let mut chunk_desc_cache = self.chunk_desc_cache.borrow_mut();

        chunk_desc_cache
            .get(&format!("sst_{sst_id}"))
            .cloned()
            .unwrap_or_else(|| {
                let chunks = self.source.list_chunks(sst_id);
                chunk_desc_cache.put(format!("sst_{sst_id}"), chunks.clone());

                chunks
            })
    }

    fn chunk_iterator(&self, sst_id: u64) -> Self::ChunkIterator {
        self.source.chunk_iterator(sst_id)
    }

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> Option<Vec<(String, Vec<u8>)>> {
        let key = (sst_id, chunk_index);

        let mut chunk_cache = self.chunk_cache.borrow_mut();

        chunk_cache.get(&key).cloned().or_else(|| {
            let chunk = self.source.read_chunk(sst_id, chunk_index);

            if let Some(chunk) = chunk {
                chunk_cache.put(key, chunk.clone());
            }

            chunk_cache.get(&key).cloned()
        })
    }
}

pub struct RawSSTableReader<F>
where
    F: Read + Seek,
{
    file: F,
}

struct Footer {
    chunk_dir_pos: u64,
    chunk_count: u32,
}

impl RawSSTableReader<File> {
    pub fn open(path: PathBuf) -> io::Result<RawSSTableReader<File>> {
        let file = File::open(path)?;
        Ok(RawSSTableReader::new(file))
    }
}

impl<F> RawSSTableReader<F>
where
    F: Read + Seek,
{
    pub fn new(file: F) -> RawSSTableReader<F> {
        RawSSTableReader { file }
    }

    pub fn list_chunks(&mut self) -> Vec<ChunkDesc> {
        self.validate_header();
        let footer = self.read_footer();

        self.read_chunk_directory(footer.chunk_dir_pos, footer.chunk_count)
    }

    pub fn read_chunk_at_index(mut self, chunk_index: usize) -> Option<Vec<(String, Vec<u8>)>> {
        self.validate_header();
        let footer = self.read_footer();

        let chunk_descs = self.read_chunk_directory(footer.chunk_dir_pos, footer.chunk_count);
        let chunk_desc = chunk_descs.get(chunk_index).unwrap();

        let chunk = self.read_chunk(chunk_desc.pos);
        Some(chunk)
    }

    fn validate_header(&mut self) {
        let _ = self.file.read_u32().unwrap();
        let _ = self.file.read_u8().unwrap();
        let _ = self.file.read_u32().unwrap();
    }

    fn read_footer(&mut self) -> Footer {
        self.file.seek(SeekFrom::End(-12)).unwrap();

        let chunk_dir_pos = self.file.read_u64().unwrap();
        let chunk_count = self.file.read_u32().unwrap();

        Footer {
            chunk_dir_pos,
            chunk_count,
        }
    }

    fn read_chunk_directory(&mut self, pos: u64, chunk_count: u32) -> Vec<ChunkDesc> {
        self.file.seek(SeekFrom::Start(pos)).unwrap();

        let mut chunk_descs = Vec::with_capacity(chunk_count as usize);

        for index in 0..chunk_count {
            let pos = self.file.read_u64().unwrap();
            let min_key = self.file.read_string().unwrap();
            let max_key = self.file.read_string().unwrap();

            chunk_descs.push(ChunkDesc {
                index: index as usize,
                pos,
                min_key,
                max_key,
            });
        }

        chunk_descs
    }

    fn read_chunk(&mut self, pos: u64) -> Vec<(String, Vec<u8>)> {
        self.file.seek(SeekFrom::Start(pos)).unwrap();

        let item_count = self.file.read_u32().unwrap();

        // Compressed size and uncompressed size not used yet
        let _ = self.file.read_u64().unwrap();
        let _ = self.file.read_u64().unwrap();

        let mut result = Vec::with_capacity(item_count as usize);

        let source = (0..item_count).map(move |_| {
            let key = self.file.read_string().unwrap();
            let value = self.file.read_bytes().unwrap();
            (key, value)
        });

        for item in source {
            result.push(item);
        }

        result
    }
}

pub struct SSTChunkIterator {
    reader: RawSSTableReader<File>,
    chunk_descs: Vec<ChunkDesc>,
    current_chunk_index: usize,
}

impl SSTChunkIterator {
    pub fn open(path: PathBuf) -> io::Result<SSTChunkIterator> {
        let mut reader = RawSSTableReader::open(path).unwrap();
        let chunk_descs = reader.list_chunks();

        Ok(SSTChunkIterator::new(reader, chunk_descs))
    }

    pub fn new(reader: RawSSTableReader<File>, chunk_descs: Vec<ChunkDesc>) -> Self {
        Self {
            reader,
            chunk_descs,
            current_chunk_index: 0,
        }
    }
}

impl Iterator for SSTChunkIterator {
    type Item = Vec<(String, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk_desc = self.chunk_descs.get(self.current_chunk_index);

        if let Some(chunk_desc) = chunk_desc {
            let chunk = self.reader.read_chunk(chunk_desc.pos);
            self.current_chunk_index += 1;
            Some(chunk)
        } else {
            None
        }
    }
}
