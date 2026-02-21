use crate::{datastructure::lru::LruCache, io_ext::ReadExt};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    ops::RangeBounds,
    path::PathBuf,
};
use std::sync::Mutex;
use std::ops::Bound::*;

use super::{ChunkDesc, sst_file_path};
use super::MAGIC;
use super::VERSION;

pub trait SSTableReader {
    type ChunkIterator: Iterator<Item = io::Result<Vec<(String, Vec<u8>)>>> + 'static;

    fn list_chunks(&self, sst_id: u64) -> io::Result<Vec<ChunkDesc>>;

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> io::Result<Vec<(String, Vec<u8>)>>;

    fn chunk_iterator(&self, sst_id: u64) -> io::Result<Self::ChunkIterator>;

    fn get_candidate_chunks_for_key(&self, sst_id: u64, key: &str) -> io::Result<Vec<ChunkDesc>> {
        let chunks = self.list_chunks(sst_id)?;
        Ok(chunks
            .into_iter()
            .filter(move |chunk| chunk.min_key.as_str() <= key && chunk.max_key.as_str() >= key)
            .collect())
    }

    fn get_candidate_chunks_for_range<Range: RangeBounds<str>>(
        &self,
        sst_id: u64,
        range: Range,
    ) -> io::Result<Vec<ChunkDesc>> {
        let is_empty = match (range.start_bound(), range.end_bound()) {
            (Included(min), Included(max)) => min > max,
            (Included(min), Excluded(max)) => min >= max,
            (Excluded(min), Included(max)) => min >= max,
            (Excluded(min), Excluded(max)) => min >= max,
            _ => false,
        };

        if is_empty {
            return Ok(vec![]);
        }

        let chunks = self.list_chunks(sst_id)?;
        Ok(chunks
            .into_iter()
            // Since chunks in SSTs are always sorted by their ranges (which are non-overlapping),
            // we can fist skip the chunks that don't fall in the given range and then take the
            // ones that do and drop everything that comes after. With this, we don't have to
            // check all the chunks.
            .skip_while(|chunk| {
                let min_matches = match range.start_bound() {
                    Included(x) => x <= chunk.max_key.as_str(),
                    Excluded(x) => x < chunk.max_key.as_str(),
                    Unbounded => true,
                };

                let max_matches = match range.end_bound() {
                    Included(x) => x >= chunk.min_key.as_str(),
                    Excluded(x) => x > chunk.min_key.as_str(),
                    Unbounded => true,
                };

                !min_matches || !max_matches
            })
            .take_while(|chunk| {
                let min_matches = match range.start_bound() {
                    Included(x) => x <= chunk.max_key.as_str(),
                    Excluded(x) => x < chunk.max_key.as_str(),
                    Unbounded => true,
                };

                let max_matches = match range.end_bound() {
                    Included(x) => x >= chunk.min_key.as_str(),
                    Excluded(x) => x > chunk.min_key.as_str(),
                    Unbounded => true,
                };

                min_matches && max_matches
            })
            .collect())
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

    fn list_chunks(&self, sst_id: u64) -> io::Result<Vec<ChunkDesc>> {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        RawSSTableReader::open(sstable_path)?.list_chunks()
    }

    fn chunk_iterator(&self, sst_id: u64) -> io::Result<Self::ChunkIterator> {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        SSTChunkIterator::open(sstable_path)
    }

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> io::Result<Vec<(String, Vec<u8>)>> {
        let sstable_path = sst_file_path(&self.directory, sst_id);
        RawSSTableReader::open(sstable_path)?
            .read_chunk_at_index(chunk_index)
    }
}

pub struct CachedSSTableReader<S: SSTableReader> {
    chunk_desc_cache: Mutex<LruCache<String, Vec<ChunkDesc>>>,
    chunk_cache: Mutex<LruCache<(u64, usize), Vec<(String, Vec<u8>)>>>,
    source: S,
}

impl<S: SSTableReader> CachedSSTableReader<S> {
    pub fn new(source: S) -> Self {
        Self {
            chunk_desc_cache: Mutex::new(LruCache::new(512)),
            chunk_cache: Mutex::new(LruCache::new(1024)),
            source,
        }
    }
}

impl<S: SSTableReader> SSTableReader for CachedSSTableReader<S> {
    type ChunkIterator = S::ChunkIterator;

    fn list_chunks(&self, sst_id: u64) -> io::Result<Vec<ChunkDesc>> {
        let mut chunk_desc_cache = self.chunk_desc_cache.lock().expect("unable to acquire LRU cache mutex");

        chunk_desc_cache
            .get(&format!("sst_{sst_id}"))
            .cloned()
            .map(io::Result::Ok)
            .unwrap_or_else(|| {
                let chunks = self.source.list_chunks(sst_id)?;
                chunk_desc_cache.put(format!("sst_{sst_id}"), chunks.clone());

                Ok(chunks)
            })
    }

    fn chunk_iterator(&self, sst_id: u64) -> io::Result<Self::ChunkIterator> {
        self.source.chunk_iterator(sst_id)
    }

    fn read_chunk(&self, sst_id: u64, chunk_index: usize) -> io::Result<Vec<(String, Vec<u8>)>> {
        let key = (sst_id, chunk_index);

        let mut chunk_cache = self.chunk_cache.lock().expect("unable to acquire LRU cache mutex");

        chunk_cache.get(&key)
            .cloned()
            .map(io::Result::Ok)
            .unwrap_or_else(|| {
                let chunk = self.source.read_chunk(sst_id, chunk_index);

                if let Ok(ref chunk) = chunk {
                    chunk_cache.put(key, chunk.clone());
                }

                chunk
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

    pub fn list_chunks(&mut self) -> io::Result<Vec<ChunkDesc>> {
        self.validate_header()?;
        let footer = self.read_footer()?;

        self.read_chunk_directory(footer.chunk_dir_pos, footer.chunk_count)
    }

    pub fn read_chunk_at_index(mut self, chunk_index: usize) -> io::Result<Vec<(String, Vec<u8>)>> {
        self.validate_header()?;
        let footer = self.read_footer()?;

        let chunk_descs = self.read_chunk_directory(footer.chunk_dir_pos, footer.chunk_count)?;
        let chunk_desc = chunk_descs.get(chunk_index);

        if let Some(chunk_desc) = chunk_desc {
            self.read_chunk(chunk_desc.pos)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Chunk index out of range"))
        }
    }

    fn validate_header(&mut self) -> io::Result<()> {
        let magic = self.file.read_u32()?;
        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid SST file magic number."));
        }

        let version = self.file.read_u8()?;
        if version != VERSION {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "Unsupported SST file version."));
        }

        Ok(())
    }

    fn read_footer(&mut self) -> io::Result<Footer> {
        self.file.seek(SeekFrom::End(-12)).unwrap();

        let chunk_dir_pos = self.file.read_u64()?;
        let chunk_count = self.file.read_u32()?;

        Ok(Footer {
            chunk_dir_pos,
            chunk_count,
        })
    }

    fn read_chunk_directory(&mut self, pos: u64, chunk_count: u32) -> io::Result<Vec<ChunkDesc>> {
        self.file.seek(SeekFrom::Start(pos))?;

        let mut chunk_descs = Vec::with_capacity(chunk_count as usize);

        for index in 0..chunk_count {
            let pos = self.file.read_u64()?;
            let min_key = self.file.read_string()?;
            let max_key = self.file.read_string()?;

            chunk_descs.push(ChunkDesc {
                index: index as usize,
                pos,
                min_key,
                max_key,
            });
        }

        Ok(chunk_descs)
    }

    fn read_chunk(&mut self, pos: u64) -> io::Result<Vec<(String, Vec<u8>)>> {
        self.file.seek(SeekFrom::Start(pos))?;

        let item_count = self.file.read_u32()?;

        // Compressed size and uncompressed size not used yet
        let _ = self.file.read_u64()?;
        let _ = self.file.read_u64()?;

        let mut result = Vec::with_capacity(item_count as usize);

        for _ in 0..item_count {
            let key = self.file.read_string()?;
            let value = self.file.read_bytes()?;
            result.push((key, value));
        }

        Ok(result)
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
        let chunk_descs = reader.list_chunks()?;

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
    type Item = io::Result<Vec<(String, Vec<u8>)>>;

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

#[cfg(test)]
mod test {
    use super::*;

    use std::ops::Bound;

    #[test]
    fn test_retrive_candidate_chunks_in_range() {
        struct MockReader(Vec<ChunkDesc>);

        fn make_desc(index: usize, min: &str, max: &str) -> ChunkDesc {
            ChunkDesc {
                index,
                pos: 0,
                min_key: min.to_owned(),
                max_key: max.to_owned(),
            }
        }

        impl SSTableReader for MockReader {
            type ChunkIterator = SSTChunkIterator;

            fn list_chunks(&self, sst_id: u64) -> io::Result<Vec<ChunkDesc>> {
                if sst_id != 0 {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "no such SST"));
                }

                Ok(self.0.clone())
            }

            fn read_chunk(&self, _: u64, _: usize) -> io::Result<Vec<(String, Vec<u8>)>> {
                unimplemented!()
            }

            fn chunk_iterator(&self, _: u64) -> io::Result<Self::ChunkIterator> {
                unimplemented!()
            }
        }

        let reader = MockReader(
            vec![
                make_desc(0, "key10", "key20"),
                make_desc(1, "key20", "key30"),
                make_desc(2, "key30", "key40"),
                make_desc(3, "key50", "key60"),
            ]
        );

        let get_candidates = |range: (Bound<&str>, Bound<&str>)| {
            reader.get_candidate_chunks_for_range(0, range)
                .unwrap()
                .iter()
                .map(|it| it.index)
                .collect::<Vec<_>>()
        };

        // empty
        assert_eq!(get_candidates((Excluded("key15"), Excluded("key15"))), vec![]);
        assert_eq!(get_candidates((Included("key15"), Excluded("key15"))), vec![]);
        assert_eq!(get_candidates((Included("key15"), Excluded("key15"))), vec![]);

        // single
        assert_eq!(get_candidates((Included("key15"), Excluded("key17"))), vec![0]);
        assert_eq!(get_candidates((Excluded("key15"), Included("key17"))), vec![0]);
        assert_eq!(get_candidates((Included("key15"), Included("key15"))), vec![0]);

        // out of range
        assert_eq!(get_candidates((Excluded("key00"), Excluded("key10"))), vec![]);

        // min unbounded
        assert_eq!(get_candidates((Unbounded, Included("key20"))), vec![0, 1]);
        assert_eq!(get_candidates((Unbounded, Excluded("key20"))), vec![0]);

        // max unbounded
        assert_eq!(get_candidates((Included("key30"), Unbounded)), vec![1, 2, 3]);
        assert_eq!(get_candidates((Excluded("key30"), Unbounded)), vec![2, 3]);

        // intersection
        assert_eq!(get_candidates((Included("key22"), Excluded("key25"))), vec![1]);
        assert_eq!(get_candidates((Included("key22"), Included("key25"))), vec![1]);
        assert_eq!(get_candidates((Included("key22"), Excluded("key30"))), vec![1]);
        assert_eq!(get_candidates((Included("key22"), Included("key30"))), vec![1, 2]);
        assert_eq!(get_candidates((Included("key22"), Included("key35"))), vec![1, 2]);
        assert_eq!(get_candidates((Included("key22"), Excluded("key35"))), vec![1, 2]);

        assert_eq!(get_candidates((Excluded("key20"), Excluded("key25"))), vec![1]);
        assert_eq!(get_candidates((Excluded("key20"), Included("key25"))), vec![1]);
        assert_eq!(get_candidates((Excluded("key20"), Excluded("key30"))), vec![1]);
        assert_eq!(get_candidates((Excluded("key20"), Included("key30"))), vec![1, 2]);
        assert_eq!(get_candidates((Excluded("key20"), Included("key35"))), vec![1, 2]);
        assert_eq!(get_candidates((Excluded("key20"), Excluded("key35"))), vec![1, 2]);
    }
}

