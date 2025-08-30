use crate::io_ext::WriteExt;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::iter::Peekable;
use std::marker::PhantomData;
use std::path::PathBuf;

use super::{ChunkDesc, DEFAULT_PAGE_SIZE, MAGIC, VERSION};

pub struct SSTableWriter<F, K, V>
where
    F: Write + Seek,
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    file: F,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V> SSTableWriter<File, K, V>
where
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    pub fn write_sstable(
        directory: PathBuf,
        sst_id: u64,
        source: &mut Peekable<impl Iterator<Item = (K, V)>>,
    ) -> io::Result<()>
    where
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {
        let file_name = format!("sstable_{sst_id:016}.sst");
        let file_path = directory.join(file_name);
        let mut file = File::create(file_path)?;

        let writer = SSTableWriter::new(&file);
        writer.write(source);

        file.flush()?;
        file.sync_all()?;

        Ok(())
    }
}

impl<F, K, V> SSTableWriter<F, K, V>
where
    F: Write + Seek,
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    pub fn new(file: F) -> Self {
        SSTableWriter {
            file,
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    pub fn write<S>(mut self, source: &mut Peekable<S>)
    where
        S: Iterator<Item = (K, V)>,
    {
        self.write_header();

        let chunks = self.write_chunks(source);
        let chunk_count = chunks.len() as u32;

        let chunk_dir_pos = self.file.stream_position().unwrap();
        self.write_chunk_directory(chunks);

        self.write_footer(chunk_dir_pos, chunk_count);
    }

    fn write_header(&mut self) {
        self.file.write_u32(MAGIC).unwrap();
        self.file.write_u8(VERSION).unwrap();
        self.file.write_u32(DEFAULT_PAGE_SIZE as u32).unwrap();
    }

    fn write_footer(&mut self, chunk_dir_pos: u64, chunk_count: u32) {
        self.file.write_u64(chunk_dir_pos).unwrap();
        self.file.write_u32(chunk_count).unwrap();
    }

    fn write_chunk_directory(&mut self, chunk_descs: Vec<ChunkDesc>) {
        for chunk_desc in chunk_descs {
            self.file.write_u64(chunk_desc.pos).unwrap();
            self.file.write_string(&chunk_desc.min_key).unwrap();
            self.file.write_string(&chunk_desc.max_key).unwrap();
        }
    }

    fn write_chunks<S>(&mut self, source: &mut Peekable<S>) -> Vec<ChunkDesc>
    where
        S: Iterator<Item = (K, V)>,
    {
        let mut chunk_descs = Vec::new();

        let mut index = 0;

        while source.peek().is_some() {
            chunk_descs.push(self.write_chunk(index, source));
            index += 1;
        }

        chunk_descs
    }

    fn write_chunk<S>(&mut self, index: usize, source: &mut Peekable<S>) -> ChunkDesc
    where
        S: Iterator<Item = (K, V)>,
    {
        const HEADER_SIZE: usize = 20;

        let pos = self.file.stream_position().unwrap();

        let min_key = source.peek().unwrap().0.as_ref().to_owned();
        let mut max_key = min_key.to_owned();

        // Reserve space for the chunk header
        self.file.write_u32(0).unwrap();
        self.file.write_u64(0).unwrap();
        self.file.write_u64(0).unwrap();

        let mut written: usize = HEADER_SIZE;
        let mut item_count: u32 = 0;

        while let Some((key, value)) = source.peek() {
            let key = key.as_ref();
            let value = value.as_ref();

            let entry_size = key.len() + value.len() + 16;

            if written + entry_size > DEFAULT_PAGE_SIZE {
                break;
            }

            self.file.write_string(key).unwrap();
            self.file.write_bytes(value).unwrap();

            if key > &max_key {
                max_key = key.to_string();
            }

            written += entry_size;
            item_count += 1;

            source.next();
        }

        // Write the chunk header
        let end_pos = self.file.stream_position().unwrap();
        self.file.seek(SeekFrom::Start(pos)).unwrap();
        self.file.write_u32(item_count).unwrap();

        // TODO: Compress the chunk
        self.file.write_u64(written as u64).unwrap();
        self.file.write_u64(written as u64).unwrap();

        // Seek back to the end of the chunk
        self.file.seek(SeekFrom::Start(end_pos)).unwrap();

        ChunkDesc {
            index,
            pos,
            min_key: min_key.to_owned(),
            max_key: max_key.to_owned(),
        }
    }
}
