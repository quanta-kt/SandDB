use std::fs::File;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::mem;
use std::path::Path;

use crate::io_ext::WriteExt;

use super::DEFAULT_PAGE_SIZE;
use super::MAGIC;
use super::VERSION;
use super::ChunkDesc;
use super::sst_file_path;

pub struct SSTableWriter {
    file: Option<File>,

    chunks: Vec<ChunkDesc>,
    curr_chunk_written: usize,
    curr_chunk_count: u32,
}

impl SSTableWriter {
    pub fn open(
        directory: &Path,
        sst_id: u64
    ) -> io::Result<Self> {
        let file_path = sst_file_path(&directory, sst_id);
        let file = File::create(file_path)?;

        SSTableWriter::new(file)
    }

    fn new(mut file: File) -> io::Result<Self> {
        file.seek(SeekFrom::Start(0))?;

        let mut ret = SSTableWriter {
            file: Some(file),
            chunks: Vec::new(),
            curr_chunk_written: 0,
            curr_chunk_count: 0,
        };

        ret.write_header()?;
        ret.start_chunk()?;

        Ok(ret)
    }

    pub fn write<K, V>(&mut self, key: K, value: V) -> io::Result<()>
    where
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {


        let key = key.as_ref();
        let value = value.as_ref();

        let entry_size = key.len() + value.len() + 16;

        if self.curr_chunk_written + entry_size > DEFAULT_PAGE_SIZE {
            self.end_chunk()?;
            self.start_chunk()?;
        }

        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        let index = self.chunks.len() - 1;
        let curr = &mut self.chunks[index];

        file.write_string(key).unwrap();
        file.write_bytes(value).unwrap();

        if key > &curr.max_key {
            curr.max_key = key.to_string();
        }

        if self.curr_chunk_written == 0 {
            // If this is the first item we are writing for this chunk,
            // it is also the min key for it.
            curr.min_key = key.to_string();
        }

        self.curr_chunk_written += entry_size;
        self.curr_chunk_count += 1;

        Ok(())
    }

    pub fn finalize(&mut self) -> io::Result<()> {
        self.end_chunk()?;

        let mut file = mem::take(&mut self.file)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        let chunk_dir_pos = file.seek(SeekFrom::Current(0))?;
        self.write_chunk_directory(&mut file)?;
        self.write_footer(&mut file, chunk_dir_pos)?;

        file.sync_all()?;

        Ok(())
    }

    fn end_chunk(&mut self) -> io::Result<()> {
        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        let chunk = &self.chunks[self.chunks.len() - 1];
        let old_pos = file.seek(SeekFrom::Current(0))?;
        file.seek(SeekFrom::Start(chunk.pos))?;


        file.write_u32(self.curr_chunk_count)?;

        // TODO: Compress the chunk
        // FIXME: this field does not really reflect the true size of the block
        // and instead is only an indicator of the sum all the key and value lengths.
        // i.o.w it does not account for things like lengths that encode prefix.
        file.write_u64(self.curr_chunk_written as u64).unwrap();
        file.write_u64(self.curr_chunk_written as u64).unwrap();

        file.seek(SeekFrom::Start(old_pos))?;

        Ok(())
    }

    fn start_chunk(&mut self) -> io::Result<()> {
        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        let chunk_pos = file.seek(SeekFrom::Current(0))?;
        self.chunks.push(ChunkDesc {
            index: self.chunks.len(),
            min_key: "".to_string(),
            max_key: "".to_string(),
            pos: chunk_pos
        });

        // Reserve space for the chunk header
        file.write_u32(0)?;
        file.write_u64(0)?;
        file.write_u64(0)?;

        self.curr_chunk_written = 0;
        self.curr_chunk_count = 0;

        Ok(())
    }

    fn write_header(&mut self) -> io::Result<()> {
        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        file.write_u32(MAGIC)?;
        file.write_u8(VERSION)?;
        file.write_u32(DEFAULT_PAGE_SIZE as u32)?;
        Ok(())
    }

    fn write_footer(&mut self, file: &mut File, chunk_dir_pos: u64) -> io::Result<()>{
        file.write_u64(chunk_dir_pos)?;
        file.write_u32(self.chunks.len() as u32)?;
        Ok(())
    }

    fn write_chunk_directory(&mut self, file: &mut File) -> io::Result<()> {
        for chunk_desc in self.chunks.iter() {
            file.write_u64(chunk_desc.pos)?;
            file.write_string(&chunk_desc.min_key)?;
            file.write_string(&chunk_desc.max_key)?;
        }

        Ok(())
    }
}

impl Drop for SSTableWriter {
    fn drop(&mut self) {
        if self.file.is_some() {
            eprintln!("BUG: SSTableWriter dropped without finalize()");
            let _ = self.finalize();
        }
    }
}

