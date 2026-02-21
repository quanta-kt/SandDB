use std::fs::File;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::mem;
use std::path::Path;

use crate::io_ext::WriteExt;

use super::CHUNK_SIZE_TARGET;
use super::MAGIC;
use super::VERSION;
use super::ChunkDesc;
use super::sst_file_path;


const CHUNK_HEADER_SIZE: usize = 20;

pub struct SSTableWriter {
    file: Option<File>,

    chunks: Vec<ChunkDesc>,
    curr_chunk_written: usize,
    curr_chunk_count: u32,

    // Last key written to current chunk
    curr_chunk_last_key: Option<String>,
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
            curr_chunk_written: CHUNK_HEADER_SIZE,
            curr_chunk_count: 0,
            curr_chunk_last_key: None,
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

        let last_key = self.curr_chunk_last_key
            .as_ref()
            .map(|it| it.as_str().as_bytes())
            .unwrap_or(b"");

        let key_bytes = key.as_bytes();

        let mut prefix_len = last_key
            .iter()
            .zip(key_bytes)
            .take_while(|(a, b)| {
                a == b
            })
            .count();

        let mut suffix = &key_bytes[prefix_len..];

        let entry_size =
            suffix.len()
            + value.len()
            + 24; // prefix length (8) + suffix length (8) + value length (8)

        // Tolerate exceeding the target if this is the first key being written to this chunk. This
        // avoids creating an empty chunk in case of a single large key.
        if self.curr_chunk_count != 0 &&
            self.curr_chunk_written + entry_size > CHUNK_SIZE_TARGET {

            self.end_chunk()?;
            self.start_chunk()?;

            // We just started a new chunk, which means our previous calculations are invalid.
            // There is no prefix since this is now the first key.
            suffix = &key_bytes;
            prefix_len = 0;
        }

        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        let index = self.chunks.len() - 1;
        let curr = &mut self.chunks[index];

        file.write_u64(prefix_len as u64)?;
        file.write_bytes(suffix).unwrap();
        file.write_bytes(value).unwrap();

        if key > &curr.max_key {
            curr.max_key = key.to_string();
        }

        if self.curr_chunk_count == 0 {
            // If this is the first item we are writing for this chunk,
            // it is also the min key for it.
            curr.min_key = key.to_string();
        }

        self.curr_chunk_written += entry_size;
        self.curr_chunk_count += 1;
        self.curr_chunk_last_key = Some(key.to_string());

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

        self.curr_chunk_written = CHUNK_HEADER_SIZE;
        self.curr_chunk_count = 0;
        self.curr_chunk_last_key = None;

        Ok(())
    }

    fn write_header(&mut self) -> io::Result<()> {
        let file = self.file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "writer already finalized."))?;

        file.write_u32(MAGIC)?;
        file.write_u8(VERSION)?;
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

#[cfg(test)]
mod test {
    use super::*;

    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_inserting_large_keys_does_not_result_in_empty_chunks() {
        use crate::sstable::reader::RawSSTableReader;

        let path = PathBuf::from("test_inserting_large_keys_does_not_result_in_empty_chunks");
        let _ = fs::remove_file(&path);

        let large_value: String = (0..1024*4).map(|_| 'a').collect();

        let mut writer = SSTableWriter::new(File::create(path.clone()).unwrap()).unwrap();

        writer.write(&large_value, &large_value.as_bytes()).unwrap();
        writer.finalize().unwrap();
        assert_eq!(writer.chunks.len(), 1);

        drop(writer);

        let mut reader = RawSSTableReader::open(path).unwrap();
        let chunks = reader.list_chunks().unwrap();
        assert_eq!(chunks.len(), 1);
    }
}

