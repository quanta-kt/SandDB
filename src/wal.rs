use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::iter::Iterator;
use std::path::Path;

use crate::crc;
use crate::io_ext::ReadExt;
use crate::io_ext::WriteExt;

const MAGIC: u32 = 0xbeef_dab3;
const VERSION: u8 = 1;

const FILENAME: &'static str = "wal.log";

pub struct Wal {
    wal: File,
}


impl Wal {

    pub fn new(directory: &Path) -> io::Result<Self> {
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(directory.join(FILENAME))
            .unwrap_or_else(|_|
                panic!("unable to open WAL file '{:?}'", directory)
            );

        #[cfg(unix)]
        {
            let dir = OpenOptions::new()
                .read(true)
                .open(directory)?;

            dir.sync_all()?;
        }

        Ok(Self {
            wal,
        })
    }

    pub fn log_one(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        self.log_one_no_sync(key, value)?;
        self.wal.sync_data()?;
        Ok(())
    }

    pub fn log_many(&mut self, items: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
        for (key, value) in items.iter() {
            self.log_one_no_sync(key, value)?;
        }

        self.wal.sync_data()?;

        Ok(())
    }

    pub fn restore<'a>(&'a mut self) -> io::Result<impl Iterator<Item = (String, Vec<u8>)> + 'a> {
        self.wal.seek(SeekFrom::Start(0))?;

        if self.wal.metadata()?.len() > 0 {
            self.parse_header()?;
        }

        Ok(std::iter::from_fn(|| {
            self.read_one().ok().flatten()
        }))
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.wal.seek(SeekFrom::Start(0))?;
        self.wal.set_len(0)?;
        self.write_header()?;

        self.wal.sync_all()?;

        Ok(())
    }

    fn read_one(&mut self) -> io::Result<Option<(String, Vec<u8>)>> {
        let crc = match self.wal.read_u32() {
            Ok(crc) => crc,

            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            },

            Err(e) => {
                return Err(e);
            }
        };

        let len = if let Ok(len) = self.wal.read_u64() {
            len as usize
        } else {
            return Ok(None);
        };

        let mut buf = vec![0u8; len];
        self.wal.read_exact(&mut buf)?;

        let expected_crc = crc::crc32c_iter(
            len.to_be_bytes()
                .iter()
                .chain(buf.iter())
                .cloned()
        );

        if crc != expected_crc {
            // Treat CRC failure as EOF
            return Ok(None);
        }

        let mut cursor = io::Cursor::new(&buf);

        let key = cursor.read_string()?;
        let value = cursor.read_bytes()?;

        Ok(Some((key, value)))
    }

    fn log_one_no_sync(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        let mut buf = Vec::new();
        buf.write_string(key)?;
        buf.write_bytes(value)?;

        let len = buf.len() as u64;

        let crc = crc::crc32c_iter(
            len.to_be_bytes()
                .iter()
                .chain(buf.iter())
                .cloned()
        );

        self.wal.write_u32(crc)?;
        self.wal.write_u64(buf.len() as u64)?;
        self.wal.write_all(&buf)?;

        Ok(())
    }

    fn write_header(&mut self) -> io::Result<()> {
        self.wal.write_u32(MAGIC)?;
        self.wal.write_u8(VERSION)?;

        Ok(())
    }

    fn parse_header(&mut self) -> io::Result<()> {
        let magic = self.wal.read_u32()?;
        let version = self.wal.read_u8()?;

        if magic != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid WAL header."));
        }

        if version != VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported WAL version."));
        }

        Ok(())
    }

}

