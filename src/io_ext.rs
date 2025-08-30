use std::io;
use std::io::{Read, Write};

pub trait ReadExt {
    fn read_u8(&mut self) -> io::Result<u8>;
    fn read_u32(&mut self) -> io::Result<u32>;
    fn read_u64(&mut self) -> io::Result<u64>;
    fn read_string(&mut self) -> io::Result<String>;
    fn read_bytes(&mut self) -> io::Result<Vec<u8>>;
    fn read_bytes_with_len(&mut self, len: usize) -> io::Result<Vec<u8>>;
}

impl<R: Read> ReadExt for R {
    fn read_u8(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_be_bytes(buf))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }

    fn read_bytes_with_len(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_u64()?;
        self.read_bytes_with_len(len as usize)
    }

    fn read_string(&mut self) -> io::Result<String> {
        let len = self.read_u64()?;
        let buf = self.read_bytes_with_len(len as usize)?;
        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub trait WriteExt {
    fn write_u8(&mut self, value: u8) -> io::Result<()>;
    fn write_u32(&mut self, value: u32) -> io::Result<()>;
    fn write_u64(&mut self, value: u64) -> io::Result<()>;
    fn write_string(&mut self, value: &str) -> io::Result<()>;
    fn write_bytes(&mut self, value: &[u8]) -> io::Result<()>;
}

impl<W: Write> WriteExt for W {
    fn write_u8(&mut self, value: u8) -> io::Result<()> {
        self.write_all(&[value])
    }

    fn write_u32(&mut self, value: u32) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    fn write_u64(&mut self, value: u64) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    fn write_string(&mut self, value: &str) -> io::Result<()> {
        self.write_u64(value.len() as u64)?;
        self.write_all(value.as_bytes())
    }

    fn write_bytes(&mut self, value: &[u8]) -> io::Result<()> {
        self.write_u64(value.len() as u64)?;
        self.write_all(value)
    }
}
