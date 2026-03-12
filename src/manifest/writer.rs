/// Manifest file readering and writing routines.
/// Manifest file format is specified in [docs/manifest-file-spec.md](docs/manifest-file-spec.md).

use std::fs::File;
use std::io;
use std::io::Write;
use std::io::SeekFrom;
use std::io::Seek;
use std::sync::atomic::Ordering;

use crate::crc::crc32c;
use crate::io_ext::WriteExt;

use super::ManifestUpdate;
use super::SSTableDesc;
use super::MAGIC;

/// Writer for manifest files.
///
/// Not thread-safe.
///
/// Manifest file format is specified in [docs/manifest-file-spec.md](docs/manifest-file-spec.md).
///
/// This struct itself does not provide any write functionality. Instead, it provides a
/// [`ManifestTransaction`] which can be used to write entries to the manifest file and atomically
/// commited to the file.
///
/// Since the Transaction borrows the writer mutably, the borrow checker ensures that only one
/// transation is running
/// at a time.
pub struct ManifestWriter {
    file: File,
}

impl ManifestWriter {

    /// Opens a manifest file for writing.
    ///
    /// If the file does not exist, it will be created.
    ///
    /// Additionally, creates a lock file to prevent multiple writers from writing to the same file.
    ///
    /// On open, it will compact the manifest file if it already exists.
    pub fn open(
        mut file: File,
    ) -> io::Result<ManifestWriter> {
        Self::ensure_header(&mut file)?;

        Ok(ManifestWriter {
            file,
        })
    }

    pub fn ensure_header(file: &mut File) -> io::Result<()> {
        let pos = file.seek(SeekFrom::End(0))?;

        // In case of an empty file, write the header
        if pos == 0 {
            Self::write_header(file)?;
        }

        Ok(())
    }

    pub fn write(
        &mut self,
        add: &[SSTableDesc],
        remove: &[u64],
        next_sst_id: u64
    ) -> io::Result<()> {
        let mut buf = Vec::new();

        buf.write_u64(next_sst_id)?;

        buf.write_u64(add.len() as u64)?;
        for sst in add.iter() {
            buf.write_u64(sst.id)?;
            buf.write_u8(sst.level)?;
            buf.write_string(&sst.min_key)?;
            buf.write_string(&sst.max_key)?;
        }

        buf.write_u64(remove.len() as u64)?;
        for sst_id in remove.iter() {
            buf.write_u64(*sst_id)?;
        }

        let crc = crc32c(&buf);
        let length = buf.len() as u32;

        self.file.write_u32(crc)?;
        self.file.write_u32(length)?;
        self.file.write_all(&buf)?;

        Ok(())
    }

    fn write_header(file: &mut File) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.set_len(0)?;

        // Magic number
        file.write_u32(MAGIC)?;

        // Version
        file.write_u8(1)?;

        file.sync_data()?;
        Ok(())
    }
}

