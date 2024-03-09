use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use parking_lot::{Mutex, MutexGuard};
use anyhow::{Context, Result};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use crate::engines::lsm::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self{
            file: Arc::new(Mutex::new(file))
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest")?;
        let mut manifest_records = Vec::new();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let record: ManifestRecord = serde_json::from_slice(slice)?;
            let checksum = buf_ptr.get_u32();
            assert_eq!(checksum,  crc32fast::hash(slice), "checksum mismatched!");
            manifest_records.push(record);
        }
        Ok((Self{
            file: Arc::new(Mutex::new(file))
        }, manifest_records))
    }

    pub fn add_record(
        &self,
        state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    // Record 格式 [record_data_len(u64), record_data, record_hash(u32)]
    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut record_data = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&record_data);
        let mut file = self.file.lock();
        file.write_all(&(record_data.len() as u64).to_be_bytes())?;
        record_data.put_u32(checksum);
        file.write_all(&record_data)?;
        file.sync_all()?;
        Ok(())
    }
}