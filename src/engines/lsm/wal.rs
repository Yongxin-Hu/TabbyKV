use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use crate::engines::lsm::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<File>>,
}

impl Wal {
    /// 创建 WAL
    /// # 参数
    /// * path memtable 对应 WAL 文件路径
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true).write(true).read(true).open(path)?;
        Ok(Wal{
            file: Arc::new(Mutex::new(file))
        })
    }

    /// 恢复 memtable
    /// # 参数
    /// * path memtable 对应 WAL 文件路径
    /// * skiplist memtable 内的跳表
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let mut data = data.as_slice();
        // recover
        while data.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = data.get_u16();
            hasher.write_u16(key_len);
            let key = &data[..key_len as usize];
            data.advance(key_len as usize);
            hasher.write(key);
            let ts = data.get_u64();
            hasher.write_u64(ts);
            let value_len = data.get_u16();
            hasher.write_u16(value_len);
            let value = Bytes::copy_from_slice(&data[..value_len as usize]);
            hasher.write(&value);
            data.advance(value_len as usize);
            let checksum = data.get_u32();
            assert_eq!(checksum, hasher.finalize(), "checksum mismatch!");
            let key_bytes = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key), ts);
            skiplist.insert(key_bytes, value);
        }
        Ok(Self{
            file: Arc::new(Mutex::new(file))
        })
    }

    /// 向 WAL 中追加写入 [key_len(2byte), key , time_stamp, value_len(2byte), value]
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut hasher = crc32fast::Hasher::new();
        let mut data = Vec::new();
        data.put_u16(key.key_len() as u16);
        hasher.write_u16(key.key_len() as u16);
        data.put_slice(key.key_ref());
        hasher.write(key.key_ref());
        data.put_u64(key.ts());
        hasher.write_u64(key.ts());
        data.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);
        data.put_slice(value);
        hasher.write(value);
        // checksum
        //let checksum = crc32fast::hash(data.as_slice());
        let checksum = hasher.finalize();
        data.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(data.as_slice())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}