use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
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
    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let mut data = data.as_slice();
        // recover
        while data.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let key_len = data.get_u16();
            hasher.write_u16(key_len);
            let key = Bytes::copy_from_slice(&data[..key_len as usize]);
            hasher.write(&key);
            data.advance(key_len as usize);
            let value_len = data.get_u16();
            hasher.write_u16(value_len);
            let value = Bytes::copy_from_slice(&data[..value_len as usize]);
            hasher.write(&value);
            data.advance(value_len as usize);
            let checksum = data.get_u32();
            assert_eq!(checksum, hasher.finalize(), "checksum mismatch!");
            skiplist.insert(key, value);
        }
        Ok(Self{
            file: Arc::new(Mutex::new(file))
        })
    }

    /// 向 WAL 中追加写入 [key_len(2byte), key , value_len(2byte), value]
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = Vec::new();
        data.put_u16(key.len() as u16);
        data.put_slice(key);
        data.put_u16(value.len() as u16);
        data.put_slice(value);
        // checksum
        let checksum = crc32fast::hash(data.as_slice());
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