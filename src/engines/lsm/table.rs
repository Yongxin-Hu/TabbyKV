#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crate::engines::lsm::block::Block;

// Block元信息
#[derive(Debug, PartialEq)]
struct BlockMeta {
    pub offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes
}

impl BlockMeta {
    pub fn encode_to_buf(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            // offset
            buf.put_u32(meta.offset as u32);
            // first_key_len
            buf.put_u16(meta.first_key.len() as u16);
            // first_key
            buf.put_slice(meta.first_key.as_ref());
            // last_key_len
            buf.put_u16(meta.last_key.len() as u16);
            // last_key
            buf.put_slice(meta.last_key.as_ref());
        }
    }

    pub fn decode_from_buf(mut buf: &[u8]) -> Result<Vec<BlockMeta>>{
        let block_num = buf.get_u32() as usize;
        let mut metas = Vec::with_capacity(block_num);
        for i in 0..block_num{
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            metas.push(BlockMeta{
                offset,
                first_key,
                last_key
            })
        }
        Ok(metas)
    }
}
// FileObject
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        let mut file = self.0.as_ref().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut data)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// 创建文件返回 FileObject
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        //File::open(path)?.sync_all();
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}


/// An SSTable.
pub struct SsTable {
    pub(crate) file: FileObject,
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
    id: usize,
    first_key: Bytes,
    last_key: Bytes,
}

impl SsTable {
    // 打开一个 sstable
    pub fn open(id: usize, file: FileObject) -> Result<Self> {
        unimplemented!()
    }

    // 获取 sstable 的第 index 个 Block
    pub fn read_block(&self, index: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    // 找到一个可能包含key的Block
    pub fn find_block_idx() -> usize{
        unimplemented!()
    }

    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> Bytes {
        self.first_key.clone()
    }

    pub fn last_key(&self) -> Bytes {
        self.last_key.clone()
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use crate::engines::lsm::table::BlockMeta;

    #[test]
    fn test_block_meta() {
        let origin_block_meta = vec![
            BlockMeta{
                offset: 0,
                first_key: Bytes::from("key1"),
                last_key:Bytes::from("key2")
            },
            BlockMeta{
                offset: 1,
                first_key: Bytes::from("key3"),
                last_key:Bytes::from("key4")
            }
        ];
        let mut buf = Vec::new();
        BlockMeta::encode_to_buf(origin_block_meta.iter().clone().as_ref(), &mut buf);
        let result = BlockMeta::decode_from_buf(&mut buf).unwrap();
        assert_eq!(origin_block_meta, result)
    }
}
