#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub mod builder;
pub mod iterator;
mod bloom_filter;

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crate::engines::lsm::block::Block;
use crate::engines::lsm::table::bloom_filter::BloomFilter;

// Block元信息
#[derive(Clone, Debug, PartialEq)]
pub struct BlockMeta {
    pub offset: usize,
    pub first_key: Bytes,
    pub last_key: Bytes
}

impl BlockMeta {
    pub fn encode_to_buf(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let block_meta_start = buf.len();
        // no of blocks
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
        println!("encode_to_buf checksum len: {}", &buf[block_meta_start+4/* no of blocks */..].len());
        // check sum
        buf.put_u32(crc32fast::hash(&buf[block_meta_start+4/* no of blocks */..]));
    }

    pub fn decode_from_buf(mut buf: &[u8]) -> Result<Vec<BlockMeta>>{
        let block_num = buf.get_u32() as usize;
        let mut metas = Vec::with_capacity(block_num);
        println!("decode_from_buf checksum len: {}", &buf[..buf.remaining() - 4].len());
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
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
        // checksum
        assert_eq!(buf.get_u32(), checksum, "meta checksum mismatched!");
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
        File::open(path)?.sync_all();
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
    pub(crate) bloom_filter: BloomFilter
}

impl SsTable {
    // 打开一个 sstable
    pub fn open(id: usize, file: FileObject) -> Result<Self> {
        //                      SSTable 数据布局
        // [   Block Section  ][        Meta Section         ]
        // [block1, block2,...][[block_meta1, block_meta2,...],block_meta_offset(u32),bloom_filter, bloom_filter_offset(u32)]
        let len = file.size();
        let bloom_filter_offset = (&file.read(len-4, 4).unwrap()[..]).get_u32() as u64;

        let buf = file.read(bloom_filter_offset, file.1-bloom_filter_offset-4/* bloom_filter_offset */)?;
        let bloom_filter = BloomFilter::decode_from_buf(&buf)?;
        let block_meta_offset = (&file.read(bloom_filter_offset-4, 4).unwrap()[..]).get_u32() as u64;
        let block_meta_data_len = file.1-block_meta_offset-4/* block_meta_offset */-(file.1-bloom_filter_offset) /*bloom filter data*/;
        let buf = file.read(block_meta_offset, block_meta_data_len)?;
        let block_meta = BlockMeta::decode_from_buf(&buf)?;
        let first_key = block_meta.get(0).unwrap().first_key.clone();
        let last_key = block_meta.get(block_meta.len()-1).unwrap().last_key.clone();
        Ok(Self{
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            first_key,
            last_key,
            bloom_filter
        })
    }

    /// 获取 sstable 的第 index 个 Block
    /// # 参数
    /// * index: sstable 中 Block 的 index
    pub fn read_block(&self, index: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[index].offset as u64;
        let offset_end = self.block_meta
            .get(index+1).map_or(self.block_meta_offset, |x| x.offset) as u64;
        // 此处有一个对文件的 IO
        let data = self.file.read(offset, offset_end-offset)?;
        let block_data = &data[..data.len()-4];
        let checksum = (&data[data.len()-4..]).get_u32();
        assert_eq!(checksum, crc32fast::hash(block_data), "block checksum mismatched!");
        Ok(Arc::new(Block::decode(block_data)))
    }

    // 找到一个可能包含 key 的 Block
    pub fn find_block_idx(&self, key: Bytes) -> usize{
        self.block_meta
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    // SsTable 中 Block 的数量
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
    use std::sync::Arc;
    use bytes::Bytes;
    use tempfile::{TempDir, tempdir};
    use crate::engines::lsm::iterators::StorageIterator;
    use crate::engines::lsm::table::{BlockMeta, SsTable};
    use crate::engines::lsm::table::builder::SsTableBuilder;
    use crate::engines::lsm::table::iterator::SsTableIterator;

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

    fn key_of(idx: usize) -> Bytes {
        Bytes::copy_from_slice(format!("key_{:03}", idx * 5).as_bytes())
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn num_of_keys() -> usize {
        100
    }

    fn generate_sst() -> (TempDir, SsTable) {
        let mut builder = SsTableBuilder::new(128);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            builder.add(&key[..], &value[..]);
        }
        let dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        (dir, builder.build(0, path).unwrap())
    }

    #[test]
    fn test_sst_build_all() {
        generate_sst();
    }

    #[test]
    fn test_sst_decode() {
        let (_dir, sst) = generate_sst();
        let meta = sst.block_meta.clone();
        let new_sst = SsTable::open(0, sst.file).unwrap();
        assert_eq!(new_sst.block_meta, meta);
        assert_eq!(
            new_sst.first_key(),
            key_of(0)
        );
        assert_eq!(
            new_sst.last_key(),
            key_of(num_of_keys() - 1)
        );
    }

    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
    }

    #[test]
    fn test_sst_iterator() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);
        let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
        for _ in 0..5 {
            for i in 0..num_of_keys() {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).as_ref()),
                    as_bytes(key.as_ref())
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.next().unwrap();
            }
            iter.seek_to_first().unwrap();
        }
    }

    #[test]
    fn test_sst_seek_key() {
        let (_dir, sst) = generate_sst();
        let sst = Arc::new(sst);
        let mut iter = SsTableIterator::create_and_seek_to_key(sst, key_of(0)).unwrap();
        for offset in 1..=5 {
            for i in 0..num_of_keys() {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).as_ref()),
                    as_bytes(key.as_ref())
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.seek_to_key(Bytes::copy_from_slice(&format!("key_{:03}", i * 5 + offset).as_bytes())).unwrap();
            }
            iter.seek_to_key(Bytes::copy_from_slice(b"k"))
                .unwrap();
        }
    }

}
