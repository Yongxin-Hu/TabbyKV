use std::path::Path;
use bytes::{BufMut, Bytes};
use crate::engines::lsm::block::builder::BlockBuilder;
use crate::engines::lsm::table::{BlockMeta, FileObject, SsTable};
use anyhow::Result;
use crate::engines::lsm::table::bloom_filter::BloomFilter;

pub struct SsTableBuilder {
    block_builder: BlockBuilder,
    // 协助记录每个block的first_key以及last_key
    first_key: Bytes,
    last_key: Bytes,
    // sstable中的数据 ([block, ...], [block_meta, ...])
    data: Vec<u8>,
    // 保存每个block的元信息
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    bloom_filter: BloomFilter
}


impl SsTableBuilder{
    /// 根据给定的 block_size 创建 sstable_builder
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder{
            block_builder: BlockBuilder::new(block_size),
            first_key: Bytes::new(),
            last_key: Bytes::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            bloom_filter: BloomFilter::new()
        }
    }

    /// 向 sstable 中添加 kv-pair (在 Block 满的时候新建 Block)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        // BloomFilter 记录 key
        self.bloom_filter.add(key);

        if self.first_key.is_empty() {
            self.first_key = Bytes::copy_from_slice(key);
        }

        if self.block_builder.add(key, value) {
            self.last_key = Bytes::copy_from_slice(key);
            return;
        }else{
            // Block已满
            self.complete_block();
        }

        assert!(self.block_builder.add(key, value));
        self.first_key = Bytes::copy_from_slice(key);
        self.last_key = Bytes::copy_from_slice(key);
    }

    /// 构建 block
    fn complete_block(&mut self) {
        let block_builder = std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        self.meta.push(BlockMeta{
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key),
            last_key: std::mem::take(&mut self.last_key),
        });
        let mut block_data = block_builder.build().encode().to_vec();
        let checksum = crc32fast::hash(&block_data);
        block_data.put_u32(checksum);
        self.data.extend(block_data);
    }

    /// 计算 sstable的估计大小
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// 构建 sstable
    /// # 参数
    /// * id: sstable 的 id
    /// * path: id.sst
    pub fn build(
        mut self,
        id: usize,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.complete_block();
        //                      SSTable 数据布局
        // [   Block Section  ][        Meta Section         ]
        // [block1, block2,...][[block_meta1, block_meta2,...],block_meta_offset(u32),bloom_filter, bloom_filter_offset(u32)]
        let block_meta_offset = self.data.len();
        BlockMeta::encode_to_buf(self.meta.as_slice(), &mut self.data);
        self.data.put_u32(block_meta_offset as u32);
        let bloom_filter_offset = self.data.len();
        self.bloom_filter.encode_to_buf(&mut self.data);
        self.data.put_u32(bloom_filter_offset as u32);
        let file = FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable{
            id,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset,
            file,
            bloom_filter: self.bloom_filter
        })
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;
    use crate::engines::lsm::table::builder::SsTableBuilder;

    #[test]
    fn simple_test_sst_builder() {
        let dir = tempdir().unwrap();
        let mut sstable_builder = SsTableBuilder::new(16);
        sstable_builder.add(b"key1", b"value1");
        let path = dir.path().join("1.sst");
        let sst = sstable_builder.build(0, path).unwrap();
    }

    #[test]
    fn test_block_split(){
        let mut builder = SsTableBuilder::new(16);
        builder.add(b"11", b"11");
        builder.add(b"22", b"22");
        builder.add(b"33", b"33");
        builder.add(b"44", b"44");
        builder.add(b"55", b"55");
        builder.add(b"66", b"66");
        assert!(builder.meta.len() >= 2);
        let mut dir = tempdir().unwrap();
        let path = dir.path().join("1.sst");
        let sst = builder.build(0, path).unwrap();
    }
}