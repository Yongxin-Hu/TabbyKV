use bytes::BufMut;
use crate::engines::lsm::block::Block;
use crate::engines::lsm::key::{KeySlice, KeyVec};

const SIZEOF_U16: usize = std::mem::size_of::<u16>();

pub struct BlockBuilder {
    /// 每个 kv-pair 的 offset
    offsets: Vec<u16>,
    /// [key_overlap_len(u16), rest_key_len(u16),key(rest_key_len), time_stamp(u64), value_len(u16), value]
    /// 由于 block 内部的 key 是有序排列的，
    /// 使用 key_overlap_len 记录 key和 block 的 first_key 重合的长度来减少重复记录前缀
    data: Vec<u8>,
    /// Block大小(Byte)
    block_size: usize,
    /// First Key
    first_key: KeyVec
}

impl BlockBuilder {
    /// 创建 BlockBuilder
    /// # 参数
    /// * `block_size`: Block大小(Byte)
    pub fn new(block_size: usize) -> Self {
        BlockBuilder{
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new()
        }
    }

    /// 计算 key 和 first_key 重叠的 byte 数
    /// # 参数
    /// * `first_key`: block first key
    /// * `key`: key
    /// # 返回值
    /// * 重叠的 byte 数
    fn calc_overlap(first_key: KeySlice, key: KeySlice) -> usize {
        let mut index = 0;
        loop {
            if index >= first_key.key_len() || index >= key.key_len(){
                break;
            }
            if first_key.key_ref()[index] != key.key_ref()[index]{
                break;
            }
            index += 1;
        }
        index
    }

    /// 将 kv-pair 添加到块中。当 Block 已满时返回 false
    /// # 参数
    /// * key: key
    /// * value: value
    /// # 返回值
    /// true: 添加成功 false: 添加失败
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        if self.check_size(key, value) || self.is_empty() /* 允许放入的第一个 kv-pair 超过 block_size */{
            self.offsets.push(self.data.len() as u16);
            let key_overlap_len = Self::calc_overlap(self.first_key.as_key_slice(), key);
            // key_overlap_len
            self.data.put_u16(key_overlap_len as u16);
            // rest_key_len
            self.data.put_u16((key.key_len() - key_overlap_len) as u16);
            // rest_key
            self.data.put(&key.key_ref()[key_overlap_len..]);
            // time_stamp
            self.data.put_u64(key.ts());
            // value_len
            self.data.put_u16(value.len() as u16);
            // value
            self.data.put(value);
            if self.first_key.is_empty() {
                self.first_key = key.to_key_vec();
            }
            return true;
        }
        false
    }

    /// 检查加入 KV-Pair 后是否超出 block_size 大小
    fn check_size(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let prev_size = SIZEOF_U16 /* num_of_elements(2byte) */
            + self.offsets.len() * SIZEOF_U16 /* offset */
            + self.data.len() /* kv-pair */;
        let new_size = SIZEOF_U16 * 3 /* key_len + value_len + offset */ + key.raw_len() + value.len();
        prev_size + new_size <= self.block_size
    }

    /// BlockBuilder里面是否为空（无 KV-Pair）
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// 构建 Block
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

#[cfg(test)]
mod test{
    use crate::engines::lsm::block::Block;
    use crate::engines::lsm::block::builder::BlockBuilder;
    use crate::engines::lsm::key::KeySlice;

    #[test]
    fn test_block_builder_1(){
        let mut block_builder = BlockBuilder::new(16);
        assert!(block_builder.add(KeySlice::for_testing_from_slice_no_ts(b"key"), b"value"));
        block_builder.build();
    }

    #[test]
    fn test_block_build_full() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"11"));
        assert!(!builder.add(KeySlice::for_testing_from_slice_no_ts(b"22"), b"22"));
        builder.build();
    }

    #[test]
    fn test_block_build_large_1() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), &b"1".repeat(100)));
        builder.build();
    }

    #[test]
    fn test_block_build_large_2() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), b"1"));
        assert!(!builder.add(KeySlice::for_testing_from_slice_no_ts(b"11"), &b"1".repeat(100)));
    }

    fn key_of(idx: usize) -> Vec<u8> {
        format!("key_{:03}", idx * 5).into_bytes()
    }

    fn value_of(idx: usize) -> Vec<u8> {
        format!("value_{:010}", idx).into_bytes()
    }

    fn num_of_keys() -> usize {
        100
    }

    fn generate_block() -> Block {
        let mut builder = BlockBuilder::new(10000);
        for idx in 0..num_of_keys() {
            let key = key_of(idx);
            let value = value_of(idx);
            assert!(builder.add(
                KeySlice::for_testing_from_slice_no_ts(&key[..]),
                &value[..]
            ));
        }
        builder.build()
    }

    #[test]
    fn test_block_build_all() {
        generate_block();
    }

    #[test]
    fn test_block_encode() {
        let block = generate_block();
        block.encode();
    }

    #[test]
    fn test_block_decode() {
        let block = generate_block();
        let encoded = block.encode();
        let decoded_block = Block::decode(&encoded);
        assert_eq!(block.offsets, decoded_block.offsets);
        assert_eq!(block.data, decoded_block.data);
    }
}