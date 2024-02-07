use bytes::BufMut;
use crate::engines::lsm::block::Block;

const SIZEOF_U16: usize = std::mem::size_of::<u16>();

pub struct BlockBuilder {
    /// 每个 kv-pair 的 offset
    offsets: Vec<u16>,
    /// [key_len(2byte), key , value_len(2byte), value]
    data: Vec<u8>,
    /// Block大小(Byte)
    block_size: usize
}

impl BlockBuilder {
    /// 创建 BlockBuilder
    pub fn new(block_size: usize) -> Self {
        BlockBuilder{
            offsets: Vec::new(),
            data: Vec::new(),
            block_size
        }
    }

    // 将 kv-pair 添加到块中。当 Block 已满时返回 false
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        if self.check_size(key, value) || self.is_empty() /* 允许放入的第一个 kv-pair 超过block_size */{
            self.offsets.push(self.data.len() as u16);
            // key_len
            self.data.put_u16(key.len() as u16);
            // key
            self.data.put(key);
            // value_len
            self.data.put_u16(value.len() as u16);
            // value
            self.data.put(value);
            return true;
        }
        false
    }

    // 检查加入 KV-Pair 后是否超出 block_size 大小
    fn check_size(&mut self, key: &[u8], value: &[u8]) -> bool {
        let prev_size = SIZEOF_U16 /* num_of_elements(2byte) */
            + self.offsets.len() * SIZEOF_U16 /* offset */
            + self.data.len() /* kv-pair */;
        let new_size = SIZEOF_U16 * 3 /* key_len + value_len + offset */ + key.len() + value.len();
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

    #[test]
    fn test_block_builder_1(){
        let mut block_builder = BlockBuilder::new(16);
        assert!(block_builder.add(b"key", b"value"));
        block_builder.build();
    }

    #[test]
    fn test_block_build_full() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(b"11", b"11"));
        assert!(!builder.add(b"22", b"22"));
        builder.build();
    }

    #[test]
    fn test_block_build_large_1() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(b"11", &b"1".repeat(100)));
        builder.build();
    }

    #[test]
    fn test_block_build_large_2() {
        let mut builder = BlockBuilder::new(16);
        assert!(builder.add(b"11", b"1"));
        assert!(!builder.add(b"11", &b"1".repeat(100)));
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
            assert!(builder.add(&key[..], &value[..]));
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