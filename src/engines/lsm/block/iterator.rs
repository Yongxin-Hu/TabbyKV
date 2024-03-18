use std::sync::Arc;
use bytes::{Buf, Bytes};
use crate::engines::lsm::block::{Block, SIZEOF_U16};

pub struct BlockIterator {
    block: Arc<Block>,
    /// 当前 key ，空表示迭代器无效
    key: Bytes,
    /// 当前 value 在 block 中的范围
    value_range: (usize, usize),
    /// 当前 kv-pair 的索引， 范围在[0, num_of_element)
    idx: usize,
    /// block 中的 first_key
    first_key: Bytes,
}

impl Block {
    fn get_first_key(&self) -> &[u8] {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        key
    }
}


impl BlockIterator{
    // 创建 BlockIterator
    fn new(block: Arc<Block>) -> Self {
        let first_key = Bytes::from(block.get_first_key().to_vec());
        BlockIterator {
            block,
            key: Bytes::new(),
            value_range: (0, 0),
            idx: 0,
            first_key
        }
    }

    // 创建 BlockIterator 并且移动到第一个 kv-pair
    pub fn create_and_move_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.move_to_first();
        iter
    }

    /// 创建 BlockIterator 并且移动到第一个 key >= `key`
    pub fn create_and_move_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.move_to_key(key);
        iter
    }

    /// 返回当前的 key
    pub fn key(&self) -> &[u8] {
        assert!(!self.key.is_empty(), "invalid iterator, key must not empty");
        self.key.as_ref()
    }

    /// 返回当前的 value
    pub fn value(&self) -> &[u8] {
        assert!(!self.key.is_empty(), "invalid iterator, key must not empty");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    // 当前 iterator 是否有效
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// 移动到第一个 kv-pair
    pub fn move_to_first(&mut self) {
        self.move_to(0);
    }

    /// 移动到下一个 kv-pair
    pub fn next(&mut self) {
        let next_index = self.idx + 1;
        self.move_to(next_index);
    }

    /// 移动到第一个 key >= `key`
    pub fn move_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.move_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.move_to(low);
    }

    /// 移动到第 index 个 kv-pair
    fn move_to(&mut self, index: usize){

        if index >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[index] as usize;
        let mut data = &self.block.data[offset..];

        // rest key
        let key_overlap_len = data.get_u16() as usize;
        let rest_key_len = data.get_u16() as usize;
        let rest_key = &data[..rest_key_len];
        data.advance(rest_key_len);
        // value
        let value_len = data.get_u16() as usize;
        let value_start = offset + 2 * SIZEOF_U16/* key_overlap_len+rest_key_len */
            + rest_key_len /* rest_key */ + SIZEOF_U16/* value_len */;
        let value_end = value_start + value_len;

        self.idx = index;
        let mut key = self.first_key.as_ref()[..key_overlap_len].to_vec();
        key.extend_from_slice(rest_key);
        self.key = Bytes::from(key);
        self.value_range = (value_start, value_end);
    }
}

#[cfg(test)]
mod test{
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::engines::lsm::block::Block;
    use crate::engines::lsm::block::builder::BlockBuilder;
    use crate::engines::lsm::block::iterator::BlockIterator;

    #[test]
    fn test_get_first_key(){
        let mut block_builder = BlockBuilder::new(4*1024);
        assert!(block_builder.add(b"hello", b"world"));
        assert!(block_builder.add(b"hello2", b"world2"));
        let block = block_builder.build();
        print!("{:?}", String::from_utf8(block.get_first_key().to_vec()));
    }

    fn as_bytes(x: &[u8]) -> Bytes {
        Bytes::copy_from_slice(x)
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
    fn test_block_iterator() {
        let block = Arc::new(generate_block());
        let mut iter = BlockIterator::create_and_move_to_first(block);

        for _ in 0..5 {
            for i in 0..100 {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i).as_slice(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).as_slice()),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.next();
            }
            iter.move_to_first();
        }
    }

    #[test]
    fn test_1() {
        let mut block_builder = BlockBuilder::new(10000);
        assert!(block_builder.add(b"key1", b"value1"));
        assert!(block_builder.add(b"key2", b"value2"));
        let block = block_builder.build();
        let encoded = block.encode();
        let decoded_block = Block::decode(&encoded);
        let mut block_iterator = BlockIterator::create_and_move_to_first(Arc::new(decoded_block));
        assert_eq!(block_iterator.key(), b"key1");
        block_iterator.next();
        assert_eq!(block_iterator.key(), b"key2");
        assert_eq!(block_iterator.value(), b"value2");
    }

    #[test]
    fn test_block_seek_key() {
        let block = Arc::new(generate_block());
        let mut iter = BlockIterator::create_and_move_to_key(block, key_of(0).as_slice());
        for offset in 1..=5 {
            for i in 0..num_of_keys() {
                let key = iter.key();
                let value = iter.value();
                assert_eq!(
                    key,
                    key_of(i).as_slice(),
                    "expected key: {:?}, actual key: {:?}",
                    as_bytes(key_of(i).as_slice()),
                    as_bytes(key)
                );
                assert_eq!(
                    value,
                    value_of(i),
                    "expected value: {:?}, actual value: {:?}",
                    as_bytes(&value_of(i)),
                    as_bytes(value)
                );
                iter.move_to_key(&format!("key_{:03}", i * 5 + offset).into_bytes());
            }
            iter.move_to_key(b"k");
        }
    }
}
