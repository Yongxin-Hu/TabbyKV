use bytes::{Buf, BufMut, Bytes};
use anyhow::Result;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use bitvec::prelude::BitVec;

#[derive(PartialEq)]
pub struct BloomFilter{
    // bit vec size
    cap: u32,
    // bit vec
    bit_vec: BitVec<u8>,
    // hash function num
    k: u8
}

impl Debug for BloomFilter{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("cap", &self.cap)
            .field("k", &self.k)
            .finish()
    }
}
const DEFAULT_CAPACITY: usize = 1024;

impl Default for BloomFilter{
    fn default() -> Self {
        Self{
            cap: DEFAULT_CAPACITY as u32,
            bit_vec: BitVec::repeat(false, DEFAULT_CAPACITY),
            k: 3
        }
    }
}

impl BloomFilter{
    pub fn new() -> Self{
        Self::default()
    }

    pub fn with_capacity(cap: u32) -> Self{
        if cap % 8 != 0 {
            panic!("cap must be 8x!")
        }
        Self{
            cap,
            bit_vec: BitVec::repeat(false, cap as usize),
            k: 3
        }
    }

    pub fn with_cap_and_hash_num(cap: u32, k: u8) -> Self{
        if cap % 8 != 0 {
            panic!("cap must be 8x!")
        }
        Self{
            cap,
            bit_vec: BitVec::repeat(false, cap as usize),
            k
        }
    }

    /// 对 key 做 hash 后将对应的bit位设置为1
    pub fn add(&mut self, key: &[u8]) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;

        for i in 0..self.k {
            let index = (hash + i as usize * 31) % self.cap as usize;
            self.bit_vec.set(index, true);
        }
    }

    /// 是否可能包含 key
    pub fn may_contain(&self, key:&[u8]) -> bool{
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish() as usize;

        for i in 0..self.k {
            let index = (hash + i as usize * 31) % self.cap as usize;
            if !self.bit_vec[index] {
                return false;
            }
        }

        true
    }

    pub fn encode_to_buf(&self, buf: &mut Vec<u8>) {
        let bloom_offset = buf.len();
        // 放置容量
        buf.put_u32(self.cap);
        // 放置哈希函数数量
        buf.put_u8(self.k);
        buf.extend_from_slice(self.bit_vec.clone().into_vec().as_slice());
        let checksum = crc32fast::hash(&buf[bloom_offset..]);
        buf.put_u32(checksum);
    }

    pub fn decode_from_buf(buf: &[u8]) -> Result<Self> {
        // 读取容量
        let cap: u32 = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        //let cap = (&buf[0..4]).get_u32();
        // 读取哈希函数数量
        let k: u8 = buf[4];
        let data_end = buf.len() - 4;
        let bit_vec = BitVec::from_slice(&buf[5..data_end]);
        let checksum = (&buf[data_end..]).get_u32();
        assert_eq!(checksum, crc32fast::hash(&buf[..data_end]), "bloom filter checksum mismatched!");

        // 返回解码后的 BloomFilter
        Ok(BloomFilter {
            cap,
            bit_vec,
            k,
        })
    }
}

#[cfg(test)]
mod test{
    use crate::engines::lsm::table::bloom_filter::BloomFilter;

    #[test]
    fn test_bloom_filter(){
        let mut bloom = BloomFilter::new();
        bloom.add(b"key1");
        bloom.add(b"key2");
        assert!(bloom.may_contain(b"key1"));
        assert!(bloom.may_contain(b"key2"));
        assert!(!bloom.may_contain(b"key3"));
        let mut buf = Vec::new();
        bloom.encode_to_buf(&mut buf);
        let mut bloom2 = BloomFilter::decode_from_buf(buf.as_slice()).unwrap();
        assert_eq!(bloom, bloom2);
        assert!(bloom2.may_contain(b"key1"));
        assert!(bloom2.may_contain(b"key2"));
        assert!(!bloom2.may_contain(b"key456"));
        bloom2.add(b"key3");
        assert!(bloom2.may_contain(b"key3"));
    }
}