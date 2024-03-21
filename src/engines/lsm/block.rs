pub(crate) mod builder;
pub(crate) mod iterator;

use bytes::{Buf, BufMut, Bytes, BytesMut};

const SIZEOF_U16: usize = std::mem::size_of::<u16>();
const SIZEOF_U64: usize = std::mem::size_of::<u64>();
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        // 预先计算出缓冲区的大小
        let buf_size = self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16;
        let mut buf = BytesMut::with_capacity(buf_size);

        // 将数据添加到缓冲区
        buf.extend_from_slice(&self.data);

        // 将偏移量添加到缓冲区
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }

        // 添加偏移量长度到缓冲区
        buf.put_u16(self.offsets.len() as u16);

        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        // 取出最后两个 u8 得到 num_of_element
        let num_of_element = (&data[data.len()-SIZEOF_U16..]).get_u16() as usize;
        // 取出 offset
        let offset_end = data.len() - SIZEOF_U16;
        let offset_start = offset_end - num_of_element * SIZEOF_U16;
        let offsets:Vec<u16> = data[offset_start..offset_end]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // 取出 data
        let data = data[..offset_start].to_vec();
        Block {data, offsets}
    }
}