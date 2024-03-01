pub mod merge_iterator;
pub mod fused_iterator;
pub mod two_merge_iterator;
pub mod lsm_iterator;
pub mod concat_iterator;

pub trait StorageIterator {
    /// 获取当前 value
    fn value(&self) -> &[u8];

    /// 获取当前 key.
    fn key(&self) -> &[u8];

    /// 检查当前 iterator 是否有效
    fn is_valid(&self) -> bool;

    /// 移动到下一个位置
    fn next(&mut self) -> anyhow::Result<()>;
}