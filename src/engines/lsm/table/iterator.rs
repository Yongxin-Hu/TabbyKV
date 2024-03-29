use std::sync::Arc;
use crate::engines::lsm::block::iterator::BlockIterator;
use crate::engines::lsm::iterators::StorageIterator;
use crate::engines::lsm::table::SsTable;
use anyhow::Result;
use bytes::Bytes;
use crate::engines::lsm::key::KeySlice;

pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iterator: BlockIterator,
    block_index: usize
}

impl SsTableIterator {
    /// 创建并且移动到第一个 Block
    pub fn create_and_move_to_first(table: Arc<SsTable>) -> Result<Self>{
        let first_block = table.read_block_with_cache(0)?;
        Ok(SsTableIterator{
            table: Arc::clone(&table),
            block_iterator: BlockIterator::create_and_move_to_first(first_block),
            block_index: 0
        })
    }

    /// 移动到第一个 Block
    pub fn move_to_first(&mut self) -> Result<()> {
        let first_block = self.table.read_block_with_cache(0)?;
        self.block_index = 0;
        self.block_iterator = BlockIterator::create_and_move_to_first(first_block);
        Ok(())
    }

    fn move_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut block_idx = table.find_block_idx(key);
        let mut block = table.read_block_with_cache(block_idx)?;
        let mut block_iterator = BlockIterator::create_and_move_to_key(block, key);
        if !block_iterator.is_valid() {
            block_idx += 1;
            if block_idx < table.num_of_blocks() {
                block = table.read_block_with_cache(block_idx)?;
                block_iterator = BlockIterator::create_and_move_to_key(block, key);
            }
        }
        Ok((block_idx, block_iterator))
    }

    /// 创建并且移动到第一个key >= `key` 的位置
    pub fn create_and_move_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self>{
        let (block_index, block_iterator) = Self::move_to_key_inner(&table, key)?;
        Ok(SsTableIterator{
            table,
            block_index,
            block_iterator
        })
    }

    /// 移动到第一个key >= `key` 的位置
    pub fn move_to_key(&mut self, key: KeySlice) -> Result<()>{
        let (block_index, block_iterator) = Self::move_to_key_inner(&self.table, key)?;
        self.block_index = block_index;
        self.block_iterator = block_iterator;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    fn key(&self) -> KeySlice {
        self.block_iterator.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iterator.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.is_valid(){
            self.block_index += 1;
            if self.block_index < self.table.num_of_blocks() {
                let block = self.table.read_block(self.block_index)?;
                self.block_iterator = BlockIterator::create_and_move_to_first(block);
            }
        }
        Ok(())
    }
}