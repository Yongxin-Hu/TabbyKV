mod lsm;

use std::collections::HashMap;
use std::panic::set_hook;
use std::path::Path;
use std::sync::Arc;
use crate::engines::lsm::storage::LsmStorage;
use anyhow::Result;
use crate::engines::lsm::storage::option::LsmStorageOptions;

pub trait Engine{
    /// 设置 key value
    fn put(&mut self, key: String, value: String) -> Result<()>;

    /// 根据 key 获取 value，key不存在则返回 None
    fn get(&self, key: &String) -> Result<Option<String>>;

    /// 删除 key
    fn delete(&mut self, keys: &String) -> Result<()>;

    /// 关闭
    fn close(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct LsmStore{
    inner: Arc<LsmStorage>
}

impl LsmStore {
    pub fn new(path: impl AsRef<Path>) -> Result<Self>{
        Self::with_options(path, Default::default())
    }

    pub fn with_options(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        Ok(Self{
            inner: LsmStorage::open(path, options)?
        })
    }
}

impl Engine for LsmStore{
    fn put(&mut self, key: String, value: String) -> Result<()> {
        self.inner.put(key.as_bytes(), value.as_bytes())
    }

    fn get(&self, key: &String) -> Result<Option<String>> {
        Ok(self.inner.get(key.as_bytes())?.map(|v| String::from_utf8(v.to_vec())).transpose()?)
    }

    fn delete(&mut self, keys: &String) -> Result<()> {
        self.inner.delete(keys.as_bytes())
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        self.close();
    }
}