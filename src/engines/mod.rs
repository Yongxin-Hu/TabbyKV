use std::collections::HashMap;

pub trait Engine{
    /// 设置 key value 返回 old value
    fn set(&mut self, key: String, value: String) -> Option<String>;

    /// 根据 key 获取 value，key不存在则返回 None
    fn get(&self, key: &String) -> Option<&String>;

    /// 删除 key
    fn remove(&mut self, keys: &String) -> Option<String>;
}

#[derive(Debug, Clone)]
pub struct KvStore{
    map: HashMap<String, String>
}

impl KvStore{
    pub fn new() -> Self{
        KvStore{
            map: HashMap::new()
        }
    }
}

impl Engine for KvStore{
    fn set(&mut self, key: String, value: String) -> Option<String>{
        self.map.insert(key, value)
    }

    fn get(&self, key: &String) -> Option<&String>{
        self.map.get(key)
    }

    fn remove(&mut self, keys: &String) -> Option<String>{
        self.map.remove(keys)
    }
}