use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Db {
    shared: Shared,
}

#[derive(Debug, Clone)]
pub struct Shared {
    data: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            shared: Shared {
                data: Arc::new(Mutex::new(HashMap::new())),
            },
        }
    }

    pub fn set(&self, key: String, value: Bytes) {
        self.shared.data.lock().unwrap().insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.shared.data.lock().unwrap().get(key).cloned()
    }

    pub fn remove(&self, key: &str) -> Option<Bytes> {
        self.shared.data.lock().unwrap().remove(key)
    }
}

impl Default for Db {
    fn default() -> Self {
        Db::new()
    }
}
