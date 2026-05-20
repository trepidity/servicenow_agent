pub mod memory;
pub mod store;

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::SnowRecord;
use memory::MemoryCache;
use store::Store;

#[derive(Clone)]
pub struct CacheManager {
    memory: Arc<Mutex<MemoryCache>>,
    store: Arc<Store>,
}

impl CacheManager {
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let store = Store::open(path).map_err(anyhow::Error::from)?;
        Ok(Self {
            memory: Arc::new(Mutex::new(MemoryCache::new(capacity))),
            store: Arc::new(store),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    pub fn get(&self, number: &str) -> Option<SnowRecord> {
        self.memory.lock().ok()?.get(number)
    }

    pub fn put(&self, record: SnowRecord) {
        if let Ok(mut memory) = self.memory.lock() {
            memory.put(record);
        }
    }

    pub fn invalidate(&self, number: &str) {
        if let Ok(mut memory) = self.memory.lock() {
            memory.invalidate(number);
        }
    }
}
