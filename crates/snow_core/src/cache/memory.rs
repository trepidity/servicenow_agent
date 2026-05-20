use std::collections::{HashMap, VecDeque};

use crate::SnowRecord;

#[derive(Debug, Clone)]
pub struct MemoryCache {
    capacity: usize,
    by_number: HashMap<String, SnowRecord>,
    order: VecDeque<String>,
}

impl MemoryCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            by_number: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, number: &str) -> Option<SnowRecord> {
        let record = self.by_number.get(number).cloned()?;
        self.touch(number);
        Some(record)
    }

    pub fn put(&mut self, record: SnowRecord) {
        let number = record.number.clone();
        self.by_number.insert(number.clone(), record);
        self.touch(&number);
        self.evict_if_needed();
    }

    pub fn invalidate(&mut self, number: &str) {
        self.by_number.remove(number);
        self.order.retain(|entry| entry != number);
    }

    fn touch(&mut self, number: &str) {
        self.order.retain(|entry| entry != number);
        self.order.push_back(number.to_string());
    }

    fn evict_if_needed(&mut self) {
        while self.by_number.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.by_number.remove(&oldest);
            } else {
                break;
            }
        }
    }
}
