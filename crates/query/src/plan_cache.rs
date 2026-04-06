use crate::plan::PlanNode;
use std::collections::HashMap;

pub struct PlanCache {
    cache: HashMap<u64, PlanNode>,
    capacity: usize,
}

impl PlanCache {
    pub fn new(capacity: usize) -> Self {
        PlanCache {
            cache: HashMap::new(),
            capacity,
        }
    }

    pub fn insert(&mut self, query: &str, plan: PlanNode) -> u64 {
        let hash = Self::hash_query(query);
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&hash) {
            self.cache.clear();
        }
        self.cache.insert(hash, plan);
        hash
    }

    pub fn get(&self, hash: u64) -> Option<&PlanNode> {
        self.cache.get(&hash)
    }

    fn hash_query(query: &str) -> u64 {
        // FNV-1a hash
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in query.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner;

    #[test]
    fn test_cache_hit() {
        let mut cache = PlanCache::new(100);
        let plan = planner::plan("User filter .age > 30").unwrap();
        let hash = cache.insert("User filter .age > 30", plan);
        let cached = cache.get(hash);
        assert!(cached.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = PlanCache::new(100);
        assert!(cache.get(99999).is_none());
    }

    #[test]
    fn test_same_query_same_hash() {
        let mut cache = PlanCache::new(100);
        let plan1 = planner::plan("User filter .age > 30").unwrap();
        let plan2 = planner::plan("User filter .age > 30").unwrap();
        let h1 = cache.insert("User filter .age > 30", plan1);
        let h2 = cache.insert("User filter .age > 30", plan2);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_eviction_on_capacity() {
        let mut cache = PlanCache::new(2);
        let p1 = planner::plan("User").unwrap();
        let p2 = planner::plan("User filter .age > 1").unwrap();
        let p3 = planner::plan("User filter .age > 2").unwrap();
        cache.insert("User", p1);
        cache.insert("User filter .age > 1", p2);
        // Cache is full (2), inserting a third should clear
        let h3 = cache.insert("User filter .age > 2", p3);
        assert!(cache.get(h3).is_some());
        assert_eq!(cache.cache.len(), 1);
    }
}
