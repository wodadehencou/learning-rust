use std::{collections::HashMap, sync::Arc, sync::RwLock};

struct ParallelMap<V> {
    m: Arc<RwLock<HashMap<String, Arc<V>>>>,
}

impl<V> ParallelMap<V> {
    fn new() -> ParallelMap<V> {
        ParallelMap {
            m: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn put(&mut self, key: String, value: Arc<V>) {
        let mut m = self.m.write().unwrap();
        m.insert(key, value);
    }

    fn get(&self, key: &str) -> Option<Arc<V>> {
        let m = self.m.read().unwrap();
        match m.get(key) {
            None => None,
            Some(value) => Some(value.clone()),
        }
    }

    fn del(&mut self, key: &str) {
        let mut m = self.m.write().unwrap();
        m.remove(key);
    }
}

impl<V> Clone for ParallelMap<V> {
    fn clone(&self) -> ParallelMap<V> {
        ParallelMap { m: self.m.clone() }
    }
}

#[cfg(test)]
mod tests {

    use rand::Rng;
    use std::thread;

    use super::*;

    #[test]
    fn test_basic_operation() {
        let mut m = ParallelMap::new();
        m.put(String::from("a"), Arc::new(String::from("a-value")));
        m.put(String::from("b"), Arc::new(String::from("b-value")));
        m.put(String::from("c"), Arc::new(String::from("c-value")));
        assert_eq!(m.get("a"), Some(Arc::new(String::from("a-value"))));
        m.del("a");
        assert_eq!(m.get("a"), None);
    }

    #[test]
    fn test_parallel_operation() {
        let mut m = ParallelMap::new();
        for i in 0..1000 {
            m.put(i.to_string(), Arc::new(2 * i));
        }
        let mut handles = vec![];
        for _ in 0..100 {
            let m = m.clone();
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..100000 {
                    let idx = rng.gen_range(0, 1000);
                    let v = m.get(&idx.to_string()).expect("value should exist");
                    assert_eq!(v, Arc::new(2 * idx));
                    // println!("idx = {} read test pass", idx);
                }
            });
            handles.push(handle);
        }
        for _ in 0..100 {
            let m = m.clone();
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..100000 {
                    let idx = rng.gen_range(1000, 2000);
                    let v = m.get(&idx.to_string());
                    assert_eq!(v, None);
                    // println!("idx = {} read none test pass", idx);
                }
            });
            handles.push(handle);
        }
        for _ in 0..10 {
            let mut m = m.clone();
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..100000 {
                    let idx = rng.gen_range(2000, 3000);
                    m.put(idx.to_string(), Arc::new(2 * idx));
                    // println!("idx = {} write test pass", idx);
                }
            });
            handles.push(handle);
        }

        handles.into_iter().for_each(move |h| h.join().unwrap());
    }
}
