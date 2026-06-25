use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct OutputBuffer {
    inner: Mutex<Inner>,
    capacity: usize,
}

#[derive(Debug)]
struct Inner {
    data: VecDeque<Arc<String>>,
    first_line_number: usize,
}

impl OutputBuffer {
    pub(in crate::tasks) fn new(capacity: usize) -> OutputBuffer {
        assert!(capacity > 0, "Capacity should be positive");
        let inner = Inner {
            data: VecDeque::with_capacity(capacity),
            first_line_number: 0,
        };
        Self {
            inner: Mutex::new(inner),
            capacity,
        }
    }

    pub(in crate::tasks) fn insert_line(&self, line: Arc<String>) {
        let mut inner = self.inner.lock().unwrap();
        if inner.data.len() >= self.capacity {
            inner.data.pop_front().expect("data shouldn't be empty");
            inner.first_line_number += 1;
        }
        inner.data.push_back(line);
    }

    pub fn line_range(&self) -> Range<usize> {
        let inner = self.inner.lock().unwrap();
        inner.first_line_number..(inner.first_line_number + inner.data.len())
    }

    pub fn get_line(&self, line_number: usize) -> Option<Arc<String>> {
        let inner = self.inner.lock().unwrap();
        if line_number < inner.first_line_number {
            return None;
        }
        inner
            .data
            .get(line_number - inner.first_line_number)
            .map(Arc::clone)
    }

    pub fn get_line_range(&self, mut range: Range<usize>) -> Vec<Arc<String>> {
        let inner = self.inner.lock().unwrap();
        if range.start > range.end || range.end < inner.first_line_number {
            return Vec::new();
        }
        range.start = std::cmp::max(range.start, inner.first_line_number);
        range.end = std::cmp::min(range.end, inner.data.len() + inner.first_line_number);
        let range = (range.start - inner.first_line_number)..(range.end - inner.first_line_number);
        if range.start > range.end {
            return Vec::new();
        }
        inner.data.range(range).map(Arc::clone).collect()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn insert_get_line() {
        let ob = OutputBuffer::new(10);
        let lines = ["line 1", "line 2", "line 3"];
        for l in &lines {
            ob.insert_line(Arc::new(l.to_string()));
        }
        for (i, &l) in lines.iter().enumerate() {
            assert_eq!(*ob.get_line(i).unwrap(), l);
        }
    }

    #[test]
    fn insert_more_than_capacity_get_line() {
        let ob = OutputBuffer::new(5);
        let make_line = |i| format!("line {i}");
        for i in 0..10 {
            ob.insert_line(Arc::new(make_line(i)));
        }
        assert_eq!(ob.line_range(), 5..10);
        for i in 0..10 {
            let get_result = ob.get_line(i);
            if i < 5 {
                assert!(get_result.is_none());
            } else {
                assert_eq!(*get_result.unwrap(), make_line(i));
            }
        }
        assert!(ob.get_line(11).is_none());
    }

    #[test]
    fn line_range() {
        let ob = OutputBuffer::new(10);
        for _ in 0..3 {
            ob.insert_line(Arc::new("line".to_string()));
        }
        assert_eq!(ob.line_range(), 0..3);
    }

    #[test]
    fn line_range_above_capacity() {
        let ob = OutputBuffer::new(5);
        for _ in 0..10 {
            ob.insert_line(Arc::new("line".to_string()));
        }
        assert_eq!(ob.line_range(), 5..10);
    }

    #[test]
    fn get_line_range() {
        let ob = OutputBuffer::new(5);
        let make_line = |i| format!("{i}");
        for i in 0..10 {
            ob.insert_line(Arc::new(make_line(i)));
        }
        // 5 6 7 8 9 is in the buffer
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert!(ob.get_line_range(2..1).is_empty());
        }
        assert!(ob.get_line_range(1..4).is_empty());
        assert_eq!(
            ob.get_line_range(5..6)
                .iter()
                .map(|s| s.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            [5]
        );
        assert_eq!(
            ob.get_line_range(7..9)
                .iter()
                .map(|s| s.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            [7, 8]
        );
        assert_eq!(
            ob.get_line_range(7..25)
                .iter()
                .map(|s| s.parse::<usize>().unwrap())
                .collect::<Vec<_>>(),
            [7, 8, 9]
        );
        assert!(ob.get_line_range(10..25).is_empty());
        assert!(ob.get_line_range(20..25).is_empty());
        assert!(ob.get_line_range(20..20).is_empty());
        assert!(ob.get_line_range(7..7).is_empty());
    }

    #[test]
    fn get_on_empty() {
        let ob = OutputBuffer::new(1);
        assert!(ob.get_line(0).is_none());
        assert!(ob.get_line(1).is_none());
        assert!(ob.get_line_range(0..5).is_empty());
        assert_eq!(ob.line_range(), 0..0);
    }

    #[test]
    fn capacity_returns_capacity() {
        let capacity = 123;
        let ob = OutputBuffer::new(capacity);
        assert_eq!(ob.capacity(), capacity);
    }
}
