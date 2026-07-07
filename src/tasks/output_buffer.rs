use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, Mutex},
};

use crate::tasks::OutputLine;

type Storage = VecDeque<Arc<OutputLine>>;
#[derive(Debug)]
pub struct OutputBuffer {
    data: Mutex<Storage>,
    capacity: usize,
}

impl OutputBuffer {
    pub(in crate::tasks) fn new(capacity: usize) -> OutputBuffer {
        assert!(capacity > 0, "Capacity should be positive");
        Self {
            data: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    pub(in crate::tasks) fn insert_line(&self, line: Arc<OutputLine>) {
        let mut data = self.data.lock().unwrap();
        if !data.is_empty() {
            assert_eq!(data.back().unwrap().line_number + 1, line.line_number);
        } else {
            assert_eq!(line.line_number, 0);
        }
        if data.len() >= self.capacity {
            data.pop_front().expect("data shouldn't be empty");
        }
        data.push_back(line);
    }

    pub fn line_range(&self) -> Range<usize> {
        let data = self.data.lock().unwrap();
        if data.is_empty() {
            0..0
        } else {
            data.front().unwrap().line_number..(data.back().unwrap().line_number + 1)
        }
    }

    pub fn get_line(&self, line_number: usize) -> Option<Arc<OutputLine>> {
        let data = self.data.lock().unwrap();
        if data.is_empty() {
            return None;
        }
        if line_number < data.front().unwrap().line_number
            || line_number > data.back().unwrap().line_number
        {
            return None;
        }
        data.get(line_number - data.front().unwrap().line_number)
            .map(Arc::clone)
    }

    pub fn get_line_range(&self, mut range: Range<usize>) -> Vec<Arc<OutputLine>> {
        let data = self.data.lock().unwrap();
        if data.is_empty()
            || range.start > range.end
            || range.end < data.front().unwrap().line_number
        {
            return Vec::new();
        }
        let first_line_number = data.front().unwrap().line_number;
        range.start = std::cmp::max(range.start, first_line_number);
        range.end = std::cmp::min(range.end, data.back().unwrap().line_number + 1);
        let range = (range.start - first_line_number)..(range.end - first_line_number);
        if range.start > range.end {
            return Vec::new();
        }
        data.range(range).map(Arc::clone).collect()
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
        for (i, &l) in lines.iter().enumerate() {
            ob.insert_line(Arc::new(OutputLine {
                content: l.into(),
                line_number: i,
            }));
        }
        for (i, &l) in lines.iter().enumerate() {
            let line = ob.get_line(i).unwrap();
            assert_eq!(line.content, l);
            assert_eq!(line.line_number, i);
        }
    }

    #[test]
    fn insert_more_than_capacity_get_line() {
        let ob = OutputBuffer::new(5);
        let make_line = |i| OutputLine {
            content: format!("line {i}"),
            line_number: i,
        };
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
    #[should_panic]
    fn insert_starting_from_non_zero_panics() {
        let ob = OutputBuffer::new(2);
        ob.insert_line(Arc::new(OutputLine {
            content: "".to_string(),
            line_number: 1,
        }));
    }

    #[test]
    #[should_panic]
    fn insert_non_contiguous_lines_panics() {
        let ob = OutputBuffer::new(2);
        ob.insert_line(Arc::new(OutputLine {
            content: "".to_string(),
            line_number: 0,
        }));
        ob.insert_line(Arc::new(OutputLine {
            content: "".to_string(),
            line_number: 2,
        }));
    }

    #[test]
    fn line_range() {
        let ob = OutputBuffer::new(10);
        for i in 0..3 {
            ob.insert_line(Arc::new(OutputLine {
                content: "line".to_string(),
                line_number: i,
            }));
        }
        assert_eq!(ob.line_range(), 0..3);
    }

    #[test]
    fn line_range_above_capacity() {
        let ob = OutputBuffer::new(5);
        for i in 0..10 {
            ob.insert_line(Arc::new(OutputLine {
                content: "line".to_string(),
                line_number: i,
            }));
        }
        assert_eq!(ob.line_range(), 5..10);
    }

    #[test]
    fn get_line_range() {
        let ob = OutputBuffer::new(5);
        let make_line = |i| OutputLine {
            content: format!("line {i}"),
            line_number: i,
        };
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
                .map(|s| s.line_number)
                .collect::<Vec<_>>(),
            [5]
        );
        assert_eq!(
            ob.get_line_range(7..9)
                .iter()
                .map(|s| s.line_number)
                .collect::<Vec<_>>(),
            [7, 8]
        );
        assert_eq!(
            ob.get_line_range(7..25)
                .iter()
                .map(|s| s.line_number)
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
