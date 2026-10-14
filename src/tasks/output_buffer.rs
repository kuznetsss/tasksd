use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, Mutex},
};

use crate::tasks::OutputLine;

#[derive(Debug)]
pub struct OutputBuffer {
    data: Mutex<LineStorage>,
}

impl OutputBuffer {
    pub(in crate::tasks) fn new(capacity: usize) -> OutputBuffer {
        Self {
            data: Mutex::new(LineStorage::new(capacity)),
        }
    }

    pub(in crate::tasks) fn insert_line(&self, line: Arc<OutputLine>) {
        self.data.lock().unwrap().insert_line(line);
    }

    pub fn line_range(&self) -> Range<usize> {
        self.data.lock().unwrap().line_range()
    }

    pub fn get_line(&self, line_number: usize) -> Option<Arc<OutputLine>> {
        self.data.lock().unwrap().get_line(line_number)
    }

    pub fn get_range_of_lines(&self, range: Range<usize>) -> Vec<Arc<OutputLine>> {
        self.data.lock().unwrap().get_range_of_lines(range)
    }

    pub fn get_last_lines(&self, n: usize) -> Vec<Arc<OutputLine>> {
        let data = self.data.lock().unwrap();
        let mut range = data.line_range();
        let n = n.min(range.len());
        range.start = range.end - n;
        data.get_range_of_lines(range)
    }

    pub fn capacity(&self) -> usize {
        self.data.lock().unwrap().capacity
    }
}

#[derive(Debug)]
struct LineStorage {
    data: VecDeque<Arc<OutputLine>>,
    capacity: usize,
}

impl LineStorage {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Capacity should be positive");
        Self {
            data: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn insert_line(&mut self, line: Arc<OutputLine>) {
        if !self.data.is_empty() {
            assert_eq!(self.data.back().unwrap().line_number + 1, line.line_number);
        } else {
            assert_eq!(line.line_number, 0);
        }
        if self.data.len() >= self.capacity {
            self.data.pop_front().expect("data shouldn't be empty");
        }
        self.data.push_back(line);
    }

    fn line_range(&self) -> Range<usize> {
        if self.data.is_empty() {
            0..0
        } else {
            self.data.front().unwrap().line_number..(self.data.back().unwrap().line_number + 1)
        }
    }

    fn get_line(&self, line_number: usize) -> Option<Arc<OutputLine>> {
        if self.data.is_empty() {
            return None;
        }
        if line_number < self.data.front().unwrap().line_number
            || line_number > self.data.back().unwrap().line_number
        {
            return None;
        }
        self.data
            .get(line_number - self.data.front().unwrap().line_number)
            .map(Arc::clone)
    }

    fn get_range_of_lines(&self, mut range: Range<usize>) -> Vec<Arc<OutputLine>> {
        if self.data.is_empty()
            || range.start > range.end
            || range.end < self.data.front().unwrap().line_number
        {
            return Vec::new();
        }
        let first_line_number = self.data.front().unwrap().line_number;
        range.start = std::cmp::max(range.start, first_line_number);
        range.end = std::cmp::min(range.end, self.data.back().unwrap().line_number + 1);
        let range = (range.start - first_line_number)..(range.end - first_line_number);
        if range.start > range.end {
            return Vec::new();
        }
        self.data.range(range).map(Arc::clone).collect()
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
    fn get_range_of_lines() {
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
            assert!(ob.get_range_of_lines(2..1).is_empty());
        }
        assert!(ob.get_range_of_lines(1..4).is_empty());
        assert_eq!(
            ob.get_range_of_lines(5..6)
                .iter()
                .map(|s| s.line_number)
                .collect::<Vec<_>>(),
            [5]
        );
        assert_eq!(
            ob.get_range_of_lines(7..9)
                .iter()
                .map(|s| s.line_number)
                .collect::<Vec<_>>(),
            [7, 8]
        );
        assert_eq!(
            ob.get_range_of_lines(7..25)
                .iter()
                .map(|s| s.line_number)
                .collect::<Vec<_>>(),
            [7, 8, 9]
        );
        assert!(ob.get_range_of_lines(10..25).is_empty());
        assert!(ob.get_range_of_lines(20..25).is_empty());
        assert!(ob.get_range_of_lines(20..20).is_empty());
        assert!(ob.get_range_of_lines(7..7).is_empty());
    }

    #[test]
    fn get_on_empty() {
        let ob = OutputBuffer::new(1);
        assert!(ob.get_line(0).is_none());
        assert!(ob.get_line(1).is_none());
        assert!(ob.get_range_of_lines(0..5).is_empty());
        assert_eq!(ob.line_range(), 0..0);
    }

    #[test]
    fn get_last_lines_returns_last_n_lines() {
        let ob = OutputBuffer::new(3);

        assert!(ob.get_last_lines(123).is_empty());

        let output_lines: Vec<Arc<OutputLine>> = (0..3)
            .map(|i| {
                OutputLine {
                    content: format!("line {i}"),
                    line_number: i,
                }
                .into()
            })
            .collect();
        ob.insert_line(output_lines[0].clone());
        let lines = ob.get_last_lines(1);
        assert_eq!(lines, [output_lines[0].clone()]);

        assert_eq!(lines, ob.get_last_lines(2));
        assert_eq!(lines, ob.get_last_lines(3));

        ob.insert_line(output_lines[1].clone());
        ob.insert_line(output_lines[2].clone());

        assert!(ob.get_last_lines(0).is_empty());
        assert_eq!(ob.get_last_lines(1), [output_lines[2].clone()]);
        assert_eq!(
            ob.get_last_lines(2),
            [output_lines[1].clone(), output_lines[2].clone()]
        );
        assert_eq!(ob.get_last_lines(3), output_lines);
        assert_eq!(ob.get_last_lines(4), output_lines);
    }

    #[test]
    fn get_last_lines_returns_last_n_lines_after_old_lines_dropped() {
        let ob = OutputBuffer::new(3);
        ob.insert_line(
            OutputLine {
                content: "line 0".into(),
                line_number: 0,
            }
            .into(),
        );
        let output_lines: Vec<Arc<_>> = (1..4)
            .map(|i| {
                OutputLine {
                    content: format!("line {i}"),
                    line_number: i,
                }
                .into()
            })
            .collect();

        for o in &output_lines {
            ob.insert_line(o.clone());
        }

        assert_eq!(ob.get_last_lines(1), [output_lines[2].clone()]);
        assert_eq!(
            ob.get_last_lines(2),
            [output_lines[1].clone(), output_lines[2].clone()]
        );
        assert_eq!(ob.get_last_lines(3), output_lines);
    }

    #[test]
    fn capacity_returns_capacity() {
        let capacity = 123;
        let ob = OutputBuffer::new(capacity);
        assert_eq!(ob.capacity(), capacity);
    }
}
