use std::{collections::VecDeque, ops::Range, sync::Arc};

pub struct OutputBuffer {
    data: VecDeque<Arc<String>>,
    capacity: usize,
    first_line_number: usize,
}

impl OutputBuffer {
    fn new(capacity: usize) -> OutputBuffer {
        Self {
            data: VecDeque::with_capacity(capacity),
            capacity,
            first_line_number: 0,
        }
    }

    pub(in crate::tasks) fn insert_line(&mut self, line: Arc<String>) {
        if self.data.len() == self.capacity {
            self.remove_last();
        }
        self.data.push_back(line);
    }

    pub fn line_range(&self) -> (usize, usize) {
        (
            self.first_line_number,
            self.first_line_number + self.data.len(),
        )
    }

    pub fn get_line(&self, line_number: usize) -> Option<Arc<String>> {
        if line_number < self.first_line_number {
            return None;
        }
        self.data
            .get(line_number - self.first_line_number)
            .map(Arc::clone)
    }

    pub fn get_line_range(&self, range: Range<usize>) -> Vec<Arc<String>> {
        todo!()
    }

    fn remove_last(&mut self) {
        self.data.pop_back().expect("data shouldn't be empty");
        self.first_line_number += 1;
    }
}
