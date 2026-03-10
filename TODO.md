## Task
- [x] test send_signal
- [x] ai code review

## Test fixes
- [x] there are some tests which have panic in a spawned task but it doesn't fail test itself
    - `thread 'transport::background_writer::tests::background_line_writer_write_test' (1438043) panicked at /Users/sergey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-test-0.4.5/src/io.rs|492| 36:`

## Task manager
- [ ] first version is implemented
- [ ] in task related_tasks should be protected by mutex to remove &mut self
- [ ] task should be a state machine: created, running, finished
- [ ] make task manager clonable by:
    ```rust
    struct TaskManagerInner {
        tasks: RwLock<HashMap<TaskId, Arc<Task>>>,
        next_id: AtomicUsize,
    }

    #[derive(Clone)]
    pub struct TaskManager(Arc<TaskManagerInner>);
    ```
    This is needed for the next entry
- [ ] task manager should somehow remove task from the map after it is finished:
    - when task is created, task manager subscribes to exit of the task
    - in that subscribtion we spawn a task which will move the task out of the map and call finish on it

## Api and handlers

