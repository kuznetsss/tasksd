## Task
- [x] test send_signal
- [x] ai code review

## Test fixes
- [x] there are some tests which have panic in a spawned task but it doesn't fail test itself
    - `thread 'transport::background_writer::tests::background_line_writer_write_test' (1438043) panicked at /Users/sergey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-test-0.4.5/src/io.rs|492| 36:`

## Task manager
- [x] first version is implemented
- [ ] refactor Task into typestate pattern:
    - [ ] TaskBuilder (replaces New state):
        - new(executable) creates builder
        - builder pattern for args, working_dir, on_output, on_exit handlers
        - start() consumes builder, spawns process, returns Task
    - [ ] Task (Running state):
        - holds Arc<TaskInfo>, pid, stdin, cancel, related_tasks etc.
        - consumable parts in Mutex<Option<TaskInner>> so finish() can take &self (needed because Task lives inside Arc)
        - methods: send_signal, write_to_stdin, wait (awaits process exit), finish (joins related tasks, returns FinishedTask)
    - [ ] FinishedTask:
        - holds TaskInfo and exit status (and output buffer later)
    - [x] create TaskError instead of using anyhow
- [ ] make task manager clonable (Arc<TaskManagerInner> pattern)
- [ ] task manager lifecycle for tasks:
    - active tasks stored in HashMap<TaskId, Arc<Task>>
    - on task creation, manager spawns a coroutine that:
        1. holds Arc<Task> + clone of TaskManager
        2. awaits process exit (on_exit_rx)
        3. removes task from active hashmap
        4. calls finish() to join related tasks
        5. moves FinishedTask into an archive

## Api and handlers

