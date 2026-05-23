## Task
- [x] test send_signal
- [x] ai code review

## Test fixes
- [x] there are some tests which have panic in a spawned task but it doesn't fail test itself
    - `thread 'transport::background_writer::tests::background_line_writer_write_test' (1438043) panicked at /Users/sergey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-test-0.4.5/src/io.rs|492| 36:`

## Task manager
- [x] first version is implemented
- [x] refactor Task into typestate pattern:
    - [x] TaskBuilder (replaces New state):
        - new(executable) creates builder
        - builder pattern for args, working_dir, on_output, on_exit handlers
        - start() consumes builder, spawns process, returns Task
    - [x] Task (Running state):
        - holds Arc<TaskInfo>, pid, stdin, cancel, related_tasks etc.
        - consumable parts in Mutex<Option<TaskInner>> so finish() can take &self (needed because Task lives inside Arc)
        - methods: send_signal, write_to_stdin, wait (awaits process exit), finish (joins related tasks, returns FinishedTask)
    - [x] FinishedTask:
        - holds TaskInfo and exit status (and output buffer later)
    - [x] create TaskError instead of using anyhow
    - [x] split common.rs into info, senders, events
    - [x] fix failing tests
    - [x] test for TaskEvents
    - [x] tests for Task
    - [x] check test coverage in tasks submodule
    - [x] Maybe? remove prefix Task in class names where possible
- [ ] task manager lifecycle for tasks:
    - [x] add mutex for internal tasks in Task, then finish() doesn't need &mut
    - [x] get task should return either task of finished task or not found error
    - [-] ~~wrap JoinSet to drain on spawn, propagate panic and drain all with panic propagation and ignoring aborted tasks~~
    - [x] finish tests for WrappedTaskTracker 
    - [x] use WrappedTaskTracker instead of Mutex<Option<JoinSet>>
    - [x] implement TaskManager
    - [x] add join() to TaskManager (also needed so TaskManager's WrappedTaskTracker doesn't panic on drop)
    - [x] collect completed completion_coroutines in TaskManager
    - [x] Tests for Task manager
    - [x] LRU cache for finished_task to limit it's memory
- [x] Task output buffer

## Minimal API
- [x] start task, send signal request and response
- [x] task output notification
- [x] task exit notification
- [~] error codes
- [~] application
- [ ] session
- [ ] figure out who should own cancellation_tokens
- [ ] maybe optimise reader's buffer because serde_json::parse consumes a reference

## Handler
- [ ]

## Session


## More API


