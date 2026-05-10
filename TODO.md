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
    - [ ] use WrappedTaskTracker instead of Mutex<Option<JoinSet>>
        - [x] Switch TaskManager to it
        - [x] Switch TaskEvents to it
        - [ ] Switch Task to it
        - [ ] fix or re-acknowledge the TOCTOU race between is_closed() and inner.spawn() in WrappedTaskTracker::spawn (TODO comment was removed but race still exists)
        - [ ] restore or properly delete `panic_if_dropped_without_finish` test in task.rs (currently commented out; double-panics on drop after Task switches to WrappedTaskTracker)
        - [ ] remove dead code: `is_joining` in tracker.rs, unused `JoinSet` import in events.rs, unused `Mutex`/`JoinSet` imports in task_manager.rs
    - [ ] implement TaskManager
    - [ ] add finish() to TaskManager (also needed so TaskManager's WrappedTaskTracker doesn't panic on drop)
    - [ ] collect completed completion_coroutines in TaskManager
    - [ ] LRU cache for finished_task to limit it's memory
- [ ] Task output buffer
- [ ] API and RPC handlers

    - active tasks are stored in HashMap<TaskId, Arc<Task>>
    - on task creation, manager spawns a coroutine that:
        1. holds Arc<Task> + clone of TaskManager
        2. calls task.wait().await to await process exit
        3. takes the task out from active hashmap
        4. problem: finish() requires exclusive ownership
        5. calls finish() to join related tasks
        6. moves FinishedTask into an archive

----

## Api and handlers

