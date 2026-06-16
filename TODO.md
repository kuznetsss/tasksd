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
- [x] figure out who should own cancellation_tokens
- [x] maybe optimise reader's buffer because serde_json::parse consumes a reference
- [x] error codes
- [x] application
- [x] session
- [x] handler:
    - [x] task builder methods should consume and return &mut self

## Improvements
- [x] Create transport::error and replace anyhow with it to be able to distinguish EOF from other errors
- [x] Support standard json rpc errors
    - -32700 - Parse error - Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text.
    - -32600 - Invalid Request - The JSON sent is not a valid Request object.
    - -32601 - Method not found - The method does not exist / is not available.
    - -32602 - Invalid params - Invalid method parameter(s).
    - -32603 - Internal error - Internal JSON-RPC error.
    - -32000 to -32099 - Server error - Reserved for implementation-defined server-errors.

- [x] Add pub use in mod.rs files to avoid long namespaces
- [x] Create module app and put application, session and handler there
- [x] Refactor handler and make creating response and notification more convenient
- [x] Shutdown all the running tasks on shutdown
- [x] Setup tracing_subscriber
- [x] Add more logging
- [x] Add tests:
    - [x] signal deserializing
    - [x] subscribe to output is true by default
- [x] License
- [x] CI
- [x] Bug: on macos when pty child dies its output buffer is dropped. So there is a race between output reader and child pty getting closed
      Solution:
        - create new PtyOutput struct which holds child pty, self read part and the future that the child process has exited
        - implement AsyncRead for PtyOutput:
            - if self pty returned pending, check whether the process has exited
- [x] Tests:
    - [x] PtyReadPart::try_read
    - [x] pty_reader.rs
- [x] Write to output buffer in output reading corotuine directly so the buffer become lossless. Because broadcast channel is lossy (see NOTE in senders.rs)
- [x] Readme
- [ ] Better test coverage:
    - Use cargo-llvm-cov for coverage: https://github.com/taiki-e/cargo-llvm-cov
- [ ] Integration tests
- [ ] Remove anyhow where possible
- [ ] Verify shutdown and cancellation paths
- [ ] Fix TODOs comments
- [ ] Documentation

## Future plans

- [ ] Add line number to output notification
- [ ] Add notifications about missed output
- [ ] Subscription control (unsubscribe)
- [ ] Broadcast shutdown notification to all connections
- [ ] Tasks chains
- [ ] Shutdown API method
- [ ] Limit log file size
