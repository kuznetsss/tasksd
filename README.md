# Tasksd

[![CI status](https://github.com/kuznetsss/tasksd/actions/workflows/ci.yml/badge.svg)](https://github.com/kuznetsss/tasksd/actions/workflows/ci.yml)
[![Crates audit](https://github.com/kuznetsss/tasksd/actions/workflows/audit.yml/badge.svg)](https://github.com/kuznetsss/tasksd/actions/workflows/audit.yml)
[![Test coverage](https://codecov.io/gh/kuznetsss/tasksd/graph/badge.svg?token=NBUAOGLWUH)](https://codecov.io/gh/kuznetsss/tasksd)

Tasksd is a daemon allowing to spawn shell commands via JSON-RPC API.
As a daemon it detaches execution from the client allowing the spawned process to run even if the client is down.

> [!WARNING]
> Tasksd is still under development. There could be bugs, API-breaking changes, and any other sort of instability.

## Why it exists

A few reasons:
- I was curious to try applying the idea of LSP to task running.
- I didn't like any existing Neovim code runners and I wanted to shift as much logic as possible from Lua to Rust

## Installation

Requirements:
- Linux or macOS (Windows is not supported)
- Maybe the latest stable Rust toolchain

For now cargo is the easiest way to install tasksd:

```bash
cargo install --git https://github.com/kuznetsss/tasksd
```

## Usage

Only one parameter - unix socket path is required to start tasksd, e.g.:
```shell
tasksd --unix-socket-path /tmp/tasksd_socket
```
Once tasksd is started it will listen for unix socket connections.
See [API doc](docs/API.md) on how to interact with tasksd.

Press `Ctrl-C` or send `SIGINT` to start graceful shutdown (shutting down all the running tasks before exiting tasksd itself).
Second `Ctrl-C` (or `SIGINT`) will force tasksd to exit immediately.

Use `--help` flag to see all the available options.

## Features

- tasksd is a daemon - tasks keep running after client disconnects
- PTY is allocated for each task - spawned command sees a real terminal
- output capture - each task output is captured into a ring buffer (by default tasksd keeps last 10 000 lines)
- streaming JSON-RPC API - clients subscribe to live output and exit notifications over a unix socket

## JSON-RPC API

API is documented in [docs/API.md](docs/API.md).

## Roadmap

`0.2.0`:
- [x] Separate task not found and task already exited errors
- [x] transport::Connection refactoring: Connection should have it's internal cancellation token and method stop()
- [ ] **BUG**: pty output couldn't be divided into chunks:
    - [x] Move task subscribers into session
        - Use joinset in task.rs
        - Remove unused modules
        - Think if WrappedTaskTracker needs a trait so handler can only spawn
    - [ ] In the current task piped (tokio's native stdout/stderr) outputs instead of pty
    - [ ] Optional: implement different task type PtyTask:
          - It should render screen from stream of bytes from pty using (libghostty-vt or vt100)
          - Share screen state via watch channel
          - Each subscriber calculates diff and sends it to the client
- [ ] Encode output lines with base64
- [ ] Add line number to output notification
- [ ] Add notifications about missed output
- [ ] Query task output buffer for line range
- [ ] Subscription control (subscribe on output/exit, unsubscribe)
- [ ] Shutdown API method

`0.3.0`:
- [ ] Broadcast shutdown notification to all connections
- [ ] Tasks chains
- [ ] Limit log file size
- [ ] Support graceful shutdown by `SIGTERM`
- [ ] Add a parameter to adjust RecentFinishedTasks size

Future ideas:
- Add suggestion module (history, runnables, tasks.json)
- Output search/filter
- TCP sockets support
- No pty tasks

## Acknowledgments

- [pty-process](https://docs.rs/pty-process/latest/pty_process/) crate for an example of how to open ptys using [rustix](https://docs.rs/rustix/latest/rustix/)
