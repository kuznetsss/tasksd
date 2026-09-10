# Tasksd

[![CI status](https://github.com/kuznetsss/tasksd/actions/workflows/ci.yml/badge.svg)](https://github.com/kuznetsss/tasksd/actions/workflows/ci.yml)
[![Crates audit](https://github.com/kuznetsss/tasksd/actions/workflows/audit.yml/badge.svg)](https://github.com/kuznetsss/tasksd/actions/workflows/audit.yml)
[![Test coverage](https://codecov.io/gh/kuznetsss/tasksd/graph/badge.svg?token=NBUAOGLWUH)](https://codecov.io/gh/kuznetsss/tasksd)

Tasksd is a daemon providing JSON-RPC API to spawn processes.
As a daemon it detaches execution from the client allowing the spawned process to run even if the client is down.

It is created as a companion for Neovim but it doesn't have anything specific for Neovim. Tasksd can be used as a general purpose terminal multiplexer with an API.

> [!WARNING]
> Tasksd is still under development. There could be bugs, API-breaking changes, and any other sort of instability.

## Installation

Requirements:
- Linux or macOS (Windows is not supported)
- Rust 1.97 or newer

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
- (Not implemented yet) PTY is allocated for each task - spawned command sees a real terminal
- output capture - each task output is captured into a ring buffer (by default tasksd keeps last 10 000 lines)
- streaming JSON-RPC API - clients subscribe to live output and exit notifications over a unix socket
- list of tasks - clients can get a list of running or recently finished processes

## JSON-RPC API

API is documented in [docs/API.md](docs/API.md).

## Why it exists

A few reasons:
- I was curious to try applying the idea of LSP to task running
- I didn't like any existing Neovim code runners and I wanted to shift as much logic as possible from Lua to Rust

## Roadmap

`0.3.0`:
- [x] starting task with an invalid working dir returns:
    `could not start `true`: Error starting child process: No such file or directory (os error 2)`
- [x] `task.info` API: get a single entry of `tasks.list` by task id
      - Unify `task.list` and `task.info`: list entry should be task info + status (running or finished)
- [ ] `task.exit` notification is sent before task is moved out of running map:
      - add a gate to the completion coroutine, when task is moved, gate opens
      - add wait for the gate in subscriber: if it gets an exit event it waits for the gate
- [ ] in `task.get_output` the parameter `from_line` should become optional:
      if it is not provided return the last `lines_number`
- [ ] `task.subscribe` should provide option `output` and by default only subscribe on exit event
- [ ] shutdown period cli option - when to shutdown if there are no tasks running and no clients connected
- [ ] Support graceful shutdown by `SIGTERM`
- [ ] Switch output stream to Vec<u8>
- [ ] Rearrange integration tests: one `it` (as integration tests) module containing common and all the tests
- [ ] Use `thiserror` crate
- [ ] Add a parameter to adjust RecentFinishedTasks size

`0.4.0`:
- [ ] Implement different task type PtyTask:
      - It should render screen from stream of bytes from pty using (libghostty-vt or vt100)
      - Share screen state via watch channel
      - Each subscriber calculates diff and sends it to the client
- [ ] For piped task reading should be similar to pty_reader:
      after child process finished drain buffers until got blocking and exit.
      This will prevent tasksd from hanging on detached grand child processes
      but it will stop capturing detached process' output
- [ ] Separate stdout and stderr in output notifications and in `OutputBuffer`

Future ideas:
- Tasks chains
- Limit log file size
- Add suggestion module (history, runnables, tasks.json)
- Output search/filter
- TCP sockets support

## Acknowledgments

- [pty-process](https://docs.rs/pty-process/latest/pty_process/) crate for an example of how to open ptys using [rustix](https://docs.rs/rustix/latest/rustix/)
