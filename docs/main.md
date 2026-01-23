# Tasksd

## Idea

Create an application with LSP-like interface utilizing stdin and stdout to communicate
with an editor and allowing to run any process or shell command in background.
The application will do extra parsing of output of each process and provide some metadata or work as LSP for output window.
First target editor is Neovim. There will be a Lua companion plugin to work with the application.


## Proof of concept

Minimal functionality to cover in a proof of concept:
- start, stop tasks
- get the list of running tasks
- subscribe/unsubscribe to the output of a running task
- get full output of a running task (history buffer)
- task lifecycle notifications (started, stopped, crashed)

Out of scope for PoC but planned:
- jump on a file path mentioned in the output
- output parsing and filtering
- persistent tasksd per working directory


## Design

### Architecture

**Communication Protocol:**
- Protocol level: LSP style JSON-RPC compatible with `vim.lsp.rpc` low level stack.
  Which means content length header + json rpc message
- Transport level: Unix domain sockets

**Runtime:**
- Tokio async runtime
- `tokio::process::Command` for spawning tasks
- Tokio's async stdin/stdout abstractions (blocking internally, async for user code)

### Components

**API Layer:**
- Tower-based service running on stdin/stdout
- JSON-RPC API definitions

**Core Layer:**
- **Task**: Running process with metadata
  - Unique task ID (auto-generated)
  - Process handle
  - Working directory
  - Custom environment variables
  - Output ring buffer (in-memory, limited number of lines per task)
  - Start time, exit code (when finished)
  - Command that was run

- **TaskManager**: Central coordinator
  - Creates new tasks with specified working directory and environment
  - Maintains registry of all tasks (running and completed)
  - Finds tasks by ID
  - Stops tasks (graceful shutdown strategy: SIGTERM with timeout, then SIGKILL)
  - Broadcasts task lifecycle events (started, stopped, crashed)

**Output Management:**
- In-memory ring buffer per task (similar to terminal scrollback)
- Configurable maximum lines per task (old output dropped when limit exceeded)
- Two modes of access:
  1. Subscribe: stream new output as it arrives
  2. Get history: retrieve current buffer contents
- Subscription model:
  - Multiple clients can subscribe to same task
  - Subscribers receive only new output after subscription
  - Backpressure handling: if client can't keep up, runs in separate coroutine from history buffer

**Process Management:**
- Tasks can crash/restart → clients receive notification
- Each task has isolated working directory for relative path resolution
- Process groups: not in PoC (future work)
- Zombie processes: edge case, not critical for PoC
- PTY vs pipes: to be determined during implementation (PTY likely needed for colored output)

### Future Features (Post-PoC)

**LSP for Output Window:**
- Colored output with ANSI escape code support
- Clickable file paths (goto definition style)
- Severity-based filtering (errors, warnings, info)
- Pluggable parser architecture:
  - Generic file path parser (regex-based, language-agnostic)
  - Language-specific parsers (Rust compiler, pytest, npm, etc.)
  - Real-time parsing as output streams

**Persistent Daemon:**
- Per-working-directory tasksd instance
- Survives editor restarts
- Socket-based communication instead of stdin/stdout
- Task persistence across sessions


## API Methods (Draft)

**Task Management:**
- `task/start` - Start a new task with command, working_dir, env
- `task/stop` - Stop a running task by ID
- `task/list` - Get list of all tasks with metadata
- `task/get` - Get detailed info about a specific task

**Output Access:**
- `output/subscribe` - Subscribe to task output stream
- `output/unsubscribe` - Unsubscribe from task output
- `output/get_history` - Get full buffered output

**Notifications (server→client):**
- `task/started` - Task began execution
- `task/stopped` - Task exited (includes exit code)
- `task/crashed` - Task terminated unexpectedly
- `output/data` - New output data for subscribed tasks
