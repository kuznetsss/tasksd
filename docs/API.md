# Tasksd JSON-RPC API

Tasksd speaks [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over the unix
socket it is started with. The client sends **requests** and receives
**responses**; the server pushes **notifications** about running tasks back over
the same connection.

By default a task's output and exit notifications are delivered to the
connection that started it. Any connection can also subscribe to a task's output
with `task.subscribe`, even one it did not start or whose starting connection
has since disconnected.

## Message framing

Messages are framed the same way as the Language Server Protocol: a
`Content-Length` header, a blank line, then the JSON payload. Header lines end
with `\r\n`.

```
Content-Length: 76\r\n
\r\n
{"jsonrpc":"2.0","id":1,"method":"task.start","params":{"executable":"ls"}}
```

`Content-Length` is the number of bytes of the JSON payload (not counting the
header or the separating blank line).

## Conventions

- `jsonrpc` is always `"2.0"`. Any other value is rejected.
- Request `id` may be a string or a number and is echoed back on the response.
- `task_id` is an unsigned integer assigned by the server when a task starts.
- All fields are required unless marked optional.
- Method names are scoped by subject: a `task.` prefix means the message is
  about one task, and a bare name means it is about the daemon or the
  connection itself. Every method name is unique across requests and
  notifications, so `method` alone identifies a message.

---

## Requests

### `hello`

Liveness and version check. A client can send this at any time to confirm that
a compatible, running tasksd is behind the socket: the client identifies itself
and the server replies with its own version. It has no side effects on tasks and
is purely informational.

**Params**

| Field            | Type   | Required | Description                        |
| ---------------- | ------ | -------- | ---------------------------------- |
| `client_name`    | string | yes      | Name of the connecting client.     |
| `client_version` | string | yes      | Version of the connecting client.  |

**Result**

| Field            | Type   | Description                            |
| ---------------- | ------ | -------------------------------------- |
| `server_version` | string | Version of the running tasksd daemon.  |

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "hello",
  "params": { "client_name": "nvim-tasksd", "client_version": "0.1.0" }
}

// ← response
{ "jsonrpc": "2.0", "id": 1, "result": { "server_version": "0.2.0" } }
```

### `task.start`

Start a new task. The executable is run inside a PTY.

**Params**

| Field                  | Type       | Required | Default | Description                                        |
| ---------------------- | ---------- | -------- | ------- | -------------------------------------------------- |
| `executable`           | string     | yes      |         | Program to run.                                    |
| `args`                 | string[]   | no       | `[]`    | Arguments passed to the executable.                |
| `working_dir`          | string     | no       | cwd     | Directory to run the task in.                      |
| `subscribe_to_output`  | bool       | no       | `true`  | Whether to receive `task.output` notifications.    |

**Result**

| Field     | Type    | Description                |
| --------- | ------- | -------------------------- |
| `task_id` | integer | Id of the started task.    |

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "task.start",
  "params": {
    "executable": "ls",
    "args": ["-la"],
    "working_dir": "/tmp",
    "subscribe_to_output": true
  }
}

// ← response
{ "jsonrpc": "2.0", "id": 1, "result": { "task_id": 1 } }
```

Regardless of `subscribe_to_output`, the client always receives the
`task.exit` notification for tasks it started.

### `task.send_signal`

Send a unix signal to a running task.

**Params**

| Field     | Type    | Required | Description                                            |
| --------- | ------- | -------- | ------------------------------------------------------ |
| `task_id` | integer | yes      | Id of the task to signal.                              |
| `signal`  | integer | yes      | Signal number (e.g. `9` for `SIGKILL`, `15` `SIGTERM`).|

`signal` must be a valid signal number for the platform; otherwise the request
is rejected with [`-32602` Invalid params](#standard-json-rpc-errors).

The signal is delivered to the task's whole process group, not just its main
process, so any child processes the task spawned are signalled too.

Delivery is attempted as long as the process group still exists in the kernel,
even after the task's main process has exited. This matters when the task's
main process forks children that outlive it (for example a shell that spawned a
still-running grandchild): those lingering group members can still be signalled.
The request is only rejected with [`5` The task has already exited](#task-errors)
when the kernel reports that the process group no longer exists (the task and all
its children are gone). Signalling a task that does not exist is rejected with
[`7` Task not found](#task-errors); any other delivery failure is rejected with
[`6` Error sending signal to the task](#task-errors).

**Result**

An empty object `{}`.

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "task.send_signal",
  "params": { "task_id": 1, "signal": 15 }
}

// ← response
{ "jsonrpc": "2.0", "id": 2, "result": {} }
```

### `task.get_output`

Query a range of buffered output lines for a task. Works for both running and
finished tasks, as long as the task still exists on the server.

The server keeps only the most recent lines of each task's output in a bounded
buffer, so older lines may no longer be available. The returned range is the
half-open interval `[from_line, from_line + lines_number)`, intersected with the
lines currently held in the buffer. Requesting a range that lies entirely
outside the buffer (or `lines_number` of `0`) yields an empty `lines` array
rather than an error.

**Params**

| Field          | Type    | Required | Description                                          |
| -------------- | ------- | -------- | ---------------------------------------------------- |
| `task_id`      | integer | yes      | Id of the task to read output from.                  |
| `from_line`    | integer | yes      | Zero-based index of the first line to return.        |
| `lines_number` | integer | yes      | Maximum number of lines to return from `from_line`.  |

**Result**

| Field     | Type      | Description                                  |
| --------- | --------- | -------------------------------------------- |
| `task_id` | integer   | Id of the queried task.                      |
| `lines`   | object[]  | Output lines in the requested range.         |

Each entry in `lines` has the same shape as a `task.output` notification's
payload:

| Field         | Type    | Description                                        |
| ------------- | ------- | -------------------------------------------------- |
| `line`        | string  | A single line of output.                           |
| `line_number` | integer | Zero-based index of the line in the task's output. |

Requesting a `task_id` that does not exist is rejected with
[`7` Task not found](#task-errors).

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "task.get_output",
  "params": { "task_id": 1, "from_line": 1, "lines_number": 2 }
}

// ← response
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "task_id": 1,
    "lines": [
      { "line": "line 2\n", "line_number": 1 },
      { "line": "line 3\n", "line_number": 2 }
    ]
  }
}
```

### `task.subscribe`

Start receiving `task.output` (and `task.missed_output`) notifications for a
task. This works for a task started with `subscribe_to_output` set to `false`,
a task whose output was paused with `task.unsubscribe`, and a task started by a
different connection (even one that has since disconnected) — any running task
can be subscribed to by its `task_id`.

Output line numbers continue from the task's current position, not from where
the subscription began.

Subscribing to a task that is already subscribed just (re)enables output
delivery.

**Params**

| Field     | Type    | Required | Description                          |
| --------- | ------- | -------- | ------------------------------------ |
| `task_id` | integer | yes      | Id of the task to subscribe to.      |

**Result**

An empty object `{}`.

Subscribing to a task that does not exist is rejected with
[`7` Task not found](#task-errors); subscribing to a task that has already
exited is rejected with [`5` The task has already exited](#task-errors).

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "task.subscribe",
  "params": { "task_id": 1 }
}

// ← response
{ "jsonrpc": "2.0", "id": 4, "result": {} }
```

### `task.unsubscribe`

Stop receiving `task.output` / `task.missed_output` notifications for a task.
The task keeps running and its `task.exit` notification is still delivered.

**Params**

| Field     | Type    | Required | Description                          |
| --------- | ------- | -------- | ------------------------------------ |
| `task_id` | integer | yes      | Id of the task to unsubscribe from.  |

**Result**

An empty object `{}`.

Unsubscribing from a task with no active subscription is rejected with
[`7` Task not found](#task-errors).

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "task.unsubscribe",
  "params": { "task_id": 1 }
}

// ← response
{ "jsonrpc": "2.0", "id": 5, "result": {} }
```

### `task.send_input`

Write input to a running task's stdin. The bytes are sent verbatim, so include
a trailing newline yourself if the task expects one.

**Params**

| Field     | Type    | Required | Description                          |
| --------- | ------- | -------- | ------------------------------------ |
| `task_id` | integer | yes      | Id of the task to send input to.     |
| `input`   | string  | yes      | Text written to the task's stdin.    |

**Result**

An empty object `{}`.

Sending input to a task that does not exist is rejected with
[`7` Task not found](#task-errors); sending to a task that has already exited is
rejected with [`5` The task has already exited](#task-errors); a failure while
writing to stdin is rejected with [`4` Error writing to process](#task-errors).

**Example**

```json
// → request
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "task.send_input",
  "params": { "task_id": 1, "input": "yes\n" }
}

// ← response
{ "jsonrpc": "2.0", "id": 6, "result": {} }
```

### `task.list`

List the tasks the daemon knows about. The listing is daemon-wide, not scoped
to the calling connection: it includes tasks started by other connections and
tasks whose starting connection has since disconnected.

**Params**

None. `params` may be omitted, `null`, or an empty object `{}`. Any other value
is rejected with [`-32602` Invalid params](#standard-json-rpc-errors).

**Result**

| Field   | Type   | Description                          |
| ------- | ------ | ------------------------------------ |
| `tasks` | object | The task listing, split by state.    |

`tasks` has two arrays:

| Field      | Type     | Description                                         |
| ---------- | -------- | --------------------------------------------------- |
| `running`  | object[] | Tasks that have not exited yet.                     |
| `finished` | object[] | Tasks that have exited and are still remembered.    |

Each entry in either array has the same shape:

| Field  | Type    | Description                       |
| ------ | ------- | --------------------------------- |
| `id`   | integer | Id of the task.                   |
| `info` | object  | How the task was started.         |

`info` describes the task as the server resolved it when it started, not the
raw `task.start` params:

| Field         | Type     | Description                                                       |
| ------------- | -------- | ----------------------------------------------------------------- |
| `executable`  | string   | Program the task runs.                                            |
| `args`        | string[] | Arguments it was started with; empty if none were given.          |
| `working_dir` | string   | Directory it runs in, resolved to the daemon's cwd if omitted.    |

The server only remembers the 100 most recently finished tasks, so `finished`
is a bounded window: older tasks fall out of it and stop appearing here (a
`task_id` that has aged out is also [`7` Task not found](#task-errors) for the
other methods). `finished` is ordered oldest first; the order of `running` is
unspecified and may differ between calls.

A task is moved from `running` to `finished` shortly after it terminates, and
that move is not synchronised with the [`task.exit`](#taskexit) notification. A
`task.list` issued immediately after receiving `task.exit` for a task may still
report it under `running`; it appears under `finished` on a later call. Clients
that need to observe the transition should re-request rather than assume the
notification has already taken effect.

Both arrays may be empty. A daemon with no tasks at all still answers with a
result rather than an error.

This method reports no errors of its own.

**Example**

```json
// → request
{ "jsonrpc": "2.0", "id": 7, "method": "task.list" }

// ← response
{
  "jsonrpc": "2.0",
  "id": 7,
  "result": {
    "tasks": {
      "running": [
        {
          "id": 2,
          "info": { "executable": "cat", "args": [], "working_dir": "/tmp" }
        }
      ],
      "finished": [
        {
          "id": 1,
          "info": { "executable": "ls", "args": ["-la"], "working_dir": "/tmp" }
        }
      ]
    }
  }
}
```

### `shutdown`

Shut down the daemon. This is not scoped to the calling connection: every task
is terminated and every client is disconnected.

**Params**

None. `params` may be omitted, `null`, or an empty object `{}`. Any other value
is rejected with [`-32602` Invalid params](#standard-json-rpc-errors).

**Result**

An empty object `{}`.

The response is always delivered before the daemon exits, so a client can rely
on receiving it. The daemon then:

1. Sends `SIGTERM` to every running task.
2. Waits a graceful period (the daemon's `--graceful-period`, in seconds) for
   tasks to exit on their own, then sends `SIGKILL` to the survivors.
3. Sends a `task.exit` notification for each task the connection is subscribed
   to.
4. Sends a [`shutting_down`](#shutting_down) notification, after every
   `task.exit` above.
5. Closes the connection, ending the stream.

As with every other method, the response is not ordered against notifications:
a `task.exit` for a task killed by the shutdown may arrive before or after the
`shutdown` response. Only the EOF is guaranteed to come last.

A client should treat the EOF after a `shutdown` response as a normal
disconnect rather than an error. Sending `shutdown` while a shutdown is already
in progress neither restarts nor extends the sequence, and may not be answered
at all, since the connection is already closing.

**Example**

```json
// → request
{ "jsonrpc": "2.0", "id": 8, "method": "shutdown" }

// ← response
{ "jsonrpc": "2.0", "id": 8, "result": {} }
```

---

## Notifications

Notifications are sent from the server to the client and have no `id`.

### `task.output`

Emitted for each line of output a subscribed task produces.

| Field         | Type    | Description                                          |
| ------------- | ------- | ---------------------------------------------------- |
| `task_id`     | integer | Task that produced the output.                       |
| `line`        | string  | A single line of output.                             |
| `line_number` | integer | Zero-based index of the line in the task's output.   |

```json
{
  "jsonrpc": "2.0",
  "method": "task.output",
  "params": { "task_id": 1, "line": "total 0", "line_number": 0 }
}
```

### `task.missed_output`

Emitted when a subscribed task produces output faster than the client consumes
it and some lines are dropped. It reports the gap so the client knows its view
of the output is incomplete. Like `task.output`, it is only sent when the task
was started with `subscribe_to_output` set to `true`.

| Field       | Type    | Description                                                    |
| ----------- | ------- | ------------------------------------------------------------- |
| `task_id`   | integer | Task whose output was dropped.                                |
| `from_line` | integer | Zero-based index of the first dropped line.                   |
| `missed`    | integer | Number of consecutive lines dropped starting at `from_line`.  |

```json
{
  "jsonrpc": "2.0",
  "method": "task.missed_output",
  "params": { "task_id": 1, "from_line": 512, "missed": 128 }
}
```

### `task.exit`

Emitted once when a task terminates.

| Field       | Type            | Description                                              |
| ----------- | --------------- | ------------------------------------------------------- |
| `task_id`   | integer         | Task that exited.                                       |
| `exit_code` | integer \| null | Exit code, or `null` if the task was killed by a signal.|
| `signal`    | integer \| null | Terminating signal, or `null` if it exited normally.    |

```json
{
  "jsonrpc": "2.0",
  "method": "task.exit",
  "params": { "task_id": 1, "exit_code": 0, "signal": null }
}
```

Receiving this notification does not guarantee that [`task.list`](#tasklist)
already reports the task under `finished`; see that method for details.

### `shutting_down`

Emitted once per connection when the daemon is terminating, whatever started
the shutdown: a [`shutdown`](#shutdown) request from this connection, a
`shutdown` request from a different connection, or a signal sent to the daemon
itself. Every connected client receives it, and it lets a client tell a
deliberate daemon shutdown apart from a crash or a dropped socket.

It carries no `params`.

It is the last message on the connection, sent after every `task.exit` the
connection is owed, and is followed only by the EOF.

Delivery is best-effort: it is dropped if the daemon has to force its way
through a stuck shutdown, and it can be lost in flight like any other message
if the connection breaks. A client should treat it as an explanation of the
disconnect, not as the disconnect signal itself — **the EOF remains the
authoritative end of the stream**, and a client must not wait for
`shutting_down` before closing.

```json
{
  "jsonrpc": "2.0",
  "method": "shutting_down"
}
```

---

## Errors

Error responses follow the JSON-RPC error object shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": { "code": -32602, "message": "Invalid params", "data": "..." }
}
```

`data` is an optional string with extra context. When a request cannot be
parsed well enough to recover its `id`, `id` is `null`.

### Standard JSON-RPC errors

| Code     | Message          | When                                            |
| -------- | ---------------- | ----------------------------------------------- |
| `-32700` | Invalid JSON     | The payload is not valid JSON.                  |
| `-32600` | Invalid Request  | The JSON is not a valid JSON-RPC request.       |
| `-32601` | Method not found | Unknown `method`.                               |
| `-32602` | Invalid params   | `params` are missing or of the wrong type.      |
| `-32603` | Internal error   | Unexpected server-side error.                   |

### Task errors

Application-defined errors returned by task methods:

| Code | Message                          | When                                          |
| ---- | -------------------------------- | --------------------------------------------- |
| `1`  | Invalid working directory        | `working_dir` does not exist / is not usable. |
| `2`  | Error creating a new pty         | The server failed to allocate a PTY.          |
| `3`  | Error starting child process     | The executable could not be spawned.          |
| `4`  | Error writing to process         | Writing to the task's stdin failed.           |
| `5`  | The task has already exited      | The target task has finished.                 |
| `6`  | Error sending signal to the task | The signal could not be delivered.            |
| `7`  | Task not found                   | No task exists with the given `task_id`.      |
