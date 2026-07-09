# Tasksd JSON-RPC API

Tasksd speaks [JSON-RPC 2.0](https://www.jsonrpc.org/specification) over the unix
socket it is started with. The client sends **requests** and receives
**responses**; the server pushes **notifications** about running tasks back over
the same connection.

Tasks are tied to the connection that started them: their output and exit
notifications are delivered only to that connection.

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

---

## Requests

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
