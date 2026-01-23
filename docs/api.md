# API

Tasksd is using [JSON-RPC 2.0](https://www.jsonrpc.org/specification) for communication.

## Request

Request is an object sent from client to server.
Request object should have the following fields:
- `jsonrpc`: string
  Must be `2.0` according to JSON-RPC 2.0.
- `method`: string
- `params`: object - optional
- `id`: number or string

Request without `id` is a notification and should not be replied.


## Response

Response is an object sent from server to client after request.
Request object should have the following fields:
- `jsonrpc`: string
  Must be `2.0` according to JSON-RPC 2.0.
- `result`: object - required on success
- `error`: object - required on failure
    - `code`: number
    - `message`: string
    - `data`: object optional
- `id`: number or string - same as in the request


## List of API:

### task/start
Start task.

Request:
- `method`: "task/start"
- `params`:
    - `working_dir`: string optional
      Working dir of starting process. Working directory of tasksd itself if not specified.
    - `executable`: string
      Executable name or absolute path or relative path of the executable to launch.
    - `args`: array of strings optional
      Array of arguments of the executable to launch. Empty by default.

Response:
- `result`:
    - `id`: number
      The id of the started task (probably PID of the proccess will be used).

### task/stop
Stop task.

Request:
- `method`: "task/stop"
- `params`:
    - id: number - task id to stop

Response:
- `result`: true

### task/list
List of the running tasks.

Request:
- `method`: "task/stop"
- `params`:
    - show_executables: bool - whether to include
    - show_workdirs: number - task id to stop

Response:
- `result`: true
