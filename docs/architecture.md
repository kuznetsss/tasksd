# Architecture Design

## Overview

Tasksd follows a layered architecture with clear separation of concerns between transport, protocol, and business logic.

## Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                        Server                            │
│  (accepts connections, spawns handler per connection)   │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                   Transport Layer                        │
│   - Connection (AsyncRead/AsyncWrite)                   │
│   - BackgroundLineWriter (async write infrastructure)   │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                     API Layer (LSP)                      │
│   - LspMessageReader/Writer (protocol format)           │
│   - Request/Response/Notification structures            │
│   - JSON serialization/deserialization                  │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                    Core/Domain Layer                     │
│   - TaskManager (manages all tasks)                     │
│   - Task (process lifecycle, stdio pipes)               │
│   - Business logic (task commands, state)               │
└─────────────────────────────────────────────────────────┘
```

## Module Structure

### Transport Module (currently `server`)

**Responsibility:** Low-level network and I/O handling

**Components:**
- `Server`: Creates and accepts connections
- `Connection`: Wrapper around `AsyncRead`/`AsyncWrite`
- `BackgroundLineWriter`: Async infrastructure for writing with cancellation support
- Basic read/write primitives

**Key design decisions:**
- Protocol-agnostic - doesn't know about LSP message format
- Provides primitive operations: `read_line()`, `read_exact(n)`, `write()`
- Handles async I/O concerns: buffering, cancellation, error propagation

### API Module (LSP Protocol)

**Responsibility:** LSP message format and protocol handling

**Components:**
- `LspMessageReader`: Reads LSP messages (Content-Length header + JSON body)
  - Parses `Content-Length: X\r\n\r\n` header
  - Reads exactly X bytes for JSON body
  - Deserializes to Request/Response structures
- `LspMessageWriter`: Writes LSP messages
  - Constructs Content-Length header
  - Serializes Request/Response to JSON
  - Writes full message via transport layer
- `Request`/`Response`/`Notification`: Type-safe message structures
- JSON-RPC 2.0 implementation

**Key design decisions:**
- Decoupled from transport - works with any `AsyncRead`/`AsyncWrite`
- Knows about LSP/JSON-RPC format but not about task management
- Can be tested independently with mock I/O

### Core Module (Domain Logic)

**Responsibility:** Task management and business logic

**Components:**
- `TaskManager`: Central coordinator for all tasks
  - Accepts commands via channels
  - Maintains task registry
  - Broadcasts lifecycle events
  - Manages task state transitions
- `Task`: Individual process wrapper
  - Process handle and metadata
  - Output buffer management
  - Lifecycle tracking
- Command/response types specific to task operations

**Key design decisions:**
- Protocol-agnostic - can work with any API layer (LSP, HTTP, etc.)
- Uses channels for communication (decoupled from network I/O)
- Single source of truth for task state

## Connection Handler Workflow

```
Connection → LSP Handler Coroutine
  │
  ├─ Read LSP Request (via LspMessageReader)
  │
  ├─ Send command to TaskManager via channel
  │    (includes oneshot channel for response)
  │
  ├─ TaskManager processes command
  │    └─ Creates/manages Task
  │    └─ Sends response back via oneshot channel
  │
  ├─ Receive response from TaskManager
  │
  └─ Write LSP Response (via LspMessageWriter)
```

### Per-connection Handler Pseudocode

```rust
async fn handle_connection(
    reader: impl AsyncRead,
    writer: impl AsyncWrite,
    task_manager_tx: Sender<Command>,
) {
    let mut lsp_reader = LspMessageReader::new(reader);
    let mut lsp_writer = LspMessageWriter::new(writer);
    
    loop {
        // Read LSP request
        let request = lsp_reader.read_request().await?;
        
        // Create oneshot channel for response
        let (response_tx, response_rx) = oneshot::channel();
        
        // Send command to TaskManager
        let command = Command::from_request(request, response_tx);
        task_manager_tx.send(command).await?;
        
        // Wait for response from TaskManager
        let result = response_rx.await?;
        
        // Write LSP response
        let response = Response::from_result(request.id, result);
        lsp_writer.write_response(response).await?;
    }
}
```

## Design Benefits

1. **Separation of Concerns**: Each layer has a single, well-defined responsibility
2. **Testability**: Layers can be tested independently with mocks
3. **Reusability**: Transport and protocol layers can be reused for other applications
4. **Maintainability**: Changes to protocol don't affect task management and vice versa
5. **Concurrency**: Multiple connections can communicate with shared TaskManager
6. **Extensibility**: Easy to add new transports (HTTP, WebSocket) or protocols

## Open Design Questions

### 1. Bi-directional Communication (Task Notifications)

**Problem:** Tasks need to send notifications back to clients (e.g., output data, status changes, crashes).

**Options:**

**A) Broadcast Channel (Pub/Sub)**
```rust
// TaskManager broadcasts all events
let (event_tx, _) = broadcast::channel(100);

// Each connection handler subscribes
let mut event_rx = event_tx.subscribe();

// Handler filters events for tasks it cares about
while let Ok(event) = event_rx.recv().await {
    if subscribed_to(event.task_id) {
        send_notification(event).await;
    }
}
```
- ✅ Simple, decoupled
- ✅ Multiple clients can receive same events
- ❌ Each client receives all events (filtering required)
- ❌ Memory overhead for broadcast buffer

**B) Per-Connection Registration**
```rust
// Connection registers a callback/channel with TaskManager
task_manager.register_subscriber(connection_id, callback_tx).await;

// TaskManager sends notifications directly to interested parties
for subscriber in task.subscribers {
    subscriber.send(notification).await;
}
```
- ✅ Efficient - only interested parties receive events
- ✅ TaskManager knows which clients care about which tasks
- ❌ More complex state management
- ❌ TaskManager coupled to connection lifecycle

**C) Bidirectional Channel per Request**
```rust
// Command includes a channel for ongoing notifications
struct Command {
    kind: CommandKind,
    response_tx: oneshot::Sender<Response>,
    notifications_tx: mpsc::Sender<Notification>, // NEW
}
```
- ✅ Natural pairing of request with ongoing updates
- ❌ Doesn't work well for unsolicited notifications
- ❌ Awkward for subscribe/unsubscribe pattern

**Current Recommendation:** Start with **Option A (Broadcast Channel)** for PoC simplicity. Can optimize to Option B later if performance becomes an issue.

### 2. Task Output Streaming

**Problem:** How to handle backpressure when client can't keep up with task output?

**Options:**
- Drop old messages (lossy)
- Block task output collection (not ideal)
- Disconnect slow clients
- Bounded buffer per subscriber with configurable behavior

**Current Recommendation:** Bounded buffer per subscriber with drop-oldest strategy for PoC.

### 3. Connection Lifecycle and Cleanup

**Problem:** What happens when a client disconnects?
- Should subscriptions be automatically cleaned up?
- Should tasks keep running?
- How to handle reconnection to same tasks?

**Current Approach (from main.md):**
- Tasks continue running after disconnect (daemon-like)
- Subscriptions are per-connection and cleaned up on disconnect
- Clients can re-subscribe after reconnect (tasks have IDs)

## Implementation Phases

### Phase 1: Core + Basic Transport (Current)
- ✅ Basic transport with `BackgroundLineWriter`
- ⏳ Refactor to separate protocol from I/O
- ⏳ Implement `LspMessageReader`/`LspMessageWriter`

### Phase 2: Core Layer
- Implement `Task` with process spawning
- Implement `TaskManager` with command channel
- Basic task lifecycle (start, stop, list)

### Phase 3: Integration
- Wire up LSP handler → TaskManager flow
- Implement broadcast channel for notifications
- End-to-end testing

### Phase 4: Output Management
- Ring buffer per task
- Subscribe/unsubscribe
- History retrieval

### Phase 5: Polish & PoC Completion
- Error handling refinement
- Graceful shutdown
- Documentation and examples
