# Design: output modes + subscription ownership

Working notes for an in-progress redesign. Status markers: **DECIDED** / **LEANING** / **OPEN**.
Nothing here is implemented yet. `file:line` refs are point-in-time and will drift.

---

## 1. The problem that started this

Every task currently spawns a **PTY** and reads it with `BufReader::read_line`
(`src/tasks/task.rs` `spawn_output_reading`), storing each "line" in a line-indexed
`OutputBuffer`. That's a category error for interactive programs: a PTY carries a
terminal **protocol** (cursor moves, `\r` overwrites, SGR, full-screen redraws), not a
sequence of log lines. `read_line` over vim/htop produces huge, meaningless
"records" that are just screen-repaint fragments, indexed by a line number that means
nothing. A terminal stream is also **not seekable** — byte position N is meaningless
without having applied bytes 0..N (no clean resync boundary, unlike `\n` in a log).

## 2. Two output modes — **DECIDED**

Split forced at both ends (OS mechanism *and* neovim render path):

### Pipe mode (common case: builds, tests, logs)
- Child sees a non-tty → line-buffered, append-only, position-independent output.
- `reader → OutputBuffer.insert_line` (lossless, line-indexed) **then** lossy
  `broadcast` to live subscribers. This is essentially today's model — it was just
  pointed at the wrong stream. **Keep `OutputBuffer` as-is.**
- Late/lagged subscriber just misses scrollback (resync from `get_line_range`), never
  corruption.
- Client renders into a **normal neovim buffer** (lines, grep, scrollback).

### Pty mode (interactive/TUI) — **deferred, "do it if time"**
- Lossy broadcast is **fatal** here: one dropped chunk desyncs the terminal forever.
- Requires authoritative **state**: a server-side terminal **grid** (the pty's
  equivalent of `OutputBuffer`; bounded by rows×cols, *not* output volume — a `yes`
  loop doesn't grow it).
- `reader → vt100 parser → publish watch<Arc<Screen>>`. **`watch`, not `broadcast`** —
  the grid makes stale frames disposable, and `watch` coalesces to the latest.
- Each client task holds its own `last_sent: Screen` and emits
  `current.contents_diff(&last)` into its own `mpsc`.
- **Why this is the whole payoff** (all fall out of the diff line, no special cases):
  - attach = first diff vs blank screen = full keyframe.
  - lag recovery = diff vs latest grid = **cannot desync**.
  - no client stalls another or the child (reader publishes once and moves on).
- This is literally how **tmux** handles a slow client: child feeds the grid (never
  clients directly), per-client output buffer, redraw-from-grid on lag. It's what makes
  "detach, task keeps running, reattach to a live vim" work — the **daemon
  differentiator**.
- Client renders into a **terminal buffer** (`nvim_open_term` + `chansend`).

### Notes / constraints
- **Single viewer wouldn't need the grid**: a backpressured `mpsc` chained to the
  child's tty (undrained tty blocks the writer = native flow control) is correct and
  tiny. The grid is what unlocks **N non-stalling viewers** + lag recovery + detached
  execution. So "multiple clients" is the argument *for* the grid.
- **Redraw-on-attach shortcut** (poke child with `TIOCSWINSZ`/SIGWINCH to force a
  repaint) is a legit *cheap approximation* for a single local cooperative client, but:
  only fires reliably when size actually changes; app-cooperation-dependent; fixes
  attach but not mid-session lag; leaks a client concern into child state. It's "beg the
  child for a keyframe"; the grid is "mint the keyframe yourself."
- **One-size constraint**: the grid has a single size (= pty winsize). Multi-client at
  different sizes = tmux's "smallest common size" problem. Defer.
- **Library** — **OPEN**: `vt100` gives `contents_formatted`/`contents_diff` for free
  (exactly this API) but is stale; `libghostty-vt` is the in-dev alternative. Pick at
  pty-time; does not affect the pipe refactor.

## 3. Sequencing — **DECIDED**

**Fix pipe first** — it's the actual bug fix, not a warm-up. Pty later / if time.

### Concrete pipe-refactor changes
- `Stdio::piped()` instead of `create_pty_pair`. Drop `PtyReader` + the macOS drain hack
  from this path (keep the files for pty). `ChildStdout`/`ChildStderr` are already
  async-readable → the whole adapter disappears.
- **stdout + stderr are now TWO streams** (PTY merged them). New decision: interleave
  into one line stream, or tag lines by source. Leaning: merge, maybe with a source tag.
  *(This is the first thing you'll hit.)*
- Split `setsid` (keep — process group for signaling the child tree) from
  `ioctl_tiocsctty` + winsize (pty-only, drop). They're fused in `pre_exec` today.
- `read_until(b'\n')` → **bytes payload** (`Arc<[u8]>`/`Bytes`), not `read_line` →
  `String`. Correctness, not just pty-prep: one non-UTF-8 byte currently *kills the
  reader* via `read_line`'s UTF-8 error.
- Report **mode in `TaskInfo`** so the neovim client picks buffer-vs-terminal rendering.

### Prep for pty (do NOT over-build)
- The only real prep is **decoupling process-control** (spawn/pid/signal/exit/wait/join/
  `TaskInfo` — shared) **from output capture+delivery** (disjoint between modes).
- Do **not** scaffold an `OutputMode` enum / shared output abstraction now. The two modes
  share ~nothing on the output side; you control both ends and it's pre-release, so it's
  cheap to add when pty actually lands. Premature shared abstraction will be wrong.

## 4. Subscription ownership refactor — **DECIDED (direction)**

**Move the "pump events → connection" task from `Task` into `application::session`.**

- **Real justification = lifetime, not dedup.** Today subscriber recv-loops live in
  `Task` (`TaskEvents.related_tasks`) but write to a `ConnectionWriter` owned by
  `Session`, and shutdown correctness depends on a *global* ordering enforced in
  `Application::shutdown()` — see the comment at `src/application/session.rs` shutdown
  ("guaranteed by the order in Application::shutdown()"). That cross-boundary
  entanglement is the smell, and almost certainly what made #22 (graceful shutdown)
  fiddly. Pump lifetime should = connection lifetime = session-owned.
- **Correct the dedup framing:** pipe and pty pumps do *not* merge (sources differ:
  `broadcast<line>` vs `watch<Screen>`). What moving to `Session` dedupes is pump
  *lifecycle* (spawn/track/abort), which folds into machinery the session already runs.
  Pump *body* stays per-mode. `subscribe()`'s return type is therefore mode-specific → no
  shared trait method; `Session` dispatches by mode.
- **Mental model:** task = source / fan-out point (owns the `Sender`); session =
  per-connection pump (owns a receiver). Multiple connections → multiple pumps off one
  task's broadcast. Don't let fan-out state drift into the session.
- **What cleanly dies (good YAGNI):** `TaskEvents`, the `TaskEventsSubscriber` trait,
  `application::subscriber::Subscriber`, `TaskSubscriberError::ShouldExit`. The trait's
  two jobs both evaporate — task exposes a bare channel (knows nothing about connections
  = *better* decoupling), and tests read the channel directly instead of implementing
  mock subscribers.
- **Bonus unlocked:** lag handling. Today `Lagged` just warns and *drops*
  (`src/tasks/events.rs`) — the resync-from-`OutputBuffer` was never built. A
  session-owned pipe pump has the task handle, so it can `get_line_range(..)` to backfill
  on lag. (Also: stray `dbg!(&status)` in `events.rs` — moot once deleted.)
- **`subscribe()` returns the channel** (`broadcast::Receiver` for pipe). Preserves
  Output-then-Exit ordering for free (single ordered channel).
- **Builder returns a receiver for early subscribers** — a receiver created at build time
  sees every line from the first (broadcast "no old messages" semantics). So the initial
  subscriber is complete without the gate blocking reads.
- **Gate — OPEN/hypothesis:** `TaskReadingGate` may become removable given
  builder-returns-receiver + lossless `OutputBuffer`. **Verify** against what
  `task_reading_gate_gates_events_sending` actually pins down and the "exit after all
  output" guarantee before deleting — it's load-bearing for ordering.
- **Sequencing:** land this as its own commit on the *current* (still-PTY) code, tests
  green, **before** the pipe swap — otherwise you restructure subscription twice. It also
  retires the #22 shutdown-ordering fragility (re-derive that ordering; expect it to
  collapse).

## 5. Concurrency / ownership mechanics — **DECIDED (direction)**

Context: for the session-owned pump, a *handler* (itself a spawned task) needs to
register a long-lived pump with the session. `Arc<Mutex<JoinSet>>` is the naive answer
and it's actively wrong: `JoinSet` is single-`&mut`-owner; the owner wants
`join_next().await` while handlers want `spawn` — a mutex held across `join_next().await`
blocks all spawning (and a `std::Mutex` can't be held across await at all).

- **Remove `WrappedTaskTracker`.** Over-engineered: 3-variant `PanicHandler` used with one
  variant, *and* one abstraction forced onto two sites with different concurrency shapes.
  Split them:
  - **task.rs → bare `JoinSet`.** Spawning is sequential/single-owner (all in
    `Task::new`). Dropping a `JoinSet` **aborts** its tasks → structured cancellation for
    free. `is_joined()` assertion becomes a small bool/emptiness check.
  - **session → `TaskTracker` + `CancellationToken`.** The handler spawning the pump is
    multi-producer → needs `&self` spawn → `TaskTracker` (a `JoinSet` here just recreates
    the `Arc<Mutex>` trap). NOTE: dropping a `TaskTracker` does **not** stop its tasks
    (they detach) → you must cancel the token explicitly (+ cancel-on-drop if you want).
    This combo is `WrappedTaskTracker` *minus the panic machinery* — that part is the
    irreducible minimum for multi-producer structured concurrency, not over-engineering.
- **Panic policy** (the real decision behind removing `PanicHandler`): dropping
  abort-on-panic is an *improvement* (nuking the whole daemon because one task's reader
  panicked kills every other client). BUT a panicked tokio task is silent by default →
  make panics surface loudly: have `join()` inspect the `JoinError` and log, or set a
  global panic hook. Otherwise you trade "nuke on panic" for "silently dead reader."
- **abort_all vs CancellationToken — prefer the token.** For I/O-bound pumps (await-loops
  on channel-recv / connection-write), cancellation via `run_until_cancelled`/`select!`
  lands at the next await = **as prompt as abort**, plus it allows an orderly goodbye
  (relevant since #22 was about graceful shutdown). `abort_all` is only strictly better
  for tasks that might be stuck *not* at an await (CPU loops) — not your case. Drop
  `abort_all`.
- **Unify the two shutdown paths with a child token.** Give the session
  `app_token.child_token()`; use it for both the pumps and the run loop
  (`run_until_cancelled(read_message)`).
  - App shutdown → parent cancels → cascades to all sessions' children → all pumps stop.
  - Client disconnect → cancel *this* session's child only → its pumps stop, others
    untouched.
  - Collapses today's run-loop-return-then-`abort_all` split into one cooperative
    mechanism.

## 6. Open questions / next steps

- [ ] pipe: interleave vs tag stdout+stderr.
- [ ] pipe: `Bytes` vs `Arc<[u8]>` payload (bias: `Bytes`).
- [ ] Confirm `TaskReadingGate` is removable (test + exit-ordering guarantee).
- [ ] Re-derive graceful-shutdown ordering after moving pumps to session (expect it to
      simplify; check against #22).
- [ ] Panic-surfacing mechanism after dropping `PanicHandler` (JoinError inspection vs
      panic hook).
- [ ] pty (deferred): grid library choice (`vt100` vs `libghostty-vt`); resize/attach
      handshake; parser-task ownership + how `write_to_stdin` interacts with it.

### Suggested commit order
1. Subscription ownership → session (delete `TaskEvents`/trait/`Subscriber`); JoinSet in
   task.rs; TaskTracker+child-token in session; remove `WrappedTaskTracker`. Tests green.
2. Swap pty → pipe reading (bytes payload, stdout/stderr merge, split setsid/ctty).
3. (later) pty mode with the grid.
