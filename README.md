# Sandbox fork

Split a live terminal in two. Each pane is a shell in its own short-lived, isolated
[Chalk](https://chalk.ai) sandbox. Press **Split** and the pane's running sandbox is snapshotted
and restored into a brand-new sandbox, which opens in a second pane beside it. The terminals are
drawn by [ghostty-web](https://github.com/coder/ghostty-web), Ghostty's VT parser compiled to
WebAssembly.

```
                                         ┌─ snapshot ─▶ CreateSandbox(snapshot_id) ─▶ fork
browser (ghostty-web) ──WebSocket──▶ server.py ──PTY session──▶ sandbox (keeps running)
       pane 2 ◀──────────── WebSocket ◀──── PTY session ◀──────────────────────────────┘
```

## What a fork keeps

A fork is restored from a checkpoint of the whole sandbox, memory included, so it starts with:

- **Files:** everything in the file system, exactly as it was at the snapshot.
- **`/tmp`:** it lives in memory, and it comes across too.
- **The live shell:** the same shell, the same scrollback, and whatever was running in it. A Python
  REPL in the middle of a session keeps its variables and even its process ID.
- **The same isolation:** the fork's spec is the source's: kernel policy, network policy, CPU,
  memory, and lifetime.

From the moment it is created, each fork is independent of its source. Change a file or a
variable in one pane and the others never see it.

## Which processes survive a fork

After a restore, gVisor kills every process that was started through `exec`, along with all of
its descendants, because the caller holding its stdin and stdout is gone. That includes
background jobs, `setsid` and `nohup` daemons, and tmux servers started from an exec session.
Only the container's init process (PID 1) and its descendants survive.

So this demo makes PID 1 a tiny spawner (`SPAWNER` in `server.py`). It reads commands from a FIFO
and runs each one as its own child. The visitor's shell lives in a tmux server started that way,
and every pane is just a tmux client attached over a PTY session. In a fork, the client is gone,
but the tmux server and everything under it survive, and a new client attaches to them.

One more rule: a surviving process must not hold a file descriptor from the host. A tmux client
passes its terminal, which is a host PTY, to the tmux server, so the server would hold a host
descriptor. Split therefore detaches the pane's client for the second it takes to snapshot, then
attaches a fresh one.

## Files

| File | Role |
| --- | --- |
| `deploy.py` | Builds the server image (with ghostty-web fetched from npm) and deploys it as a `chalkcompute.ScalingGroup`. |
| `server.py` | FastAPI app: `GET /` serves the page, `WS /ws` hands out a sandbox, relays its tmux session over a PTY, and forks it on request. |
| `index.html` | The split-terminal page. Add `?embed=1` to drop the title when you put it in an iframe. |

## Run it

You need [`uv`](https://docs.astral.sh/uv/) and a Chalk environment with sandboxes and sandbox
snapshots enabled. Snapshots need host-backed sandboxes (`ComputeClass.HOST`). Credentials come
from the SDK’s ambient credentials from `chalk login`. You can also set `CHALK_API_SERVER`,
`CHALK_CLIENT_ID`, `CHALK_CLIENT_SECRET` and `CHALK_ENVIRONMENT_ID` yourself.

```bash
./deploy.py
```

Open the printed `web_url`, press **Launch sandbox**, make some state, and press **Split**.

The deployed server creates, snapshots, and forks sandboxes with its own workload identity
(`chalk_identity=True`), so no client secret is baked into the image. The identity acts on behalf
of whoever deployed it, so deploy with credentials that can create sandboxes.

## Forking from your own code

The released `chalkcompute` SDK does not wrap the snapshot API yet, so `server.py` calls it
directly over Connect's JSON protocol, authenticated with the SDK's own credentials. A fork is two
calls:

1. `chalk.sandbox.v2.SandboxSnapshotService/CreateSandboxSnapshot` with `{"sandboxId": ...}`
   checkpoints the running sandbox. The source keeps running.
2. `chalk.sandbox.v2.SandboxService/CreateSandbox` with `{"snapshotId": ...}` boots a new sandbox
   from the snapshot. Call it again to fork the same state again.

See `SnapshotClient` and `fork_sandbox` in `server.py`. The server deletes each snapshot as soon
as its fork is running.

## How a visitor's sandbox is locked down

- **Kernel:** `KernelPolicy.RESTRICTED` runs the sandbox under gVisor.
- **Network:** no `network_policy`, which means no egress at all, not even DNS.
- **Credentials:** none. The sandbox gets no workload identity and no secrets, and neither does a
  fork, because it copies the source's spec.
- **Size:** half a CPU and 1 GiB of memory by default, for every pane.
- **Lifetime:** every sandbox has a hard `lifetime`, so Chalk deletes it even if the server crashes.
- **Single use:** a sandbox serves exactly one pane and is terminated when that pane closes.

## Abuse limits

All of these are environment variables on the ScalingGroup.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TERMINAL_MAX_SANDBOXES` | `12` | Sandboxes in use across all visitors, root panes and forks together. |
| `TERMINAL_MAX_PANES` | `4` | Panes per visitor, so at most three splits. |
| `TERMINAL_SESSION_SECONDS` | `300` | Hard cap on a visitor's session. Every fork shares its root pane's deadline. |
| `TERMINAL_IDLE_SECONDS` | `120` | A pane with no keystrokes for this long is closed. |
| `TERMINAL_STARTS_PER_MINUTE` | `20` | Sandbox starts and forks across all visitors. |
| `TERMINAL_PER_IP_PER_HOUR` | `10` | New sessions per client IP, plus one live session per IP. |
| `TERMINAL_WARM_POOL` | `2` | Started sandboxes kept ready, so a visitor never waits for a cold start. |
| `TERMINAL_POOL_MAX_AGE_SECONDS` | `600` | A pooled sandbox nobody claimed is recycled after this long. |
| `TERMINAL_SANDBOX_CPU` / `TERMINAL_SANDBOX_MEMORY` | `500m` / `1Gi` | Per-sandbox resources. |

A fork the browser never opens is terminated after 60 seconds. Per-IP limits apply only when the
server can see the visitor's public address. Behind a gateway that forwards only its own private
address, every visitor would look identical, so the server falls back to the global limits.

## WebSocket protocol

Open a pane at `/ws?cols=<n>&rows=<n>`, or at `/ws?cols=<n>&rows=<n>&fork=<token>` to attach to
a fork.

| Direction | Message |
| --- | --- |
| client → server | `{"type": "input", "data": "<keystrokes>"}` |
| client → server | `{"type": "resize", "cols": 120, "rows": 40}` |
| client → server | `{"type": "split"}` |
| server → client | binary frames of raw PTY output |
| server → client | `{"type": "status", "message": "..."}` while the sandbox starts |
| server → client | `{"type": "ready", "expires_in": 300, "sandbox": "1a2b3c4d", "pane": 1}` |
| server → client | `{"type": "forked", "token": "...", "pane": 2, "snapshot_ms": 800, "restore_ms": 1000}` |
| server → client | `{"type": "split_failed", "message": "..."}` |
| server → client | `{"type": "busy" \| "limited", "message": "..."}`, then close |
| server → client | `{"type": "ended", "reason": "exited" \| "idle" \| "time_limit" \| "fork_expired" \| "error"}`, then close |

## Clean up

```bash
uv run --with chalkcompute python -c \
  "from chalkcompute import ScalingGroup; ScalingGroup.from_name('sandbox-fork').delete()"
```

Pooled sandboxes and forks expire on their own lifetime within about 16 minutes.

## License

Apache-2.0. ghostty-web is MIT.

## Local authentication

The SDK reads credentials, API host, and the selected environment from your existing
`chalk login` in `~/.chalk.yml`, including project-scoped and default logins.
For unattended runs, `CHALK_CLIENT_ID` and `CHALK_CLIENT_SECRET` remain supported.
The examples use the current environment selected by your Chalk login.
