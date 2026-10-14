"""Split a browser terminal by forking its Chalk sandbox from a CPU snapshot.

Each pane on the page is a WebSocket on `/ws` with its own sandbox: gVisor kernel, no network
egress, half a CPU, a hard lifetime, and no credentials. Pressing "Split" on a pane snapshots
that pane's running sandbox, restores the snapshot into a brand-new sandbox, and opens a second
pane on the fork. The snapshot includes memory, so the fork gets the same files, the same `/tmp`,
and the same live shell, with whatever was running in it: a Python REPL keeps its variables.

Wire protocol on `/ws?cols=<n>&rows=<n>[&fork=<token>]`:
  client -> server  text {"type": "input", "data": "<keystrokes>"}
  client -> server  text {"type": "resize", "cols": <n>, "rows": <n>}
  client -> server  text {"type": "split"}: fork this pane's sandbox
  server -> client  binary frames: raw PTY output
  server -> client  text {"type": "status", "message"}: progress while a sandbox starts
  server -> client  text {"type": "ready", "expires_in": <s>, "sandbox": "<short id>", "pane": <n>}
  server -> client  text {"type": "forked", "token", "snapshot_ms", "restore_ms"}: open a pane with it
  server -> client  text {"type": "split_failed", "message"}
  server -> client  text {"type": "busy" | "limited", "message"}, then close
  server -> client  text {"type": "ended", "reason"}, then close
"""

from __future__ import annotations

import asyncio
import base64
import collections
import ipaddress
import json
import logging
import os
import secrets
import shlex
import threading
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from chalkcompute import ComputeClass, ContainerSecurityPolicy, ContainerSession, Image, KernelPolicy, Sandbox

# Interactive PTY sessions are exposed through the session client, which is not yet part of
# the public chalkcompute namespace.
from chalkcompute._connect import ConnectClient
from chalkcompute._container import ContainerSessionClient
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

PORT = int(os.environ.get("PORT", "8080"))
HERE = Path(__file__).resolve().parent
STATIC_DIR = Path(os.environ.get("TERMINAL_STATIC_DIR", str(HERE / "static")))

# Sandboxes alive at once across every visitor, root panes and forks together.
MAX_SANDBOXES = int(os.environ.get("TERMINAL_MAX_SANDBOXES", "12"))
MAX_PANES = int(os.environ.get("TERMINAL_MAX_PANES", "4"))
SESSION_SECONDS = int(os.environ.get("TERMINAL_SESSION_SECONDS", "300"))
IDLE_SECONDS = int(os.environ.get("TERMINAL_IDLE_SECONDS", "120"))
PER_IP_PER_HOUR = int(os.environ.get("TERMINAL_PER_IP_PER_HOUR", "10"))
STARTS_PER_MINUTE = int(os.environ.get("TERMINAL_STARTS_PER_MINUTE", "20"))
WARM_POOL = int(os.environ.get("TERMINAL_WARM_POOL", "2"))
POOL_MAX_AGE_SECONDS = int(os.environ.get("TERMINAL_POOL_MAX_AGE_SECONDS", "600"))
SANDBOX_CPU = os.environ.get("TERMINAL_SANDBOX_CPU", "500m")
SANDBOX_MEMORY = os.environ.get("TERMINAL_SANDBOX_MEMORY", "1Gi")
# A fork token is single use and must be redeemed by the browser within this window.
FORK_TOKEN_SECONDS = 60
FORK_READY_TIMEOUT_SECONDS = 60
SANDBOX_NAME_PREFIX = "web-fork-"
SANDBOX_TAG = {"app": "web-fork-demo"}
MAX_INPUT_BYTES = 16 * 1024

# Visitor sandboxes run this image: Python, tmux (which holds each shell), and a few tools.
SANDBOX_IMAGE = Image.debian_slim(python_version="3.12").apt_install(["tmux", "procps", "less", "tree"])

# After a restore, gVisor kills every process that was started through exec, because the caller
# holding its stdio is gone; only the container's init and its descendants survive. So PID 1 is
# this spawner, which runs each line written to SPAWN_FIFO as its own child. The shell lives in a
# tmux server started that way, and a pane is only a tmux client, so a fork keeps the live shell.
SPAWN_FIFO = "/run/spawn"
SPAWNER = (
    f"rm -f {SPAWN_FIFO}; mkfifo {SPAWN_FIFO}; while true; do while read -r cmd; do "
    + f'setsid sh -c "$cmd" </dev/null >/dev/null 2>&1 & done < {SPAWN_FIFO}; done'
)
TMUX_SOCKET = "/run/tmux.sock"
TMUX_CONF = "/run/tmux.conf"
MOTD_FILE = "/run/motd"
# A bare terminal: no status line, no prefix key, and no bindings, so every key reaches the shell.
TMUX_CONFIG = (
    "set -g status off\n"
    + "set -g prefix None\n"
    + "unbind-key -a\n"
    + "set -g escape-time 0\n"
    + "set -g history-limit 5000\n"
    + "set -g default-terminal xterm-256color\n"
    + "set -g window-size latest\n"
)
SHELL_ENV = {
    "HOME": "/workspace",
    "PS1": r"\[\e[38;5;150m\]sandbox\[\e[0m\]:\[\e[38;5;114m\]\w\[\e[0m\]$ ",
}

MOTD = """
\x1b[38;5;150m  chalk sandbox\x1b[0m  ·  gVisor kernel · no network · {minutes} minutes · yours alone

  Start something, then press \x1b[38;5;150mSplit\x1b[0m. The whole sandbox, memory included, is
  snapshotted and restored into a second one: same files, same shell, same running programs.

  Try:  \x1b[38;5;114mpython3\x1b[0m, then  \x1b[38;5;114mx = 41\x1b[0m  and Split. Ask each pane for  \x1b[38;5;114mx + 1\x1b[0m.

"""
MOTD_COMPACT = """
\x1b[38;5;150m  chalk sandbox\x1b[0m
  gVisor kernel · no network
  Run python3, set x = 41,
  then press \x1b[38;5;150mSplit\x1b[0m.

"""
MOTD_MIN_COLS = 76

log = logging.getLogger("fork-terminal")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def is_public(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_global
    except ValueError:
        return False


class RateLimiter:
    """Caps sandbox starts globally, and per client IP when the real client IP is visible.

    Some gateways forward only their own private address, which every visitor shares; limiting
    by it would lock everyone out at once, so private addresses get only the global caps.
    """

    def __init__(self, per_hour: int, global_per_minute: int) -> None:
        super().__init__()
        self.per_hour = per_hour
        self.global_per_minute = global_per_minute
        self.starts: dict[str, collections.deque[float]] = collections.defaultdict(collections.deque)
        self.global_starts: collections.deque[float] = collections.deque()
        self.live: set[str] = set()

    @staticmethod
    def _trim(window: collections.deque[float], now: float, horizon: float) -> None:
        while window and now - window[0] > horizon:
            window.popleft()

    def acquire_global(self) -> str | None:
        """Records one sandbox start against the global window, or returns a refusal."""
        now = time.monotonic()
        self._trim(self.global_starts, now, 60)
        if len(self.global_starts) >= self.global_per_minute:
            return "Lots of people are launching sandboxes right now. Try again in a minute."
        self.global_starts.append(now)
        return None

    def acquire(self, ip: str) -> str | None:
        """Admits a new visitor from `ip`, or returns a refusal message."""
        if is_public(ip):
            if ip in self.live:
                return "You already have a terminal open in another tab."
            window = self.starts[ip]
            self._trim(window, time.monotonic(), 3600)
            if len(window) >= self.per_hour:
                return "That's a lot of sandboxes for one hour. Try again later."
        refusal = self.acquire_global()
        if refusal is None and is_public(ip):
            self.starts[ip].append(time.monotonic())
            self.live.add(ip)
        return refusal

    def release(self, ip: str) -> None:
        self.live.discard(ip)


@dataclass
class Group:
    """One visitor's panes. Forks inherit the root pane's deadline."""

    ip: str
    deadline: float
    panes: int = 1
    open_panes: int = 0
    next_pane: int = 2


@dataclass
class PendingFork:
    sandbox: Sandbox
    group: Group
    parent_pane: int
    pane: int
    snapshot_ms: int
    restore_ms: int
    created: float = field(default_factory=time.monotonic)


state: dict[str, Any] = {}


def make_sandbox() -> Sandbox:
    # No network_policy means no egress at all (not even DNS). No chalk_identity and no
    # secrets: nothing inside the sandbox can call back into Chalk. A fork copies this spec.
    sandbox = Sandbox(
        image=SANDBOX_IMAGE,
        entrypoint=["/bin/sh", "-c", SPAWNER],
        name=f"{SANDBOX_NAME_PREFIX}{uuid.uuid4().hex[:10]}",
        cpu=SANDBOX_CPU,
        memory=SANDBOX_MEMORY,
        lifetime=f"{POOL_MAX_AGE_SECONDS + SESSION_SECONDS + 60}s",
        compute_class=ComputeClass.HOST,
        security_policy=ContainerSecurityPolicy(kernel_policy=KernelPolicy.RESTRICTED),
        tags=SANDBOX_TAG,
    )
    sandbox.run()
    sandbox.exec("mkdir", "-p", "/workspace")
    write_file(sandbox, TMUX_CONF, TMUX_CONFIG)
    return sandbox


def terminate_quietly(sandbox: Sandbox) -> None:
    try:
        sandbox.terminate()
    except Exception:
        log.exception("terminate failed for %s", sandbox.id)


def write_file(sandbox: Sandbox, path: str, content: str) -> None:
    encoded = base64.b64encode(content.encode()).decode()
    sandbox.exec("sh", "-c", f"printf %s {encoded} | base64 -d > {path}")


def start_shell(sandbox: Sandbox, cols: int, rows: int, motd: str) -> None:
    """Has PID 1 start the tmux server that holds the visitor's shell, and waits for it."""
    write_file(sandbox, MOTD_FILE, motd.replace("\n", "\r\n"))
    shell_env = shlex.join(f"{key}={value}" for key, value in SHELL_ENV.items())
    shell = f"cat {MOTD_FILE}; exec env {shell_env} bash --norc --noprofile -i"
    command = (
        f"tmux -f {TMUX_CONF} -S {TMUX_SOCKET} new-session -d -s main -x {cols} -y {rows} "
        + f"-c /workspace {shlex.quote(shell)}"
    )
    # The command reaches PID 1 as one line of the FIFO; it contains no newlines.
    result = sandbox.exec(
        "sh",
        "-c",
        # printf rather than echo: dash's echo would interpret the prompt's backslashes.
        f"printf '%s\\n' {shlex.quote(command)} > {SPAWN_FIFO}; "
        + f"for i in $(seq 100); do tmux -S {TMUX_SOCKET} has-session -t main 2>/dev/null && exit 0; sleep 0.05; done; exit 1",
    )
    if result.exit_code != 0:
        raise RuntimeError("the shell did not start")


def attach_tmux(sandbox: Sandbox, cols: int, rows: int) -> ContainerSession:
    """Opens a PTY session running a tmux client on the sandbox's shell."""
    return state["sessions"].new_session(
        sandbox.id,
        "tmux",
        "-S",
        TMUX_SOCKET,
        "attach-session",
        "-t",
        "main",
        env={"TERM": "xterm-256color"},
        pty=(cols, rows),
    )


def reap_orphans() -> None:
    """Terminates sandboxes a previous replica left behind; their lifetime would also end them.

    Forks are created without a name, so only their copied lifetime bounds a fork left behind.
    """
    try:
        for info in Sandbox.list_all():
            if (info.name or "").startswith(SANDBOX_NAME_PREFIX) and info.status in ("pending", "running"):
                terminate_quietly(Sandbox.from_id(info.id))
    except Exception:
        log.exception("orphan reap failed")


class SnapshotClient:
    """Creates and deletes sandbox snapshots and forks over Connect's JSON protocol.

    The released chalkcompute SDK does not wrap the snapshot RPCs yet, so they are called
    directly, authenticated with the same credentials the SDK uses.
    """

    def __init__(self) -> None:
        super().__init__()
        self.connect = ConnectClient()
        self.connect._ensure_auth()
        self.api = (self.connect._api_server or "").rstrip("/")
        self.http = httpx.Client(timeout=120)

    def _call(self, method: str, body: dict[str, Any]) -> dict[str, Any]:
        headers = dict(self.connect._rpc_metadata() or [])
        headers["content-type"] = "application/json"
        resp = self.http.post(f"{self.api}/{method}", json=body, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(f"{method} failed with {resp.status_code}: {resp.text[:300]}")
        return resp.json()

    def snapshot(self, sandbox_id: str) -> str:
        snap = self._call("chalk.sandbox.v2.SandboxSnapshotService/CreateSandboxSnapshot", {"sandboxId": sandbox_id})
        status = snap["snapshot"].get("status")
        if status != "SANDBOX_SNAPSHOT_STATUS_COMPLETED":
            raise RuntimeError(f"snapshot finished with status {status}")
        return snap["snapshot"]["id"]

    def fork(self, snapshot_id: str) -> str:
        created = self._call("chalk.sandbox.v2.SandboxService/CreateSandbox", {"snapshotId": snapshot_id})
        return created["sandbox"]["id"]

    def delete(self, snapshot_id: str) -> None:
        try:
            self._call("chalk.sandbox.v2.SandboxSnapshotService/DeleteSandboxSnapshot", {"snapshotId": snapshot_id})
        except Exception:
            log.exception("deleting snapshot %s failed", snapshot_id)


def fork_sandbox(snapshots: SnapshotClient, source: Sandbox) -> tuple[Sandbox, int, int]:
    """Snapshots `source` (which keeps running) and boots a new sandbox from it.

    Returns the fork and how long the snapshot and the restore took, in milliseconds.
    """
    t0 = time.monotonic()
    snapshot_id = snapshots.snapshot(source.id)
    t1 = time.monotonic()
    fork: Sandbox | None = None
    try:
        fork = Sandbox.from_id(snapshots.fork(snapshot_id))
        deadline = t1 + FORK_READY_TIMEOUT_SECONDS
        while True:
            status = fork.refresh().status
            if status == "running":
                break
            if status in ("failed", "terminated", "stopped") or time.monotonic() > deadline:
                raise RuntimeError(f"fork did not start (status {status})")
            time.sleep(0.2)
        t2 = time.monotonic()
        return fork, round((t1 - t0) * 1000), round((t2 - t1) * 1000)
    except Exception:
        if fork is not None:
            terminate_quietly(fork)
        raise
    finally:
        # A running fork no longer needs the snapshot it booted from.
        snapshots.delete(snapshot_id)


class WarmPool:
    """Keeps `size` started sandboxes ready, because a cold start takes anywhere from 2 to 60 s.

    Every sandbox handed out is fresh: a visitor never gets one another visitor has used.
    """

    def __init__(self, size: int) -> None:
        super().__init__()
        self.size = size
        self.ready: collections.deque[tuple[float, Sandbox]] = collections.deque()
        self.filling = 0

    def fill(self) -> None:
        while len(self.ready) + self.filling < self.size:
            self.filling += 1
            asyncio.create_task(self._create())

    async def prime(self) -> None:
        """Starts one sandbox and waits for it, then fills the rest in the background."""
        self.filling += 1
        await self._create()
        self.fill()

    async def _create(self) -> None:
        try:
            t0 = time.monotonic()
            sandbox = await asyncio.to_thread(make_sandbox)
            self.ready.append((time.monotonic(), sandbox))
            log.info("pooled sandbox %s started in %.1fs", sandbox.id, time.monotonic() - t0)
        except Exception:
            log.exception("pooled sandbox start failed")
            await asyncio.sleep(10)
        finally:
            self.filling -= 1

    async def take(self) -> Sandbox:
        while self.ready:
            created, sandbox = self.ready.popleft()
            if time.monotonic() - created < POOL_MAX_AGE_SECONDS:
                self.fill()
                return sandbox
            asyncio.create_task(asyncio.to_thread(terminate_quietly, sandbox))
        self.fill()
        return await asyncio.to_thread(make_sandbox)

    async def recycle_forever(self) -> None:
        while True:
            await asyncio.sleep(30)
            now = time.monotonic()
            while self.ready and now - self.ready[0][0] >= POOL_MAX_AGE_SECONDS:
                _, stale = self.ready.popleft()
                await asyncio.to_thread(terminate_quietly, stale)
            self.fill()

    async def drain(self) -> None:
        while self.ready:
            _, sandbox = self.ready.popleft()
            await asyncio.to_thread(terminate_quietly, sandbox)


async def expire_pending_forks_forever() -> None:
    """Terminates forks whose browser never came back for them."""
    pending: dict[str, PendingFork] = state["pending_forks"]
    while True:
        await asyncio.sleep(5)
        now = time.monotonic()
        for token in [t for t, p in pending.items() if now - p.created > FORK_TOKEN_SECONDS]:
            stale = pending.pop(token)
            stale.group.panes -= 1
            state["sandboxes"] -= 1
            await asyncio.to_thread(terminate_quietly, stale.sandbox)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await asyncio.to_thread(reap_orphans)
    pool = WarmPool(WARM_POOL)
    # The first sandbox also builds the visitor image; wait for it so the replica only
    # reports ready once a visitor can get a terminal immediately.
    await pool.prime()
    state["pool"] = pool
    state["limiter"] = RateLimiter(PER_IP_PER_HOUR, STARTS_PER_MINUTE)
    state["sandboxes"] = 0
    state["pending_forks"] = {}
    state["sessions"] = ContainerSessionClient.from_connect(ConnectClient())
    state["snapshots"] = SnapshotClient()
    background = [
        asyncio.create_task(pool.recycle_forever()),
        asyncio.create_task(expire_pending_forks_forever()),
    ]
    yield
    for task in background:
        task.cancel()
    await pool.drain()
    for pending in list(state["pending_forks"].values()):
        await asyncio.to_thread(terminate_quietly, pending.sandbox)


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse((HERE / "index.html").read_text())


# ghostty-web's bundle (its WASM is inlined) plus the empty chunk it imports.
STATIC_FILES = {"ghostty-web.js", "__vite-browser-external-2447137e.js"}


@app.get("/static/{name}")
async def static_file(name: str) -> FileResponse:
    if name not in STATIC_FILES:
        raise HTTPException(status_code=404)
    return FileResponse(
        STATIC_DIR / name,
        media_type="text/javascript",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.get("/status")
async def status() -> JSONResponse:
    return JSONResponse({"ok": True, "sandboxes": state["sandboxes"], "capacity": MAX_SANDBOXES})


def client_ip(websocket: WebSocket) -> str:
    forwarded = websocket.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"


def clamp_dim(value: Any, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(hi, int(value)))
    except (TypeError, ValueError):
        return default


def short_id(sandbox: Sandbox) -> str:
    return (sandbox.id or "")[:8]


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await websocket.accept()
    token = websocket.query_params.get("fork")
    if token:
        await fork_pane(websocket, token)
    else:
        await root_pane(websocket)


async def send(websocket: WebSocket, msg: dict[str, Any]) -> None:
    await websocket.send_text(json.dumps(msg))


async def root_pane(websocket: WebSocket) -> None:
    ip = client_ip(websocket)
    limiter: RateLimiter = state["limiter"]
    refusal = limiter.acquire(ip)
    if refusal is not None:
        await send(websocket, {"type": "limited", "message": refusal})
        await websocket.close()
        return
    if state["sandboxes"] >= MAX_SANDBOXES:
        limiter.release(ip)
        await send(websocket, {"type": "busy", "message": "Every demo sandbox is in use. Try again in a minute."})
        await websocket.close()
        return

    group = Group(ip=ip, deadline=time.monotonic() + SESSION_SECONDS)
    state["sandboxes"] += 1
    sandbox: Sandbox | None = None
    try:
        await send(websocket, {"type": "status", "message": "Starting a fresh sandbox…"})
        pool: WarmPool = state["pool"]
        sandbox = await pool.take()
        cols = clamp_dim(websocket.query_params.get("cols"), 20, 300, 100)
        rows = clamp_dim(websocket.query_params.get("rows"), 5, 120, 30)
        motd = (MOTD if cols >= MOTD_MIN_COLS else MOTD_COMPACT).format(minutes=SESSION_SECONDS // 60)
        await asyncio.to_thread(start_shell, sandbox, cols, rows, motd)
        await run_pane(websocket, sandbox, group, 1, None)
    finally:
        state["sandboxes"] -= 1
        if group.open_panes == 0:
            limiter.release(ip)
        if sandbox is not None:
            await asyncio.to_thread(terminate_quietly, sandbox)


async def fork_pane(websocket: WebSocket, token: str) -> None:
    pending: dict[str, PendingFork] = state["pending_forks"]
    fork = pending.pop(token, None)
    if fork is None:
        await send(websocket, {"type": "ended", "reason": "fork_expired"})
        await websocket.close()
        return
    try:
        # The restored tmux server still holds the parent's shell; attaching shows it as it was.
        note = f"fork of pane {fork.parent_pane} · snapshot {fork.snapshot_ms} ms · restore {fork.restore_ms} ms"
        await run_pane(websocket, fork.sandbox, fork.group, fork.pane, note)
    finally:
        fork.group.panes -= 1
        state["sandboxes"] -= 1
        if fork.group.open_panes == 0:
            state["limiter"].release(fork.group.ip)
        await asyncio.to_thread(terminate_quietly, fork.sandbox)


async def run_pane(websocket: WebSocket, sandbox: Sandbox, group: Group, pane: int, note: str | None) -> None:
    """Attaches the browser to the sandbox's tmux session, showing `note` briefly if given."""
    session: ContainerSession | None = None
    group.open_panes += 1
    try:
        cols = clamp_dim(websocket.query_params.get("cols"), 20, 300, 100)
        rows = clamp_dim(websocket.query_params.get("rows"), 5, 120, 30)
        session = await asyncio.to_thread(attach_tmux, sandbox, cols, rows)
        if note is not None:
            await asyncio.to_thread(
                sandbox.exec, "tmux", "-S", TMUX_SOCKET, "display-message", "-d", "5000", "-t", "main", note
            )
        remaining = max(1, int(group.deadline - time.monotonic()))
        await send(websocket, {"type": "ready", "expires_in": remaining, "sandbox": short_id(sandbox), "pane": pane})
        reason = await proxy(websocket, session, sandbox, group, pane, [cols, rows])
        await send(websocket, {"type": "ended", "reason": reason})
        await websocket.close()
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("pane %d (sandbox %s) failed", pane, sandbox.id)
        try:
            await send(websocket, {"type": "ended", "reason": "error"})
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        group.open_panes -= 1


def split_refusal(group: Group) -> str | None:
    """Returns why a split is not allowed right now, or None after reserving a start for it."""
    if group.panes >= MAX_PANES:
        return f"Up to {MAX_PANES} panes per visitor."
    if state["sandboxes"] >= MAX_SANDBOXES:
        return "Every demo sandbox is in use right now."
    return state["limiter"].acquire_global()


async def split(websocket: WebSocket, sandbox: Sandbox, group: Group, pane: int) -> None:
    """Forks `sandbox` and tells the browser how to open a pane on the fork.

    The caller has already detached every tmux client from the sandbox (see `proxy`).
    """
    group.panes += 1
    state["sandboxes"] += 1
    try:
        fork, snapshot_ms, restore_ms = await asyncio.to_thread(fork_sandbox, state["snapshots"], sandbox)
    except Exception:
        group.panes -= 1
        state["sandboxes"] -= 1
        log.exception("split of %s failed", sandbox.id)
        await send(websocket, {"type": "split_failed", "message": "That split didn't work. Try again."})
        return
    token = secrets.token_urlsafe(16)
    child_pane = group.next_pane
    group.next_pane += 1
    state["pending_forks"][token] = PendingFork(fork, group, pane, child_pane, snapshot_ms, restore_ms)
    log.info("split %s -> %s (snapshot %d ms, restore %d ms)", sandbox.id, fork.id, snapshot_ms, restore_ms)
    await send(
        websocket,
        {"type": "forked", "token": token, "pane": child_pane, "snapshot_ms": snapshot_ms, "restore_ms": restore_ms},
    )


async def proxy(
    websocket: WebSocket, session: ContainerSession, sandbox: Sandbox, group: Group, pane: int, size: list[int]
) -> str:
    """Pumps PTY output to the browser and keystrokes to the PTY; returns why it stopped.

    A split swaps the pane's tmux client for a fresh one, so the session in use can change.
    Every event is tagged with the generation of the session that produced it, and events from
    a retired session, including its exit, are ignored.
    """
    loop = asyncio.get_running_loop()
    events: asyncio.Queue[Any] = asyncio.Queue()
    last_input = time.monotonic()
    splitting = False
    attached = {"session": session, "gen": 0}

    # The SDK's event stream is a blocking iterator, so each session gets its own thread.
    def read_events(sess: ContainerSession, gen: int) -> None:
        try:
            for event in sess.events():
                loop.call_soon_threadsafe(events.put_nowait, (gen, event))
        except Exception as e:
            loop.call_soon_threadsafe(events.put_nowait, (gen, e))
        loop.call_soon_threadsafe(events.put_nowait, (gen, None))

    threading.Thread(target=read_events, args=(session, 0), daemon=True).start()

    async def pump_output() -> str:
        while True:
            gen, event = await events.get()
            if gen != attached["gen"]:
                continue
            if event is None or isinstance(event, Exception):
                return "exited"
            if isinstance(event, ContainerSession.OutputEvent):
                await websocket.send_bytes(event.data)
            elif isinstance(
                event,
                (
                    ContainerSession.ExitedEvent,
                    ContainerSession.FailedEvent,
                    ContainerSession.TimedOutEvent,
                    ContainerSession.ErrorEvent,
                ),
            ):
                return "exited"

    async def run_split() -> None:
        nonlocal splitting
        try:
            refusal = split_refusal(group)
            if refusal is not None:
                await send(websocket, {"type": "split_failed", "message": refusal})
                return
            # A tmux client hands its terminal, a host-backed PTY, to the tmux server. A
            # surviving process that holds a host fd cannot be restored, so the snapshot is taken
            # with no client attached, and the pane gets a fresh client afterwards.
            attached["gen"] += 1
            await asyncio.to_thread(sandbox.exec, "tmux", "-S", TMUX_SOCKET, "detach-client", "-s", "main")
            try:
                await split(websocket, sandbox, group, pane)
            finally:
                fresh = await asyncio.to_thread(attach_tmux, sandbox, size[0], size[1])
                attached["session"] = fresh
                threading.Thread(target=read_events, args=(fresh, attached["gen"]), daemon=True).start()
        finally:
            splitting = False

    async def pump_input() -> str:
        nonlocal last_input, splitting
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                raise WebSocketDisconnect
            text = msg.get("text")
            if not text or len(text) > MAX_INPUT_BYTES:
                continue
            ctrl = json.loads(text)
            kind = ctrl.get("type")
            if kind == "input" and isinstance(ctrl.get("data"), str):
                last_input = time.monotonic()
                # Keystrokes typed while the pane is detached for a split are dropped.
                if not splitting:
                    await asyncio.to_thread(attached["session"].write, ctrl["data"].encode())
            elif kind == "resize":
                size[0] = clamp_dim(ctrl.get("cols"), 20, 300, 100)
                size[1] = clamp_dim(ctrl.get("rows"), 5, 120, 30)
                if not splitting:
                    await asyncio.to_thread(attached["session"].resize, size[0], size[1])
            elif kind == "split" and not splitting:
                # The split runs beside the pumps so the pane keeps receiving output meanwhile.
                splitting = True
                last_input = time.monotonic()
                asyncio.create_task(run_split())

    async def watchdog() -> str:
        while True:
            await asyncio.sleep(1)
            now = time.monotonic()
            if now >= group.deadline:
                return "time_limit"
            if now - last_input >= IDLE_SECONDS:
                return "idle"

    tasks = [asyncio.create_task(c) for c in (pump_output(), pump_input(), watchdog())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        return next(iter(done)).result()
    finally:
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    import uvicorn

    # Pings well under the gateway's 60 s idle timeout keep a quiet terminal connected.
    uvicorn.run(app, host="0.0.0.0", port=PORT, ws_ping_interval=20, ws_ping_timeout=20)
