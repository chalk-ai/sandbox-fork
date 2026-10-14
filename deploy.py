#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12,<3.14"
# dependencies = ["chalkcompute==2.13.5"]
# ///
"""Deploy the split-terminal sandbox fork demo (server.py + index.html) as a Chalk ScalingGroup.

The server creates, snapshots, and forks visitor sandboxes with its own workload identity
(`chalk_identity=True`), so no client secret is baked into the image. Visitor sandboxes get no
identity at all. Snapshots must be enabled for the environment.

Credentials for the deploy itself come from `chalk config` in the current directory (or the
CHALK_* env vars):
    ./deploy.py [--env-id <environment>] [--name sandbox-fork]
Then open the printed URL, press "Launch sandbox", and press "Split".
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GHOSTTY_WEB_VERSION = "0.4.0"
STATIC_DIR = "/app/static"
APP_DIR = "/app/srv"

# ghostty-web ships its WASM inlined in the ESM bundle, so the page needs only dist/*.js.
FETCH_GHOSTTY = (
    'python -c "import io, tarfile, urllib.request; '
    + f"data = urllib.request.urlopen('https://registry.npmjs.org/ghostty-web/-/ghostty-web-{GHOSTTY_WEB_VERSION}.tgz').read(); "
    + "tar = tarfile.open(fileobj=io.BytesIO(data)); "
    + "members = [m for m in tar.getmembers() if m.name.startswith('package/dist/') and m.name.endswith('.js')]; "
    + "[setattr(m, 'name', m.name.split('/')[-1]) for m in members]; "
    + f"tar.extractall('{STATIC_DIR}', members=members)\""
)


def load_chalk_env(env_id: str | None) -> None:
    if "CHALK_CLIENT_ID" not in os.environ:
        cfg = json.loads(subprocess.check_output(["chalk", "config", "--json"]))
        os.environ["CHALK_API_SERVER"] = cfg["apiServer"]["value"]
        os.environ["CHALK_CLIENT_ID"] = cfg["clientId"]["value"]
        os.environ["CHALK_CLIENT_SECRET"] = cfg["clientSecret"]["value"]
        os.environ.setdefault("CHALK_ENVIRONMENT_ID", cfg["environment"]["value"])
    if env_id:
        os.environ["CHALK_ENVIRONMENT_ID"] = env_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="sandbox-fork")
    # Defaults to the environment in `chalk config` (or CHALK_ENVIRONMENT_ID).
    parser.add_argument("--env-id", default=None)
    parser.add_argument("--max-sandboxes", default="12")
    parser.add_argument("--warm-pool", default="2")
    parser.add_argument("--ready-timeout", type=float, default=900.0)
    args = parser.parse_args()

    load_chalk_env(args.env_id)
    from chalkcompute import Image, ScalingGroup

    image = (
        Image.debian_slim(python_version="3.12")
        .pip_install(["chalkcompute==2.13.5", "fastapi==0.116.1", "uvicorn[standard]==0.35.0", "httpx==0.28.1"])
        .run_commands(FETCH_GHOSTTY)
        # Together the two files exceed the 32 KiB inline-copy limit, so they ship as a mounted
        # volume. A volume shadows its whole directory, so it gets one of its own.
        .add_local_file(str(HERE / "server.py"), f"{APP_DIR}/server.py", strategy="volume")
        .add_local_file(str(HERE / "index.html"), f"{APP_DIR}/index.html", strategy="volume")
        .workdir("/app")
    )

    sg = ScalingGroup(
        image=image,
        name=args.name,
        cpu="1",
        memory="2Gi",
        env={
            # Not a secret: which API the server's workload identity talks to.
            "CHALK_API_SERVER": os.environ["CHALK_API_SERVER"],
            "PORT": "8080",
            "TERMINAL_STATIC_DIR": STATIC_DIR,
            "TERMINAL_MAX_SANDBOXES": args.max_sandboxes,
            "TERMINAL_WARM_POOL": args.warm_pool,
        },
        entrypoint=["python", f"{APP_DIR}/server.py"],
        port=8080,
        chalk_identity=True,
        # Session state lives in the replica (the warm pool, pending forks, and per-IP limits),
        # and each WebSocket must stay on the replica that holds its sandbox, so this stays at one.
        min_replicas=1,
        max_replicas=1,
    )
    t0 = time.monotonic()
    try:
        sg.deploy(build_timeout=1800.0, ready_timeout=args.ready_timeout)
    finally:
        print(f"deploy returned after {time.monotonic() - t0:.0f}s")
    print("web_url:", sg.web_url)


if __name__ == "__main__":
    main()
