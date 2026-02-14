#!/usr/bin/env python3
"""
run_thread_sweep.py

Runs:
  python3 shard_Thread.py 2
  python3 shard_Thread.py 4
  ...
  python3 shard_Thread.py 64

For each run:
  - drops OS page cache (sync + drop_caches) BEFORE running
  - creates folder: shared_Thread_logs/thread=<N>/
  - writes stdout+stderr to: shared_Thread_logs/thread=<N>/run.log
  - also writes metadata to: shared_Thread_logs/thread=<N>/meta.txt

IMPORTANT:
  Dropping caches requires root:
    sudo -v
    sudo python3 run_thread_sweep.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

LOG_ROOT = Path("shared_Thread_logs")
TARGET_SCRIPT = Path("shard_Thread.py")

THREAD_COUNTS = list(range(2, 65, 2))  # 2,4,6,...,64


def drop_caches() -> None:
    """
    Drop Linux page cache/dentries/inodes.
    Requires sudo/root.
    """
    # Flush dirty pages first
    subprocess.run(["sync"], check=False)

    # Drop caches
    # echo 3 > /proc/sys/vm/drop_caches must be done as root.
    # Use sh -c to handle redirection.
    r = subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=False)
    if r.returncode != 0:
        raise RuntimeError(
            "Failed to drop caches. Run this script with sudo (or ensure sudo works without prompt)."
        )

    # Small settle time (optional)
    time.sleep(1)


def run_one(n_threads: int) -> int:
    run_dir = LOG_ROOT / f"thread={n_threads}"
    run_dir.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "run.log"
    meta_path = run_dir / "meta.txt"

    # metadata
    meta = []
    meta.append(f"timestamp: {datetime.now().isoformat()}")
    meta.append(f"cmd: python3 {TARGET_SCRIPT} {n_threads}")
    meta.append(f"cwd: {os.getcwd()}")
    meta.append(f"python: {sys.executable}")
    meta.append("")

    # drop caches before run
    meta.append("drop_caches: begin")
    t0 = time.time()
    drop_caches()
    meta.append(f"drop_caches: end ({time.time() - t0:.3f}s)")
    meta.append("")

    meta_path.write_text("\n".join(meta), encoding="utf-8")

    # run and capture stdout+stderr
    with open(log_path, "w", encoding="utf-8") as f_log:
        f_log.write(f"=== RUN START {datetime.now().isoformat()} ===\n")
        f_log.write(f"$ python3 {TARGET_SCRIPT} {n_threads}\n\n")
        f_log.flush()

        start = time.time()
        p = subprocess.Popen(
            ["python3", str(TARGET_SCRIPT), str(n_threads)],
            stdout=f_log,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        rc = p.wait()
        elapsed = time.time() - start

        f_log.write("\n")
        f_log.write(f"=== RUN END {datetime.now().isoformat()} rc={rc} elapsed={elapsed:.3f}s ===\n")

    # append result summary to meta
    with open(meta_path, "a", encoding="utf-8") as f_meta:
        f_meta.write(f"rc: {rc}\n")
        f_meta.write(f"elapsed_s: {elapsed:.3f}\n")

    return rc


def main() -> None:
    if not TARGET_SCRIPT.exists():
        print(f"ERROR: {TARGET_SCRIPT} not found in {os.getcwd()}", file=sys.stderr)
        sys.exit(2)

    LOG_ROOT.mkdir(parents=True, exist_ok=True)

    # Pre-flight sudo (optional): will prompt once
    # If you run the whole script with sudo, this is unnecessary.
    subprocess.run(["sudo", "-v"], check=False)

    failures = []
    for n in THREAD_COUNTS:
        print(f"[RUN] threads={n} -> {LOG_ROOT}/thread={n}/run.log")
        try:
            rc = run_one(n)
        except Exception as e:
            print(f"[FAIL] threads={n}: {e}", file=sys.stderr)
            failures.append((n, str(e)))
            continue

        if rc != 0:
            failures.append((n, f"nonzero return code {rc}"))

    if failures:
        print("\nFailures:")
        for n, msg in failures:
            print(f"  threads={n}: {msg}")
        sys.exit(1)

    print("\nAll runs completed successfully.")


if __name__ == "__main__":
    main()

