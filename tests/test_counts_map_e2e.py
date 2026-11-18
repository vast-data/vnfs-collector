"""Counts-map size wiring and optional privileged NFS e2e.

Light checks always run (subprocess --ebpf). The full loopback-NFS test is
gated by VNFS_E2E=1 and needs root, nfs-kernel-server, and CAP for BPF/kprobes:

  sudo VNFS_E2E=1 venv/bin/pytest tests/test_counts_map_e2e.py -m e2e -v -s
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

from vnfs_collector.main import _exec

MAP_SIZE = 32
WORKER_COUNT = 120
COLLECT_INTERVAL = 2


def _log(msg: str) -> None:
    print(f"[vnfs-e2e] {msg}", flush=True)


def _collector_cmd(*extra):
    exe = shutil.which("vnfs-collector")
    if exe:
        return [exe, *extra]
    return [
        sys.executable,
        "-c",
        "from vnfs_collector.main import main; raise SystemExit(main())",
        *extra,
    ]


def _tee_pipe(pipe, log_path: Path, stop: threading.Event):
    """Copy collector output to the console and a log file."""
    with log_path.open("a") as log_f:
        while not stop.is_set():
            line = pipe.readline()
            if line == "":
                break
            sys.stdout.write(line)
            sys.stdout.flush()
            log_f.write(line)
            log_f.flush()


@pytest.mark.asyncio
async def test_counts_map_size_injected_into_ebpf_text(cli_factory, capfd):
    """--counts-map-size must reach the BPF C text (no real bcc / subprocess)."""
    cli_factory(f"-d=screen --ebpf --counts-map-size={MAP_SIZE}")
    with pytest.raises(SystemExit) as exc:
        await _exec()
    assert exc.value.code in (0, None)
    out, err = capfd.readouterr()
    text = out + err
    assert f"#define COUNTS_MAX_ENTRIES {MAP_SIZE}" in text
    assert "BPF_HASH(counts, struct info_t, struct stats_t, COUNTS_MAX_ENTRIES)" in text


def _have_e2e_prereqs():
    if os.environ.get("VNFS_E2E") != "1":
        return False, "set VNFS_E2E=1 to enable privileged NFS e2e"
    if os.geteuid() != 0:
        return False, "must run as root (BPF kprobes + NFS mount/export)"
    for tool in ("exportfs", "mount.nfs", "systemctl"):
        if not shutil.which(tool):
            return False, f"missing {tool} (install nfs-kernel-server / nfs-common)"
    try:
        subprocess.run(
            [sys.executable, "-c", "import bcc"],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return False, f"bcc not importable: {e}"
    return True, ""


@pytest.fixture(scope="module")
def loopback_nfs():
    ok, reason = _have_e2e_prereqs()
    if not ok:
        pytest.skip(reason)

    tmp = Path(tempfile.mkdtemp(prefix="vnfs-e2e-"))
    export_dir = tmp / "export"
    mount_dir = tmp / "mnt"
    export_dir.mkdir()
    mount_dir.mkdir()
    (export_dir / "payload").write_bytes(b"x" * 4096)

    _log("starting nfs-server")
    subprocess.run(
        ["systemctl", "start", "nfs-server"],
        check=False,
        capture_output=True,
        timeout=60,
    )
    _log(f"exporting {export_dir}")
    subprocess.run(
        [
            "exportfs",
            "-o",
            "rw,sync,no_root_squash,insecure,fsid=21601",
            f"127.0.0.1:{export_dir}",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    try:
        _log(f"mounting nfs -> {mount_dir}")
        subprocess.run(
            [
                "mount",
                "-t",
                "nfs",
                "-o",
                "vers=4.2,addr=127.0.0.1",
                f"127.0.0.1:{export_dir}",
                str(mount_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.CalledProcessError as e:
        subprocess.run(
            ["exportfs", "-u", f"127.0.0.1:{export_dir}"],
            check=False,
            capture_output=True,
        )
        pytest.skip(f"NFS mount failed: {e.stderr or e.stdout or e}")

    _log(f"loopback NFS ready at {mount_dir}")
    yield {"export": export_dir, "mount": mount_dir, "tmp": tmp}

    _log("cleaning up NFS mount/export")
    subprocess.run(["umount", "-f", str(mount_dir)], check=False, capture_output=True)
    subprocess.run(
        ["exportfs", "-u", f"127.0.0.1:{export_dir}"],
        check=False,
        capture_output=True,
    )
    shutil.rmtree(tmp, ignore_errors=True)


def _burst_nfs_readers(mount_dir: Path, n: int):
    """Spawn n short-lived processes that each open/read the NFS file (distinct PIDs)."""
    target = mount_dir / "payload"
    script = (
        "import os,sys\n"
        f"p={target.as_posix()!r}\n"
        "fd=os.open(p, os.O_RDONLY)\n"
        "os.read(fd, 4096)\n"
        "os.close(fd)\n"
    )
    procs = [
        subprocess.Popen([sys.executable, "-c", script]) for _ in range(n)
    ]
    for p in procs:
        p.wait(timeout=30)


@pytest.mark.e2e
def test_counts_map_size_crops_busy_nfs_node(loopback_nfs, tmp_path):
    """With a tiny counts map, unique PIDs in a sample stay <= map size; process stays up."""
    samples = tmp_path / "samples.jsonl"
    log_path = tmp_path / "collector.log"
    log_path.write_text("")

    cmd = _collector_cmd(
        "-d",
        "file",
        "--samples-path",
        str(samples),
        "-i",
        str(COLLECT_INTERVAL),
        "--counts-map-size",
        str(MAP_SIZE),
        "--squash-pid",
        "false",
        "--mnt-id-segmentation",
        "false",
    )
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    _log(f"starting collector: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        bufsize=1,
    )
    stop_tee = threading.Event()
    tee = threading.Thread(
        target=_tee_pipe,
        args=(proc.stdout, log_path, stop_tee),
        daemon=True,
    )
    tee.start()
    try:
        _log("waiting for BPF compile/attach (can take tens of seconds)...")
        deadline = time.time() + 120
        while time.time() < deadline:
            if proc.poll() is not None:
                stop_tee.set()
                tee.join(timeout=5)
                pytest.fail(
                    f"collector exited early rc={proc.returncode}: {log_path.read_text()}"
                )
            if "StatsCollector has been attached" in log_path.read_text():
                _log("collector attached")
                break
            time.sleep(0.5)
        else:
            stop_tee.set()
            tee.join(timeout=5)
            pytest.fail(f"timeout waiting for BPF attach:\n{log_path.read_text()}")

        for i in range(3):
            _log(f"burst {i + 1}/3: {WORKER_COUNT} NFS readers")
            _burst_nfs_readers(loopback_nfs["mount"], WORKER_COUNT)
            time.sleep(COLLECT_INTERVAL + 0.5)

        _log("waiting for final drain interval")
        time.sleep(COLLECT_INTERVAL + 1)
    finally:
        if proc.poll() is None:
            _log("stopping collector (SIGINT)")
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                _log("collector did not exit; killing")
                proc.kill()
                proc.wait(timeout=10)
        stop_tee.set()
        if proc.stdout:
            try:
                proc.stdout.close()
            except Exception:
                pass
        tee.join(timeout=5)

    err = log_path.read_text()
    assert proc.returncode in (0, -signal.SIGINT, 130), (
        f"collector failed rc={proc.returncode}\n{err}"
    )

    assert samples.exists(), f"no samples written; log:\n{err}"
    by_ts = {}
    with samples.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            by_ts.setdefault(str(row.get("TIMESTAMP")), set()).add(row.get("PID"))

    assert by_ts, f"empty samples file; log:\n{err}"
    max_unique = max(len(pids) for pids in by_ts.values())
    _log(
        f"result: max unique PIDs/interval={max_unique} "
        f"(map_size={MAP_SIZE}, workers/burst={WORKER_COUNT}, "
        f"intervals={list(map(len, by_ts.values()))})"
    )
    assert max_unique <= MAP_SIZE, (
        f"interval unique PIDs {max_unique} exceeded counts-map-size={MAP_SIZE}; "
        f"per-ts={[len(v) for v in by_ts.values()]}. log:\n{err}"
    )
    assert max_unique < WORKER_COUNT, (
        f"expected crop below worker count {WORKER_COUNT}, got {max_unique}"
    )
    # Failed inserts are counted in BPF (counts_drops); warn only when drops > 0.
    assert "counts map full: dropped" in err, (
        f"expected drop warning at counts-map-size={MAP_SIZE}; log:\n{err}"
    )
