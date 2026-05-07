# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

"""
Compute byte offset from struct vfsmount* to struct mount's mnt_id field.

Uses only /sys/kernel/btf/vmlinux (requires CONFIG_DEBUG_INFO_BTF) + pahole.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

_SYSFS_BTF_VMLINUX = Path("/sys/kernel/btf/vmlinux")

_PAHOLE_PATHS = (
    "/usr/bin/pahole",
    "/usr/sbin/pahole",
    "/bin/pahole",
)


def _find_pahole() -> str | None:
    w = shutil.which("pahole")
    if w:
        return w
    for p in _PAHOLE_PATHS:
        if Path(p).is_file():
            return p
    return None


def vfsmount_to_mnt_id_delta() -> int:
    """
    Return (offsetof(struct mount, mnt_id) - offsetof(struct mount, mnt)).

    Raises FileNotFoundError if sysfs BTF is missing, EnvironmentError if pahole
    is missing, RuntimeError if layout cannot be parsed.
    """
    if not _SYSFS_BTF_VMLINUX.is_file():
        raise FileNotFoundError(
            f"Kernel BTF not found at {_SYSFS_BTF_VMLINUX} (need CONFIG_DEBUG_INFO_BTF)"
        )
    pahole = _find_pahole()
    if not pahole:
        raise EnvironmentError(
            "The 'pahole' utility is required (install package 'dwarves'; "
            "systemd services may need PATH=/usr/bin:/usr/sbin)"
        )
    try:
        out = subprocess.check_output(
            [pahole, "-C", "mount", str(_SYSFS_BTF_VMLINUX)],
            text=True,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"pahole failed to describe struct mount: {e.stderr or e}"
        ) from e

    off_mnt = None
    off_mnt_id = None
    for line in out.splitlines():
        if "struct vfsmount" in line and " mnt;" in line:
            m = re.search(r"/\*\s*(\d+)\s+\d+\s*\*/", line)
            if m:
                off_mnt = int(m.group(1))
        if re.search(r"\bmnt_id\s*;", line):
            m = re.search(r"/\*\s*(\d+)\s+\d+\s*\*/", line)
            if m:
                off_mnt_id = int(m.group(1))

    if off_mnt is None or off_mnt_id is None:
        raise RuntimeError(
            "Could not parse struct mount layout from pahole output "
            f"(mnt={off_mnt}, mnt_id={off_mnt_id})"
        )
    return off_mnt_id - off_mnt
