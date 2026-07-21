import datetime

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from vnfs_collector.nfsops import (
    group_stats,
    filter_stats,
    anonymize_stats,
    MountInfo,
    MountsMap,
    PidEnvMap,
    MaintenanceScheduler,
)
from tests.conftest import ROOT

import pandas.testing as pdt


def test_group_stats_by_comm(data):
    # pid is squashed. TAGS not included in grouping for testing purposes
    grouped_by_comm = group_stats(data, ["MOUNT", "COMM"])
    total_readdr_duration = data.READDIR_DURATION.sum()

    assert len(grouped_by_comm) == 2
    first_row = grouped_by_comm.iloc[0]
    second_row = grouped_by_comm.iloc[1]

    assert first_row.PID == 2
    assert second_row.PID == 2
    assert first_row.MOUNT == second_row.MOUNT == "/mnt"
    assert first_row.COMM == "bash"
    assert second_row.COMM == "ls"

    assert first_row.OPEN_COUNT == 1
    assert second_row.OPEN_COUNT == 5
    # Only 1 raw is grouped by 'bash' comm.
    assert first_row.READDIR_DURATION == 0.000100742
    # Rest of raws are grouped by 'ls' comm.
    # We can calculate the duration by subtracting the first row duration from total duration.
    assert (
        second_row.READDIR_DURATION
        == total_readdr_duration - first_row.READDIR_DURATION
    )


def test_group_stats_by_pid(data):
    # We have 2 different pids in test data. 2 with value 1 and 2 with value 2.
    # TAGS not included in grouping for testing purposes
    grouped_by_comm = group_stats(data, ["MOUNT", "PID"])
    total_readdr_duration = data.READDIR_DURATION.sum()
    assert len(grouped_by_comm) == 2
    first_row = grouped_by_comm.iloc[0]
    second_row = grouped_by_comm.iloc[1]

    assert first_row.PID == 1
    assert second_row.PID == 2
    assert first_row.MOUNT == second_row.MOUNT == "/mnt"
    assert first_row.COMM == "ls"
    assert second_row.COMM == "ls"

    assert first_row.OPEN_COUNT == 4
    assert second_row.OPEN_COUNT == 2

    assert first_row.READDIR_DURATION == 0.0016027250000000002
    assert second_row.READDIR_DURATION == 0.000201484


def test_filter_stats_any(data):
    """Test at least one of the tags is present in the row"""
    filtered = filter_stats(data, filter_tags=["FOO"], filter_condition="any")
    assert len(filtered) == 4
    pdt.assert_frame_equal(data, filtered)

    filtered = filter_stats(data, filter_tags=["JOB"], filter_condition="any")
    assert len(filtered) == 2

    filtered = filter_stats(data, filter_tags=["TAR"], filter_condition="any")
    assert len(filtered) == 1


def test_filter_stats_all(data):
    """Test all the tags are present in the row"""
    filtered = filter_stats(data, filter_tags=["FOO"], filter_condition="all")
    assert len(filtered) == 4
    pdt.assert_frame_equal(data, filtered)

    filtered = filter_stats(data, filter_tags=["FOO", "JOB"], filter_condition="all")
    assert len(filtered) == 2

    filtered = filter_stats(data, filter_tags=["FOO", "TAR"], filter_condition="all")
    assert len(filtered) == 1

    # No rows with such combination of tags
    filtered = filter_stats(data, filter_tags=["TAR", "JOB"], filter_condition="all")
    assert len(filtered) == 0


@pytest.mark.parametrize("remote_path", ["/", "/mnt", "/mnt/test"])
@pytest.mark.parametrize(
    "addr",
    [
        "::",
        "[::]",
        "172.17.0.2",
        "[2001::1]",
        "mydomain",
        "mydomain.com",
        "mydomain.com:8080",
    ],
)
def test_remote_path(addr, remote_path):
    # Test with a standard device string
    mount_info = MountInfo("/mnt/test", f"{addr}:{remote_path}")
    assert mount_info.remote_path == remote_path


@patch.object(MountsMap, "get_mountinfo", MagicMock(return_value=f"{ROOT}/data/mounts_self"))
def test_refresh_map_mountinfo():
    mounts_map = MountsMap()
    mounts_map.refresh_map_mountinfo("self")
    by_mnt_id = mounts_map.pid_maps["self"]["by_mnt_id"]
    by_devt = mounts_map.pid_maps["self"]["by_devt"]
    assert len(by_mnt_id) == 2
    assert len(by_devt) == 2
    assert "2585" in by_mnt_id
    assert "446" in by_mnt_id
    assert "0:321" in by_devt
    assert "0:69" in by_devt
    assert by_mnt_id["2585"].mountpoint == "/mnt/test"
    assert by_mnt_id["446"].mountpoint == "/mnt/test2"
    assert by_devt["0:321"].device == "172.17.0.3:/"
    assert by_devt["0:69"].device == "172.17.0.2:/"


@patch.object(MountsMap, "get_mountinfo", MagicMock(return_value=f"{ROOT}/data/mounts_self"))
def test_get_mountpoint_ignores_mnt_id_when_segmentation_disabled():
    mounts_map = MountsMap(mnt_id_segmentation=False)
    assert mounts_map.get_mountpoint(2585, 999, "self") is None
    mount_info = mounts_map.get_mountpoint(2585, 321, "self")
    assert mount_info.mountpoint == "/mnt/test"


@patch.object(MountsMap, "get_mountinfo", MagicMock(return_value=f"{ROOT}/data/mounts_self"))
def test_get_mountpoint_sbdev_fallback():
    mounts_map = MountsMap()
    mount_info1 = mounts_map.get_mountpoint(0, 321, "self")
    mount_info2 = mounts_map.get_mountpoint(0, 69, "self")

    assert mount_info1.mountpoint == "/mnt/test"
    assert mount_info1.device == "172.17.0.3:/"
    assert mount_info2.mountpoint == "/mnt/test2"
    assert mount_info2.device == "172.17.0.2:/"


def _mountinfo_side_effect(pid):
    if pid == "self":
        return f"{ROOT}/data/mounts_self"
    elif pid == "162148":
        return f"{ROOT}/data/mounts_162148"
    elif pid == "162149":
        return f"{ROOT}/data/mounts_162149"
    raise NotImplementedError()


@patch.object(MountsMap, "get_mountinfo", side_effect=_mountinfo_side_effect)
def test_get_mount_info_from_different_mountinfo_files(*_):
    mounts_map = MountsMap()
    mount_info = mounts_map.get_mountpoint(2585, 321, "self")
    assert mount_info.remote_path == "/"
    assert mount_info.mountpoint == "/mnt/test"
    assert set(mounts_map.pid_maps["self"]["by_mnt_id"]) == {'2585', '446'}
    assert set(mounts_map.pid_maps["self"]["by_devt"]) == {'0:321', '0:69'}

    # Doesn't exist in mounts_self file (sbdev-only lookup misses too)
    mount_info = mounts_map.get_mountpoint(0, 420, "self")
    assert mount_info is None
    assert set(mounts_map.pid_maps["self"]["by_mnt_id"]) == {'2585', '446'}

    mount_info = mounts_map.get_mountpoint(3518, 420, "162148")
    assert mount_info.remote_path == "/remote"
    assert mount_info.mountpoint == "/mnt/mydir"
    assert set(mounts_map.pid_maps["162148"]["by_mnt_id"]) == {'3518'}
    assert "162149" not in mounts_map.pid_maps

    # Same mnt_id/devt, different mount namespace -> separate per-pid maps
    mount_info = mounts_map.get_mountpoint(3518, 420, "162149")
    assert mount_info.remote_path == "/remote"
    assert mount_info.mountpoint == "/mnt/mydir2"
    assert mounts_map.pid_maps["162148"]["by_mnt_id"]["3518"].mountpoint == "/mnt/mydir"
    assert mounts_map.pid_maps["162149"]["by_mnt_id"]["3518"].mountpoint == "/mnt/mydir2"


@patch.object(MountsMap, "get_mountinfo", side_effect=_mountinfo_side_effect)
def test_mounts_map_purge_stale_pids(*_):
    from pathlib import Path

    mounts_map = MountsMap()
    mounts_map.get_mountpoint(3518, 420, "162148")
    assert "162148" in mounts_map.pid_maps

    real_exists = Path.exists

    def exists(self):
        if str(self) == "/proc/162148":
            return False
        return real_exists(self)

    with patch.object(Path, "exists", exists):
        mounts_map.purge_stale_pids()

    assert "162148" not in mounts_map.pid_maps
    assert "self" in mounts_map.pid_maps


def test_maintenance_scheduler_purges_mounts_without_env_tracer():
    from pathlib import Path

    mounts_map = MountsMap()
    mounts_map.pid_maps["162148"] = {"by_mnt_id": {}, "by_devt": {}}
    pid_env_map = PidEnvMap(mounts_map=mounts_map, vaccum_interval=60)
    scheduler = MaintenanceScheduler(
        mounts_map=mounts_map,
        pid_env_map=pid_env_map,
        vaccum_interval=60,
        env_tracer=None,
    )

    real_exists = Path.exists

    def exists(self):
        if str(self) == "/proc/162148":
            return False
        return real_exists(self)

    with patch.object(Path, "exists", exists):
        scheduler._purge_mounts_if_needed()
        assert "162148" in mounts_map.pid_maps

        scheduler._mounts_start = datetime.datetime.now() - datetime.timedelta(seconds=61)
        scheduler._purge_mounts_if_needed()

    assert "162148" not in mounts_map.pid_maps
    assert "self" in mounts_map.pid_maps


def test_pid_env_map_vaccum_does_not_purge_mounts():
    mounts_map = MountsMap()
    mounts_map.pid_maps["162148"] = {"by_mnt_id": {}, "by_devt": {}}
    pid_env_map = PidEnvMap(mounts_map=mounts_map, vaccum_interval=60)
    pid_env_map.pidmap["162148"] = {"JOBID": "1"}

    with patch.object(mounts_map, "purge_stale_pids") as purge:
        with patch("vnfs_collector.nfsops.Path") as mock_path:
            mock_path.return_value.exists.return_value = False
            pid_env_map.vaccum()
        purge.assert_not_called()
    assert "162148" not in pid_env_map.pidmap


@patch("vnfs_collector.nfsops.BPF")
def test_stats_collector_attach_respects_flags(mock_bpf):
    from vnfs_collector.nfsops import StatsCollector

    mock_bpf.get_kprobe_functions.return_value = []
    bpf = mock_bpf.return_value
    collector = StatsCollector(
        MagicMock(),
        bpf,
        MagicMock(),
        MagicMock(),
        use_mnt_id_attribution=False,
        track_lookup_access=False,
    )
    collector.attach()

    attached = [c.kwargs["event"] for c in bpf.attach_kprobe.call_args_list]
    assert "nfs_do_access" not in attached
    assert "nfs_lookup_revalidate" not in attached
    assert "security_path_mkdir" not in attached
    assert "nfs_file_read" in attached


def test_anonymize_valid_fields(data):
    data_copy = data.copy(deep=True)
    anon_fields = ["MOUNT", "COMM", "TAGS"]
    result = anonymize_stats(data_copy, anon_fields)
    pdt.assert_series_equal(
        result.MOUNT, pd.Series(["--", "--", "--", "--"], name="MOUNT")
    )
    pdt.assert_series_equal(
        result.COMM, pd.Series(["--", "--", "--", "--"], name="COMM")
    )
    pdt.assert_series_equal(
        result.TAGS,
        pd.Series(
            [
                {"FOO": "--", "JOB": "--"},
                {"FOO": "--", "TAR": "--"},
                {"FOO": "--"},
                {"FOO": "--", "JOB": "--"},
            ],
            name="TAGS",
        ),
    )
