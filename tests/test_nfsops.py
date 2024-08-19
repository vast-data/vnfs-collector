import pytest
from unittest.mock import patch
from vast_client_tools.nfsops import group_stats, filter_stats, MountInfo, MountsMap

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

    print(grouped_by_comm)

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


@patch("pathlib.Path.exists", return_value=True)
@patch("os.listdir", return_value=["dev1", "net", "dev2"])
@patch(
    "vast_client_tools.nfsops.MountsMap._findmount",
    return_value=MountInfo("/mnt/test", "172.17.0.2:/mnt/test"),
)
def test_refresh_map(*_, **__):
    mounts_map = MountsMap()
    mounts_map.refresh_map()

    # Check that the map was populated correctly
    assert "dev1" in mounts_map.map
    assert isinstance(mounts_map.map["dev1"], MountInfo)
    assert mounts_map.map["dev1"].device == "172.17.0.2:/mnt/test"
