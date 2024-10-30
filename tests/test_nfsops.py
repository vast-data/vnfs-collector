import pandas as pd
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
import vast_client_tools.nfsops as nfsops
from vast_client_tools.nfsops import (
    group_stats,
    filter_stats,
    anonymize_stats,
    MountInfo,
    MountsMap,
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


mountpoint_to_dev = {
    '/mnt/test1': 123456,
    '/mnt/test2': 654321
}

def fake_os_stat(mountpoint):
    """Define fake_os_stat to return different st_dev values based on mountpoint"""
    st_dev = mountpoint_to_dev.get(mountpoint, 0)  # Default to 0 if not found
    return MagicMock(st_dev=st_dev)


fake_disk_partitions = [
    MagicMock(fstype='nfs', mountpoint='/mnt/test1', device='172.17.0.1:/mnt/test1'),
    MagicMock(fstype='nfs', mountpoint='/mnt/test2', device='172.17.0.2:/mnt/test2'),
]


@patch('psutil.disk_partitions', MagicMock(return_value=fake_disk_partitions))
@patch('os.stat', side_effect=lambda mountpoint: fake_os_stat(mountpoint))
def test_refresh_map(mock_stat):
    mounts_map = MountsMap()
    mounts_map.refresh_map()

    devt1 = mounts_map.devt_to_str(123456)
    devt2 = mounts_map.devt_to_str(654321)

    assert devt1 in mounts_map.map
    assert devt2 in mounts_map.map

    assert mounts_map.map[devt1].mountpoint == '/mnt/test1'
    assert mounts_map.map[devt1].device == '172.17.0.1:/mnt/test1'

    assert mounts_map.map[devt2].mountpoint == '/mnt/test2'
    assert mounts_map.map[devt2].device == '172.17.0.2:/mnt/test2'


@patch('psutil.disk_partitions', MagicMock(return_value=fake_disk_partitions))
@patch('os.stat', side_effect=lambda mountpoint: fake_os_stat(mountpoint))
def test_get_mountpoint(mock_stat):

    mounts_map = MountsMap()
    devt1 = mounts_map.devt_to_str(123456)
    devt2 = mounts_map.devt_to_str(654321)

    mount_info1 = mounts_map.get_mountpoint(123456)
    mount_info2 = mounts_map.get_mountpoint(654321)

    assert isinstance(mount_info1, MountInfo)
    assert mount_info1.mountpoint == '/mnt/test1'
    assert mount_info1.device == '172.17.0.1:/mnt/test1'

    assert isinstance(mount_info2, MountInfo)
    assert mount_info2.mountpoint == '/mnt/test2'
    assert mount_info2.device == '172.17.0.2:/mnt/test2'


@patch.object(MountsMap, "get_mountinfo", MagicMock(return_value=f"{ROOT}/data/mounts"))
@patch.object(MountsMap, "refresh_map", MagicMock())
def test_refresh_map_mountinfo():
    mounts_map = MountsMap()
    mounts_map.refresh_map_mountinfo()
    map = mounts_map.map
    assert len(map) == 2
    assert "0:321" in map
    assert "0:69" in map
    assert map["0:321"].mountpoint == "/mnt/test"
    assert map["0:69"].mountpoint == "/mnt/test2"
    assert map["0:321"].device == "172.17.0.3:/"
    assert map["0:69"].device == "172.17.0.2:/"


@patch('psutil.disk_partitions')
@patch('os.stat', side_effect=lambda mountpoint: fake_os_stat(mountpoint))
def test_get_mountpoint_with_missing_device(mock_stat, mock_disk_partitions):
    mock_disk_partitions.return_value = []

    mounts_map = MountsMap()
    devt = mounts_map.devt_to_str(123456)
    mount_info = mounts_map.get_mountpoint(123456)

    assert mount_info is None


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
