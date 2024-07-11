#!/usr/bin/env python
# @lint-avoid-python-3-compatibility-imports
#
# nfsops:  Track NFS operations by process.
#          For Linux, uses BCC, eBPF.
#
# uses in-kernel eBPF maps to store per process summaries for efficiency.

import os, argparse, signal, psutil
from threading import Thread
from time import sleep
from datetime import datetime
from pathlib import Path
from bcc import BPF
os.environ['PROMETHEUS_DISABLE_CREATED_SERIES'] = "1"
import prometheus_client as prom
from prometheus_client.registry import Collector
from prometheus_client.core import GaugeMetricFamily

STATKEYS = {
        "OPEN_COUNT":       "Number of NFS OPEN calls",
        "OPEN_ERRORS":      "Number of NFS OPEN errors",
        "OPEN_DURATION":    "Total NFS OPEN duration (in seconds)",
        "CLOSE_COUNT":      "Number of NFS CLOSE calls",
        "CLOSE_ERRORS":     "Number of NFS CLOSE errors",
        "CLOSE_DURATION":   "Total NFS CLOSE duration (in seconds)",
        "READ_COUNT":       "Number of NFS READ calls",
        "READ_ERRORS":      "Number of NFS READ errors",
        "READ_DURATION":    "Total NFS READ duration (in seconds)",
        "READ_BYTES":       "Total NFS READ bytes",
        "WRITE_COUNT":      "Number of NFS WRITE calls",
        "WRITE_ERRORS":     "Number of NFS WRITE errors",
        "WRITE_DURATION":   "Total NFS WRITE duration (in seconds)",
        "WRITE_BYTES":      "Total NFS WRITE bytes",
        "GETATTR_COUNT":    "Number of NFS GETATTR calls",
        "GETATTR_ERRORS":   "Number of NFS GETATTR errors",
        "GETATTR_DURATION": "Total NFS GETATTR duration (in seconds)",
        "SETATTR_COUNT":    "Number of NFS SETATTR calls",
        "SETATTR_ERRORS":   "Number of NFS SETATTR errors",
        "SETATTR_DURATION": "Total NFS SETATTR duration (in seconds)",
        "FLUSH_COUNT":      "Number of NFS FLUSH calls",
        "FLUSH_ERRORS":     "Number of NFS FLUSH errors",
        "FLUSH_DURATION":   "Total NFS FLUSH duration (in seconds)",
        "FSYNC_COUNT":      "Number of NFS FSYNC calls",
        "FSYNC_ERRORS":     "Number of NFS FSYNC errors",
        "FSYNC_DURATION":   "Total NFS FSYNC duration (in seconds)",
        "LOCK_COUNT":       "Number of NFS LOCK calls",
        "LOCK_ERRORS":      "Number of NFS LOCK errors",
        "LOCK_DURATION":    "Total NFS LOCK duration (in seconds)",
        "MMAP_COUNT":       "Number of NFS MMAP calls",
        "MMAP_ERRORS":      "Number of NFS MMAP errors",
        "MMAP_DURATION":    "Total NFS MMAP duration (in seconds)",
        "READDIR_COUNT":    "Number of NFS READDIR calls",
        "READDIR_ERRORS":   "Number of NFS READDIR errors",
        "READDIR_DURATION": "Total NFS READDIR duration (in seconds)",
        "CREATE_COUNT":     "Number of NFS CREATE calls",
        "CREATE_ERRORS":    "Number of NFS CREATE errors",
        "CREATE_DURATION":  "Total NFS CREATE duration (in seconds)",
        "LINK_COUNT":       "Number of NFS LINK calls",
        "LINK_ERRORS":      "Number of NFS LINK errors",
        "LINK_DURATION":    "Total NFS LINK duration (in seconds)",
        "UNLINK_COUNT":     "Number of NFS UNLINK calls",
        "UNLINK_ERRORS":    "Number of NFS UNLINK errors",
        "UNLINK_DURATION":  "Total NFS UNLINK duration (in seconds)",
        "SYMLINK_COUNT":    "Number of NFS SYMLINK calls",
        "SYMLINK_ERRORS":   "Number of NFS SYMLINK errors",
        "SYMLINK_DURATION": "Total NFS SYMLINK duration (in seconds)",
        "LOOKUP_COUNT":     "Number of NFS LOOKUP calls",
        "LOOKUP_ERRORS":    "Number of NFS LOOKUP errors",
        "LOOKUP_DURATION":  "Total NFS LOOKUP duration (in seconds)",
        "RENAME_COUNT":     "Number of NFS RENAME calls",
        "RENAME_ERRORS":    "Number of NFS RENAME errors",
        "RENAME_DURATION":  "Total NFS RENAME duration (in seconds)",
        "ACCESS_COUNT":     "Number of NFS ACCESS calls",
        "ACCESS_ERRORS":    "Number of NFS ACCESS errors",
        "ACCESS_DURATION":  "Total NFS ACCESS duration (in seconds)",
        "LISTXATTR_COUNT":  "Number of NFS LISTXATTR calls",
        "LISTXATTR_ERRORS": "Number of NFS LISTXATTR errors",
        "LISTXATTR_DURATION": "Total NFS LISTXATTR duration (in seconds)",
}

class MountsMap:
    def __init__(self):
        self.map = {}
        self.refresh_map()

    def refresh_map(self):
        mountmap = {}
        if not Path("/sys/fs/nfs").exists():
            return
        for f in os.listdir("/sys/fs/nfs"):
            if f == "net":
                continue
            mountmap[f] = self._findmount(f)
        self.map = mountmap

    def devt_to_str(self, st_dev):
        MINORBITS = 20
        return "{}:{}".format(st_dev >> MINORBITS, st_dev & 2**MINORBITS-1)

    def _findmount(self, devname):
        for p in psutil.disk_partitions(all=True):
            if 'nfs' not in p.fstype:
                continue
            devt = self.devt_to_str(os.stat(p.mountpoint).st_dev)
            if devname == devt:
                return p.mountpoint
        print("WARNING: No mountpoint found for devt {}".format(devname))
        return ""

    def get_mountpoint(self, st_dev):
        dev = self.devt_to_str(st_dev)
        try:
            return self.map[dev]
        except KeyError:
            self.refresh_map()
            if dev in self.map.keys():
                return self.map[dev]
        print("WARNING: No mountpoint found for devt {}".format(dev))
        return ""


class PidEnvMap:
    """
    Map interface of pid and the dictionary of the tracked environment
    variables.
    """
    def __init__(self, vaccum_interval=600):
        self.pidmap = {}
        self.vaccum_interval = vaccum_interval
        self.start = datetime.now()

    def vaccum(self):
        for pid in list(self.pidmap):
            if not Path("/proc/%s/environ" % pid).exists():
                del self.pidmap[pid]
        self.start = datetime.now()
        if debug:
            print("PidEnvMap: vaccumed...")
            print(self.pidmap)

    def vaccum_if_needed(self):
        if (datetime.now() - self.start).total_seconds() > self.vaccum_interval:
            self.vaccum()

    def insert(self, pid, envs):
        self.pidmap[str(pid)] = envs
        if debug:
            print("PidEnvMap: insert pid[%d]" % pid)
            print(self.pidmap)

    def get(self, pid):
        try:
            return self.pidmap[str(pid)]
        except:
            if debug:
                print("pid %d not found" % pid)
                print(self.pidmap)
            return {}

class ScreenOutputTarget:
    def __init__(self):
        pass

    def output(self, statistics):
        for stat in statistics:
            print(stat)


def get_pid_envs(cpu, data, size):
    data = bpf["events"].event(data)
    try:
        environ = open("/proc/%d/environ" % data.pid).read().split('\x00')[:-1]
    except:
        return

    def match(envs, env):
        for e in envs:
            if env.startswith(e):
                return True
        return False

    envs = {env.split('=')[0]:env.split('=')[1] for env in environ if match(args.envs, env)}
    if envs:
        pidEnvMap.insert(data.pid, envs)

class EnvTracer:
    """
    Tracer traps pid execution and collects the existance of the tracked
    environment variables.
    """
    def __init__(self, envs, bpf, PidEnvMap):
        self.envs = envs
        self.b = bpf
        self.PidEnvMap = PidEnvMap

    def start(self):
        self.b["events"].open_perf_buffer(get_pid_envs)
        self.t = Thread(target=self.trace_pid_exec)
        self.t.daemon = True
        self.t.start()

    def attach(self):
        if self.envs:
            self.b.attach_kretprobe(event=self.b.get_syscall_fnname("execve"), fn_name="trace_execve")

    def trace_pid_exec(self):
        while True:
            self.b.perf_buffer_poll()
            self.PidEnvMap.vaccum_if_needed()


class StatsCollector:
    """
    Tracer traps pid execution and collects the existance of the tracked
    environment variables.
    """
    def __init__(self, bpf, PidEnvMap, OutputTarget, interval=60):
        self.b = bpf
        self.PidEnvMap = PidEnvMap
        self.interval = interval
        self.hostname = os.getenv("HOSTNAME")
        self.output_target = OutputTarget
        # check whether hash table batch ops is supported
        self.batch_ops = True if BPF.kernel_struct_has_field(b'bpf_map_ops',
            b'map_lookup_and_delete_batch') == 1 else False

    def attach(self):
        # file attachments
        self.b.attach_kprobe(event="nfs_file_read", fn_name="trace_nfs_file_read")               # updates read count,rbytes
        self.b.attach_kretprobe(event="nfs_file_read", fn_name="trace_nfs_file_read_ret")        # updates read errors,duration
        self.b.attach_kprobe(event="nfs_file_write", fn_name="trace_nfs_file_write")             # updates write count,bytes
        self.b.attach_kretprobe(event="nfs_file_write", fn_name="trace_nfs_file_write_ret")      # updates write errors,duration
        self.b.attach_kprobe(event="nfs_file_open", fn_name="trace_nfs_file_open")               # updates open count
        self.b.attach_kretprobe(event="nfs_file_open", fn_name="trace_nfs_file_open_ret")        # updates open errors,duration
        self.b.attach_kprobe(event="nfs_getattr", fn_name="trace_nfs_getattr")                   # updates getattr count
        self.b.attach_kretprobe(event="nfs_getattr", fn_name="trace_nfs_getattr")                # updates getattr errors,duration
        self.b.attach_kprobe(event="nfs_setattr", fn_name="trace_nfs_setattr")                   # updates setattr count
        self.b.attach_kretprobe(event="nfs_setattr", fn_name="trace_nfs_setattr_ret")            # updates setattr errors,duration
        self.b.attach_kprobe(event="nfs_file_flush", fn_name="trace_nfs_file_flush")             # updates flush count
        self.b.attach_kretprobe(event="nfs_file_flush", fn_name="trace_nfs_file_flush_ret")      # updates flush errors,duration
        self.b.attach_kprobe(event="nfs_file_fsync", fn_name="trace_nfs_file_fsync")             # updates fsync count
        self.b.attach_kretprobe(event="nfs_file_fsync", fn_name="trace_nfs_file_fsync_ret")      # updates fsync errors,duration
        # (XXX: should we track unlocks as well)
        self.b.attach_kprobe(event="nfs_lock", fn_name="trace_nfs_lock")                         # updates lock count
        self.b.attach_kretprobe(event="nfs_lock", fn_name="trace_nfs_lock_ret")                  # updates lock errors,duration
        self.b.attach_kprobe(event="nfs_flock", fn_name="trace_nfs_lock")                        # updates lock count
        self.b.attach_kretprobe(event="nfs_flock", fn_name="trace_nfs_lock_ret")                 # updates lock errors,duration
        self.b.attach_kprobe(event="nfs_file_splice_read", fn_name="trace_nfs_file_splice_read") # updates reads,rbytes
        self.b.attach_kprobe(event="nfs_file_mmap", fn_name="trace_nfs_file_mmap")               # updates mmap count
        self.b.attach_kretprobe(event="nfs_file_mmap", fn_name="trace_nfs_file_mmap_ret")        # updates mmap errors,duration
        self.b.attach_kprobe(event="nfs_file_release", fn_name="trace_nfs_file_release")         # updates close count
        self.b.attach_kretprobe(event="nfs_file_release", fn_name="trace_nfs_file_release_ret")  # updates close errors,duration
        # directory attachments
        self.b.attach_kprobe(event="nfs_readdir", fn_name="trace_nfs_readdir")                   # updates readdir count
        self.b.attach_kretprobe(event="nfs_readdir", fn_name="trace_nfs_readdir_ret")            # updates readdir errors,duration
        self.b.attach_kprobe(event="nfs_create", fn_name="trace_nfs_create")                     # updates create count
        self.b.attach_kretprobe(event="nfs_create", fn_name="trace_nfs_create_ret")              # updates create errors,duration
        self.b.attach_kprobe(event="nfs_link", fn_name="trace_nfs_link")                         # updates link count
        self.b.attach_kretprobe(event="nfs_link", fn_name="trace_nfs_link_ret")                  # updates link errors,duration
        self.b.attach_kprobe(event="nfs_unlink", fn_name="trace_nfs_unlink")                     # updates unlink count
        self.b.attach_kretprobe(event="nfs_unlink", fn_name="trace_nfs_unlink_ret")              # updates unlink errors,duration
        self.b.attach_kprobe(event="nfs_symlink", fn_name="trace_nfs_symlink")                   # updates symlink count
        self.b.attach_kretprobe(event="nfs_symlink", fn_name="trace_nfs_symlink_ret")            # updates symlink errors,duration
        self.b.attach_kprobe(event="nfs_lookup", fn_name="trace_nfs_lookup")                     # updates lookup count
        self.b.attach_kretprobe(event="nfs_lookup", fn_name="trace_nfs_lookup_ret")              # updates lookup errors,duration
        self.b.attach_kprobe(event="nfs_rename", fn_name="trace_nfs_rename")                     # updates rename count
        self.b.attach_kretprobe(event="nfs_rename", fn_name="trace_nfs_rename_ret")              # updates rename errors,duration
        self.b.attach_kprobe(event="nfs_do_access", fn_name="trace_nfs_do_access")               # updates access
        self.b.attach_kprobe(event="nfs_do_access", fn_name="trace_nfs_do_access_ret")           # updates access errors,duration
        # nfs4 attachments
        if BPF.get_kprobe_functions(b'nfs4_file_open'):
            self.b.attach_kprobe(event="nfs4_file_open", fn_name="trace_nfs_file_open")         # updates open count
            self.b.attach_kretprobe(event="nfs4_file_open", fn_name="trace_nfs_file_open_ret")  # updates open errors,duration
            self.b.attach_kprobe(event="nfs4_file_flush", fn_name="trace_nfs_file_flush")       # updates flush count
            self.b.attach_kretprobe(event="nfs4_file_flush", fn_name="trace_nfs_file_flush_ret")# updates flush errors,duration
        if BPF.get_kprobe_functions(b'nfs4_listxattr'):
            self.b.attach_kprobe(event="nfs4_listxattr", fn_name="trace_nfs_listxattrs")        # updates listxattr count
            self.b.attach_kretprobe(event="nfs4_listxattr", fn_name="trace_nfs_listxattrs_ret") # updates listxattr errors,duration
        if BPF.get_kprobe_functions(b'nfs3_listxattr'):
            self.b.attach_kprobe(event="nfs3_listxattr", fn_name="trace_nfs_listxattrs")        # updates listxattr count
            self.b.attach_kretprobe(event="nfs3_listxattr", fn_name="trace_nfs_listxattrs_ret") # updates listxattr errors,duration

    def start(self):
        self.t = Thread(target=self.poll_stats)
        self.t.daemon = True
        self.t.start()

    def stat_match(self, stat, new):
        return stat["PID"] == new["PID"] and stat["MOUNT"] == new["MOUNT"]

    def combine_stat(self, stat, new):
        for key in STATKEYS.keys():
            stat[key] += new[key]

    def collect_stats(self):
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("######## SAMPLE: " + timestamp + " ########")

        counts = self.b.get_table("counts")
        statistics = []
        for k, v in (counts.items_lookup_and_delete_batch() if self.batch_ops else counts.items()):
            output = {
                    "TIMESTAMP":        timestamp,
                    "HOSTNAME":         self.hostname,
                    "PID":              k.tgid, # real pid is the thread-group id
                    "UID":              k.uid,
                    "COMM":             k.comm.decode('utf-8', 'replace'),
                    "OPEN_COUNT":       v.open.count,
                    "OPEN_ERRORS":      v.open.errors,
                    "OPEN_DURATION":    v.open.duration,
                    "CLOSE_COUNT":      v.close.count,
                    "CLOSE_ERRORS":     v.close.errors,
                    "CLOSE_DURATION":   v.close.duration,
                    "READ_COUNT":       v.read.count,
                    "READ_ERRORS":      v.read.errors,
                    "READ_DURATION":    v.read.duration,
                    "READ_BYTES":       v.rbytes,
                    "WRITE_COUNT":      v.write.count,
                    "WRITE_ERRORS":     v.write.errors,
                    "WRITE_DURATION":   v.write.duration,
                    "WRITE_BYTES":      v.wbytes,
                    "GETATTR_COUNT":    v.getattr.count,
                    "GETATTR_ERRORS":   v.getattr.errors,
                    "GETATTR_DURATION": v.getattr.duration,
                    "SETATTR_COUNT":    v.setattr.count,
                    "SETATTR_ERRORS":   v.setattr.errors,
                    "SETATTR_DURATION": v.setattr.duration,
                    "FLUSH_COUNT":      v.flush.count,
                    "FLUSH_ERRORS":     v.flush.errors,
                    "FLUSH_DURATION":   v.flush.duration,
                    "FSYNC_COUNT":      v.fsync.count,
                    "FSYNC_ERRORS":     v.fsync.errors,
                    "FSYNC_DURATION":   v.fsync.duration,
                    "LOCK_COUNT":       v.lock.count,
                    "LOCK_ERRORS":      v.lock.errors,
                    "LOCK_DURATION":    v.lock.duration,
                    "MMAP_COUNT":       v.mmap.count,
                    "MMAP_ERRORS":      v.mmap.errors,
                    "MMAP_DURATION":    v.mmap.duration,
                    "READDIR_COUNT":    v.readdir.count,
                    "READDIR_ERRORS":   v.readdir.errors,
                    "READDIR_DURATION": v.readdir.duration,
                    "CREATE_COUNT":     v.create.count,
                    "CREATE_ERRORS":    v.create.errors,
                    "CREATE_DURATION":  v.create.duration,
                    "LINK_COUNT":       v.link.count,
                    "LINK_ERRORS":      v.link.errors,
                    "LINK_DURATION":    v.link.duration,
                    "UNLINK_COUNT":     v.unlink.count,
                    "UNLINK_ERRORS":    v.unlink.errors,
                    "UNLINK_DURATION":  v.unlink.duration,
                    "SYMLINK_COUNT":    v.symlink.count,
                    "SYMLINK_ERRORS":   v.symlink.errors,
                    "SYMLINK_DURATION": v.symlink.duration,
                    "LOOKUP_COUNT":     v.lookup.count,
                    "LOOKUP_ERRORS":    v.lookup.errors,
                    "LOOKUP_DURATION":  v.lookup.duration,
                    "RENAME_COUNT":     v.rename.count,
                    "RENAME_ERRORS":    v.rename.errors,
                    "RENAME_DURATION":  v.rename.duration,
                    "ACCESS_COUNT":     v.access.count,
                    "ACCESS_ERRORS":    v.access.errors,
                    "ACCESS_DURATION":  v.access.duration,
                    "LISTXATTR_COUNT":  v.listxattr.count,
                    "LISTXATTR_ERRORS": v.listxattr.errors,
                    "LISTXATTR_DURATION":v.listxattr.duration,
                    "TAGS":         self.PidEnvMap.get(k.tgid),
                    "MOUNT":        mountsMap.get_mountpoint(k.sbdev),
            }

            # search of we have multiple threads (pid) of the same process (tgid)
            match = list(filter(lambda stat: self.stat_match(stat, output), statistics))
            if match:
                if debug:
                    print("StatsCollector: combined stat for PID %d (thread %d)" %
                            (output["PID"], k.pid))
                self.combine_stat(match[0], output)
            else:
                statistics.append(output)

        if not self.batch_ops:
            counts.clear()

        return statistics

    def poll_stats(self):
        while True:
            try:
                sleep(self.interval)
            except:
                return
            statistics = self.collect_stats()
            self.output_target.output(statistics)


class PromCollector(Collector):
    def __init__(self, StatsCollector):
        self.collector = StatsCollector

    def _create_gauge(self, name, help_text, labels, value):
        gauge = GaugeMetricFamily(name, help_text, labels=labels.keys())
        gauge.add_metric(labels.values(), value)
        return gauge

    def collect(self):
        statistics = self.collector.collect_stats()
        for stat in statistics:
            labels_kwargs = {
                "HOSTNAME": stat["HOSTNAME"],
                "UID": str(stat["UID"]),
                "COMM": stat["COMM"],
                "MOUNT": stat["MOUNT"],
            }
            if args.envs:
                for env in args.envs:
                    try:
                        labels_kwargs.update({env: stat["TAGS"][env]})
                    except:
                        labels_kwargs.update({env: ""})
            for s in STATKEYS.keys():
                if "DURATION" in s: # adjust duration samples to seconds
                    stat[s] = stat[s] / 1000000000
                yield self._create_gauge(s, STATKEYS[s], labels_kwargs, stat[s])


class PrometheusExporter(StatsCollector):
    """
    Tracer traps pid execution and collects the existance of the tracked
    environment variables.
    """
    def __init__(self, bpf, PidEnvMap, OutputTarget, port=9000, interval=60):
        super().__init__(bpf, PidEnvMap, OutputTarget, interval)
        self.port = port
        self.collector = PromCollector(self)


    def start(self):
        prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
        prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
        prom.REGISTRY.unregister(prom.GC_COLLECTOR)
        prom.REGISTRY.register(self.collector)
        prom.start_http_server(port=self.port, addr="::")

    def stat_match(self, stat, new):
        return stat["COMM"] == new["COMM"] and  \
                stat["TAGS"] == new["TAGS"] and \
                stat["MOUNT"] == new["MOUNT"]


def split_list(values):
    return values.split(',')

def sighandler(signum, frame):
    exit(0)

debug = 0
bpf = None
pidEnvMap = None
mountsMap = None
if __name__ == "__main__":
    # arguments
    examples ="""examples:
        ./nfsops.py                         # NFS operations trace every 5 seconds
        ./nfsops.py -i 60                   # NFS operations trace every 60 seconds
        ./nfsops.py -v 700                  # Vaccume every 700 seconds
        ./nfsops.py -e JOBID,SCHEDID        # Decorate samples with env variables JOBID and SCHEDID
        ./nfsops.py --ebpf                  # dump ebpf program text and exit
        ./nfsops.py -P                      # start a prometheus endpoint (default address is [::]:<port>
    """
    parser = argparse.ArgumentParser(
        description="Track NFS statistics by process",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=examples)
    parser.add_argument("-i", "--interval", default=5,
            help="output interval, in seconds")
    parser.add_argument("-v", "--vaccum", default=600,
            help="pid env map vaccum interval, in seconds")
    parser.add_argument("-e", "--envs", type=split_list,
            help="comma separated list of env vars")
    parser.add_argument("--debug", action="store_true",
            help="enable debug prints")
    parser.add_argument("--ebpf", action="store_true",
            help="dump BPF program text and exit")
    parser.add_argument("-P", "--prometheus", action="store_true",
            help="start a prometheus exporter")
    parser.add_argument("-p", "--prometheus-port", default=9000,
            help="start a prometheus exporter")
    parser.add_argument("-o", "--output", choices=["screen"],
            default="screen", help="samples output target")
    args = parser.parse_args()
    debug=args.debug

    # read BPF program text
    bpf_text = open("nfsops.c").read()
    if debug or args.ebpf:
        print(bpf_text)
        if args.ebpf:
            exit()

    signal.signal(signal.SIGTERM, sighandler)
    signal.signal(signal.SIGINT, sighandler)

    # probe needed modules (nfsv4 autoloads nfs)
    os.system("/usr/sbin/modprobe kheaders")
    os.system("/usr/sbin/modprobe nfsv4")
    # initialize BPF
    bpf = BPF(text=bpf_text)

    pidEnvMap = PidEnvMap(vaccum_interval=args.vaccum)
    mountsMap = MountsMap()
    # if no envs are given, no need to track
    if args.envs:
        envTracer = EnvTracer(envs=args.envs, bpf=bpf, PidEnvMap=pidEnvMap)
        envTracer.attach()
        envTracer.start()

    if args.output == "screen":
        output = ScreenOutputTarget()
    if args.prometheus:
        collector = PrometheusExporter(bpf=bpf, PidEnvMap=pidEnvMap,
                                OutputTarget=output, interval=args.interval)
    else:
        collector = StatsCollector(bpf=bpf, PidEnvMap=pidEnvMap,
                                OutputTarget=output, interval=args.interval)
    collector.attach()
    collector.start()

    while True:
        try:
            sleep(args.interval)
        except:
            print("bye bye")
            exit(0)
