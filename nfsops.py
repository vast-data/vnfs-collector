#!/usr/bin/env python
# @lint-avoid-python-3-compatibility-imports
#
# nfsops:  Track NFS operations by process.
#          For Linux, uses BCC, eBPF.
#
# uses in-kernel eBPF maps to store per process summaries for efficiency.

import os, argparse, signal
from threading import Thread
from time import sleep
from datetime import datetime
from pathlib import Path
from bcc import BPF

STATKEYS = ["OPEN","CLOSE","READ","RBYTES","WRITE","WBYTES","GETATTR","SETATTR","FLUSH",
        "FSYNC","LOCK","MMAP","READDIR","CREATE","LINK","UNLINK","LOOKUP","RENAME","ACCESS","LISTXATTR"]
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
    def __init__(self, bpf, PidEnvMap, interval=60):
        self.b = bpf
        self.PidEnvMap = PidEnvMap
        self.interval = interval
        self.hostname = os.getenv("HOSTNAME")
        # check whether hash table batch ops is supported
        self.batch_ops = True if BPF.kernel_struct_has_field(b'bpf_map_ops',
            b'map_lookup_and_delete_batch') == 1 else False

    def attach(self):
        # file attachments
        self.b.attach_kprobe(event="nfs_file_read", fn_name="trace_nfs_file_read")               # updates reads,rbytes
        self.b.attach_kprobe(event="nfs_file_write", fn_name="trace_nfs_file_write")             # updates writes,wbytes
        self.b.attach_kprobe(event="nfs_file_open", fn_name="trace_nfs_file_open")               # updates opens
        self.b.attach_kprobe(event="nfs_getattr", fn_name="trace_nfs_getattr")                   # updates getattrs
        self.b.attach_kprobe(event="nfs_setattr", fn_name="trace_nfs_setattr")                   # updates setattrs
        self.b.attach_kprobe(event="nfs_file_flush", fn_name="trace_nfs_file_flush")             # updates flushes
        self.b.attach_kprobe(event="nfs_file_fsync", fn_name="trace_nfs_file_fsync")             # updates fsyncs
        # (XXX: should we track unlocks as well)
        self.b.attach_kprobe(event="nfs_lock", fn_name="trace_nfs_lock")                         # updates locks
        self.b.attach_kprobe(event="nfs_flock", fn_name="trace_nfs_lock")                        # updates locks
        self.b.attach_kprobe(event="nfs_file_splice_read", fn_name="trace_nfs_file_splice_read") # updates reads,rbytes
        self.b.attach_kprobe(event="nfs_file_mmap", fn_name="trace_nfs_file_mmap")               # updates mmaps
        self.b.attach_kprobe(event="nfs_file_release", fn_name="trace_nfs_file_release")         # updates closes
        # directory attachments
        self.b.attach_kprobe(event="nfs_readdir", fn_name="trace_nfs_readdir")                   # updates readdirs
        self.b.attach_kprobe(event="nfs_create", fn_name="trace_nfs_create")                     # updates creates
        self.b.attach_kprobe(event="nfs_link", fn_name="trace_nfs_link")                         # updates links
        self.b.attach_kprobe(event="nfs_unlink", fn_name="trace_nfs_unlink")                     # updates unlinks
        self.b.attach_kprobe(event="nfs_symlink", fn_name="trace_nfs_symlink")                   # updates symlinks
        self.b.attach_kprobe(event="nfs_lookup", fn_name="trace_nfs_lookup")                     # updates lookups
        self.b.attach_kprobe(event="nfs_rename", fn_name="trace_nfs_rename")                     # updates renames
        self.b.attach_kprobe(event="nfs_do_access", fn_name="trace_nfs_do_access")               # updates accesses
        # nfs4 attachments
        if BPF.get_kprobe_functions(b'nfs4_file_open'):
            self.b.attach_kprobe(event="nfs4_file_open", fn_name="trace_nfs_file_open")         # updates opens
            self.b.attach_kprobe(event="nfs4_file_flush", fn_name="trace_nfs_file_flush")       # updates flushes
        if BPF.get_kprobe_functions(b'nfs4_listxattr'):
            self.b.attach_kprobe(event="nfs4_listxattr", fn_name="trace_nfs_listxattrs")        # updates listxattrs
        if BPF.get_kprobe_functions(b'nfs3_listxattr'):
            self.b.attach_kprobe(event="nfs3_listxattr", fn_name="trace_nfs_listxattrs")        # updates listxattrs

    def start(self):
        self.t = Thread(target=self.poll_stats)
        self.t.daemon = True
        self.t.start()

    def combine_stat(self, stat, new):
        for key in STATKEYS:
            stat[key] += new[key]

    def collect_stats(self):
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print("######## SAMPLE: " + timestamp + " ########")

        counts = self.b.get_table("counts")
        statistics = []
        for k, v in (counts.items_lookup_and_delete_batch() if self.batch_ops else counts.items()):
            output = {
                    "TIMESTAMP":    timestamp,
                    "HOSTNAME":     self.hostname,
                    "PID":          k.tgid, # real pid is the thread-group id
                    "UID":          k.uid,
                    "COMM":         k.comm.decode('utf-8', 'replace'),
                    "OPEN":         v.opens,
                    "CLOSE":        v.closes,
                    "READ":         v.reads,
                    "RBYTES":       v.rbytes,
                    "WRITE":         v.writes,
                    "WBYTES":       v.wbytes,
                    "GETATTR":      v.getattrs,
                    "SETATTR":      v.setattrs,
                    "FLUSH":        v.flushes,
                    "FSYNC":        v.fsyncs,
                    "LOCK":         v.locks,
                    "MMAP":         v.mmaps,
                    "READDIR":      v.readdirs,
                    "CREATE":       v.creates,
                    "LINK":         v.links,
                    "UNLINK":       v.unlinks,
                    "LOOKUP":       v.lookups,
                    "RENAME":       v.renames,
                    "ACCESS":       v.accesses,
                    "LISTXATTR":    v.listxattrs,
                    "TAGS":         self.PidEnvMap.get(k.tgid),
            }

            # search of we have multiple threads (pid) of the same process (tgid)
            match = list(filter(lambda stat: stat["PID"] == output["PID"], statistics))
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
            for stat in statistics:
                print(stat)


def split_list(values):
    return values.split(',')

def sighandler(signum, frame):
    exit(0)

debug = 0
bpf = None
pidEnvMap = None
if __name__ == "__main__":
    # arguments
    examples ="""examples:
        ./nfsops.py                         # NFS operations trace every 5 seconds
        ./nfsops.py -i 60                   # NFS operations trace every 60 seconds
        ./nfsops.py -v 700                  # Vaccume every 700 seconds
        ./nfsops.py -e JOBID,SCHEDID        # Decorate samples with env variables JOBID and SCHEDID
        ./nfsops.py --ebpf                  # dump ebpf program text and exit
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

    # initialize BPF
    os.system("/usr/sbin/modprobe kheaders")
    bpf = BPF(text=bpf_text)

    pidEnvMap = PidEnvMap(vaccum_interval=args.vaccum)
    # if no envs are given, no need to track
    if args.envs:
        envTracer = EnvTracer(envs=args.envs, bpf=bpf, PidEnvMap=pidEnvMap)
        envTracer.attach()
        envTracer.start()

    statsCollector = StatsCollector(bpf=bpf, PidEnvMap=pidEnvMap,
                            interval=args.interval)
    statsCollector.attach()
    statsCollector.start()

    print('Tracing... Output every %d secs...' % args.interval)
    while True:
        try:
            sleep(args.interval)
        except:
            print("bye bye")
            exit(0)
