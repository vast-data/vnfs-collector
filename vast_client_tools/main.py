import os
import sys
import logging
import argparse
import asyncio
import signal
from pathlib import Path
try:
    from importlib import metadata
except ImportError:
    import importlib_metadata as metadata

import yaml
from bcc import BPF, __version__
from stevedore.named import NamedExtensionManager, ExtensionManager

from vast_client_tools.logger import COLORS
from vast_client_tools.drivers import InvalidArgument
from vast_client_tools.nfsops import StatsCollector, PidEnvMap, MountsMap, EnvTracer, logger


ENTRYPOINT_GROUP = "drivers"
entry_points = metadata.entry_points()
if sys.version_info >= (3, 10):
    # Use the select method for Python 3.10+
    ENTRYPOINTS = entry_points.select(group=ENTRYPOINT_GROUP)
else:
    # Access the entry points dictionary for earlier versions
    ENTRYPOINTS = entry_points.get(ENTRYPOINT_GROUP, [])


def set_signal_handler(handler):
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


class HelpFormatter(argparse.HelpFormatter):
    def format_help(self):
        prog = self._prog
        usages = []
        help_text = []
        # Helper function to clean up usage strings
        strip = lambda txt: txt.replace(prog, "").replace("[-h]", "").strip("usage:").strip()
        # Gather all parsers and their actions
        all_parsers = [
            ("Configuration Options", conf_parser),
        ]
        # Add extension parsers
        mgr = ExtensionManager(namespace=ENTRYPOINT_GROUP)
        for ext in mgr.extensions:
            all_parsers.append((f"{ext.plugin.__name__} Options", ext.plugin.parser))
        # Collect usage strings and format help text
        for section_name, parser in all_parsers:
            usages.append(strip(
                argparse.HelpFormatter._format_usage(self,None, parser._actions, [], None))
            )
            help_text.append(f"\n{COLORS.intense_blue(section_name)}:")
            max_option_length = max(len(", ".join(action.option_strings)) for action in parser._actions)
            for action in parser._actions:
                options = ", ".join(action.option_strings)
                default_text = f" {COLORS.green('[ default')}: {action.default!r} {COLORS.green(']')}" if (action.default is not None and action.default != argparse.SUPPRESS) else ""
                help_text.append(f"  {options.ljust(max_option_length)}: {action.help}{default_text}")
        # Construct the final help message
        return f"Usage: {prog} " + " ".join(usages) +  "\n" + "\n".join(help_text) + "\n\n"


available_drivers = sorted(set([e.name for e in ENTRYPOINTS]))
conf_parser = argparse.ArgumentParser(formatter_class=HelpFormatter)
conf_parser.add_argument(
    '-d', '--driver',
    help="driver to enable. "
         f"Available options: {available_drivers}. "
         f"User can specify multiple options.",
    choices=available_drivers, action='append', required=False,
)
conf_parser.add_argument(
    "--debug", action="store_true",
    help="enable debug prints"
)
conf_parser.add_argument(
    "-i", "--interval", default=5,
    help="output interval, in seconds"
)
conf_parser.add_argument(
    "-v", "--vaccum", default=600,
    help="pid env map vaccum interval, in seconds"
)
conf_parser.add_argument(
    "-e", "--envs", type=lambda v: v.split(','),
    help="comma separated list of env vars"
)
conf_parser.add_argument(
    "--ebpf", action="store_true",
    help="dump BPF program text and exit"
)
conf_parser.add_argument(
    "-C", "--cfg", default=None,
    help="config yaml"
)


async def _exec():
    args, remaining = conf_parser.parse_known_args()
    cfg_opts = None
    if args.cfg:
        if not os.path.exists(args.cfg):
            raise FileNotFoundError(args.cfg)
        with open(args.cfg) as f:
            cfg_opts = yaml.safe_load(f)
            if cfg_opts:
                args, _ = conf_parser.parse_known_args(namespace=argparse.Namespace(**cfg_opts))
                args.driver = sorted(set(available_drivers).intersection(set(cfg_opts.keys())))

    drivers = args.driver
    if not drivers:
        logger.error(f"No driver specified.")
        conf_parser.print_help()
        exit(0)

    logger.info(f"BPF version: {__version__}")
    logger.info(
        f"Configuration options: "
        f"drivers={drivers}, "
        f"interval={args.interval}, "
        f"vaccum={args.vaccum}, "
        f"envs={args.envs}, "
        f"ebpf={args.ebpf}, "
        f"config={args.cfg}"
    )
    # read BPF program text
    with Path(__file__).parents[1].joinpath("nfsops.c").open() as f:
        bpf_text = f.read()
    debug = args.debug
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    if debug or args.ebpf:
        print(bpf_text)
    if args.ebpf:
        exit()

    # initialize BPF
    bpf = BPF(text=bpf_text)
    pidEnvMap = PidEnvMap(vaccum_interval=args.vaccum)
    mountsMap = MountsMap()
    collector = StatsCollector(bpf=bpf, pid_env_map=pidEnvMap, mounts_map=mountsMap)
    mgr = NamedExtensionManager(
        namespace=ENTRYPOINT_GROUP,
        invoke_on_load=True,
        names=drivers,
        invoke_kwds=dict(envs=args.envs)
    )

    def on_exit(sig=None, frame=None):
        """Teardown drivers gracefully on exit"""
        if sig:
            signals = dict(
                (getattr(signal, n), n) for n in dir(signal) if n.startswith("SIG") and "_" not in n
            )
            sig_name = signals[sig]
            logger.info(f"Got signal {sig_name!r}. Terminating...")
        mgr.map_method("teardown")
        exit(0)

    set_signal_handler(on_exit)

    if cfg_opts:
        setup_coros = mgr.map(lambda e: e.obj.setup(namespace=cfg_opts[e.name]))
    else:
        setup_coros = mgr.map_method("setup", remaining)
    try:
        await asyncio.gather(*setup_coros)
    except InvalidArgument as e:
        logger.error(f"Setup failed: {e}")
        conf_parser.print_help()
        on_exit()
    except Exception:
        logger.error(f"Setup failed", exc_info=True)
        on_exit()

    # if no envs are given, no need to track
    if args.envs:
        envTracer = EnvTracer(envs=args.envs, bpf=bpf, pid_env_map=pidEnvMap)
        envTracer.attach()
        envTracer.start()

    collector.attach()
    logger.info("All good! StatsCollector has been attached.")

    # probe needed modules (nfsv4 autoloads nfs)
    os.system("/usr/sbin/modprobe kheaders")
    os.system("/usr/sbin/modprobe nfsv4")

    while True:
        await asyncio.sleep(args.interval)
        data = collector.collect_stats()
        if not data:
            continue
        await asyncio.gather(*mgr.map_method("store_sample", data=data))


def main():
    asyncio.run(_exec())
