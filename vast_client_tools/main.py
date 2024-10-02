import os
import sys
import urllib3
import logging
import argparse
import asyncio
from pathlib import Path
try:
    from importlib import metadata
except ImportError:
    import importlib_metadata as metadata

import yaml
from bcc import BPF, __version__
from stevedore.named import NamedExtensionManager, ExtensionManager

from vast_client_tools.logger import COLORS
from vast_client_tools.utils import (
    InvalidArgument,
    set_signal_handler,
    await_until_event_or_timeout,
    parse_args_options_from_namespace,
    maybe_list_parse,
)
from vast_client_tools.nfsops import StatsCollector, PidEnvMap, MountsMap, EnvTracer, logger

urllib3.disable_warnings()

BASE_PATH = Path(__file__).parents[1]
ENTRYPOINT_GROUP = "drivers"
ANON_FIELDS = {"COMM", "MOUNT", "PID", "UID", "TAGS", "REMOTE_PATH"}

entry_points = metadata.entry_points()
if sys.version_info >= (3, 10):
    # Use the select method for Python 3.10+
    ENTRYPOINTS = entry_points.select(group=ENTRYPOINT_GROUP)
else:
    # Access the entry points dictionary for earlier versions
    ENTRYPOINTS = entry_points.get(ENTRYPOINT_GROUP, [])


class HelpFormatter(argparse.HelpFormatter):
    """
    Custom help formatter for argparse to format help messages with colors and additional information.
    """

    def format_help(self):
        """
        Format the help message to include usage, options, and their descriptions.
        """
        prog = self._prog
        usages = []
        help_text = []
        required_mark = " " + COLORS.intense_red("\u26A0")
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
                if "--driver" in options:
                    required = required_mark
                else:
                    required = required_mark if action.required else "  "
                if action.choices:
                    choices = f" {COLORS.yellow('[ choices')}: {', '.join(map(str, action.choices))} {COLORS.yellow(']')}"
                else:
                    choices = ""
                default_text = f" {COLORS.green('[ default')}: {action.default!r} {COLORS.green(']')}" if (action.default is not None and action.default != argparse.SUPPRESS) else ""
                help_section = f"{action.help}{choices}{default_text}"
                help_section = ("\n" + " " * (max_option_length + 5 )).join(help_section.splitlines())
                help_text.append(f"  {options.ljust(max_option_length)}:{required} {help_section}")
        # Construct the final help message
        return f"Usage: {prog} " + " ".join(usages) + f"\n\n({required_mark} - option is required if driver is enabled )" + "\n".join(help_text) + "\n\n"


available_drivers = sorted(set([e.name for e in ENTRYPOINTS]))
conf_parser = argparse.ArgumentParser(formatter_class=HelpFormatter)
conf_parser.add_argument(
    '-d', '--driver',
    help="Driver to enable. "
         f"User can specify multiple options.",
    choices=available_drivers, action='append', required=False, default=None
)
conf_parser.add_argument(
    "--debug", action="store_true",
    help="Enable debug prints."
)
conf_parser.add_argument(
    "-i", "--interval", default=5, type=int,
    help="Output interval, in seconds."
)
conf_parser.add_argument(
    "-v", "--vaccum", default=600, type=int,
    help="Pid env map vaccum interval, in seconds."
)
conf_parser.add_argument(
    "-e", "--envs", type=maybe_list_parse,
    help="Comma separated list of env vars."
)
conf_parser.add_argument(
    "--ebpf", action="store_true",
    help="Dump BPF program text and exit."
)
conf_parser.add_argument(
    "--squash-pid", type=bool, default=True,
    help="Squash PIDs during statistics aggregation. This will group statistics by command, mount, and tags."
)
conf_parser.add_argument(
    "--tag-filter", type=str, choices=("all", "any"), default=None,
    help="Specify how to filter statistics based on tags.\n"
         "- `all`: Requires all specified tag keys to match provided envs.\n"
         "- `any`: Requires at least one of the specified tag keys to match provided env.\n"
)
conf_parser.add_argument(
    "--anon-fields", type=maybe_list_parse,
    help="Comma separated list of fields to anonymize."
         " Field values for such fields becomes '--' for string and 0 for integers/floats."
)
conf_parser.add_argument(
    "-C", "--cfg", default=None,
    help="Config yaml. When provided it takes precedence over command line arguments."
)


async def _exec():
    """
    Main execution function to set up and run the BPF program and drivers.
    """
    exit_error = None
    stop_event = asyncio.Event()
    args, remaining = conf_parser.parse_known_args()
    cfg_opts = None
    if args.cfg:
        if not os.path.exists(args.cfg):
            raise FileNotFoundError(args.cfg)
        with open(args.cfg) as f:
            cfg_opts = yaml.safe_load(f)
            if cfg_opts:
                args = parse_args_options_from_namespace(namespace=cfg_opts, parser=conf_parser)
                args.driver = sorted(set(available_drivers).intersection(set(cfg_opts.keys())))
    drivers = args.driver
    if not drivers:
        conf_parser.error("No driver specified.")

    # Validate mutual dependencies
    if args.tag_filter and not args.envs:
        conf_parser.error("--tag-filter requires --envs to be specified.")

    if args.anon_fields:
        invalid_fields = set(args.anon_fields).difference(ANON_FIELDS)
        if invalid_fields:
            conf_parser.error(f"Invalid anonymized fields specified: {', '.join(invalid_fields)}")

    logger.info(f"BPF version: {__version__}")
    collector_version = BASE_PATH.joinpath("version.txt").read_text()
    logger.info(f"VNFS Collector<{COLORS.intense_blue(collector_version)}> initialization")
    logger.info(
        f"Configuration options: "
        f"drivers={drivers}, "
        f"interval={args.interval}, "
        f"vaccum={args.vaccum}, "
        f"envs={args.envs}, "
        f"ebpf={args.ebpf}, "
        f"squash-pid={args.squash_pid}, "
        f"tag-filter={args.tag_filter}, "
        f"anon-fields={args.anon_fields}, "
        f"config={args.cfg}"
    )
    # read BPF program text
    with BASE_PATH.joinpath("nfsops.c").open() as f:
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
        invoke_kwds=dict(common_args=args)
    )

    def on_exit(sig=None, frame=None):
        """Teardown drivers gracefully on exit"""
        logger.info("Exiting...")
        stop_event.set()

    set_signal_handler(on_exit)

    if cfg_opts:
        setup_coros = mgr.map(lambda e: e.obj.setup(namespace=cfg_opts[e.name]))
    else:
        setup_coros = mgr.map_method("setup", remaining)
    try:
        await asyncio.gather(*setup_coros)
    except InvalidArgument as e:
        exit_error = e
        conf_parser.print_help()
        on_exit()
    except Exception as e:
        exit_error = e
        on_exit()

    if not stop_event.is_set():
        # if no envs are given, no need to track
        if args.envs:
            envTracer = EnvTracer(envs=args.envs, bpf=bpf, pid_env_map=pidEnvMap)
            envTracer.attach()
            envTracer.start()

        # probe needed modules (nfsv4 autoloads nfs)
        os.system(f"modprobe kheaders  > {os.devnull} 2>&1")
        os.system(f"modprobe nfsv4 > {os.devnull} 2>&1")

        while True:
            try:
                collector.attach()
                break
            except Exception as e:
                if "Failed to attach" in str(e):
                    logger.error(f"{e}. Do you have any mounts?")
                    await asyncio.sleep(10)
                    continue
                raise

        logger.info("All good! StatsCollector has been attached.")

    while not stop_event.is_set():
        canceled = await await_until_event_or_timeout(timeout=args.interval, stop_event=stop_event)
        if canceled:
            break

        data = collector.collect_stats(
            squash_pid=args.squash_pid,
            filter_tags=args.envs,
            filter_condition=args.tag_filter,
            anon_fields=args.anon_fields,
        )
        if data.empty:
            continue
        await asyncio.gather(*mgr.map_method("store_sample", data=data))

    await asyncio.gather(*mgr.map_method("teardown"))
    if exit_error:
        logger.error(str(exit_error))

def main():
    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(_exec())
    finally:
        loop.close()
