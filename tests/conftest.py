import sys
import shlex
import re
import json
from pathlib import Path
import importlib

import pytest
import yaml
import pandas as pd


ROOT = Path(__file__).parent.resolve()

spec = importlib.util.spec_from_file_location("bcc", ROOT / "mock_bcc.py")

module = importlib.util.module_from_spec(spec)

# Execute the module in its own namespace
sys.modules["bcc"] = module
spec.loader.exec_module(module)

@pytest.fixture(scope="session")
def data():
    from vnfs_collector.nfsops import hashabledict

    with (ROOT / "data" / "data.json").open() as f:
        df = pd.DataFrame(json.load(f))
        df["TAGS"] = df["TAGS"].apply(hashabledict)
    yield df


@pytest.fixture
def cli_factory():
    sys_argv_orig = sys.argv[1:]

    def dec(cmd: str):
        sys.argv[1:] = shlex.split(cmd)

    yield dec
    sys.argv[1:] = sys_argv_orig


@pytest.fixture
def config_factory(tmpdir):

    def dec(cmd: str):
        """
        Convert CLI argument string into a dictionary and store it as yaml config in temp file.
        eg. cmd = '-d=file --samples-path=/foo/bar -d=screen --table-format'
        will be converted to:
        {
            'file': {'samples_path': '/foo/bar'},
            'screen': {'table-format': True}
        }
        and stored.
        """
        # Initialize the dictionary to store results
        drivers_dict = {}

        # Regular expression to find driver and its arguments
        pattern = re.compile(r"-d=(\w+)(?:\s+((?:--[\w-]+(?:=[\w/:.-]+)?\s*)+))?")

        # Find all matches in the input string
        matches = pattern.finditer(cmd)

        # Iterate over matches to extract drivers and their arguments
        for match in matches:
            driver = match.group(1)
            driver_args = match.group(2)

            if driver not in drivers_dict:
                drivers_dict[driver] = {}

            # If there are additional arguments for this driver
            if driver_args:
                # Split additional arguments
                for arg in driver_args.strip().split():
                    if "=" in arg:
                        key, value = arg.split("=", 1)
                        # Replace dashes with underscores in the key
                        drivers_dict[driver][key.lstrip("--").replace("-", "_")] = value
                    else:
                        # Replace dashes with underscores in the key
                        drivers_dict[driver][arg.lstrip("--").replace("-", "_")] = True

        config_file = tmpdir.join("conf.yaml")
        config_file.write(yaml.dump(drivers_dict))
        return config_file

    return dec
