from setuptools import setup, find_packages
import os
import sys
import subprocess

PACKAGE = "vast_client_tools"
ROOT = os.path.dirname(__file__)
try:
    VERSION = open(os.path.join(ROOT, "version.txt")).read().strip()
except:
    VERSION = "0.0+local.dummy"

# link bcc lib from dist-packages.
subprocess.run([sys.executable, os.path.join(ROOT, PACKAGE, "link_bcc.py")])


requires = [
    "psutil==6.0.0",
    "PyYAML==6.0.1",
    "stevedore==3.5.2",
    "prometheus_client==0.17.1",
    "vastdb",
    "aiokafka",
    "pandas",
    "pyarrow",
    'importlib-metadata; python_version>="3.9"',
    'colorama==0.4.6; sys.platform == "win32"',
]

# pip install .[test]
extras_require = {
    "test": [
        "pytest>=6.2.4",
        "pytest-asyncio==0.23.8",
    ]
}


package_data = {
    "vast_client_tools": [
        "../nfsops.c",
        "../nfsops.yaml",
        "../version.txt",
    ]
}

setup(
    name="vast_client_tools",
    author="Sagi Grimberg, Volodymyr Boiko",
    author_email="sagi@grimberg.vastdata.com, volodymyr.boiko@vastdata.com",
    version=VERSION,
    license="Apache License 2.0",
    description="Console utility for tracking NFS statistics by process",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Environment :: Console",
    ],
    provides=["vast_client_tools"],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data=package_data,
    entry_points={
        "console_scripts": [
            "vnfs-collector = vast_client_tools.main:main",
        ],
        "drivers": [
            "screen = vast_client_tools.drivers:ScreenDriver",
            "file = vast_client_tools.drivers:FileDriver",
            "vdb = vast_client_tools.drivers:VdbDriver",
            "prometheus = vast_client_tools.drivers:PrometheusDriver",
            "kafka = vast_client_tools.drivers:KafkaDriver",
        ],
    },
    install_requires=requires,
    extras_require=extras_require,
    python_requires=">=3.9",
)
