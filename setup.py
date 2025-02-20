from setuptools import setup, find_packages
import os
import sys
import subprocess

PACKAGE = "vnfs_collector"
ROOT = os.path.dirname(__file__)
try:
    VERSION = open(os.path.join(ROOT, "version.txt")).read().strip()
except:
    VERSION = "0.0+local.dummy"
assert VERSION, "Failed to determine version"

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
    "vnfs_collector": [
        "../nfsops.c",
        "../nfsops.yaml",
        "../version.txt",
    ]
}

setup(
    name="vnfs_collector",
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
    provides=["vnfs_collector"],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data=package_data,
    entry_points={
        "console_scripts": [
            "vnfs-collector = vnfs_collector.main:main",
        ],
        "drivers": [
            "screen = vnfs_collector.drivers:ScreenDriver",
            "file = vnfs_collector.drivers:FileDriver",
            "vdb = vnfs_collector.drivers:VdbDriver",
            "prometheus = vnfs_collector.drivers:PrometheusDriver",
            "kafka = vnfs_collector.drivers:KafkaDriver",
        ],
    },
    install_requires=requires,
    extras_require=extras_require,
    python_requires=">=3.9",
)
