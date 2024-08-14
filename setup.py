# Copyright 2024 VAST Data Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
import os
import sys
import subprocess

PACKAGE = "vast_client_tools"
ROOT = os.path.dirname(__file__)
VERSION = open(os.path.join(ROOT, "version.txt")).read().strip()


class CustomInstallCommand(install):
    """Custom handler for the 'install' command to link bcc lib from dist-packages."""
    def run(self):
        install.run(self)
        subprocess.run([sys.executable, os.path.join(ROOT, PACKAGE, "link_bcc.py")])


class CustomDevelopCommand(develop):
    """Custom handler for the 'develop' command to link bcc lib from dist-packages."""
    def run(self):
        develop.run(self)
        subprocess.run([sys.executable, os.path.join(ROOT, PACKAGE, "link_bcc.py")])


requires = [
    "psutil==6.0.0",
    "PyYAML==6.0.1",
    "stevedore==3.5.2",
    "prometheus_client==0.17.1",
    "vastdb==0.0.5.4",
    "pandas",
    "pyarrow",
    'importlib-metadata; python_version<"3.7"',
    'colorama==0.4.6; sys.platform == "win32"',
]

package_data = {
    "vast_client_tools": [
        "../nfsops.c",
        "../nfsops.yaml",
        "../version.txt",
    ]
}

setup(
    name='vast_client_tools',
    author='Sagi Grimberg, Volodymyr Boiko',
    author_email='sagi@grimberg.vastdata.com, volodymyr.boiko@vastdata.com',
    version=VERSION,
    description='Console utility for tracking NFS statistics by process',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Environment :: Console',
    ],
    provides=['vast_client_tools'],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data=package_data,
    entry_points={
        'console_scripts': [
            'vnfs = vast_client_tools.main:main',
        ],
        'drivers': [
            'screen = vast_client_tools.drivers:ScreenDriver',
            'file = vast_client_tools.drivers:FileDriver',
            'vdb = vast_client_tools.drivers:VdbDriver',
            'prometheus = vast_client_tools.drivers:PrometheusDriver',
        ],
    },
    install_requires=requires,
    python_requires='>=3.6',
    cmdclass={
        'install': CustomInstallCommand,
        'develop': CustomDevelopCommand,
    }
)
