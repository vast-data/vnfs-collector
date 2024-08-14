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
import glob
import site


VERSION = open(os.path.join(os.path.dirname(__file__), 'version.txt')).read().strip()


def find_python3_packages(base_dirs):
    """Find all Python 3 dist-packages and site-packages directories."""
    python_dirs = []
    # Define possible package directories
    package_dirs = ['dist-packages', 'site-packages']

    for base_dir in base_dirs:
        # Search for Python version directories
        for py_version_dir in glob.glob(os.path.join(base_dir, 'python3*')):
            for package_dir in package_dirs:
                path = os.path.join(py_version_dir, package_dir)
                if os.path.isdir(path):
                    python_dirs.append(path)
    return python_dirs


def find_package_in_dirs(package_name, dirs):
    """Find package in the provided directories."""
    found_paths = []
    for directory in dirs:
        for root, _, files in os.walk(directory):
            # Check for the presence of the package folder
            if package_name in root and any(file.endswith('.py') for file in files):
                found_paths.append(root)
    return found_paths


def link_bcc():
    base_dirs = ['/usr/lib', os.path.expanduser('~/.local/lib')]
    python3_dirs = find_python3_packages(base_dirs)
    package_name = 'bcc'
    found_paths = find_package_in_dirs(package_name, python3_dirs)
    if found_paths:
        bcc_path = found_paths[0]
        site_packages_dirs = site.getsitepackages()
        for site_pkg_dir in site_packages_dirs:
            symlink_path = os.path.join(site_pkg_dir, package_name)
            if not os.path.exists(symlink_path):
                try:
                    os.symlink(bcc_path, symlink_path)
                except OSError as e:
                    print(f"Failed to create symlink {symlink_path}: {e}")


class CustomInstallCommand(install):
    """Custom handler for the 'install' command to copy extra data files."""
    def run(self):
        install.run(self)
        link_bcc()


class CustomDevelopCommand(develop):
    """Custom handler for the 'develop' command to copy extra data files."""
    def run(self):
        develop.run(self)
        link_bcc()


requires = [
    "psutil==6.0.0",
    "PyYAML==6.0.1",
    "stevedore==3.5.2",
    "tabulate==0.8.9",
    "prometheus_client==0.17.1",
    "vastdb==1.1.1",
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
    python_requires='>=3.9',
    cmdclass={
        'install': CustomInstallCommand,
        'develop': CustomDevelopCommand,
    }
)
