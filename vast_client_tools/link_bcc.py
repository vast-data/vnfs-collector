# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import os
import glob
import site

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
            if not os.path.exists(symlink_path) and os.path.exists(site_pkg_dir):
                try:
                    os.symlink(bcc_path, symlink_path)
                    print(f"Created symlink {symlink_path} -> {bcc_path}")
                except OSError as e:
                    print(f"Failed to create symlink {symlink_path}: {e}")


if __name__ == '__main__':
    link_bcc()
