#!/bin/bash
set -e

# Define paths
VENV_PATH="/opt/vnfs-collector/src/venv"
SYMLINK_PATH="/usr/local/bin/vnfs"
VERSION_FILE="/opt/vnfs-collector/src/version.txt"
SYSTEMD="/opt/vnfs-collector/src/vnfs-collector.service"
PYLIB_VERSION=$(cat "${VERSION_FILE}")
TAR_FILE="/opt/vnfs-collector/src/vast_client_tools-${PYLIB_VERSION}.tar.gz"

# Ensure the virtual environment directory exists
mkdir -p "${VENV_PATH}"

# Remove previous virtual environment if it exists
if [ -d "${VENV_PATH}" ]; then
    rm -rf "${VENV_PATH}"
fi

# Create a new virtual environment
python3 -m venv "${VENV_PATH}"

# Activate the virtual environment and install the package
source "${VENV_PATH}/bin/activate"
pip install "${TAR_FILE}" && rm -f "${TAR_FILE}"
deactivate

# Remove the old symlink if it exists
if [ -L "${SYMLINK_PATH}" ]; then
    echo "Removing existing symlink at ${SYMLINK_PATH}"
    rm -f "${SYMLINK_PATH}"
fi

# Create a new symlink
echo "Creating symlink from ${VENV_PATH}/bin/vnfs to ${SYMLINK_PATH}"
ln -s "${VENV_PATH}/bin/vnfs" "${SYMLINK_PATH}"


cp "${SYSTEMD}" /etc/systemd/system/vnfs-collector.service && rm -f "${SYSTEMD}"
# Check if systemctl can be executed
if systemctl daemon-reload > /dev/null 2>&1; then
  # systemctl is available and systemd is likely the init system.
  systemctl enable vnfs-collector
fi
