#!/bin/bash
set -e

# Define paths
VENV_PATH="/opt/vnfs-collector/src/venv"
SYMLINK_PATH="/usr/local/bin/vnfs"
VERSION_FILE="/opt/vnfs-collector/src/version.txt"
SYSTEMD="/opt/vnfs-collector/src/vnfs-collector.service"
PYLIB_VERSION=$(cat "${VERSION_FILE}")
PY_WHEEL="/opt/vnfs-collector/src/vast_client_tools-${PYLIB_VERSION}-py3-none-any.whl"

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
pip install --upgrade pip || true
pip install --upgrade Cython || true
pip install "${PY_WHEEL}" && rm -f "${PY_WHEEL}"
python3 -c "from vast_client_tools.link_bcc import link_bcc; link_bcc()"
deactivate

# Remove the old symlink if it exists
if [ -L "${SYMLINK_PATH}" ]; then
    echo "Removing existing symlink at ${SYMLINK_PATH}"
    rm -f "${SYMLINK_PATH}"
fi

# Create a new symlink
echo "Creating symlink from ${VENV_PATH}/bin/vnfs -> ${SYMLINK_PATH}"
ln -s "${VENV_PATH}/bin/vnfs" "${SYMLINK_PATH}"


cp "${SYSTEMD}" /etc/systemd/system/vnfs-collector.service && rm -f "${SYSTEMD}"
# Check if systemctl can be executed
if systemctl daemon-reload > /dev/null 2>&1; then
  # systemctl is available and systemd is likely the init system.
  systemctl enable vnfs-collector
fi
