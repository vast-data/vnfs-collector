#!/bin/sh
set -e

# Define variables
SERVICE_NAME="vnfs-collector"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
SYMLINK_PATH="/usr/local/bin/vnfs"
INSTALL_PATH="/opt/vnfs-collector"
VENV_PATH="${INSTALL_PATH}/src/venv"

# Stop the systemd service if it's running
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    systemctl stop "${SERVICE_NAME}"
fi

# Disable the systemd service
systemctl disable "${SERVICE_NAME}" 2>/dev/null || true

# Remove the systemd service file
if [ -e "${SERVICE_PATH}" ]; then
    rm "${SERVICE_PATH}"
fi

# Remove the symlink if it exists
if [ -L "${SYMLINK_PATH}" ]; then
    echo "Removing symlink at ${SYMLINK_PATH}"
    rm "${SYMLINK_PATH}"
fi

# Remove the virtual environment if it exists
if [ -d "${VENV_PATH}" ]; then
    rm -rf "${VENV_PATH}"
fi

# Remove the application directory
if [ -d "${INSTALL_PATH}" ]; then
    rm -rf "${INSTALL_PATH}"
fi

 systemctl daemon-reload > /dev/null 2>&1 || true
