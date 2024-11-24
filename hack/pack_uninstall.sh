#!/bin/bash
set -e

# Define variables
SYMLINK_PATH="/usr/local/bin/vnfs-collector"
INSTALL_PATH="/opt/vnfs-collector"
VENV_PATH="${INSTALL_PATH}/src/venv"

# Remove the symlink if it exists
if [ -L "${SYMLINK_PATH}" ]; then
    echo "Removing symlink at ${SYMLINK_PATH}"
    rm "${SYMLINK_PATH}"
fi

# Remove the virtual environment if it exists
if [ -d "${VENV_PATH}" ]; then
    rm -rf "${VENV_PATH}"
fi

rm -f ${INSTALL_PATH}/src/errorlog
rm -f ${INSTALL_PATH}/vnfs-collector.log
