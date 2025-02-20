#!/bin/bash
set -e

version() {
    echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }';
}

latest_python() {
    echo $(find $(echo $PATH | tr ':' ' ') -type f -regex '.*/python[0-9]+\.[0-9]+$' 2>/dev/null | sort -V | uniq | tail -1)
}

# Redirect stderr to both a file and the console
exec 2> >(tee -a "/opt/vnfs-collector/src/errorlog" >&2)

# Define paths
VENV_PATH="/opt/vnfs-collector/src/venv"
SYMLINK_PATH="/usr/local/bin/vnfs-collector"
VERSION_FILE="/opt/vnfs-collector/src/version.txt"
SYSTEMD="/opt/vnfs-collector/src/vnfs-collector.service"
PYLIB_VERSION=$(cat "${VERSION_FILE}" | sed 's/-/.post/')
PY_WHEEL="/opt/vnfs-collector/src/vnfs_collector-${PYLIB_VERSION}-py3-none-any.whl"

# Ensure the virtual environment directory exists
mkdir -p "${VENV_PATH}"

# Remove previous virtual environment if it exists
if [ -d "${VENV_PATH}" ]; then
    rm -rf "${VENV_PATH}"
fi


py3_version=$(python3 --version | awk {'print $2'})
if [ $(version $py3_version) -ge $(version "3.9") ]; then
    py3=python3
else
    py3=$(latest_python)
fi

# Create a new virtual environment
$py3 -m venv "${VENV_PATH}"

# Activate the virtual environment and install the package
source "${VENV_PATH}/bin/activate"
pip install --upgrade pip || true
pip install --upgrade Cython || true
pip install --upgrade setuptools || true
pip install "${PY_WHEEL}"
python3 -c "from vnfs_collector.link_bcc import link_bcc; link_bcc()"
deactivate

# Remove the old symlink if it exists
if [ -L "${SYMLINK_PATH}" ]; then
    echo "Removing existing symlink at ${SYMLINK_PATH}"
    rm -f "${SYMLINK_PATH}"
fi

# Create a new symlink
echo "Creating symlink from ${VENV_PATH}/bin/vnfs-collector -> ${SYMLINK_PATH}"
ln -s "${VENV_PATH}/bin/vnfs-collector" "${SYMLINK_PATH}"
