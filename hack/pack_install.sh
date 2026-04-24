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
WHL_PATH="/opt/vnfs-collector/src/wheelhouse"
PYLIB_VERSION=$(cat "${VERSION_FILE}" | sed 's/-/.post/')
PY_WHEEL="/opt/vnfs-collector/src/vnfs_collector-${PYLIB_VERSION}-py3-none-any.whl"

# Ensure the virtual environment directory exists
mkdir -p "${VENV_PATH}"

# Remove previous virtual environment if it exists
if [ -d "${VENV_PATH}" ]; then
    rm -rf "${VENV_PATH}"
fi


# Pick a python3 >= 3.9 available on the target. The bundled wheelhouse
# contains wheels for every python minor version supported by the
# package (see WHL_PYVERS in the build), so pip will match whichever
# interpreter we choose here.
py3_version=$(python3 --version | awk {'print $2'})
if [ $(version $py3_version) -ge $(version "3.9") ]; then
    py3=python3
else
    py3=$(latest_python)
fi

# Create a new virtual environment
$py3 -m venv "${VENV_PATH}"

# Activate the virtual environment and install the package.
#
# If the package ships a bundled wheelhouse (built via `make wheelhouse`),
# install with --no-index so no internet access is required. This covers
# installations on air-gapped hosts. Otherwise fall back to the default
# online behavior (PyPI or any index overridden via PIP_* environment
# variables passed through by the package manager).
source "${VENV_PATH}/bin/activate"
if [ -d "${WHL_PATH}" ] && [ -n "$(ls -A "${WHL_PATH}" 2>/dev/null)" ]; then
    echo "Installing from bundled wheelhouse at ${WHL_PATH} (offline)"
    PIP_OFFLINE_OPTS="--no-index --find-links=${WHL_PATH}"
    pip install ${PIP_OFFLINE_OPTS} --upgrade pip || true
    pip install ${PIP_OFFLINE_OPTS} --upgrade setuptools || true
    pip install ${PIP_OFFLINE_OPTS} "${PY_WHEEL}"
else
    echo "No bundled wheelhouse found, installing from configured pip index"
    pip install --upgrade pip || true
    pip install --upgrade Cython || true
    pip install --upgrade setuptools || true
    pip install "${PY_WHEEL}"
fi
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
