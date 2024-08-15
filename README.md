# NFS Statistics Collector

## Overview
The NFS Statistics Collector is a console utility designed to gather and export NFS (Network File System) statistics to various destinations.
This tool helps in monitoring and analyzing NFS performance by providing flexible options for data storage and output.

## Features
- Primal focus on CLI distribution via DEB and RPM packages.
- Foundation for Python Package.
- Flexible Data Storage: Includes drivers for exporting statistics to multiple destinations:
    - VDB: A database solution for structured data storage.
    - Local Files: Save statistics to local files for offline analysis.
    - Prometheus: Integrate with Prometheus for real-time metrics monitoring and alerting.
    - Console Output: Print statistics directly to the console for quick inspection.

- Testing and Deployment:
  - Provided Dockerfiles and Docker Compose scripts to facilitate easy testing and deployment in containerized environments.
- Package Building:
  - Improved the build process for generating DEB and RPM packages, ensuring smooth installation and management on various Linux distributions.


### Building Distribution Packages
To simplify the process of building distribution packages, use the provided Makefile, which includes commands for generating DEB and RPM packages. Follow the instructions below based on the package type you need to build.

General Prerequisites:

Make sure you have make installed. This is typically available by default on most Linux distributions.
- For DEB Packages:

Install the necessary tools for building DEB packages:
```bash
sudo apt-get install build-essential debhelper dh-make
```
Navigate to the directory containing the Makefile and run:
```bash
make deb
```

- For RPM Packages:

Install the necessary tools for building RPM packages:
```bash
sudo dnf install rpm-build rpmlint
```
Navigate to the directory containing the Makefile and run:
```bash
make rpm
```

Forementioned commands will generate the respective DEB and RPM packages in the `dist` directory.

### Installation

<div style="border: 1px solid yellow; background-color: #fffadd; padding: 10px; margin: 10px 0;">
  <strong>Warning:</strong> 
Package requires python3.9 or above istalled on the target system.

Additionaly python executable should be presents as `python3` in the system PATH.
You can create alias `alias python3=python` before installation.
</div>

- For DEB Packages:
```bash
apt install ./dist/nfs-stats-collector_1.0.0-1_amd64.deb 
```

- For RPM Packages:
```bash
dnf install ./dist/nfs-stats-collector-1.0.0-1.x86_64.rpm
```

Both packages install a basic console utility **vnfs-collector** which will be added to the system PATH. This allows you to use the utility directly from the command line.
```bash
vnfs-collector --help
```

Additionally:
- the **vnfs-collector** systemd service will be installed and enabled. The service configuration file can be found at `/etc/systemd/system/vnfs-collector.service`.
- the **vnfs-collector** configuration file will be placed at `/etc/vnfs-collector/vnfs-collector.conf`. This file contains the default configuration options for the utility.

systemd service can be started, stopped, and restarted using the following commands:
```bash
systemctl start vnfs-collector
systemctl stop vnfs-collector
systemctl restart vnfs-collector
```

Logs can be viewed using the following command:
```bash
journalctl -u vnfs-collector
```

### "Stand-alone" vnfs-collector Usage
<div style="border: 1px solid yellow; background-color: #fffadd; padding: 10px; margin: 10px 0;">
  <strong>Warning:</strong> 
Due to internal package requirements, the utility must be run exclusively as the root user.
</div>

The **vnfs-collector** utility can be used as a stand-alone tool without the need for the systemd service. This can be useful for testing and debugging purposes.
Helpful information can be found by running:
```bash
vnfs-collector --help
```
Using help you can check all required and optional arguments of the utility.

At least one driver must be specified when running the utility. For example, to output statistics to the console, use:
```bash
[sudo] vnfs-collector -d screen
```

To output statistics to a local file, use:
```bash
[sudo] vnfs-collector -d file --samples-path /path/to/logfile
```

All available options can be provided as command-line arguments or using configuration file:
```bash
[sudo] vnfs-collector -C /path/to/config/file
```
Configuration file example:
```yaml
interval: 5
vaccum: 600
screen: {}
file:
  samples_path: /opt/vnfs-collector/vnfs-collector.log
  max_backups: 5
  max_size_mb: 200
vdb:
  db_endpoint: http://vippool-1.vast217-az.VastENG.lab
  db_access_key: 55G3ZXQ5RRXVDW58I207
  db_secret_key: +D2ChyTd1xeIaULSXl/BaBDsP+8TUsx/rL31APmb
  db_bucket: nfsops-metrics
  db_schema: nfsops
  db_table: nfsops
prometheus:
  prometheus_host: 0.0.0.0
  prometheus_port: 9000
```

**screen**, **file**, **vdb**, and **prometheus** are the names of appropriate drivers.

Note: **screen** driver in this example is empty section:
```yaml
screen: {}
```
Presence of this key means that driver is enabled and uses default options.

Each driver has its own set of options that can be configured in the configuration file.
All these options are the same as CLI flags but hyphens are replaced with underscores.
For example, `--samples-path` becomes `samples_path`, `db-schema` -> `db_schema` and so on.

### Docker Usage
<div style="border: 1px solid #002aff; background-color: #dde9ff; padding: 10px; margin: 10px 0;">
  <strong>Note:</strong> 
Usage with docker is experimental and not yet fully tested.
</div>

The utility can be run in a Docker container using the provided Dockerfile. This can be useful for testing and deployment in containerized environments.
##### Prerequisites:
- To build docker image you should have package distribution files in the `dist` directory.
##### Start debian based container:
```bash
docker build -f docker/debian.Dockerfile -t vnfs-collector .
docker run \
  --privileged \
  --volume $(pwd):/opt/nfsops \
  --volume /lib/modules:/lib/modules:ro \
  --volume /usr/src:/usr/src:ro \
  --volume /usr/sbin:/usr/sbin:ro \
  --volume /sys/fs:/sys/fs:ro \
  --volume /sys/kernel:/sys/kernel:ro \
  --name vnfs-collector \
  -t vnfs-collector \
  -C nfsops.yaml
```

##### Start rocky based container:
```bash
docker build -f docker/rocky.Dockerfile -t vnfs-collector .
docker run \
  --privileged \
  --volume $(pwd):/opt/nfsops \
  --volume /lib/modules:/lib/modules:ro \
  --volume /usr/src:/usr/src:ro \
  --volume /usr/sbin:/usr/sbin:ro \
  --volume /sys/fs:/sys/fs:ro \
  --volume /sys/kernel:/sys/kernel:ro \
  --name vnfs-collector \
  -t vnfs-collector \
  -C nfsops.yaml
```

If you want docker container to survive machine restarts
you can add `--restart always` or `--restart unless-stopped` option to `docker run` command.
