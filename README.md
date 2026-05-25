# NFS Statistics Collector

## Overview
The NFS Statistics Collector utility is a lightweight process/application aware NFS
statistics collector. The tool is designed to track per-application NFS operations
and export the stats to various destinations (like prometheus, Vast Data native DB, local
log, etc).

## Features
- Packaging and distribution via DEB and RPM packages as well as a docker image.
- Flexible Data Storage: Includes drivers for exporting statistics to multiple destinations:
  - VDB: Vast Data native database solution.
  - Local log: Save statistics to local files for offline analysis.
  - Prometheus: Integrate with Prometheus for real-time metrics monitoring and alerting.
  - OpenTelemetry: Push metrics to an OpenTelemetry collector via OTLP/gRPC.
  - Kafka: Stream metrics to a predefined kafka broker in a specific topic
  - Console Output: Print statistics directly to the console.

- Testing and Deployment:
  - Provided Dockerfiles and Docker Compose scripts to facilitate easy testing and deployment in
    containerized environments.
- Package Building:
  - Improved the build process for generating DEB and RPM packages, ensuring smooth installation
    and management on various Linux distributions.


## Releases
- See rpm/deb released packages in [Releases](https://github.com/vast-data/vnfs-collector/releases)
- See container images in [Dockerhub](https://hub.docker.com/r/vastdataorg/vnfs-collector/tags)

### Building Distribution Packages
To simplify the process of building distribution packages, use the provided Makefile, which
includes commands for generating DEB and RPM packages.
Follow the instructions below based on the package type you need to build.

General Prerequisites:
Make sure you have make installed. This is typically available by default on most Linux distributions.
In addition, python library build requires python3 build module:
```bash
pip3 install build
```

- DEB Packaging:

Install the necessary tools for building DEB packages:
```bash
sudo apt-get install build-essential debhelper dh-make
```
Navigate to the directory containing the Makefile and run:
```bash
make deb
```

- RPM Packaging:

Install the necessary tools for building RPM packages:
```bash
sudo dnf install rpm-build rpmlint
```
Navigate to the directory containing the Makefile and run:
```bash
make rpm
```
Build artifact will be located in the `dist` directory.

#### Offline (air-gapped) installation

By default (`OFFLINE=0`), `make rpm` and `make deb` produce a single
`noarch` package. At install time the `postinst` script `pip install`s
the python deps from the configured pip index, so the target host must
have network access (or a reachable index configured via `PIP_*`
environment variables).

To build artifacts suitable for air-gapped installation, pass
`OFFLINE=1`:

```bash
make rpm OFFLINE=1
make deb OFFLINE=1
```

This produces **one package per architecture**. Each package is
properly arch-tagged and bundles a wheelhouse containing wheels for
every supported python minor version. The `postinst` script picks
whichever `python3 >= 3.9` is installed on the target host and lets
`pip install --no-index` match the right wheels out of the bundle —
so a single artifact works across any python3 version the host happens
to ship.

##### Artifact naming

For `make rpm deb OFFLINE=1` with the default matrix, `dist/` ends up
containing:

```
dist/vnfs-collector-1.5-0.g5ee6ea618199.x86_64.rpm
dist/vnfs-collector-1.5-0.g5ee6ea618199.aarch64.rpm
dist/vnfs-collector_1.5-0.g5ee6ea618199_amd64.deb
dist/vnfs-collector_1.5-0.g5ee6ea618199_arm64.deb
```

Customers just pick the one matching their host architecture.

##### Matrix knobs (OFFLINE=1)

| Knob                      | Default                                            |
|---------------------------|----------------------------------------------------|
| `WHL_ARCHS`               | `x86_64 aarch64`                                   |
| `WHL_PYVERS`              | `3.9 3.10 3.11 3.12 3.13 3.14`                     |
| `WHL_PLATFORMS_x86_64`    | `manylinux_2_28_x86_64 manylinux_2_17_x86_64`      |
| `WHL_PLATFORMS_aarch64`   | `manylinux_2_28_aarch64 manylinux_2_17_aarch64`    |
| `WHL_EXTRAS`              | `pip setuptools`                                   |

`WHL_ARCHS` controls how many artifacts are produced (one per entry).
`WHL_PYVERS` controls which python minor versions each package
supports at install time — the wheelhouse pulls wheels for every
listed version so pip can match the target host's python3.

For each `(py, arch)` download, pip is given both manylinux platform
tags at once so it can accept wheels regardless of which generation
they declare.

Single-arch build for a smaller/faster release:

```bash
make rpm OFFLINE=1 WHL_ARCHS="x86_64"
```

Extend supported python versions (e.g. add 3.X):

```bash
make rpm OFFLINE=1 WHL_PYVERS="3.9 3.10 3.11 3.12 3.13 3.14 3.X"
```

Shrink to a single-version build (smaller package; at install time the
`postinst` will locate and use a matching `pythonX.Y` on the target
host, failing fast with a clear error if none is found):

```bash
make rpm OFFLINE=1 WHL_PYVERS="3.12" WHL_ARCHS="x86_64"
```

Build only the combined dist/wheelhouse (useful for debugging the
download matrix without invoking `rpmbuild`/`dpkg-buildpackage`):

```bash
make wheelhouse OFFLINE=1
```

### Installation

<div style="border: 1px solid yellow; background-color: #fffadd; padding: 10px; margin: 10px 0;">
  <strong>Warning:</strong> 
Package requires python3.9 or above installed on the target system.

Additionally python executable should be presents as `python3` in the system
PATH. You can create alias `alias python3=python` before installation.
</div>

- For DEB Packages:
```bash
apt install ./dist/vnfs-collector_1.0.0-1_amd64.deb 
```

- For RPM Packages:
```bash
dnf install ./dist/vnfs-collector-1.0.0-1.noarch.rpm
```

Both packages install a basic console utility **vnfs-collector** which will be
added to the system PATH. This allows you to use the utility directly from the
command line.
```bash
vnfs-collector --help
```

Additionally:
- the **vnfs-collector** systemd service will be installed and enabled.
  The service configuration file can be found at `/etc/systemd/system/vnfs-collector.service`.
- the **vnfs-collector** configuration file will be placed at
  `/opt/vnfs-collector/nfsops.yaml`.
  This file contains the default configuration options for the utility.

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

The **vnfs-collector** utility can be used as a stand-alone tool without the
need for the systemd service. This can be useful for testing and debugging
purposes. Helpful information can be found by running:
```bash
vnfs-collector --help
```
Using help you can check all required and optional arguments of the utility.

At least one driver must be specified when running the utility.
For example, to output statistics to the console, use:
```bash
[sudo] vnfs-collector -d screen
```

To output statistics to a local file, use:
```bash
[sudo] vnfs-collector -d file --samples-path /path/to/logfile
```

All available options can be provided as command-line arguments or using
a configuration file:
```bash
[sudo] vnfs-collector -C /path/to/config/file
```
Configuration file example:
```yaml
interval: 5
sink_batch_size: 1
vaccum: 600
screen: {}
file:
  samples_path: /opt/vnfs-collector/vnfs-collector.log
  max_backups: 5
  max_size_mb: 200
vdb:
  db_endpoint: <endpoint>
  db_access_key: <access_key>
  db_secret_key: <secret_key>
  db_bucket: <bucket>
  db_schema: <schema>
  db_table: <table>
prometheus:
  prom_exporter_host: 0.0.0.0
  prom_exporter_port: 9000
otel:
  otel_collector_host: localhost
  otel_collector_port: 4317
```

**screen**, **file**, **vdb**, **kafka**, **prometheus** and **otel** are the names of
appropriate drivers.

Note: **screen** driver in this example is empty section:
```yaml
screen: {}
```
This key means that driver is enabled and uses default options.


### Drivers Usage Examples

**Note**: The examples below are not exhaustive combinations of possible values for each driver.  
They are brief illustrations to demonstrate basic usage. For detailed information about each driver's options, use the command:  

```bash
vnfs-collector --help
```
Options marked with ⚠ are mandatory for the respective driver.

#### Batching Configuration

The collector supports batching samples before sending to sinks using `--sink-batch-size`:

```bash
# Collect every 5 seconds, send to sinks after 6 samples (every 30 seconds)
vnfs-collector -d prometheus -i 5 --sink-batch-size 6
```

- **Prometheus**: Aggregates batched samples into a single data point (sums counts, bytes, durations)
- **Other drivers** (OTEL, Kafka, VDB, File, Screen): Process each sample in the batch individually

Default is `--sink-batch-size 1` (no batching, send immediately after each collection).

#### Mount attribution

**`--mnt-id-segmentation`** (default: `true`): When enabled, the BPF program reads
per-mount IDs from the kernel, resolves mount points via `/proc/<pid>/mountinfo`,
and attaches `security_path_*` LSM probes so inode-only NFS operations (create,
unlink, mkdir, etc.) can be tied to a mount. When disabled, aggregation and
resolution use superblock device id (`sbdev`, `major:minor` from mountinfo column 3)
only; `security_path_*` probes are not attached. Use `false` only if you accept
coarser attribution when multiple NFS mounts share one superblock or you know
that your NFS mounts do not share the same fsid.

**`--track-lookup-access`** (default: `true`): When enabled, `nfs_lookup` and
`nfs_do_access` are traced. When disabled, those probes are not loaded. Lookup and
access counters use inode/sbdev attribution only and are **ambiguous** when several
mount points share the same superblock; disable if you do not need these metrics or
your mounts do not share `sbdev`.

Example (sbdev-only, no lookup/access):

```bash
vnfs-collector -d file --mnt-id-segmentation false --track-lookup-access false
```

YAML:

```yaml
mnt_id_segmentation: true
track_lookup_access: true
interval: 5
file:
  samples_path: /opt/vnfs-collector/vnfs-collector.log
```

#### File Driver
The file driver stores collected statistics in a local file. It provides the following configuration options:

```yaml
file:
  samples_path: /opt/vnfs-collector/vnfs-collector.log  # Path to the log file
  max_backups: 5                                        # Maximum number of backups to retain
  max_size_mb: 200                                      # Maximum file size in MB before rollover
```

#### Screen Driver
The screen driver outputs statistics directly to the console.

```yaml
screen:
  table_format: true  # Display statistics in a tabular format
```

#### Kafka Driver
The Kafka driver sends collected statistics to a Kafka topic.

```yaml
kafka:
  bootstrap_servers: broker1:9093,broker2:9093  # Kafka bootstrap servers
  topic: vnfs-collector                        # Kafka topic name
  sasl_username: admin                         # SASL username (optional, required for SASL protocols)
  sasl_password: password123                   # SASL password (optional, required for SASL protocols)
  security_protocol: SASL_PLAINTEXT            # Security protocol (e.g., PLAINTEXT, SSL, SASL_PLAINTEXT) 
```

#### Prometheus Driver
The Prometheus driver exposes statistics via an HTTP endpoint for Prometheus to scrape.

```yaml
prometheus:
  prom_exporter_host: 0.0.0.0     # Hostname or IP address for the Prometheus exporter
  prom_exporter_port: 9000        # Port for the Prometheus exporter
```

#### OpenTelemetry Driver
The OpenTelemetry driver pushes metrics to an OpenTelemetry collector via OTLP/gRPC protocol.

```yaml
otel:
  otel_collector_host: localhost  # OpenTelemetry collector hostname
  otel_collector_port: 4317       # OpenTelemetry collector gRPC port
  otel_insecure: true             # Use insecure connection (default: true)
```

#### VDB Driver
The VDB (VAST database) driver connects to a database to store statistics.

Example: Specifying db_bucket, db_schema, and db_table
    
```yaml
vdb:
  db_endpoint: database.example.com          # Database endpoint
  db_access_key: my-access-key               # Access key for the database
  db_secret_key: my-secret-key               # Secret key for the database
  db_bucket: custom-bucket                   # Custom database bucket name
  db_schema: custom-schema                   # Custom database schema name (optional)
  db_table: custom-table                     # Custom database table name (optional)
```


### Docker Usage
The utility can be run in a Docker container using the provided Dockerfile.
This can be useful for testing and deployment in containerized environments.
##### Prerequisites:
- To build docker image you should have package distribution files in the `dist`
  directory.
##### Start debian based container:
```bash
docker build --build-arg="VERSION=$(cat version.txt)" -f docker/debian.Dockerfile -t vnfs-collector .
docker run \
  --privileged \
  --volume $(pwd):/opt/nfsops \
  --volume /lib/modules:/lib/modules:ro \
  --volume /usr/src:/usr/src:ro \
  --volume /sys/fs:/sys/fs:ro \
  --volume /sys/kernel:/sys/kernel:ro \
  --volume /proc:/proc:rw \
  --volume /host:/:ro \
  --name vnfs-collector \
  -t vnfs-collector \
  -C nfsops.yaml
```

Make sure you specified correct volume bindings:

| Mount                          | Description                                                                                                                                                                                                                                                                            | Required |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `$(pwd):/opt/nfsops`           | Binds the host working directory to the working directory of `vnfs-collector` within the Docker container. This is optional and is used if you want to pass full configuration from the host. CLI arguments can also be provided separately (e.g., `-envs "JOBID,MYENV" -interval 5`). | No       |
| `/lib/modules:/lib/modules:ro` | Required for kernel extensions.                                                                                                                                                                                                                                                        | Yes      |
| `/usr/src:/usr/src:ro`         | Required for kernel extensions.                                                                                                                                                                                                                                                        | Yes      |
| `/sys/fs:/sys/fs:ro`           | Required for tracking active mounts.                                                                                                                                                                                                                                                   | Yes      |
| `/sys/kernel:/sys/kernel:ro`   | Required for kernel extensions.                                                                                                                                                                                                                                                        | Yes      |
| `/proc:/proc:rw`               | Required only if the `--envs` CLI flag is provided. This binding allows `vnfs-collector` to access environment variables per process.                                                                                                                                                  | No       |
| `/host:/:ro`                   | Required for sys calls execution from host                                                                                                                                                                                                                                             | Yes      |


If you want docker container to start at system boot, use `--restart always`
or `--restart unless-stopped` option to `docker run` command.


### VNFS Collector DaemonSet
The VNFS Collector DaemonSet deploys a BPF application on all nodes in a Kubernetes cluster.
It collects networking and performance metrics from the host system.

Key Features
- Runs with host PID and IPC for deep integration with the host.
- Privileged mode with elevated permissions.
- Access to critical system directories mounted as read-only.
- Automatically uses the hostname of the node for context.
-
Usage
To deploy, apply the YAML configuration:

```bash
kubectl apply -f k8s/daemonset.yaml
```

Check the status with:

```bash
kubectl get ds vnfs-collector
```

To view the logs, use:

```bash
kubectl logs ds/vnfs-collector -f
```

<div style="border: 1px solid #002aff; background-color: #dde9ff; padding: 10px; margin: 10px 0;">
  <strong>Note:</strong> 
Adjust the configuration, which can be found in the 
ConfigMap located in the k8s/daemonset.yaml file, to suit your environment.
</div>


To get information about all available options use:
```bash
kubectl exec -it ds/vnfs-collector -- vnfs-collector --help
```

To update the and apply a modified [configuration configmap](./k8s/daemonset.yaml), restart the daemonset using:
```bash
kubectl rollout restart ds/vnfs-collector
```

You can watch the restart rollout progress using:
```bash
kubectl rollout status ds/vnfs-collector
```

##### Access prometheus exported metrics:

Enable vnfs-collector built-in prometheus exporter by setting the exporter *local* address **prom_exporter_host**
and port **prom_exporter_port**.

In addition, the collector sampling is mandated by the *\<interval\>* argument, and prometheus metrics
are tracking the latest sample. In order to have correlated scrapes, set the prometheus scrape
interval to match the collector *\<interval\>* argument.

To expose the metrics to Prometheus when deployed on k8s, you need to create a service.
The type of service you choose will depend on whether your Prometheus instance is running within the same cluster.
If Prometheus is in the same cluster, you can use a [Headless service](./k8s/service-headless-prom-exporter.yaml)

Complete setup can be achived by applying the following files
- [vnfs-collector daemonset](./k8s/daemonset.yaml)
- [vnfs-collector service](./k8s/service-headless-prom-exporter.yaml)
- [prometheus configuration](./k8s/prometheus.yaml)

to get into prometheus ui execute:
```bash
 kubectl port-forward svc/prometheus-service 9090:9090
```

and follow http://localhost:9090/


If Prometheus is running outside the cluster, you should use a [NodePort service type](./k8s/service-external-prom-exporter.yaml)

To check:
```bash
curl http://<node-ip>:30900/metrics
```
