# VNFS Collector Prometheus/Grafana Configuration for Slurm

This directory contains the configuration files needed to set up vnfs-collector monitoring with Prometheus and Grafana in a Slurm HPC environment.

## Overview

The setup consists of three main components:

1. **vnfs-collector configuration** (`nfsops.yaml`) - Configures the collector to export metrics with Slurm-specific labels
2. **Prometheus recording rules** (`vnfs_recording_rules.yaml`) - Pre-computes aggregated metrics for efficient dashboard queries
3. **Grafana dashboard** (`vnfs_collector_grafana_dashboard.json`) - Visualizes NFS metrics by various dimensions

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  vnfs-collector │────▶│   Prometheus    │────▶│     Grafana     │
│  (per node)     │     │  + Recording    │     │   Dashboard     │
│                 │     │    Rules        │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │
        │ Exports:              │ Pre-computes:
        │ - vnfs_READ_COUNT     │ - vnfs:read_iops:total
        │ - vnfs_WRITE_BYTES    │ - vnfs:read_bytes:by_hostname
        │ - SLURM_JOB_ID label  │ - vnfs:metadata_ops:by_slurm_job_id
        │ - VAST_QOS_POLICY     │ - etc.
        │   label               │
        └───────────────────────┘
```

## Configuration Files

### 1. nfsops.yaml - vnfs-collector Configuration

```yaml
envs:
 - SLURM_JOB_ID
 - VAST_QOS_POLICY
 - SLURM_JOB_NAME
interval: 30
prometheus:
 prom_exporter_host: 0.0.0.0
 prom_exporter_port: 9000
```

| Setting | Value | Description |
|---------|-------|-------------|
| `envs` | `SLURM_JOB_ID`, `VAST_QOS_POLICY`, `SLURM_JOB_NAME` | Environment variables to capture as metric labels. These are read from the process environment of NFS-accessing processes. |
| `interval` | `30` | Collection interval in seconds. Must match the recording rules interval. |
| `prometheus.prom_exporter_host` | `0.0.0.0` | Listen on all interfaces |
| `prometheus.prom_exporter_port` | `9000` | Prometheus scrape port |

**Deployment:** Copy this file to `/etc/vnfs-collector/nfsops.yaml` on each compute node, or mount it as a ConfigMap in Kubernetes.

### 2. vnfs_recording_rules.yaml - Prometheus Recording Rules

Recording rules pre-compute aggregated metrics every 30 seconds, reducing query load on the Prometheus server and enabling faster dashboard rendering.

**Rule Groups:**

| Group Name | Description | Example Metrics |
|------------|-------------|-----------------|
| `vnfs_total` | Cluster-wide totals | `vnfs:read_iops:total`, `vnfs:write_bytes:total`, `vnfs:errors:total` |
| `vnfs_by_hostname` | Per-host breakdown | `vnfs:read_bytes:by_hostname`, `vnfs:metadata_ops:by_hostname` |
| `vnfs_by_op_type` | Per-operation breakdown | `vnfs:iops:by_op_type{op_type="read"}`, `vnfs:duration:by_op_type` |
| `vnfs_by_mount` | Per-mount breakdown | `vnfs:read_iops:by_mount`, `vnfs:errors:by_mount` |
| `vnfs_by_slurm_job_id` | Per-Slurm-job breakdown | `vnfs:read_bytes:by_slurm_job_id`, `vnfs:metadata_ops:by_slurm_job_id` |
| `vnfs_by_vast_qos_policy` | Per-QoS-policy breakdown | `vnfs:write_iops:by_vast_qos_policy` |

**Installation (Standalone Prometheus):**

```bash
# Copy rules to Prometheus rules directory
cp vnfs_recording_rules.yaml /etc/prometheus/rules/

# Ensure prometheus.yml includes the rules directory
# rule_files:
#   - "rules/*.yaml"

# Reload Prometheus
kill -HUP $(pidof prometheus)
# Or via API (if --web.enable-lifecycle is enabled):
curl -X POST http://localhost:9090/-/reload

# Verify rules are loaded
curl -s http://localhost:9090/api/v1/rules | \
  jq '.data.groups[] | select(.name | startswith("vnfs_"))'
```

**Important:** The recording rules assume:
- Prometheus scrapes vnfs-collector with `job="vnfs-collector"` label
- The scrape and rule evaluation intervals are aligned (30 seconds)
- Metrics are divided by 30 to convert counts to per-second rates (IOPS, throughput)

### 3. vnfs_collector_grafana_dashboard.json - Grafana Dashboard

The dashboard provides visualization panels organized into sections:

| Section | Description |
|---------|-------------|
| **Overview (Totals)** | Stat panels showing cluster-wide Read IOPS, Write IOPS, Metadata IOPS, Throughput, and Errors |
| **Breakdown by Host** | Time series graphs showing metrics per compute node |
| **Breakdown by NFS Operation Type** | Throughput, IOPS, duration, and errors by operation (read, write, open, close, getattr, etc.) |
| **Breakdown by Mount** | Metrics grouped by NFS mount point |
| **Breakdown by SLURM Job ID** | Metrics grouped by Slurm job, useful for identifying I/O-heavy jobs |
| **Breakdown by VAST QoS Policy** | Metrics grouped by QoS policy for capacity planning |

**Installation:**

1. In Grafana, go to **Dashboards** → **Import**
2. Upload `vnfs_collector_grafana_dashboard.json` or paste its contents
3. Select your Prometheus data source
4. Click **Import**

**Dashboard Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `$topk` | `10` | Number of top series to display in time series panels |

## Metric Labels

Raw metrics exported by vnfs-collector include these labels:

| Label | Description |
|-------|-------------|
| `HOSTNAME` | Compute node hostname |
| `MOUNT` | NFS mount point path |
| `COMM` | Process command name |
| `UID` | User ID |
| `REMOTE_PATH` | Remote NFS path |
| `SLURM_JOB_ID` | Slurm job ID (from environment) |
| `SLURM_JOB_NAME` | Slurm job name (from environment) |
| `VAST_QOS_POLICY` | VAST QoS policy (from environment) |

## Troubleshooting

### No metrics in Grafana

1. Check vnfs-collector is running: `systemctl status vnfs-collector`
2. Verify metrics are exported: `curl http://<node>:9000/metrics`
3. Check Prometheus targets: `http://<prometheus>:9090/targets`
4. Verify recording rules: `http://<prometheus>:9090/rules`

### Missing SLURM_JOB_ID labels

Ensure Slurm is configured to propagate environment variables to jobs. In `slurm.conf`:

```
PropagateResourceLimits=ALL
```

Or explicitly export in job scripts:

```bash
export SLURM_JOB_ID
export SLURM_JOB_NAME
```

### Recording rules not evaluating

1. Check rule syntax: `promtool check rules vnfs_recording_rules.yaml`
2. Verify the `job="vnfs-collector"` label matches your scrape config
3. Check Prometheus logs for evaluation errors

## Customization

### Adjusting the collection interval

If you change the `interval` in `nfsops.yaml`, you must also:

1. Update all recording rules to divide by the new interval (instead of `/30`)
2. Update `interval: 30s` in each rule group

### Adding custom environment variables

To track additional environment variables as labels:

1. Add them to the `envs` list in `nfsops.yaml`
2. Create new recording rule groups aggregating by the new label
3. Add corresponding panels to the Grafana dashboard
