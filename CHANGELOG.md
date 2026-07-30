# CHANGELOG

## Version 1.7
* updated grafana slurm dashboard and recording rules examples

## Version 1.6
* fixed per-mount stats when multiple mounts share the same fsid
* Added offline make target for pkg install without internet access
* few fixes to wrong ebpf kprobes
* add TruffleHog scanning
* fixed wrong perms for nfsops.yaml file
* support python >3.12
* fixed support for tag-filters when envs come from vdb schema

## Version 1.5
* fixed prometheus sink driver to present the latest sample as read-only
  and not clear it, causing misses for additional scrapes
* added otel sink driver
* added support for sink-batch-size to allow different intervals for stats
  collection and sink store
* added support for kernel >= 6.17
* Add example prometheus/grafana dashboard for a slurm environment
* fix infinite loop when bpf attach fails
* fix debian pkg install after remove
* minor code/unittests fixes

## Version 1.4
* deprecate vdb argument `--db-tenant`
* remove vdb argument `--db-bucket` default value - make it mandatory for vdb driver

## Version 1.3
* rename package to vnfs-collector
* github hosting (public repo)
* spdx license statements
* minor logging fixes

## Version 1.2
* Added `--db-tenant` flag to specify the tenant for the vdb driver. Default options for `db-bucket`, `db-schema` and `db-table`
* Support env variables from VDB schema (ORION-199045).
* Added timedelta column to vdb schema and updated schema types
* Require python >= 3.9
* README updates
* Various fixes around versioning, argument parsing and installation

## Version 1.1
* various bug fixes in the collector ebpf
* Fix some naming/documentation related to prometheus exporter
* added k8s daemonset deployment
* fix mount resolution (again)
* args parsing fixes

## Version 1.0
* Initial release
