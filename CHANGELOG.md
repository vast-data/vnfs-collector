# CHANGELOG

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
