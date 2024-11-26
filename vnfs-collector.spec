Name:           vnfs-collector
Version:        %{_version}
Release:        1%{?dist}
Summary:        NFS metrics collector based on eBPF
BuildArch:      noarch
License:        Apache-2.0
Provides:       vnfs-collector
Requires:       python3 python3-bcc >= 0.23.0
Requires(post): systemd

%description
eBPF-based metrics collector that tracks NFS operations per process/mount
with the ability to route statistics to multiple sinks such as Vast native DB,
Prometheus, and more, as well as outputting to a local log file or stdout.

%prep
# No preparatory steps needed in this case.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/vnfs-collector/src/hack

# Extract version from version.txt

pname=vnfs-collector
pylib_wheel=vast_client_tools-%{_version}-py3-none-any.whl

# Install the tarball and other files
install -m 755 %{_sourcedir}/dist/$pylib_wheel %{buildroot}/opt/$pname/src/
install -m 755 %{_sourcedir}/version.txt %{buildroot}/opt/$pname/src/
install -m 755 %{_sourcedir}/nfsops.yaml %{buildroot}/opt/$pname/
mkdir -p %{buildroot}/etc/systemd/system/
install -m 644 %{_sourcedir}/systemd/$pname.service %{buildroot}/etc/systemd/system/
cp -r %{_sourcedir}/hack/* %{buildroot}/opt/$pname/src/hack/

%files
/opt/vnfs-collector
%config(noreplace)/opt/vnfs-collector/nfsops.yaml
/etc/systemd/system/vnfs-collector.service

%post
#!/bin/bash
set -e
/opt/vnfs-collector/src/hack/pack_install.sh
if systemctl daemon-reload > /dev/null 2>&1; then
	systemctl enable vnfs-collector
fi

%preun
#!/bin/bash
set -e
if [ $1 == "0" ]; then # uninstall
	SERVICE_NAME="vnfs-collector"
	# Stop the systemd service if it's running
	if systemctl is-active --quiet "${SERVICE_NAME}"; then
	    systemctl stop "${SERVICE_NAME}"
	fi

	# Disable the systemd service
	systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
	/opt/vnfs-collector/src/hack/pack_uninstall.sh
fi

%changelog
* Mon Oct 28 2024 Sagi Grimberg <sagi@grimberg.me> - 1.1
- various bug fixes in the collector ebpf
- Fix some naming/documentation related to prometheus exporter
- added k8s daemonset deployment
- fix mount resolution (again)
- args parsing fixes
* Thu Jul 11 2024 Sagi Grimberg <sagi@grimberg.me> - 1.0
- First version being packaged
