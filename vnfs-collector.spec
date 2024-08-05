Name:           vnfs-collector
Version:        0.0.1
Release:        1%{?dist}
Summary:        NFS metrics collector based on ebpf
BuildArch:      noarch
License:        GPL
Provides:       vnfs-collector
Requires:       python3 python3-bcc python3-psutil python3-prometheus_client
Requires(post): systemd

%description
ebpf based metrics collector that tracks NFS operations per process/mount
with ability to route statistics to multiple sinks such as vast native DB,
prometheus and more as well outputing to a local log file or stdout.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/opt/vnfs-collector
install -m 0755 %{_sourcedir}/nfsops.py %{_sourcedir}/nfsops.c %{buildroot}/opt/vnfs-collector/
install -m 0644 %{_sourcedir}/nfsops.yaml %{buildroot}/opt/vnfs-collector/
mkdir -p %{buildroot}/etc/systemd/system/
install -m 0755 %{_sourcedir}/systemd/vnfs-collector.service %{buildroot}/etc/systemd/system

%files
%dir /opt/vnfs-collector
/opt/vnfs-collector/nfsops.yaml
/opt/vnfs-collector/nfsops.py
/opt/vnfs-collector/nfsops.c
/etc/systemd/system/vnfs-collector.service

%post
if [ $1 -eq 1 ]; then # 1 : This package is being installed for the first time
       systemctl daemon-reload
fi

%changelog
* Thu Jul  11 2024 Sagi Grimberg <sagi@grimberg.me> - 0.0.1
- First version being packaged
