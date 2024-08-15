Name:           vnfs-collector
Version:        0.0.1
Release:        1%{?dist}
Summary:        NFS metrics collector based on eBPF
BuildArch:      noarch
License:        GPL
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
pylib_version=$(cat %{_sourcedir}/version.txt)
pylib_wheel=vast_client_tools-${pylib_version}-py3-none-any.whl

# Install the tarball and other files
install -m 755 %{_sourcedir}/dist/$pylib_wheel %{buildroot}/opt/$pname/src/
install -m 755 %{_sourcedir}/version.txt %{buildroot}/opt/$pname/src/
install -m 755 %{_sourcedir}/nfsops.yaml %{buildroot}/opt/$pname/
install -m 644 %{_sourcedir}/systemd/$pname.service %{buildroot}/opt/$pname/src/
cp -r %{_sourcedir}/hack/* %{buildroot}/opt/$pname/src/hack/

%files
/opt/vnfs-collector

%post
#!/bin/bash
set -e
/opt/vnfs-collector/src/hack/pack_install.sh

%preun
#!/bin/bash
set -e
/opt/vnfs-collector/src/hack/pack_uninstall.sh


%changelog
* Thu Jul 11 2024 Sagi Grimberg <sagi@grimberg.me> - 0.0.1
- First version being packaged
