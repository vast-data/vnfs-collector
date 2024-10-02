FROM python:3.12-slim-bookworm

ARG VERSION=${VERSION}
LABEL version=$VERSION
LABEL vendor=vastdata

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

COPY dist/vnfs-collector_${VERSION}_all.deb .
RUN apt update && apt install ./vnfs-collector_${VERSION}_all.deb -y && rm -f vnfs-collector_${VERSION}_all.deb

RUN mkdir /opt/vnfs-collector/src/bin
ADD scripts/host-chroot.sh /opt/vnfs-collector/src/bin

RUN \
   ln -s /opt/vnfs-collector/src/bin/host-chroot.sh /opt/vnfs-collector/src/bin/xz \
   && ln -s /opt/vnfs-collector/src/bin/host-chroot.sh /opt/vnfs-collector/src/bin/modprobe

ENV PATH="/opt/vnfs-collector/src/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

ENTRYPOINT ["vnfs-collector"]
