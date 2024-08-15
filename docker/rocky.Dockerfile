FROM rockylinux/rockylinux:8

ARG VERSION=${VERSION}
LABEL version=$VERSION
LABEL vendor=vastdata

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

COPY dist/vnfs-collector-${VERSION}-1.noarch.rpm .
RUN dnf -y install vnfs-collector-${VERSION}-1.noarch.rpm && rm -f vnfs-collector-${VERSION}-1.noarch.rpm

ENTRYPOINT ["vnfs-collector"]
