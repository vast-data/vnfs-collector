FROM rockylinux/rockylinux:8

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

COPY dist/vnfs-collector-0.0.1-1.noarch.rpm .
RUN dnf -y install vnfs-collector-0.0.1-1.noarch.rpm && rm -f vnfs-collector-0.0.1-1.noarch.rpm

ENTRYPOINT ["vnfs"]
