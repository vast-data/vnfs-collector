FROM rockylinux/rockylinux:8

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

# Install required packages and enable the Python 3.9 module
RUN dnf -y update && \
    dnf -y install epel-release && \
    dnf -y install dnf-utils && \
    dnf -y module enable python39 && \
    dnf -y install python39 python39-devel python39-pip && \
    dnf clean all

# Set Python 3.9 as the default python3
RUN alternatives --set python3 /usr/bin/python3.9

# Copy and install Python package
COPY dist/vnfs-collector-0.0.1-1.noarch.rpm .
RUN dnf -y install vnfs-collector-0.0.1-1.noarch.rpm && rm -f vnfs-collector-0.0.1-1.noarch.rpm

ENTRYPOINT ["vnfs"]
