FROM fedora:35

WORKDIR /opt/nfsops

ENV PYTHONUNBUFFERED=1

# Copy and install Python package
COPY dist/vnfs-collector-0.0.1-1.noarch.rpm .
RUN dnf install -y vnfs-collector-0.0.1-1.noarch.rpm && rm -f vnfs-collector-0.0.1-1.noarch.rpm

# Set the entry point
ENTRYPOINT ["vnfs"]
