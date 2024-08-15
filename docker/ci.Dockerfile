FROM docker:latest

RUN apk add --no-cache \
        python3 \
        py3-pip \
        make \
        build-base \
        curl \
        rpm \
        fakeroot \
        dpkg-dev \
        bash

# Upgrade pip to the latest version
RUN python3 -m pip install --upgrade pip pytest build --break-system-packages
