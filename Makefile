LOCAL_VERSION=$(shell cat version.txt)
GIT_VERSION=$(shell git rev-parse --short HEAD)
VERSION=${LOCAL_VERSION}-${GIT_VERSION}
.DEFAULT_GOAL := all

all: pkg

pkg: rpm deb

distdir:
	@mkdir -p dist

pylib:
	@echo "Building python library"
	@python3 -m build
	@rm -f dist/*.gz

rpm: distdir pylib
	rpmbuild -bb vnfs-collector.spec --define "_sourcedir `pwd`" --define "_version ${LOCAL_VERSION}" --define "_release ${GIT_VERSION}"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir pylib
	@cp debian/changelog.in debian/changelog
	@sed -i "1s/_VERSION_/${VERSION}/" debian/changelog
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/

clean:
	@rm -rf dist/

up:
	@export VERSION=$(VERSION) && docker compose up

docker_build: deb
	@docker build -f docker/debian.Dockerfile -t vnfs-collector --build-arg="VERSION=${VERSION}" .
