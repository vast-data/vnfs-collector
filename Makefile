VERSION=$(shell git describe --tags --long --abbrev=12 | sed 's/^v//')
SEMANTIC_VERSION=$(shell git describe --tags --abbrev=0 | sed 's/^v//')
GIT_VERSION=$(shell echo ${VERSION} | cut -d'-' -f 2,3 | sed 's/-/./g')
PY_VERSION=$(shell echo ${VERSION} | cut -d'-' -f 1,2)
COMMIT_COUNT=$(shell echo ${VERSION} | cut -d'-' -f 2)
.DEFAULT_GOAL := all

all: pkg

pkg: rpm deb

distdir:
	@mkdir -p dist

versionfile:
	@echo ${PY_VERSION} > version.txt

pylib: versionfile
	@echo "Building python library"
	@python3 -m build
	@rm -f dist/*.gz

rpm: distdir pylib
	rpmbuild -bb vnfs-collector.spec --define "_sourcedir `pwd`" --define "_version ${SEMANTIC_VERSION}" --define "_release ${GIT_VERSION}" --define "_post post${COMMIT_COUNT}"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir pylib
	@cp debian/changelog.in debian/changelog
	@sed -i "1s/_VERSION_/${SEMANTIC_VERSION}-${GIT_VERSION}/" debian/changelog
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/

clean:
	@rm -rf dist/ version.txt

up: deb
	@export VERSION=${SEMANTIC_VERSION}-${GIT_VERSION} && docker compose up

docker_build: deb
	@docker build -f docker/debian.Dockerfile -t vnfs-collector --build-arg="VERSION=${SEMANTIC_VERSION}-${GIT_VERSION}" .
