VERSION=$(shell cat version.txt)
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
	rpmbuild -bb vnfs-collector.spec --define "_sourcedir `pwd`" --define "_version ${VERSION}"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir pylib
	@sed -i "s/^vnfs-collector ([0-9.]*).*/vnfs-collector (${VERSION}) unstable; urgency=low/" debian/changelog
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/

clean:
	@rm -rf dist/

up:
	@export VERSION=$(VERSION) && docker compose up
