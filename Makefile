.DEFAULT_GOAL := all

all: pkg

pkg: rpm deb

distdir:
	@mkdir -p dist

pylib:
	@echo "Building python library"
	@python3 -m build
	@rm -f dist/*.whl

rpm: distdir pylib
	rpmbuild -bb vnfs-collector.spec --define "_sourcedir `pwd`"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir pylib
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/

clean:
	@rm -rf dist/
