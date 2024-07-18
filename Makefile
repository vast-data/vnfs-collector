
.DEFAULT_GOAL := all
all: pkg

pkg: rpm deb

distdir:
	@mkdir -p dist

rpm: distdir
	rpmbuild -bb vnfs-collector.spec --define "_sourcedir `pwd`"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@rm -f ../vnfs-collector*.buildinfo ../vnfs-collector*.changes

clean:
	@rm -rf dist/
