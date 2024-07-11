
.DEFAULT_GOAL := all
all: pkg

pkg: rpm deb

distdir:
	@mkdir -p dist

rpm: distdir
	rpmbuild -bb client-metrics.spec --define "_sourcedir `pwd`"
	@mv ~/rpmbuild/RPMS/noarch/client-metrics*.rpm dist/

deb: distdir
	dpkg-buildpackage -b -us -uc
	@mv ../client-metrics*.deb dist/
	@rm -f ../client-metrics*.buildinfo ../client-metrics*.changes

clean:
	@rm -rf dist/
