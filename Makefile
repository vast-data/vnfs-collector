VERSION=$(shell git describe --tags --long --abbrev=12)
SEMANTIC_VERSION=$(shell git describe --tags --abbrev=0)
GIT_VERSION=$(shell echo ${VERSION} | cut -d'-' -f 2,3 | sed 's/-/./g')
PY_VERSION=$(shell echo ${VERSION} | cut -d'-' -f 1,2)
COMMIT_COUNT=$(shell echo ${VERSION} | cut -d'-' -f 2)


OFFLINE ?= 0
WHL_ARCHS ?= x86_64 aarch64
WHL_PYVERS ?= 3.9 3.10 3.11 3.12
WHL_PLATFORMS_x86_64  ?= manylinux_2_28_x86_64 manylinux_2_17_x86_64
WHL_PLATFORMS_aarch64 ?= manylinux_2_28_aarch64 manylinux_2_17_aarch64
WHL_EXTRAS ?= pip setuptools
DEB_ARCH_x86_64  := amd64
DEB_ARCH_aarch64 := arm64

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

ifeq ($(OFFLINE),0)

rpm: distdir pylib
	@echo "Building single noarch RPM (OFFLINE=0, no wheelhouse bundled)"
	rpmbuild -bb vnfs-collector.spec \
		--target noarch \
		--define "_sourcedir $(CURDIR)" \
		--define "_version $(SEMANTIC_VERSION)" \
		--define "_release $(GIT_VERSION)" \
		--define "_post post$(COMMIT_COUNT)"
	@mv ~/rpmbuild/RPMS/noarch/vnfs-collector*.rpm dist/

deb: distdir pylib
	@echo "Building single noarch DEB (OFFLINE=0, no wheelhouse bundled)"
	@sed -e "s/@ARCH@/all/g" debian/control.in > debian/control
	@cp debian/changelog.in debian/changelog
	@sed -i "1s/_VERSION_/$(SEMANTIC_VERSION)-$(GIT_VERSION)/" debian/changelog
	dpkg-buildpackage -b -us -uc
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/

else

rpm: distdir pylib
	@set -e; for arch in $(WHL_ARCHS); do \
		echo "=== Building RPM: arch=$$arch ==="; \
		$(MAKE) --no-print-directory _rpm-one ARCH=$$arch; \
	done
	@echo "Built RPM artifacts:"; ls -1 dist/*.rpm 2>/dev/null || true

deb: distdir pylib
	@set -e; for arch in $(WHL_ARCHS); do \
		echo "=== Building DEB: arch=$$arch ==="; \
		$(MAKE) --no-print-directory _deb-one ARCH=$$arch; \
	done
	@echo "Built DEB artifacts:"; ls -1 dist/*.deb 2>/dev/null || true

_wheelhouse-one:
	@echo "--- wheelhouse: arch=$(ARCH) pythons=$(WHL_PYVERS) ---"
	@rm -rf dist/wheelhouse
	@mkdir -p dist/wheelhouse
	@cp dist/vnfs_collector-*.whl dist/wheelhouse/
	@for py in $(WHL_PYVERS); do \
		abi="cp$$(echo $$py | tr -d .)"; \
		echo "  downloading wheels for python=$$py arch=$(ARCH)"; \
		python3 -m pip download \
			--dest dist/wheelhouse \
			--only-binary=:all: \
			--python-version $$py \
			--abi $$abi \
			$(foreach p,$(WHL_PLATFORMS_$(ARCH)),--platform $(p)) \
			dist/vnfs_collector-*.whl $(WHL_EXTRAS); \
	done
	@echo "wheelhouse: $$(ls dist/wheelhouse | wc -l) wheels, $$(du -sh dist/wheelhouse | cut -f1) total"

_rpm-one: _wheelhouse-one
	@rpmbuild -bb vnfs-collector.spec \
		--target $(ARCH) \
		--define "_sourcedir $(CURDIR)" \
		--define "_version $(SEMANTIC_VERSION)" \
		--define "_release $(GIT_VERSION)" \
		--define "_post post$(COMMIT_COUNT)"
	@mv ~/rpmbuild/RPMS/$(ARCH)/vnfs-collector-$(SEMANTIC_VERSION)-$(GIT_VERSION).$(ARCH).rpm dist/

_deb-one: _wheelhouse-one
	@sed -e "s/@ARCH@/any/g" debian/control.in > debian/control
	@cp debian/changelog.in debian/changelog
	@sed -i "1s/_VERSION_/$(SEMANTIC_VERSION)-$(GIT_VERSION)/" debian/changelog
	@DEB_HOST_ARCH=$(DEB_ARCH_$(ARCH)) \
		dpkg-buildpackage -b -us -uc -a $(DEB_ARCH_$(ARCH)) --no-check-builddeps
	@mv ../vnfs-collector*.deb dist/
	@mv ../vnfs-collector*.buildinfo ../vnfs-collector*.changes dist/ 2>/dev/null || true

wheelhouse: distdir pylib
	@echo "Building offline wheelhouse in dist/wheelhouse"
	@echo "  python versions: $(WHL_PYVERS)"
	@echo "  architectures  : $(WHL_ARCHS)"
	@rm -rf dist/wheelhouse
	@mkdir -p dist/wheelhouse
	@cp dist/vnfs_collector-*.whl dist/wheelhouse/
	@for pyver in $(WHL_PYVERS); do \
		abi="cp$$(echo $$pyver | tr -d .)"; \
		for arch in $(WHL_ARCHS); do \
			case $$arch in \
			  x86_64)  plats="$(WHL_PLATFORMS_x86_64)";; \
			  aarch64) plats="$(WHL_PLATFORMS_aarch64)";; \
			  *)       plats="$$arch";; \
			esac; \
			plat_args=""; \
			for p in $$plats; do plat_args="$$plat_args --platform $$p"; done; \
			echo "==> Downloading wheels for python=$$pyver abi=$$abi arch=$$arch"; \
			python3 -m pip download \
				--dest dist/wheelhouse \
				--only-binary=:all: \
				--python-version $$pyver \
				--abi $$abi \
				$$plat_args \
				dist/vnfs_collector-*.whl $(WHL_EXTRAS) \
				|| echo "WARN: pip download failed for py=$$pyver abi=$$abi arch=$$arch (some wheels may be unavailable; offline install on that target will not be possible)" >&2; \
		done; \
	done
	@echo "Wheelhouse: $$(ls dist/wheelhouse | wc -l) wheels, $$(du -sh dist/wheelhouse | cut -f1) total"

endif

clean:
	@rm -rf dist/ version.txt debian/control

up: deb
	@export VERSION=${SEMANTIC_VERSION}-${GIT_VERSION} && docker compose up

docker_build: deb
	@docker build -f docker/debian.Dockerfile -t vnfs-collector --build-arg="VERSION=${SEMANTIC_VERSION}-${GIT_VERSION}" .
