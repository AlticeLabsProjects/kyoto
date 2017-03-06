#
# Makefile (for GNU Make)
#


PREFIX ?= /usr/local
DESTDIR =
OS := $(shell uname -s)
NPROCS = 1
CONFIG_FLAGS =


# Parallelize the build on Linux...
ifeq ($(OS),Linux)
	NPROCS := $(shell grep -c ^processor /proc/cpuinfo)
endif
ifeq ($(OS),Darwin)
	NPROCS := $(shell sysctl "hw.ncpu" | grep -o "[0-9]\+\$$")
endif
ifeq ($(OS),FreeBSD)
	NPROCS := $(shell sysctl "hw.ncpu" | grep -o "[0-9]\+\$$")
endif

# Too much parallelism actually hurts the build...
ifeq ($(shell test $(NPROCS) -gt 4; echo $$?),0)
	NPROCS := 4
endif

# Avoid resource exhaustion on the Raspberry Pi...
ifeq ($(shell test $(NPROCS) -gt 2 && egrep -qsi "raspberry\s*pi" /boot/LICENCE.broadcom; echo $$?),0)
	NPROCS := 2
endif

# For dependencies below...
ifeq ($(OS),Darwin)
	SO_EXTENSION = dylib
else
	SO_EXTENSION = so
endif

all: cabinet tycoon

# Bring both packages back to their pristine state...
clean:
ifneq ("","$(wildcard kyotocabinet/Makefile)")
	$(MAKE) -C kyotocabinet distclean
endif

ifneq ("","$(wildcard kyototycoon/Makefile)")
	$(MAKE) -C kyototycoon distclean
endif
	rm -rf build

check: all
	$(MAKE) -C kyototycoon check

kyotocabinet/Makefile:
	test -x kyotocabinet/configure && cd kyotocabinet && ./configure --prefix="$(PREFIX)" --enable-lzo $(CONFIG_FLAGS)

cabinet: kyotocabinet/Makefile
	$(MAKE) -j$(NPROCS) -C kyotocabinet
	grep -q '^CPPFLAGS.*-D_KCDEBUG' kyotocabinet/Makefile || $(MAKE) -j$(NPROCS) -C kyotocabinet strip

kyotocabinet/libkyotocabinet.a: cabinet
kyotocabinet/libkyotocabinet.$(SO_EXTENSION): cabinet

kyototycoon/Makefile: kyotocabinet/libkyotocabinet.a kyotocabinet/libkyotocabinet.$(SO_EXTENSION)
	$(eval CABINET_ROOT := $(shell awk '/^prefix *=/ {print $$3}' kyotocabinet/Makefile))
	test -x kyototycoon/configure && cd kyototycoon && \
	PKG_CONFIG_PATH="../kyotocabinet" CPPFLAGS="-I../kyotocabinet" LDFLAGS="-L../kyotocabinet" \
	./configure --prefix="$(PREFIX)" --with-kc="$(CABINET_ROOT)" --enable-lua $(CONFIG_FLAGS)

tycoon: kyototycoon/Makefile
	$(MAKE) -j$(NPROCS) -C kyototycoon
	grep -q '^CPPFLAGS.*-D_KCDEBUG' kyotocabinet/Makefile || $(MAKE) -j$(NPROCS) -C kyototycoon strip

install: all kyotocabinet/Makefile kyototycoon/Makefile
	$(MAKE) -j$(NPROCS) -C kyotocabinet install DESTDIR="$(DESTDIR)"
	$(MAKE) -j$(NPROCS) -C kyototycoon install DESTDIR="$(DESTDIR)"

# Update the system linker search path if allowed (and not installing into a packaging root).
# For packages (eg. ".deb", ".rpm"), this should be done in the packaging configuration/scripts...
ifeq ("yes","$(shell test -z '$(DESTDIR)' && test -w /etc/ld.so.conf.d && echo yes)")
	$(eval CABINET_LIBDIR := $(shell awk '/^prefix *=/ {print $$3}' kyotocabinet/Makefile)/lib)
	test -n "$(shell /sbin/ldconfig -vN 2>&1 | grep -o '^$(CABINET_LIBDIR):')" || echo "$(CABINET_LIBDIR)" > /etc/ld.so.conf.d/kyoto-cabinet.conf
	$(eval TYCOON_LIBDIR := $(shell awk '/^prefix *=/ {print $$3}' kyototycoon/Makefile)/lib)
	test -n "$(shell /sbin/ldconfig -vN 2>&1 | grep -o '^$(TYCOON_LIBDIR):')" || echo "$(TYCOON_LIBDIR)" > /etc/ld.so.conf.d/kyoto-tycoon.conf
	/sbin/ldconfig
endif

deb:
	rm -rf build
	test -x /usr/bin/dpkg-deb && test -x /usr/bin/fakeroot

	# Check for build dependencies...
	dpkg-checkbuilddeps packaging/debian/control

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g')-$(shell date +%Y%m%d)~$(shell lsb_release -c | awk '{print $$2}' | tr '[A-Z]' '[a-z]'))
	$(eval PACKAGE_ARCH := $(shell dpkg-architecture -qDEB_BUILD_ARCH))
	$(eval PACKAGE_NAME := kyoto-tycoon_$(PACKAGE_VERSION)_$(PACKAGE_ARCH))

	$(MAKE) install PREFIX=/usr DESTDIR="$(PWD)/build/$(PACKAGE_NAME)"

	mkdir -p "build/$(PACKAGE_NAME)/etc/init.d"
	cp packaging/debian/kyoto-init.sh "build/$(PACKAGE_NAME)/etc/init.d/kyoto"
	chmod 755 "build/$(PACKAGE_NAME)/etc/init.d/kyoto"

	mkdir -p "build/$(PACKAGE_NAME)/etc/default"
	cp packaging/scripts/kyoto.conf "build/$(PACKAGE_NAME)/etc/default/kyoto"

	mkdir -p "build/$(PACKAGE_NAME)/etc/logrotate.d"
	cp packaging/scripts/logrotate.conf "build/$(PACKAGE_NAME)/etc/logrotate.d/kyoto"

	mkdir -p "build/$(PACKAGE_NAME)/var/lib/kyoto"
	mkdir -p "build/$(PACKAGE_NAME)/var/log/kyoto"

	mkdir -p "build/$(PACKAGE_NAME)/DEBIAN"
	cd packaging/debian && cp compat conffiles control postinst postrm prerm "../../build/$(PACKAGE_NAME)/DEBIAN"
	printf "Version: $(PACKAGE_VERSION)\nArchitecture: $(PACKAGE_ARCH)\n" >> "build/$(PACKAGE_NAME)/DEBIAN/control"

	cd packaging && find "../build/$(PACKAGE_NAME)/usr" -perm /ugo+x -type f ! -name '*.sh' | xargs dpkg-shlibdeps -xkyoto-tycoon --ignore-missing-info -T../build/dependencies
	sed -i 's/^shlibs:Depends=//' build/dependencies
	sed -i "s/\(Depends:.*\)$$/\1, $$(cat build/dependencies)/" "build/$(PACKAGE_NAME)/DEBIAN/control"

	chmod -R u+w,go-w "build/$(PACKAGE_NAME)"
	fakeroot dpkg-deb --build "build/$(PACKAGE_NAME)"

rpm:
	test -d "$(HOME)/rpmbuild" || test -x /usr/bin/rpmdev-setuptree && rpmdev-setuptree
	test -d "$(HOME)/rpmbuild" && test -x /usr/bin/rpmbuild

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g'))
	$(eval PACKAGE_DATE := $(shell date +%Y%m%d))

	rm -f "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"
	cp packaging/redhat/kyoto-tycoon.spec "$(HOME)/rpmbuild/SPECS/"
	sed -i 's/__KT_VERSION_PLACEHOLDER__/$(PACKAGE_VERSION)/' "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"

	rm -f "$(HOME)/rpmbuild/SOURCES/kyoto-$(PACKAGE_DATE).tar.gz"
	tar zcf "$(HOME)/rpmbuild/SOURCES/kyoto-$(PACKAGE_DATE).tar.gz" .
	rpmbuild -ba "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"
	rpmbuild --clean "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"

rpm-selinux:
	test -d "$(HOME)/rpmbuild" || test -x /usr/bin/rpmdev-setuptree && rpmdev-setuptree
	test -d "$(HOME)/rpmbuild" && test -x /usr/bin/rpmbuild

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g'))
	$(eval PACKAGE_DATE := $(shell date +%Y%m%d))

	rm -f "$(HOME)/rpmbuild/SPECS/kyoto-tycoon-selinux.spec"
	cp packaging/redhat/kyoto-tycoon-selinux.spec "$(HOME)/rpmbuild/SPECS/"
	sed -i 's/__KT_VERSION_PLACEHOLDER__/$(PACKAGE_VERSION)/' "$(HOME)/rpmbuild/SPECS/kyoto-tycoon-selinux.spec"

	cp -p selinux/kyoto.{te,fc,if} "$(HOME)/rpmbuild/SOURCES/"
	rpmbuild -ba "$(HOME)/rpmbuild/SPECS/kyoto-tycoon-selinux.spec"
	rpmbuild --clean "$(HOME)/rpmbuild/SPECS/kyoto-tycoon-selinux.spec"

pac:
	rm -rf "$(HOME)/archbuild"
	mkdir "$(HOME)/archbuild"
	test -d "$(HOME)/archbuild" && test -x /usr/bin/makepkg
	cp "packaging/arch/PKGBUILD" "$(HOME)/archbuild/PKGBUILD"
	cp "packaging/arch/kyoto.service" "$(HOME)/archbuild/"
	cp "packaging/arch/kyoto.install" "$(HOME)/archbuild/"
	cp "packaging/arch/kyoto.conf" "$(HOME)/archbuild/"

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g'))
	$(eval PACKAGE_DATE := $(shell date +%Y%m%d))

	rm -f "$(HOME)/archbuild/kyoto-$(PACKAGE_VERSION).tar.gz"
	tar zcf "$(HOME)/archbuild/kyoto-$(PACKAGE_VERSION).tar.gz" .

	sed -i 's/__KT_VERSION_PLACEHOLDER__/$(PACKAGE_VERSION)/' "$(HOME)/archbuild/PKGBUILD"

	cd $(HOME)/archbuild && makepkg --nocheck --skipinteg --skipchecksums --skippgpcheck -s


.PHONY: all clean check cabinet tycoon install deb rpm rpm-selinux pac


# EOF - Makefile
