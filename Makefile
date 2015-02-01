#
# Makefile (for GNU Make)
#


PREFIX ?= /usr/local
DESTDIR =
OS := $(shell uname -s)
NPROCS = 1

# Parallelize the build on Linux...
ifeq ($(OS),Linux)
	NPROCS := $(shell grep -c ^processor /proc/cpuinfo)
endif
ifeq ($(OS),Darwin)
	NPROCS := $(shell sysctl -a | grep "hw.ncpu " | cut -d" " -f3)
endif
ifeq ($(OS),FreeBSD)
	NPROCS := $(shell sysctl -a | grep "hw.ncpu " | cut -d" " -f3)
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

kyotocabinet/Makefile:
	test -x kyotocabinet/configure && cd kyotocabinet && ./configure --prefix="$(PREFIX)"

cabinet: kyotocabinet/Makefile
	$(MAKE) -j$(NPROCS) -C kyotocabinet

kyotocabinet/libkyotocabinet.a: cabinet
kyotocabinet/libkyotocabinet.$(SO_EXTENSION): cabinet

kyototycoon/Makefile: kyotocabinet/libkyotocabinet.a kyotocabinet/libkyotocabinet.$(SO_EXTENSION)
	$(eval CABINET_ROOT := $(shell awk '/^prefix *=/ {print $$3}' kyotocabinet/Makefile))
	test -x kyototycoon/configure && cd kyototycoon && \
	PKG_CONFIG_PATH="../kyotocabinet" CPPFLAGS="-I../kyotocabinet" LDFLAGS="-L../kyotocabinet" \
	./configure --prefix="$(PREFIX)" --with-kc="$(CABINET_ROOT)" --enable-lua

tycoon: kyototycoon/Makefile
	$(MAKE) -j$(NPROCS) -C kyototycoon

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

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g')-$(shell date +%Y%m%d)~$(shell lsb_release -c | awk '{print $$2}' | tr '[A-Z]' '[a-z]'))
	$(eval PACKAGE_ARCH := $(shell dpkg-architecture -qDEB_BUILD_ARCH))
	$(eval PACKAGE_NAME := kyoto-tycoon-$(PACKAGE_VERSION))

	$(MAKE) install PREFIX=/usr DESTDIR="$(PWD)/build/$(PACKAGE_NAME)"

	mkdir -p "build/$(PACKAGE_NAME)/etc/init.d"
	cp scripts/debian-init.sh "build/$(PACKAGE_NAME)/etc/init.d/kyoto"
	chmod 755 "build/$(PACKAGE_NAME)/etc/init.d/kyoto"

	mkdir -p "build/$(PACKAGE_NAME)/etc/default"
	cp scripts/kyoto.conf "build/$(PACKAGE_NAME)/etc/default/kyoto"
	mkdir -p "build/$(PACKAGE_NAME)/var/lib/kyoto"

	cp -dr debian "build/$(PACKAGE_NAME)/DEBIAN"
	printf "Version: $(PACKAGE_VERSION)\nArchitecture: $(PACKAGE_ARCH)\n" >> "build/$(PACKAGE_NAME)/DEBIAN/control"

	find "build/$(PACKAGE_NAME)/usr" -perm /ugo+x -type f ! -name '*.sh' | xargs dpkg-shlibdeps -xkyoto-tycoon --ignore-missing-info -Tbuild/dependencies
	sed -i 's/^shlibs:Depends=//' build/dependencies
	sed -i "s/\(Depends:.*\)$$/\1, $$(cat build/dependencies)/" "build/$(PACKAGE_NAME)/DEBIAN/control"

	chmod -R u+w,go-w "build/$(PACKAGE_NAME)"
	fakeroot dpkg-deb --build "build/$(PACKAGE_NAME)"

rpm:
	test -d "$(HOME)/rpmbuild" || test -x /usr/bin/rpmdev-setuptree && rpmdev-setuptree
	test -d "$(HOME)/rpmbuild" && test -x /usr/bin/rpmbuild

	$(eval PACKAGE_VERSION := $(shell grep _KT_VERSION kyototycoon/myconf.h | awk '{print $$3}' | sed 's/"//g'))
	$(eval PACKAGE_DATE := $(shell date +%Y%m%d))

	cp redhat/kyoto-tycoon.spec "$(HOME)/rpmbuild/SPECS/"
	sed -i 's/__KT_VERSION_PLACEHOLDER__/$(PACKAGE_VERSION)/' "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"
	tar zcf "$(HOME)/rpmbuild/SOURCES/kyoto-$(PACKAGE_DATE).tar.gz" .
	rpmbuild -bb "$(HOME)/rpmbuild/SPECS/kyoto-tycoon.spec"


# EOF - Makefile
