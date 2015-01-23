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


# EOF - Makefile
