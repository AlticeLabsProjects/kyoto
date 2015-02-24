Kyoto Tycoon
============

Kyoto Tycoon is a lightweight network server on top of the Kyoto Cabinet key-value database, built for high-performance and concurrency. Some of its features include:

  * _master-slave_ and _master-master_ replication
  * in-memory and persistent databases (with optional binary logging)
  * hash and tree-based database formats
  * server-side scripting in [Lua](http://www.lua.org/)

It has its own fully-featured [protocol](http://fallabs.com/kyototycoon/spex.html#protocol) based on HTTP and a (limited) binary protocol for even better performance. There are several client libraries implementing them for multiple languages (we're maintaining one for Python [here](https://github.com/sapo/python-kyototycoon)).

It can also be configured with simultaneous support for the [memcached](http://www.memcached.org/) protocol, with some [limitations](http://fallabs.com/kyototycoon/spex.html#tips_pluggableserver). This is useful if you wish to replace _memcached_ in larger-than-memory/persistency scenarios.

What's this fork?
-----------------

The development of [Kyoto Tycoon](http://fallabs.com/kyototycoon/) and [Kyoto Cabinet](http://fallabs.com/kyotocabinet/) by their original authors at [FAL Labs](http://fallabs.com/) seems to have halted around 2012. The software works as advertised and is very reliable, which may explain the lack of activity, but the unmodified upstream sources fail to build in recent operating system releases (with recent compilers).

We at [SAPO](http://www.sapo.pt/) don't really mean this repository as an "hostile" fork for any of these packages, but as a place to keep readily usable versions for modern machines. Nevertheless, pull requests are welcome.

What's included?
----------------

Here you can find the latest upstream releases with additional modifications, intended to be used together. The changes include patches sourced from Linux distribution packages and some custom patches which have been tested in real-world production environments. Check the commit history for more information.

Supported Platforms
-------------------

Our primary target platform for these packages is Linux (64-bit). Mostly Debian, but we've also done some testing on CentOS and some non-Linux platforms such as FreeBSD and MacOS X.

The upstream sources claim to support additional platforms, but we haven't tested them (yet).

Installing
----------

Download the [latest source release](https://github.com/sapo/kyoto/releases/latest) or clone the [repository](https://github.com/sapo/kyoto) from GitHub. Then, to build and install everything in one go, run:

    $ make PREFIX=/custom/install/root
    $ sudo make install

**Notes:**

  * Make sure you have [Lua 5.1](http://www.lua.org/versions.html#5.1) already installed (later versions are not supported);
  * The installation root directory (`PREFIX`) is optional. By default it installs into `/usr/local`;
  * If you're building on FreeBSD, use `gmake` instead of `make`.

Packaging
---------

On Debian, create a `.deb` package (into the `./build` directory) by running:

    $ make deb

On RHEL/CentOS, create a `.rpm` package (into the `$HOME/rpmbuild/RPMS` directory) by running:

    $ make rpm

Besides being cleaner than installing directly from source, both of these packages provide an `/etc/init.d/kyoto` init script to run the server with minimal privileges and an `/etc/default/kyoto` configuration file with some examples.

On Arch, create a `.pkg.tar.xz` package (into the `$HOME/archbuild/` directory) by running:

    $ make pac

This package provides a `systemd service` to run the server with minimal privileges and an `/etc/conf.d/kyoto` configuration file with some examples.

Running
-------

If there's a place in need of improvement it's the documentation for the available server options in Kyoto Tycoon. Make sure to check the [command-line reference](http://fallabs.com/kyototycoon/command.html#ktserver) to understand what each option means and how it affects performance vs. data protection. But you may want to try this as a quick start for realistic use:

    $ mkdir -p /data/kyoto/db
    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid \
                              -log /data/kyoto/ktserver.log -oat -uasi 10 -asi 10 -ash \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 \
                              '/data/kyoto/db/db.kct#opts=l#bnum=100000#msiz=256m#dfunit=8'

This will start a standalone server using a persistent B-tree database with binary logging enabled. The immediately tweakable bits are `bnum=100000`, which roughly optimizes for (but doesn't limit to) 100 thousand stored keys, and `msiz=256m`, which limits memory usage to around 256MB.

Another example, this time for an in-memory cache hash database limited to 256MB of stored data:

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls '*#bnum=100000#capsiz=256m'

Forced object removal when the maximum database size is reached in databases using the `capsiz` option is LRU-based. Based on our experience, we don't recommend using this option with persistent (on-disk) databases as the server will temporarily stop responding to free up space when the maximum capacity is reached. In this case, try to keep the database size under control using auto-expiring keys instead.

To enable simultaneous support for the _memcached_ protocol, use the `-plsv` and `-plex` parameters. The previous example would then become:

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls \
                              -plsv /usr/local/libexec/ktplugservmemc.so \
                              -plex 'port=11211#opts=f' \
                              '*#bnum=100000#capsiz=256m'

The `opts=f` parameter enables _flags_ support for the _memcached_ protocol. These are stored by Kyoto Tycoon as the last 4 bytes of the value, which means some care must be taken when mixing protocols (our [python library](https://github.com/sapo/python-kyototycoon#memcache-enabled-servers) can handle this for you, for example).

[![Build Status](https://travis-ci.org/sapo/kyoto.svg?branch=master)](https://travis-ci.org/sapo/kyoto)
