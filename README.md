Kyoto Tycoon
============

Kyoto Tycoon is a lightweight network server on top of the Kyoto Cabinet key-value database, built for high-performance and concurrency. Some of its features include:

  * _master-slave_ and _master-master_ replication
  * in-memory and persistent databases (with optional binary logging)
  * hash and tree-based database formats (with optional compression)
  * server-side scripting in [Lua](http://www.lua.org/) ([API](http://sapo.github.io/kyoto/kyototycoon/doc/luadoc/))

It has its own fully-featured [protocol](http://sapo.github.io/kyoto/kyototycoon/doc/spex.html#protocol) based on HTTP and a (limited) binary protocol for even better performance. There are several client libraries implementing them for multiple languages (we're maintaining one for Python [here](https://github.com/sapo/python-kyototycoon)).

It can also be configured with simultaneous support for the [memcached](http://www.memcached.org/) protocol, with some [limitations](http://sapo.github.io/kyoto/kyototycoon/doc/spex.html#tips_pluggableserver) on available data update commands. This is useful if you wish to replace _memcached_ in larger-than-memory/persistency scenarios.

![Example Architecture](https://raw.githubusercontent.com/sapo/kyoto/master/example.png)

What's this fork?
-----------------

The development of [Kyoto Tycoon](http://sapo.github.io/kyoto/kyototycoon/doc/) and [Kyoto Cabinet](http://sapo.github.io/kyoto/kyotocabinet/doc/) by their original authors at [FAL Labs](http://fallabs.com/) seems to have halted around 2012. The software works as advertised and is very reliable (see: [Caveats](#Caveats)), which may explain the lack of activity, but the unmodified upstream sources fail to build in recent operating system releases (with recent compilers).

We at [SAPO](http://www.sapo.pt/) intend this repository to be a place to keep readily usable (but conservative) versions for modern machines. Nevertheless, pull requests containing bug fixes or new features are welcome.

What's included?
----------------

Here you can find improved versions of the latest available upstream releases, intended to be used together and tested in real-world production environments. The changes include bug fixes, minor new features and packaging for a few Linux distributions.

Check our _stable_ [release history](https://github.com/sapo/kyoto/releases) to see what's new.

Supported Platforms
-------------------

Our primary target platform for these packages is Linux (64-bit). Mostly Debian, but we've also done some testing on CentOS and some non-Linux platforms such as FreeBSD and MacOS X.

The upstream sources claim to support additional platforms, but we haven't got around to test them for ourselves (yet). If you do, let us know.

Installing
----------

Download our latest _stable_ [source release](https://github.com/sapo/kyoto/releases/latest) or clone the [repository](https://github.com/sapo/kyoto) from GitHub. Then, to build and install the Kyoto Cabinet library and the Kyoto Tycoon server in one go, run:

    $ make PREFIX=/usr/local
    $ sudo make install

**Notes:**

  * Make sure you have [zlib](http://www.zlib.net/) and [Lua 5.1](http://www.lua.org/versions.html#5.1) already installed (later versions of Lua are not supported);
  * The installation root directory (`PREFIX`) is optional. By default it already installs into `/usr/local`;
  * If you're building on FreeBSD, use `gmake` from the ports collection instead of standard `make`.

Packaging
---------

Instead of installing directly from source, you can optionally build packages for one of the Linux distributions listed below. If you distribution of choice doesn't happen to be supported, patches are always welcome.

On Debian, build a binary `.deb` package (into the `./build` directory) by running:

    $ make deb

On Red Hat Linux (or a derivative such as CentOS), build both source and binary `.rpm` packages (into the `$HOME/rpmbuild` directory) by running:

    $ make rpm

On Arch Linux, build a binary `.pkg.tar.xz` package (into the `$HOME/archbuild` directory) by running:

    $ make pac

Besides being cleaner and more maintainable than installing directly from source, these packages also register init scripts to run the server with minimal privileges and install configuration files pre-filled with some examples.

Running
-------

If there's a place in need of improvement it's the documentation for the available server options in Kyoto Tycoon. Make sure to check the [command-line reference](http://sapo.github.io/kyoto/kyototycoon/doc/command.html#ktserver) to understand what each option means and how it affects performance vs. data protection. But you may want to try this as a quick start for realistic use:

    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid \
                              -log /data/kyoto/ktserver.log -oat -uasi 10 -asi 10 -ash \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 \
                              '/data/kyoto/db/db.kct#opts=c#pccap=256m#dfunit=8'

This will start a standalone server using a persistent B-tree database with compression (`opts=c`) and binary logging enabled. The `pccap=256m` option increases the default page cache memory to 256MB.
Some of the durability options have a significant performance impact and you may leave them out if you're storing volatile data (the main use case for Kyoto Tycoon anyway). In this example, the server is wrapping each write in a transaction (`-oat`) to ensure consistency in case of a process crash, but also periodically synchronizing the database (`-asi`) and binary logs (`-uasi`) to disk to ensure consistency in case of a system crash (`-ash` enables the use of `fsync()` for this).

If you have an idea of how many objects you'll be storing, you can use a persistent hash database instead:

    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid \
                              -log /data/kyoto/ktserver.log -oat -uasi 10 -asi 10 -ash \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 \
                              '/data/kyoto/db/db.kch#opts=l#bnum=1000000#msiz=256m#dfunit=8'

In this case `bnum=1000000` tunes the hash for 1 million buckets (should be about twice the number of expected stored keys) and `msiz=256m` sets the size of the memory mapped region (larger is better, provided you have enough RAM). As for the B-tree database above, the same performance considerations about durability options apply here.

Another example, this time for an in-memory cache hash database limited to 256MB of stored data (LRU-based):

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls '*#bnum=100000#capsiz=256m'

To enable simultaneous support for the _memcached_ protocol, use the `-plsv` and `-plex` parameters. The previous example would then become:

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls \
                              -plsv /usr/local/libexec/ktplugservmemc.so \
                              -plex 'port=11211#opts=f' \
                              '*#bnum=100000#capsiz=256m'

The `opts=f` parameter enables _flags_ support for the _memcached_ protocol. These are stored by Kyoto Tycoon as the last 4 bytes of the value, which means some care must be taken when mixing protocols (our [python library](https://github.com/sapo/python-kyototycoon#memcache-enabled-servers) can handle this for you, for example).

Caveats
-------

Based on our experience, there's a few things you should consider doing (or not doing) when using Kyoto Tycoon in production:

  * Don't use the `capsiz` option with persistent (on-disk) databases as the server will temporarily stop responding to free up space when the maximum capacity is reached. In this case, try to keep the database size under control using auto-expiring keys instead.
  * Persistent databases are sensitive to disk write performance (affecting not only record updates but also reads). Enabling transactions and/or synchronization makes this worse (increases disk writes), as does increasing the number of buckets in case of hash databases (larger structures to write). Having a disk controller with some kind of battery-backed write-cache enabled makes these issues mute. This is fairly standard in enterprise servers and storage systems, so you should be safe. You're also safe when using in-memory databases, of course.
  * Choose your persistent database tuning options carefully, but don't tune unless you need to. Some options can be modified by a simple restart of the service (eg. `pccap`, `msiz`) but others require creating the database from scratch (eg. `bnum`, `opts=c`).
  * Make sure you have enough disk space to store your persistent databases as they grow. The server uses `mmap()` for file access and handles out-of-space conditions by terminating immediately. The database should still be consistent if this happens, so don't fret too much about it.
  * The unique server ID (`-sid`) is used to break replication loops (a server ignores keys with its own SID). Keep this in mind when restoring failed _master-master_ instances. The documentation recommends always choosing a new SID for new instances, but this doesn't seem a good idea if the existing master still has keys from the failed master pending replication (if the new master doesn't have the same SID, it will accept and propagate them back).


[![Build Status](https://travis-ci.org/sapo/kyoto.svg?branch=master)](https://travis-ci.org/sapo/kyoto)
