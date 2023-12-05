Kyoto Tycoon
============

[Kyoto Tycoon](http://alticelabs.github.io/kyoto/kyototycoon/doc/) is a lightweight network server on top of the [Kyoto Cabinet](http://alticelabs.github.io/kyoto/kyotocabinet/doc/) key-value database, built for high-performance and concurrency. Some of its features include:

  * _master-slave_ and _master-master_ replication
  * in-memory and persistent databases (with optional binary logging)
  * hash and tree-based database formats (with optional compression)
  * server-side scripting in [Lua](http://www.lua.org/) ([API](http://alticelabs.github.io/kyoto/kyototycoon/doc/luadoc/))

It has its own fully-featured [protocol](http://alticelabs.github.io/kyoto/kyototycoon/doc/spex.html#protocol) based on HTTP and a (limited) binary protocol for even better performance. There are several client libraries implementing them for multiple languages (we're maintaining one for Python [here](https://github.com/alticelabs/python-kyototycoon-ng)).

It can also be configured with simultaneous support for the [memcached](http://www.memcached.org/) protocol, with some [limitations](http://alticelabs.github.io/kyoto/kyototycoon/doc/spex.html#tips_pluggableserver) on available data update commands. This is useful if you wish to replace _memcached_ in larger-than-memory/persistency scenarios.

![Example Architecture](https://raw.githubusercontent.com/alticelabs/kyoto/master/example.png)

What's this fork?
-----------------

The development of [Kyoto Tycoon](http://alticelabs.github.io/kyoto/kyototycoon/doc/) and [Kyoto Cabinet](http://alticelabs.github.io/kyoto/kyotocabinet/doc/) by their original authors at [FAL Labs](http://fallabs.com/) seems to have halted around 2012. The software works as advertised and is very reliable, which may explain the lack of activity, but the unmodified upstream sources fail to build in recent operating system releases (with recent compilers).

We at [Altice Labs](http://www.alticelabs.com/) (previously at [SAPO](http://www.sapo.pt/)) intend this repository to be a place to keep readily usable (but conservative) versions for modern machines. Nevertheless, pull requests containing bug fixes or new features are welcome.

What's included?
----------------

Here are enhanced versions of the most recent upstream releases, designed for seamless integration and testing in real-world production environments. The updates encompass bug fixes, minor feature additions, and tailored packaging for specific Linux distributions.

Check our _stable_ [release history](https://github.com/alticelabs/kyoto/releases) to see what's new.

Supported Platforms
-------------------

We primarily focus on delivering packages optimized for the Linux (64-bit) environment, with a primary emphasis on Debian and CentOS distributions. Additionally, we conduct testing on diverse platforms, including non-Linux systems like FreeBSD and MacOS X.

While the upstream sources assert support for various additional platforms, we have not yet had the opportunity to conduct testing on them ourselves. If you undertake such testing, kindly inform us of your findings.

Installing
----------

Download our latest _stable_ [source release](https://github.com/alticelabs/kyoto/releases/latest) or clone the [repository](https://github.com/alticelabs/kyoto) from GitHub. Then, to build and install the Kyoto Cabinet library and the Kyoto Tycoon server in one go, run:

    $ make PREFIX=/usr/local
    $ sudo make install

**Notes:**

  * Make sure you have [zlib](http://www.zlib.net/), [lzo](http://www.oberhumer.com/opensource/lzo/) and [Lua 5.1.x](http://www.lua.org/versions.html#5.1) already installed;
  * The installation root directory (`PREFIX`) is optional. By default it already installs into `/usr/local`;
  * If you're building on FreeBSD, use `gmake` from the ports collection instead of standard `make`.

Packaging
---------

Rather than installing directly from the source, an alternative option is to choose package building for any of the listed Linux distributions below. If your preferred distribution is not currently supported, we welcome contributions in the form of patches.

On Debian, build a binary `.deb` package (into the `./build` directory) by running:

    $ make deb

On Red Hat Linux (or a derivative such as CentOS), build both source and binary `.rpm` packages (into the `$HOME/rpmbuild` directory) by running:

    $ make rpm
    $ make rpm-selinux  # (optional) builds an extra package containing a hardened SELinux policy

In addition to providing a cleaner and more maintainable alternative to direct source installation, these packages come equipped with registered init scripts. These scripts are designed to run the server with minimal privileges, and the packages also include pre-filled configuration files featuring practical examples.

Running
-------

If there's a place in need of improvement it's the documentation for the available server options in Kyoto Tycoon. Make sure to check the [command-line reference](http://alticelabs.github.io/kyoto/kyototycoon/doc/command.html#ktserver) to understand what each option means and how it affects performance vs. data protection. But you may want to try this as a quick start for realistic use:

    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid \
                              -log /data/kyoto/ktserver.log -oat -uasi 10 -asi 10 -ash \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 \
                              '/data/kyoto/db/db.kct#opts=c#pccap=256m#dfunit=8'

This will start a standalone server using a persistent B-tree database with compression (`opts=c`) and binary logging enabled. The `pccap=256m` option increases the default page cache memory to 256MB.

The data durability options (`-oat`, `-uasi`, `-asi`, `-ash`) have a significant performance impact and you may want to consider leaving them out if you're storing volatile data (which is the main use case for Kyoto Tycoon anyway).

If you have an estimate of the number of objects you intend to store, consider opting for a persistent hash database instead:

    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid \
                              -log /data/kyoto/ktserver.log -oat -uasi 10 -asi 10 -ash \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 \
                              '/data/kyoto/db/db.kch#opts=l#bnum=1000000#msiz=256m#dfunit=8'

In this case `bnum=1000000` configures 1 million hash buckets (should be set to about twice the number of expected keys) and `msiz=256m` sets the size of the memory mapped region (larger is better, provided you have enough RAM). Like the B-tree database above, performance considerations about data durability options also apply here.

As another example, you might opt for an in-memory cache hash database specifically constrained to store up to 256MB of data, utilizing an LRU (Least Recently Used) strategy.

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls '*#bnum=100000#capsiz=256m'

To enable simultaneous support for the _memcached_ protocol, use the `-plsv` and `-plex` parameters. The previous example would then become:

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls \
                              -plsv /usr/local/libexec/ktplugservmemc.so \
                              -plex 'port=11211#opts=f' \
                              '*#bnum=100000#capsiz=256m'

The `opts=f` parameter enables _flags_ support for the _memcached_ protocol. These are stored by Kyoto Tycoon as the last 4 bytes of the value, which means some care must be taken when mixing protocols (our [python library](https://github.com/alticelabs/python-kyototycoon-ng#memcache-enabled-servers) can handle this for you, for example).

Caveats
-------

Drawing from our practical experience, it is advisable to take certain factors into consideration when deploying Kyoto Tycoon in a production environment:

  * Don't use the `capsiz` option with on-disk databases as the server will temporarily stop responding to free up space when the maximum capacity is reached. In this case, try to keep the database size under control using auto-expiring keys instead.

  * On-disk databases are sensitive to disk write performance (impacting record updates as well as reads). Enabling transactions and/or synchronization makes this worse, as does increasing the number of buckets for hash databases (larger structures to write). Having a disk controller with some kind of battery-backed write-cache makes these issues mute.

  * Choose your on-disk database tuning options carefully and don't tune unless you need to. Some options can be modified by a simple restart of the server (eg. `pccap`, `msiz`) but others require creating the database from scratch (eg. `bnum`, `opts=c`).

  * Make sure you have enough disk space to store your on-disk databases as they grow. The server uses `mmap()` for file access and handles out-of-space conditions by terminating immediately. The database should still be consistent if this happens, so don't fret too much about it.

  * The unique server ID (`-sid`) is used to break replication loops (a server instance ignores keys with its own SID). Keep this in mind when restoring failed _master-master_ instances. The documentation recommends always choosing a new SID but this doesn't seem a good idea in this case. If the existing master still has keys from the failed master with the old SID pending replication, the new master with a new SID will propagate them back.


[![Build Status](https://travis-ci.org/alticelabs/kyoto.svg?branch=master)](https://travis-ci.org/alticelabs/kyoto)
