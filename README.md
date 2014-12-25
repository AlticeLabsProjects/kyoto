Kyoto Tycoon
============

Kyoto Tycoon is a lightweight network server on top of the Kyoto Cabinet key-value database, built for high-performance and concurrency.

Some of its features include _master-slave_ and _master-master_ replication, in-memory hash databases or persistent hash/tree databases with binary logging, and optional server-side scripting in Lua.

It has its own fully-featured protocol based on HTTP and a simpler binary protocol for even better performance. The HTTP protocol isn't hard to use directly, but you can also find client libraries for multiple programming languages in the wild[1].

It also supports the [memcached](http://www.memcached.org/) protocol an can be used as a drop-in replacement for _memcached_ in scenarios where you need replication or persistent/larger-than-memory storing capabilities[2].

  * http://fallabs.com/kyototycoon/
  * http://fallabs.com/kyotocabinet/

What's this fork?
-----------------

The development of Kyoto Tycoon and Kyoto Cabinet by the original authors seem to have halted around 2012. Both are very reliable and this may explain the lack of activity, but the software ecosystem has evolved and there are some issues preventing compilation with recent compilers and operating system releases.

This repository isn't meant as a divergent fork for any of these packages, but as a place to keep readily usable versions for modern machines.

What's included?
----------------

Here you can find the latest upstream releases with additional modifications, intended to be used together. The changes include patches sourced from Linux distribution packages and some custom patches and have been tested in real-world production environments. Check the commit history for more information.

Installing
----------

To build and install everything in one go, just run:

    $ ./install.sh /custom/install/root
    
Specifying the installation root directory is optional. By default it installs into `/usr/local`.

Running
-------

If there's a place in need of improvement it's the documentation for the available server options in Kyoto Tycoon. Try this as a quick start for a realistic use:

    $ mkdir -p /data/kyoto/db
    $ /usr/local/bin/ktserver -ls -th 16 -port 1978 -pid /data/kyoto/kyoto.pid -log /data/kyoto/ktserver.log \
                              -sid 1001 -ulog /data/kyoto/db -ulim 104857600 -oat -uasi 10 -asi 10 -ash \
                              /data/kyoto/db/db.kct#opts=l#bnum=100000#msiz=256m#dfunit=8

This will start a standalone server using a persistent B-tree database with binary logging enabled. Make sure to check the [command-line reference](http://fallabs.com/kyototycoon/command.html#ktserver) to understand what each option means and how it affects performance vs. data protection. The immediately tweakable bits are `bnum=100000`, which roughly optimizes for an expected 100 thousand stored keys, and `msiz=256m`, which limits memory usage to around 256MB.

Another example, this time for an in-memory hash database using the default settings:

    $ /usr/local/bin/ktserver -log /var/log/ktserver.log -ls '%'

References
----------

[1] If you happen to be using Python, we're maintaining a production-ready library for it here: https://github.com/sapo/python-kyototycoon

[2] http://www.guguncube.com/1935/kyoto-tycoon-installation-with-lua-and-memcached-protocol
