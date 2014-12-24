Kyoto Tycoon
============

Kyoto Tycoon is a lightweight network server on top of the Kyoto Cabinet key-value database, built for high-performance and concurrency.

It has its own fully-featured protocol based on HTTP and a simpler binary protocol for increased performance. It also supports the [memcached](http://www.memcached.org/) protocol and can be easily configured as a drop-in replacement for _memcached_ in larger-than-RAM scenarios.

  * http://fallabs.com/kyototycoon/
  * http://fallabs.com/kyotocabinet/

What's this fork?
=================

The development of Kyoto Tycoon and Kyoto Cabinet by the original authors seem to have halted around 2012. Both are very reliable and this may explain the lack of activity, but the software ecosystem has evolved and there are some issues preventing compilation with recent compilers and operating system releases.

This repository isn't meant as a divergent fork for any of these packages, but as a place to keep readily usable versions for modern machines.

What's included?
================

Here you can find the latest upstream releases with additional modifications, intended to be used together. The changes include patches sourced from Linux distribution packages and some custom patches. Check the commit history for more information.

Installing
==========

To build and install everything in one go, just run:

    ./install.sh /custom/install/root
    
Specifying the installation root directory is optional. By default it installs into `/usr/local`.
