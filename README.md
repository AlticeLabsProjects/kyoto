Kyoto Tycoon
============

Kyoto Tycoon is lightweight network server on top of the Kyoto Cabinet key-value database, built for high-performance and concurrency.

It has its own fully-featured protocol based on HTTP and a simpler binary protocol for increased performance. It also supports the [memcached](http://www.memcached.org/) procotol and can be easily configured as a drop-in replacement for _memcached_ in larger-than-RAM scenarios.

  * http://fallabs.com/kyototycoon/
  * http://fallabs.com/kyotocabinet/

What's this fork?
=================

The development of Kyoto Tycoon and Kyoto Cabinet by the original authors seem to have halted around 2012. Both are very reliable and this may explain the lack of activity, but the software ecosystem has evolved and there are some issues preventing compilation with recent compilers and operating system releases.

This repository contains the latest upstream releases with additional changes, and are intended to be used together. These changes include patches sourced from Linux distribution packages and some custom patches. Check the commit history for more information.

  * Kyoto Tycoon 0.9.56
  * Kyoto Cabinet 1.2.76

This repository isn't mean as a divergent fork for any of these packages, but as a place to keep readily usable versions for modern machines.
