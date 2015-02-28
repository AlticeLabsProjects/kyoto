/*************************************************************************************************
 * System-dependent configurations
 *                                                               Copyright (C) 2009-2012 FAL Labs
 * This file is part of Kyoto Tycoon.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#ifndef _MYCONF_H                        // duplication check
#define _MYCONF_H



/*************************************************************************************************
 * system discrimination
 *************************************************************************************************/


#if defined(__linux__)

#define _SYS_LINUX_
#define _KT_OSNAME     "Linux"

#elif defined(__FreeBSD__)

#define _SYS_FREEBSD_
#define _KT_OSNAME     "FreeBSD"

#elif defined(__NetBSD__)

#define _SYS_NETBSD_
#define _KT_OSNAME     "NetBSD"

#elif defined(__OpenBSD__)

#define _SYS_OPENBSD_
#define _KT_OSNAME     "OpenBSD"

#elif defined(__sun__) || defined(__sun)

#define _SYS_SUNOS_
#define _KT_OSNAME     "SunOS"

#elif defined(__hpux)

#define _SYS_HPUX_
#define _KT_OSNAME     "HP-UX"

#elif defined(__osf)

#define _SYS_TRU64_
#define _KT_OSNAME     "Tru64"

#elif defined(_AIX)

#define _SYS_AIX_
#define _KT_OSNAME     "AIX"

#elif defined(__APPLE__) && defined(__MACH__)

#define _SYS_MACOSX_
#define _KT_OSNAME     "Mac OS X"

#elif defined(_MSC_VER)

#define _SYS_MSVC_
#define _KT_OSNAME     "Windows (VC++)"

#elif defined(_WIN32)

#define _SYS_MINGW_
#define _KT_OSNAME     "Windows (MinGW)"

#elif defined(__CYGWIN__)

#define _SYS_CYGWIN_
#define _KT_OSNAME     "Windows (Cygwin)"

#else

#define _SYS_GENERIC_
#define _KT_OSNAME     "Generic"

#endif

#define _KT_VERSION    "0.9.56b"
#define _KT_LIBVER     2
#define _KT_LIBREV     19

#if ! defined(_MYNOEVENT)
#if defined(_SYS_LINUX_)
#define _KT_EVENT_EPOLL
#elif defined(_SYS_FREEBSD_) || defined(_SYS_MACOSX_)
#define _KT_EVENT_KQUEUE
#endif
#endif

#if defined(_MYLUA)
#define _KT_LUA        1
#else
#define _KT_LUA        0
#endif



/*************************************************************************************************
 * general headers
 *************************************************************************************************/


extern "C" {
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
}

extern "C" {
#include <stdint.h>
}

#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)

#include <windows.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <direct.h>
#include <io.h>
#include <process.h>

#else

extern "C" {
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/select.h>
#include <fcntl.h>
#include <poll.h>
#include <dirent.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <dlfcn.h>
}

#endif


#endif                                   // duplication check


// END OF FILE
