/*************************************************************************************************
 * Network functions
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


#include "ktsocket.h"
#include "myconf.h"

#if defined(_KT_EVENT_EPOLL)
extern "C" {
#include <sys/epoll.h>
}
#elif defined(_KT_EVENT_KQUEUE)
#include <sys/event.h>
#endif

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const int32_t NAMEBUFSIZ = 256;          ///< size of the name buffer
const int32_t IOBUFSIZ = 4096;           ///< size of the IO buffer
const double WAITTIME = 0.1;             ///< interval to check timeout
const int32_t RECVMAXSIZ = 1 << 30;      ///< maximum size to receive
}


/**
 * Hidden initializer.
 */
static int32_t _init_func() {
  struct ::sigaction sa;
  std::memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_IGN;
  sigemptyset(&sa.sa_mask);
  ::sigaction(SIGPIPE, &sa, NULL);
  ::sigset_t sigmask;
  sigemptyset(&sigmask);
  sigaddset(&sigmask, SIGPIPE);
  ::pthread_sigmask(SIG_BLOCK, &sigmask, NULL);
  return 0;
}
int32_t _init_var = _init_func();


/**
 * Socket internal.
 */
struct SocketCore {
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::string expr;                      ///< address expression
  double timeout;                        ///< timeout
  bool aborted;                          ///< flag for abortion
  uint32_t evflags;                      ///< event flags
  char* buf;                             ///< receiving buffer
  const char* rp;                        ///< reading pointer
  const char* ep;                        ///< end pointer
};


/**
 * ServerSocket internal.
 */
struct ServerSocketCore {
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::string expr;                      ///< address expression
  double timeout;                        ///< timeout
  bool aborted;                          ///< flag for abortion
  uint32_t evflags;                      ///< event flags
};


/**
 * Poller internal.
 */
struct PollerCore {
#if defined(_KT_EVENT_EPOLL)
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::set<Pollable*> events;            ///< monitored events
  std::set<Pollable*> hits;              ///< notified file descriptors
  kc::SpinLock elock;                    ///< lock for events
  bool aborted;                          ///< flag for abortion
#elif defined(_KT_EVENT_KQUEUE)
  const char* errmsg;                    ///< error message
  int32_t fd;                            ///< file descriptor
  std::set<Pollable*> events;            ///< monitored events
  std::set<Pollable*> hits;              ///< notified file descriptors
  kc::SpinLock elock;                    ///< lock for events
  bool aborted;                          ///< flag for abortion
#else
  const char* errmsg;                    ///< error message
  bool open;                             ///< flag for open
  std::set<Pollable*> events;            ///< monitored events
  std::set<Pollable*> hits;              ///< notified file descriptors
  kc::SpinLock elock;                    ///< lock for events
  ::pthread_t th;                        ///< main thread ID
  bool aborted;                          ///< flag for abortion
#endif
};


/**
 * Dummy signal handler.
 * @param signum the signal number.
 */
static void dummysighandler(int signum);


/**
 * Parse an address expression.
 * @param expr an address expression.
 * @param buf the pointer to the buffer into which the address is written.
 * @param portp the pointer to the variable into which the port is assigned.
 */
static void parseaddr(const char* expr, char* addr, int32_t* portp);


/**
 * Check whether an error code is retriable.
 * @param ecode the error code.
 * @return true if the error code is retriable, or false if not.
 */
static bool checkerrnoretriable(int32_t ecode);


/**
 * Set options of a sockeet.
 * @param fd the file descriptor of the socket.
 * @return true on success, or false on failure.
 */
static bool setsocketoptions(int32_t fd);


/**
 * Clear the error status of a socket.
 * @param fd the file descriptor of the socket.
 */
static void clearsocketerror(int32_t fd);


/**
 * Wait an I/O event of a socket.
 * @param fd the file descriptor of the socket.
 * @param mode the kind of events: 0 for reading, 1 for writing, 2 for exception.
 * @param timeout the timeout in seconds.
 * @return true on success, or false on failure.
 */
static bool waitsocket(int32_t fd, uint32_t mode, double timeout);


/**
 * Set the error message of a socket.
 * @param core the inner condition of the socket.
 * @param msg the error message.
 */
static void sockseterrmsg(SocketCore* core, const char* msg);


/**
 * Receive one byte from a socket.
 * @param core the inner condition of the socket.
 * @return the received byte or -1 on failure.
 */
static int32_t sockgetc(SocketCore* core);


/**
 * Set the error message of a server.
 * @param core the inner condition of the server.
 * @param msg the error message.
 */
static void servseterrmsg(ServerSocketCore* core, const char* msg);


/**
 * Set the error message of a poller.
 * @param core the inner condition of the poller.
 * @param msg the error message.
 */
static void pollseterrmsg(PollerCore* core, const char* msg);


/**
 * Default constructor.
 */
Socket::Socket() {
  _assert_(true);
  SocketCore* core = new SocketCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->timeout = kc::UINT32MAX;
  core->aborted = false;
  core->buf = NULL;
  core->rp = NULL;
  core->ep = NULL;
  core->evflags = 0;
  opq_ = core;
}


/**
 * Destructor.
 */
Socket::~Socket() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
}


/**
 * Get the last happened error information.
 * @return the last happened error information.
 */
const char* Socket::error() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
}


/**
 * Open a client socket.
 */
bool Socket::open(const std::string& expr) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd > 0) {
    sockseterrmsg(core, "already opened");
    return false;
  }
  char addr[NAMEBUFSIZ];
  int32_t port;
  parseaddr(expr.c_str(), addr, &port);
  if (kc::atoi(addr) < 1 || port < 1 || port > kc::INT16MAX) {
    sockseterrmsg(core, "invalid address expression");
    return false;
  }
  struct ::sockaddr_in sain;
  std::memset(&sain, 0, sizeof(sain));
  sain.sin_family = AF_INET;
  if (::inet_aton(addr, &sain.sin_addr) == 0) {
    sockseterrmsg(core, "inet_aton failed");
    return false;
  }
  uint16_t snum = port;
  sain.sin_port = htons(snum);
  int32_t fd = ::socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    sockseterrmsg(core, "socket failed");
    return false;
  }
  if (!setsocketoptions(fd)) {
    sockseterrmsg(core, "setsocketoptions failed");
    ::close(fd);
    return false;
  }
  int32_t flags = ::fcntl(fd, F_GETFL, NULL);
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    sockseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  double ct = kc::time();
  while (true) {
    if (::connect(fd, (struct sockaddr*)&sain, sizeof(sain)) == 0 || errno == EISCONN) break;
    if (!checkerrnoretriable(errno)) {
      sockseterrmsg(core, "connect failed");
      ::close(fd);
      return false;
    }
    if (kc::time() > ct + core->timeout) {
      sockseterrmsg(core, "operation timed out");
      ::close(fd);
      return false;
    }
    if (core->aborted) {
      sockseterrmsg(core, "operation was aborted");
      ::close(fd);
      return false;
    }
    if (!waitsocket(fd, 1, WAITTIME)) {
      sockseterrmsg(core, "waitsocket failed");
      ::close(fd);
      return false;
    }
  }
  if (::fcntl(fd, F_SETFL, flags) != 0) {
    sockseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  core->fd = fd;
  core->expr.clear();
  kc::strprintf(&core->expr, "%s:%d", addr, port);
  return true;
}


/**
 * Close the socket.
 */
bool Socket::close(bool grace) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  int32_t flags = ::fcntl(core->fd, F_GETFL, NULL);
  if (::fcntl(core->fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    sockseterrmsg(core, "fcntl failed");
    err = true;
  }
  if (grace) {
    double ct = kc::time();
    while (true) {
      int32_t rv = ::shutdown(core->fd, SHUT_RDWR);
      if (rv == 0 || !checkerrnoretriable(errno)) break;
      if (kc::time() > ct + core->timeout) {
        sockseterrmsg(core, "operation timed out");
        err = true;
        break;
      }
      if (core->aborted) break;
      if (!waitsocket(core->fd, 1, WAITTIME)) {
        sockseterrmsg(core, "waitsocket failed");
        break;
      }
    }
  } else {
    struct ::linger optli;
    optli.l_onoff = 1;
    optli.l_linger = 0;
    ::setsockopt(core->fd, SOL_SOCKET, SO_LINGER, (char*)&optli, sizeof(optli));
  }
  if (::close(core->fd) != 0) {
    sockseterrmsg(core, "close failed");
    err = true;
  }
  core->fd = -1;
  delete[] core->buf;
  core->buf = NULL;
  core->rp = NULL;
  core->ep = NULL;
  core->aborted = false;
  return !err;
}


/**
 * Send data.
 */
bool Socket::send(const void* buf, size_t size) {
  _assert_(buf && size <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  const char* rp = (const char*)buf;
  double ct = kc::time();
  while (size > 0) {
    int32_t wb = ::send(core->fd, rp, size, 0);
    switch (wb) {
      case -1: {
        if (!checkerrnoretriable(errno)) {
          sockseterrmsg(core, "send failed");
          return false;
        }
        if (kc::time() > ct + core->timeout) {
          sockseterrmsg(core, "operation timed out");
          return false;
        }
        if (core->aborted) {
          sockseterrmsg(core, "operation was aborted");
          return false;
        }
        if (!waitsocket(core->fd, 1, WAITTIME)) {
          sockseterrmsg(core, "waitsocket failed");
          return false;
        }
        break;
      }
      case 0: {
        break;
      }
      default: {
        rp += wb;
        size -= wb;
        break;
      }
    }
  }
  return true;
}


/**
 * Send data.
 */
bool Socket::send(const std::string& str) {
  _assert_(true);
  return send(str.data(), str.size());
}


/**
 * Send formatted data.
 */
bool Socket::printf(const char* format, ...) {
  _assert_(format);
  va_list ap;
  va_start(ap, format);
  bool rv = vprintf(format, ap);
  va_end(ap);
  return rv;
}


/**
 * Send formatted data.
 */
bool Socket::vprintf(const char* format, va_list ap) {
  _assert_(format);
  std::string str;
  kc::vstrprintf(&str, format, ap);
  return send(str);
}


/**
 * Receive data.
 */
bool Socket::receive(void* buf, size_t size) {
  _assert_(buf && size <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  if (core->rp + size <= core->ep) {
    std::memcpy(buf, core->rp, size);
    core->rp += size;
    return true;
  }
  bool err = false;
  char* wp = (char*)buf;
  while (size > 0) {
    int32_t c = sockgetc(core);
    if (c < 0) {
      err = true;
      break;
    }
    *(wp++) = c;
    size--;
  }
  return !err;
}


/**
 * Receive one byte.
 */
int32_t Socket::receive_byte() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  return sockgetc(core);
}


/**
 * Push one byte back.
 */
bool Socket::undo_receive_byte(int32_t c) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  if (core->rp <= core->buf) return false;
  core->rp--;
  *(unsigned char*)core->rp = c;
  return true;
}


/**
 * Receive one line of characters.
 */
bool Socket::receive_line(void* buf, size_t max) {
  _assert_(buf && max > 0 && max <= kc::MEMMAXSIZ);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  char* wp = (char*)buf;
  while (max > 1) {
    int32_t c = sockgetc(core);
    if (c == '\n') break;
    if (c < 0) {
      err = true;
      break;
    }
    if (c != '\r') {
      *(wp++) = c;
      max--;
    }
  }
  *wp = '\0';
  return !err;
}


/**
 * Get the size of left data in the receiving buffer.
 */
size_t Socket::left_size() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  return core->ep - core->rp;
}


/**
 * Abort the current operation.
 */
bool Socket::abort() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 1) {
    sockseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
}


/**
 * Set the timeout of each operation.
 */
bool Socket::set_timeout(double timeout) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd > 0) {
    sockseterrmsg(core, "already opened");
    return false;
  }
  core->timeout = timeout > 0 ? timeout : kc::UINT32MAX;
  if (timeout > kc::UINT32MAX) timeout = kc::UINT32MAX;
  return true;
}


/**
 * Get the expression of the socket.
 */
const std::string Socket::expression() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 0) {
    sockseterrmsg(core, "not opened");
    return "";
  }
  return core->expr;
}


/**
 * Get the descriptor integer.
 */
int32_t Socket::descriptor() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  if (core->fd < 0) {
    sockseterrmsg(core, "not opened");
    return -1;
  }
  return core->fd;
}


/**
 * Set event flags.
 */
void Socket::set_event_flags(uint32_t flags) {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  core->evflags = flags;
}


/**
 * Get the current event flags.
 */
uint32_t Socket::event_flags() {
  _assert_(true);
  SocketCore* core = (SocketCore*)opq_;
  return core->evflags;
}


/**
 * Get the primary name of the local host.
 */
std::string Socket::get_local_host_name() {
  _assert_(true);
  char name[NAMEBUFSIZ];
  if (::gethostname(name, sizeof(name) - 1) != 0) return "";
  return name;
}


/**
 * Get the address of a host.
 */
std::string Socket::get_host_address(const std::string& name) {
  _assert_(true);
  struct ::addrinfo hints, *result;
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = 0;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_canonname = NULL;
  hints.ai_addr = NULL;
  hints.ai_next = NULL;
  if (::getaddrinfo(name.c_str(), NULL, &hints, &result) != 0) return "";
  if (!result || result->ai_addr->sa_family != AF_INET) {
    ::freeaddrinfo(result);
    return "";
  }
  char addr[NAMEBUFSIZ];
  if (::getnameinfo(result->ai_addr, result->ai_addrlen,
                    addr, sizeof(addr) - 1, NULL, 0, NI_NUMERICHOST) != 0) {
    ::freeaddrinfo(result);
    return "";
  }
  ::freeaddrinfo(result);
  return addr;
}


/**
 * Default constructor.
 */
ServerSocket::ServerSocket() {
  _assert_(true);
  ServerSocketCore* core = new ServerSocketCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->timeout = kc::UINT32MAX;
  core->aborted = false;
  core->evflags = 0;
  opq_ = core;
}


/**
 * Destructor.
 */
ServerSocket::~ServerSocket() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
}


/**
 * Get the last happened error information.
 */
const char* ServerSocket::error() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
}


/**
 * Open a server socket.
 */
bool ServerSocket::open(const std::string& expr) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd > 0) {
    servseterrmsg(core, "already opened");
    return false;
  }
  char addr[NAMEBUFSIZ];
  int32_t port;
  parseaddr(expr.c_str(), addr, &port);
  if (*addr == '\0') {
    std::sprintf(addr, "0.0.0.0");
  } else if (kc::atoi(addr) < 1) {
    servseterrmsg(core, "invalid address expression");
    return false;
  }
  if (port < 1 || port > kc::INT16MAX) {
    servseterrmsg(core, "invalid address expression");
    return false;
  }
  struct ::sockaddr_in sain;
  std::memset(&sain, 0, sizeof(sain));
  sain.sin_family = AF_INET;
  if (::inet_aton(addr, &sain.sin_addr) == 0) {
    servseterrmsg(core, "inet_aton failed");
    return false;
  }
  uint16_t snum = port;
  sain.sin_port = htons(snum);
  int32_t fd = ::socket(PF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    servseterrmsg(core, "socket failed");
    return false;
  }
  int32_t optint = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&optint, sizeof(optint));
  if (::bind(fd, (struct sockaddr*)&sain, sizeof(sain)) != 0) {
    servseterrmsg(core, "bind failed");
    ::close(fd);
    return false;
  }
  if (::listen(fd, SOMAXCONN) != 0) {
    servseterrmsg(core, "listen failed");
    ::close(fd);
    return -1;
  }
  int32_t flags = ::fcntl(fd, F_GETFL, NULL);
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    servseterrmsg(core, "fcntl failed");
    ::close(fd);
    return false;
  }
  core->fd = fd;
  core->expr.clear();
  kc::strprintf(&core->expr, "%s:%d", addr, port);
  core->aborted = false;
  return true;
}


/**
 * Close the socket.
 */
bool ServerSocket::close() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  if (::close(core->fd) != 0) {
    servseterrmsg(core, "close failed");
    err = true;
  }
  core->fd = -1;
  core->aborted = false;
  return !err;
}


/**
 * Accept a connection from a client.
 */
bool ServerSocket::accept(Socket* sock) {
  _assert_(sock);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  SocketCore* sockcore = (SocketCore*)sock->opq_;
  if (sockcore->fd >= 0) {
    servseterrmsg(core, "socket was already opened");
    return false;
  }
  double ct = kc::time();
  while (true) {
    struct sockaddr_in sain;
    std::memset(&sain, 0, sizeof(sain));
    sain.sin_family = AF_INET;
    ::socklen_t slen = sizeof(sain);
    int32_t fd = ::accept(core->fd, (struct sockaddr*)&sain, &slen);
    if (fd >= 0) {
      if (!setsocketoptions(fd)) {
        servseterrmsg(core, "setsocketoptions failed");
        ::close(fd);
        return false;
      }
      char addr[NAMEBUFSIZ];
      if (::getnameinfo((struct sockaddr*)&sain, sizeof(sain), addr, sizeof(addr),
                        NULL, 0, NI_NUMERICHOST) != 0) std::sprintf(addr, "0.0.0.0");
      int32_t port = ntohs(sain.sin_port);
      sockcore->fd = fd;
      sockcore->expr.clear();
      kc::strprintf(&sockcore->expr, "%s:%d", addr, port);
      sockcore->aborted = false;
      return true;
    } else {
      if (!checkerrnoretriable(errno)) {
        servseterrmsg(core, "accept failed");
        break;
      }
      if (kc::time() > ct + core->timeout) {
        servseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        servseterrmsg(core, "operation was aborted");
        break;
      }
      if (!waitsocket(core->fd, 1, WAITTIME)) {
        servseterrmsg(core, "waitsocket failed");
        break;
      }
    }
  }
  return false;
}


/**
 * Abort the current operation.
 */
bool ServerSocket::abort() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 1) {
    servseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
}


/**
 * Set the timeout of each operation.
 */
bool ServerSocket::set_timeout(double timeout) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd > 0) {
    servseterrmsg(core, "already opened");
    return false;
  }
  core->timeout = timeout > 0 ? timeout : kc::UINT32MAX;
  if (timeout > kc::UINT32MAX) timeout = kc::UINT32MAX;
  return true;
}


/**
 * Get the expression of the socket.
 */
const std::string ServerSocket::expression() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 0) {
    servseterrmsg(core, "not opened");
    return "";
  }
  return core->expr;
}


/**
 * Get the descriptor integer.
 */
int32_t ServerSocket::descriptor() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  if (core->fd < 0) {
    servseterrmsg(core, "not opened");
    return -1;
  }
  return core->fd;
}


/**
 * Set event flags.
 */
void ServerSocket::set_event_flags(uint32_t flags) {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  core->evflags = flags;
}


/**
 * Get the current event flags.
 */
uint32_t ServerSocket::event_flags() {
  _assert_(true);
  ServerSocketCore* core = (ServerSocketCore*)opq_;
  return core->evflags;
}


/**
 * Default constructor.
 */
Poller::Poller() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = new PollerCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->aborted = false;
  opq_ = core;
  dummysighandler(0);
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = new PollerCore;
  core->errmsg = NULL;
  core->fd = -1;
  core->aborted = false;
  opq_ = core;
  dummysighandler(0);
#else
  _assert_(true);
  PollerCore* core = new PollerCore;
  core->errmsg = NULL;
  core->open = false;
  core->th = ::pthread_self();
  core->aborted = false;
  opq_ = core;
  dummysighandler(0);
#endif
}


/**
 * Destructor.
 */
Poller::~Poller() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd >= 0) close();
  delete core;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->open) close();
  delete core;
#endif
}


/**
 * Get the last happened error information.
 */
const char* Poller::error() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->errmsg) return "no error";
  return core->errmsg;
#endif
}


/**
 * Open the poller.
 */
bool Poller::open() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd >= 0) {
    pollseterrmsg(core, "already opened");
    return false;
  }
  int32_t fd = ::epoll_create(256);
  if (fd < 0) {
    pollseterrmsg(core, "epoll_create failed");
    return false;
  }
  core->fd = fd;
  return true;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd >= 0) {
    pollseterrmsg(core, "already opened");
    return false;
  }
  int32_t fd = ::kqueue();
  if (fd < 0) {
    pollseterrmsg(core, "kqueue failed");
    return false;
  }
  core->fd = fd;
  return true;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->open) {
    pollseterrmsg(core, "already opened");
    return false;
  }
  core->open = true;
  return true;
#endif
}


/**
 * Close the poller.
 */
bool Poller::close() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  if (::close(core->fd) != 0) {
    pollseterrmsg(core, "close failed");
    return false;
  }
  core->hits.clear();
  core->events.clear();
  core->fd = -1;
  core->aborted = false;
  return !err;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  if (::close(core->fd) != 0) {
    pollseterrmsg(core, "close failed");
    return false;
  }
  core->hits.clear();
  core->events.clear();
  core->fd = -1;
  core->aborted = false;
  return !err;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->hits.clear();
  core->events.clear();
  core->open = false;
  core->aborted = false;
  return true;
#endif
}


/**
 * Add a pollable I/O event to the monitored list.
 */
bool Poller::deposit(Pollable* event) {
#if defined(_KT_EVENT_EPOLL)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  struct ::epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  uint32_t flags = event->event_flags();
  ev.events = EPOLLONESHOT;
  if (flags & Pollable::EVINPUT) ev.events |= EPOLLIN;
  if (flags & Pollable::EVOUTPUT) ev.events |= EPOLLOUT;
  if (flags & Pollable::EVEXCEPT) ev.events |= EPOLLHUP | EPOLLPRI;
  ev.data.ptr = event;
  if (::epoll_ctl(core->fd, EPOLL_CTL_ADD, event->descriptor(), &ev) != 0) {
    pollseterrmsg(core, "epoll_ctl failed");
    core->elock.unlock();
    return false;
  }
  core->events.insert(event);
  core->elock.unlock();
  return true;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  struct ::kevent ev;
  std::memset(&ev, 0, sizeof(ev));
  uint32_t flags = event->event_flags();
  uint32_t filter = 0;
  if (flags & Pollable::EVINPUT) filter |= EVFILT_READ;
  if (flags & Pollable::EVOUTPUT) filter |= EVFILT_WRITE;
  EV_SET(&ev, event->descriptor(), filter, EV_ADD | EV_ONESHOT, 0, 0, event);
  if (::kevent(core->fd, &ev, 1, NULL, 0, NULL) != 0) {
    pollseterrmsg(core, "kevent failed a");
    core->elock.unlock();
    return false;
  }
  core->events.insert(event);
  core->elock.unlock();
  return true;
#else
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  if (!core->events.insert(event).second) {
    pollseterrmsg(core, "duplicated");
    core->elock.unlock();
    return false;
  }
  core->elock.unlock();
  return true;
#endif
}


/**
 * Remove a pollable I/O from the monitored list.
 */
bool Poller::withdraw(Pollable* event) {
#if defined(_KT_EVENT_EPOLL)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  core->elock.lock();
  core->events.erase(event);
  if (::epoll_ctl(core->fd, EPOLL_CTL_DEL, event->descriptor(), NULL) != 0) {
    pollseterrmsg(core, "epoll_ctl failed");
    err = true;
  }
  core->elock.unlock();
  return !err;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  bool err = false;
  core->elock.lock();
  core->events.erase(event);
  core->elock.unlock();
  return !err;
#else
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  return true;
#endif
}


/**
 * Fetch the next notified I/O event.
 */
Pollable* Poller::next() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return NULL;
  }
  core->elock.lock();
  if (core->hits.empty()) {
    pollseterrmsg(core, "no event");
    core->elock.unlock();
    return NULL;
  }
  Pollable* item = *core->hits.begin();
  core->hits.erase(item);
  core->elock.unlock();
  return item;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return NULL;
  }
  core->elock.lock();
  if (core->hits.empty()) {
    pollseterrmsg(core, "no event");
    core->elock.unlock();
    return NULL;
  }
  Pollable* item = *core->hits.begin();
  core->hits.erase(item);
  core->elock.unlock();
  return item;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return NULL;
  }
  core->elock.lock();
  if (core->hits.empty()) {
    pollseterrmsg(core, "no event");
    core->elock.unlock();
    return NULL;
  }
  Pollable* item = *core->hits.begin();
  core->hits.erase(item);
  core->elock.unlock();
  return item;
#endif
}


/**
 * Enable the next notification of a pollable event.
 */
bool Poller::undo(Pollable* event) {
#if defined(_KT_EVENT_EPOLL)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  struct ::epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  uint32_t flags = event->event_flags();
  ev.events = EPOLLONESHOT;
  if (flags & Pollable::EVINPUT) ev.events |= EPOLLIN;
  if (flags & Pollable::EVOUTPUT) ev.events |= EPOLLOUT;
  if (flags & Pollable::EVEXCEPT) ev.events |= EPOLLHUP | EPOLLPRI;
  ev.data.ptr = event;
  if (::epoll_ctl(core->fd, EPOLL_CTL_MOD, event->descriptor(), &ev) != 0) {
    pollseterrmsg(core, "epoll_ctl failed");
    core->elock.unlock();
    return false;
  }
  core->elock.unlock();
  return true;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  struct ::kevent ev;
  std::memset(&ev, 0, sizeof(ev));
  uint32_t flags = event->event_flags();
  uint32_t filter = 0;
  if (flags & Pollable::EVINPUT) filter |= EVFILT_READ;
  if (flags & Pollable::EVOUTPUT) filter |= EVFILT_WRITE;
  EV_SET(&ev, event->descriptor(), filter, EV_ADD | EV_ONESHOT, 0, 0, event);
  if (::kevent(core->fd, &ev, 1, NULL, 0, NULL) != 0) {
    pollseterrmsg(core, "kevent failed");
    core->elock.unlock();
    return false;
  }
  core->elock.unlock();
  return true;
#else
  _assert_(event);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  if (!core->events.insert(event).second) {
    pollseterrmsg(core, "duplicated");
    core->elock.unlock();
    return false;
  }
  core->elock.unlock();
  ::pthread_kill(core->th, SIGUSR2);
  return true;
#endif
}


/**
 * Wait one or more notifying events.
 */
bool Poller::wait(double timeout) {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->hits.clear();
  double ct = kc::time();
  while (true) {
    struct ::epoll_event events[256];
    int32_t tout = timeout > 0 ? timeout * 1000 : -1;
    int32_t rv = epoll_wait(core->fd, events, sizeof(events) / sizeof(*events), tout);
    if (rv > 0) {
      for (int32_t i = 0; i < rv; i++) {
        Pollable* item = (Pollable*)events[i].data.ptr;
        uint32_t epflags = events[i].events;
        uint32_t flags = 0;
        if (epflags & EPOLLIN) flags |= Pollable::EVINPUT;
        if (epflags & EPOLLOUT) flags |= Pollable::EVOUTPUT;
        if ((epflags & EPOLLHUP) || (epflags & EPOLLPRI)) flags |= Pollable::EVEXCEPT;
        core->elock.lock();
        if (core->hits.insert(item).second) {
          item->set_event_flags(flags);
        } else {
          uint32_t oflags = item->event_flags();
          item->set_event_flags(oflags | flags);
        }
        core->elock.unlock();
      }
      return true;
    } else if (rv == 0 || checkerrnoretriable(errno)) {
      if (kc::time() > ct + timeout) {
        pollseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        pollseterrmsg(core, "operation was aborted");
        break;
      }
    } else {
      pollseterrmsg(core, "epoll_wait failed");
      break;
    }
  }
  return false;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  if (timeout <= 0) timeout = kc::UINT32MAX;
  core->hits.clear();
  double ct = kc::time();
  while (true) {
    double integ, fract;
    fract = std::modf(timeout, &integ);
    struct ::timespec ts;
    ts.tv_sec = integ;
    ts.tv_nsec = fract * 999999000;
    struct ::kevent events[256];
    int32_t rv = ::kevent(core->fd, NULL, 0, events, sizeof(events) / sizeof(*events), &ts);
    if (rv > 0) {
      for (int32_t i = 0; i < rv; i++) {
        Pollable* item = (Pollable*)events[i].udata;
        uint32_t filter = events[i].filter;
        uint32_t flags = 0;
        if (filter & EVFILT_READ) flags |= Pollable::EVINPUT;
        if (filter & EVFILT_WRITE) flags |= Pollable::EVOUTPUT;
        core->elock.lock();
        if (core->hits.insert(item).second) {
          item->set_event_flags(flags);
        } else {
          uint32_t oflags = item->event_flags();
          item->set_event_flags(oflags | flags);
        }
        core->elock.unlock();
      }
      return true;
    } else if (rv == 0 || checkerrnoretriable(errno)) {
      if (kc::time() > ct + timeout) {
        pollseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        pollseterrmsg(core, "operation was aborted");
        break;
      }
    } else {
      pollseterrmsg(core, "epoll_wait failed");
      break;
    }
  }
  return false;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  if (timeout <= 0) timeout = kc::UINT32MAX;
  core->hits.clear();
  ::sigset_t sigmask;
  sigemptyset(&sigmask);
  sigaddset(&sigmask, SIGUSR2);
  ::pthread_sigmask(SIG_BLOCK, &sigmask, NULL);
  struct ::sigaction sa;
  std::memset(&sa, 0, sizeof(sa));
  sa.sa_flags = 0;
  sa.sa_handler = dummysighandler;
  sigemptyset(&sa.sa_mask);
  ::sigaction(SIGUSR2, &sa, NULL);
  ::sigset_t empmask;
  sigemptyset(&empmask);
  double ct = kc::time();
  while (true) {
    core->elock.lock();
    ::fd_set rset, wset, xset;
    FD_ZERO(&rset);
    FD_ZERO(&wset);
    FD_ZERO(&xset);
    std::map<int32_t, Pollable*> rmap;
    std::map<int32_t, Pollable*> wmap;
    std::map<int32_t, Pollable*> xmap;
    int32_t maxfd = 0;
    std::set<Pollable*>::iterator it = core->events.begin();
    std::set<Pollable*>::iterator itend = core->events.end();
    while (it != itend) {
      Pollable* item = *it;
      int32_t fd = item->descriptor();
      uint32_t flags = item->event_flags();
      if (flags & Pollable::EVINPUT) {
        FD_SET(fd, &rset);
        rmap[fd] = item;
      }
      if (flags & Pollable::EVOUTPUT) {
        FD_SET(fd, &wset);
        wmap[fd] = item;
      }
      if (flags & Pollable::EVEXCEPT) {
        FD_SET(fd, &xset);
        xmap[fd] = item;
      }
      if (fd > maxfd) maxfd = fd;
      ++it;
    }
    core->elock.unlock();
    double integ, fract;
    fract = std::modf(WAITTIME, &integ);
    struct ::timespec ts;
    ts.tv_sec = integ;
    ts.tv_nsec = fract * 999999000;
    int32_t rv = ::pselect(maxfd + 1, &rset, &wset, &xset, &ts, &empmask);
    if (rv > 0) {
      if (!rmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = rmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = rmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &rset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVINPUT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVINPUT);
            }
            core->elock.lock();
            core->events.erase(item);
            core->elock.unlock();
          }
          ++mit;
        }
      }
      if (!wmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = wmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = wmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &wset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVOUTPUT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVOUTPUT);
            }
            core->elock.lock();
            core->events.erase(item);
            core->elock.unlock();
          }
          ++mit;
        }
      }
      if (!xmap.empty()) {
        std::map<int32_t, Pollable*>::iterator mit = xmap.begin();
        std::map<int32_t, Pollable*>::iterator mitend = xmap.end();
        while (mit != mitend) {
          if (FD_ISSET(mit->first, &xset)) {
            Pollable* item = mit->second;
            if (core->hits.insert(item).second) {
              item->set_event_flags(Pollable::EVEXCEPT);
            } else {
              uint32_t flags = item->event_flags();
              item->set_event_flags(flags | Pollable::EVEXCEPT);
            }
            core->elock.lock();
            core->events.erase(item);
            core->elock.unlock();
          }
          ++mit;
        }
      }
      return true;
    } else if (rv == 0 || checkerrnoretriable(errno)) {
      if (kc::time() > ct + timeout) {
        pollseterrmsg(core, "operation timed out");
        break;
      }
      if (core->aborted) {
        pollseterrmsg(core, "operation was aborted");
        break;
      }
    } else {
      pollseterrmsg(core, "pselect failed");
      break;
    }
  }
  return false;
#endif
}


/**
 * Notify all registered events.
 */
bool Poller::flush() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  core->hits.clear();
  std::set<Pollable*>::iterator it = core->events.begin();
  std::set<Pollable*>::iterator itend = core->events.end();
  while (it != itend) {
    Pollable* item = *it;
    item->set_event_flags(0);
    core->hits.insert(item);
    ++it;
  }
  core->elock.unlock();
  return true;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  core->hits.clear();
  std::set<Pollable*>::iterator it = core->events.begin();
  std::set<Pollable*>::iterator itend = core->events.end();
  while (it != itend) {
    Pollable* item = *it;
    item->set_event_flags(0);
    core->hits.insert(item);
    ++it;
  }
  core->elock.unlock();
  return true;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->elock.lock();
  core->hits.clear();
  std::set<Pollable*>::iterator it = core->events.begin();
  std::set<Pollable*>::iterator itend = core->events.end();
  while (it != itend) {
    Pollable* item = *it;
    item->set_event_flags(0);
    core->hits.insert(item);
    ++it;
  }
  core->elock.unlock();
  return true;
#endif
}


/**
 * Get the number of events to watch.
 */
int64_t Poller::count() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return -1;
  }
  core->elock.lock();
  int64_t count = core->events.size();
  core->elock.unlock();
  return count;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return -1;
  }
  core->elock.lock();
  int64_t count = core->events.size();
  core->elock.unlock();
  return count;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return -1;
  }
  core->elock.lock();
  int64_t count = core->events.size();
  core->elock.unlock();
  return count;
#endif
}


/**
 * Abort the current operation.
 */
bool Poller::abort() {
#if defined(_KT_EVENT_EPOLL)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
#elif defined(_KT_EVENT_KQUEUE)
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (core->fd < 0) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
#else
  _assert_(true);
  PollerCore* core = (PollerCore*)opq_;
  if (!core->open) {
    pollseterrmsg(core, "not opened");
    return false;
  }
  core->aborted = true;
  return true;
#endif
}


/**
 * Dummy signal handler.
 */
static void dummysighandler(int signum) {
  _assert_(true);
}


/**
 * Parse an address expression.
 */
static void parseaddr(const char* expr, char* addr, int32_t* portp) {
  _assert_(expr && addr && portp);
  while (*expr > '\0' && *expr <= ' ') {
    expr++;
  }
  const char* pv = std::strchr(expr, ':');
  if (pv) {
    size_t len = pv - expr;
    if (len > NAMEBUFSIZ - 1) len = NAMEBUFSIZ - 1;
    std::memcpy(addr, expr, len);
    addr[len] = '\0';
    *portp = kc::atoi(pv + 1);
  } else {
    size_t len = std::strlen(expr);
    if (len > NAMEBUFSIZ - 1) len = NAMEBUFSIZ - 1;
    std::memcpy(addr, expr, len);
    addr[len] = '\0';
    *portp = DEFPORT;
  }
}


/**
 * Check whether an error code is retriable.
 */
static bool checkerrnoretriable(int32_t ecode) {
  switch (ecode) {
    case EINTR: return true;
    case EAGAIN: return true;
    case EINPROGRESS: return true;
    case EALREADY: return true;
    case ETIMEDOUT: return true;
  }
  if (ecode == EWOULDBLOCK) return true;
  return false;
}


/**
 * Set options of a sockeet.
 */
static bool setsocketoptions(int32_t fd) {
  _assert_(fd >= 0);
  bool err = false;
  double integ;
  double fract = std::modf(WAITTIME, &integ);
  struct ::timeval opttv;
  opttv.tv_sec = (time_t)integ;
  opttv.tv_usec = (long)(fract * 999999);
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char*)&opttv, sizeof(opttv));
  opttv.tv_sec = (time_t)integ;
  opttv.tv_usec = (long)(fract * 999999);
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char*)&opttv, sizeof(opttv));
  int32_t optint = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&optint, sizeof(optint)) != 0)
    err = true;
  optint = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&optint, sizeof(optint)) != 0)
    err = true;
  return !err;
}


/**
 * Clear the error status of a socket.
 */
static void clearsocketerror(int32_t fd) {
  _assert_(fd >= 0);
  int32_t optint = 1;
  ::socklen_t len = sizeof(optint);
  ::getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&optint, &len);
}


/**
 * Wait an I/O event of a socket.
 */
static bool waitsocket(int32_t fd, uint32_t mode, double timeout) {
#if defined(_KT_EVENT_EPOLL) || defined(_KT_EVENT_KQUEUE)
  _assert_(fd >= 0);
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = 0;
  pfd.revents = 0;
  switch (mode) {
    case 0: {
      pfd.events |= POLLIN;
      break;
    }
    case 1: {
      pfd.events |= POLLOUT;
      break;
    }
    case 2: {
      pfd.events |= POLLERR;
      break;
    }
  }
  int32_t rv = poll(&pfd, 1, timeout * 1000);
  if (rv >= 0 || checkerrnoretriable(errno)) {
    clearsocketerror(fd);
    return true;
  }
  clearsocketerror(fd);
  return false;
#else
  _assert_(fd >= 0);
  ::fd_set set;
  FD_ZERO(&set);
  FD_SET(fd, &set);
  double integ, fract;
  fract = std::modf(timeout, &integ);
  struct ::timespec ts;
  ts.tv_sec = integ;
  ts.tv_nsec = fract * 999999000;
  int32_t rv = -1;
  switch (mode) {
    case 0: {
      rv = ::pselect(fd + 1, &set, NULL, NULL, &ts, NULL);
      break;
    }
    case 1: {
      rv = ::pselect(fd + 1, NULL, &set, NULL, &ts, NULL);
      break;
    }
    case 2: {
      rv = ::pselect(fd + 1, NULL, NULL, &set, &ts, NULL);
      break;
    }
  }
  if (rv >= 0 || checkerrnoretriable(errno)) {
    clearsocketerror(fd);
    return true;
  }
  clearsocketerror(fd);
  return false;
#endif
}


/**
 * Set the error message of a socket.
 */
static void sockseterrmsg(SocketCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


/**
 * Receive one byte from a socket.
 */
static int32_t sockgetc(SocketCore* core) {
  _assert_(core);
  if (core->rp < core->ep) return *(unsigned char*)(core->rp++);
  if (!core->buf) {
    core->buf = new char[IOBUFSIZ];
    core->rp = core->buf;
    core->ep = core->buf;
  }
  double ct = kc::time();
  while (true) {
    int32_t rv = ::recv(core->fd, core->buf, IOBUFSIZ, 0);
    if (rv > 0) {
      core->rp = core->buf + 1;
      core->ep = core->buf + rv;
      return *(unsigned char*)core->buf;
    } else if (rv == 0) {
      sockseterrmsg(core, "end of stream");
      return -1;
    }
    if (!checkerrnoretriable(errno)) break;
    if (kc::time() > ct + core->timeout) {
      sockseterrmsg(core, "operation timed out");
      return -1;
    }
    if (core->aborted) {
      sockseterrmsg(core, "operation was aborted");
      return -1;
    }
    if (!waitsocket(core->fd, 0, WAITTIME)) {
      sockseterrmsg(core, "waitsocket failed");
      return -1;
    }
  }
  sockseterrmsg(core, "recv failed");
  return -1;
}


/**
 * Set the error message of a server.
 */
static void servseterrmsg(ServerSocketCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


/**
 * Set the error message of a poller.
 */
static void pollseterrmsg(PollerCore* core, const char* msg) {
  _assert_(core && msg);
  core->errmsg = msg;
}


}                                        // common namespace

// END OF FILE
