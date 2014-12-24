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


#ifndef _KTSOCKET_H                      // duplication check
#define _KTSOCKET_H

#include <ktcommon.h>
#include <ktutil.h>

namespace kyototycoon {                  // common namespace


/**
 * Interface of poolable I/O event.
 */
class Pollable {
  friend class Poller;
 public:
  /**
   * Event flags.
   */
  enum EventFlag {
    EVINPUT = 1 << 0,                    ///< input
    EVOUTPUT = 1 << 1,                   ///< output
    EVEXCEPT = 1 << 2                    ///< exception
  };
  /**
   * Default constructor.
   */
  explicit Pollable() : opq_(NULL) {}
  /**
   * Destructor.
   */
  virtual ~Pollable() {}
  /**
   * Get the descriptor integer.
   * @return the descriptor integer, or -1 on failure.
   */
  virtual int32_t descriptor() = 0;
  /**
   * Set event flags.
   * @param flags specifies the event mode.  The following may be added by bitwise-or:
   * Pollable::EVINPUT for input events, Pollable::EVOUTPUT for output events, Pollable::EVEXCEPT
   * for exception events.
   */
  virtual void set_event_flags(uint32_t flags) = 0;
  /**
   * Get the current event flags.
   * @return the current event flags.
   */
  virtual uint32_t event_flags() = 0;
 private:
  /** Dummy constructor to forbid the use. */
  Pollable(const Pollable&);
  /** Dummy Operator to forbid the use. */
  Pollable& operator =(const Pollable&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Network stream abstraction based on TCP/IP.
 */
class Socket : public Pollable {
  friend class ServerSocket;
 public:
  /**
   * Default constructor.
   */
  explicit Socket();
  /**
   * Destructor.
   */
  ~Socket();
  /**
   * Get the last happened error information.
   * @return the last happened error information.
   */
  const char* error();
  /**
   * Open a client socket.
   * @param expr an expression of the address and the port of the server.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& expr);
  /**
   * Close the socket.
   * @param grace true for graceful shutdown, or false for immediate disconnection.
   * @return true on success, or false on failure.
   */
  bool close(bool grace = true);
  /**
   * Send data.
   * @param buf the pointer to a data region to send.
   * @param size the size of the data region.
   * @return true on success, or false on failure.
   */
  bool send(const void* buf, size_t size);
  /**
   * Send data.
   * @param str a string to send.
   * @return true on success, or false on failure.
   */
  bool send(const std::string& str);
  /**
   * Send formatted data.
   * @param format the printf-like format string.  The conversion character `%' can be used with
   * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
   * @param ... used according to the format string.
   * @return true on success, or false on failure.
   */
  bool printf(const char* format, ...);
  /**
   * Send formatted data.
   * @param format the printf-like format string.  The conversion character `%' can be used with
   * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
   * @param ap used according to the format string.
   * @return true on success, or false on failure.
   */
  bool vprintf(const char* format, va_list ap);
  /**
   * Receive data.
   * @param buf the pointer to the buffer into which the received data is written.
   * @param size the size of the data to receive.
   * @return true on success, or false on failure.
   */
  bool receive(void* buf, size_t size);
  /**
   * Receive one byte.
   * @return the received byte or -1 on failure.
   */
  int32_t receive_byte();
  /**
   * Push one byte back.
   * @param c specifies the byte.
   * @return true on success, or false on failure.
   * @note The character is available for subsequent receive operations.  Only one pushback is
   * guaranteed.
   */
  bool undo_receive_byte(int32_t c);
  /**
   * Receive one line of characters.
   * @param buf the pointer to the buffer into which the received data is written.
   * @param max the maximum size of the data to receive.  It must be more than 0.
   * @return true on success, or false on failure.
   */
  bool receive_line(void* buf, size_t max);
  /**
   * Get the size of left data in the receiving buffer.
   * @return the size of left data in the receiving buffer.
   */
  size_t left_size();
  /**
   * Abort the current operation.
   * @return true on success, or false on failure.
   */
  bool abort();
  /**
   * Set the timeout of each operation.
   * @param timeout the timeout of each operation in seconds.
   * @return true on success, or false on failure.
   */
  bool set_timeout(double timeout);
  /**
   * Get the expression of the socket.
   * @return the expression of the socket or an empty string on failure.
   */
  const std::string expression();
  /**
   * Get the descriptor integer.
   * @return the descriptor integer, or -1 on failure.
   */
  int32_t descriptor();
  /**
   * Set event flags.
   * @param flags specifies the event mode.  The following may be added by bitwise-or:
   * Socket::EVINPUT for input events, Socket::EVOUTPUT for output events, Socket::EVEXCEPT for
   * exception events.
   */
  void set_event_flags(uint32_t flags);
  /**
   * Get the current event flags.
   * @return the current event flags.
   */
  uint32_t event_flags();
  /**
   * Get the primary name of the local host.
   * @return the host name, or an empty string on failure.
   */
  static std::string get_local_host_name();
  /**
   * Get the address of a host.
   * @return the host address, or an empty string on failure.
   */
  static std::string get_host_address(const std::string& name);
 private:
  /** Dummy constructor to forbid the use. */
  Socket(const Socket&);
  /** Dummy Operator to forbid the use. */
  Socket& operator =(const Socket&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * Network server abstraction based on TCP/IP.
 */
class ServerSocket : public Pollable {
 public:
  /**
   * Default constructor.
   */
  explicit ServerSocket();
  /**
   * Destructor.
   */
  ~ServerSocket();
  /**
   * Get the last happened error information.
   * @return the last happened error information.
   */
  const char* error();
  /**
   * Open a server socket.
   * @param expr an expression of the address and the port of the server.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& expr);
  /**
   * Close the socket.
   * @return true on success, or false on failure.
   */
  bool close();
  /**
   * Accept a connection from a client.
   * @param sock the socket object to manage the connection.
   * @return true on success, or false on failure.
   */
  bool accept(Socket* sock);
  /**
   * Abort the current operation.
   * @return true on success, or false on failure.
   */
  bool abort();
  /**
   * Set the timeout of each operation.
   * @param timeout the timeout of each operation in seconds.
   * @return true on success, or false on failure.
   */
  bool set_timeout(double timeout);
  /**
   * Get the expression of the socket.
   * @return the expression of the socket or an empty string on failure.
   */
  const std::string expression();
  /**
   * Get the descriptor integer.
   * @return the descriptor integer, or -1 on failure.
   */
  int32_t descriptor();
  /**
   * Set event flags.
   * @param flags specifies the event mode.  The following may be added by bitwise-or:
   * ServerSocket::EVINPUT for input events, ServerSocket::EVOUTPUT for output events,
   * ServerSocket::EVERROR for error events.
   */
  void set_event_flags(uint32_t flags);
  /**
   * Get the current event flags.
   * @return the current event flags.
   */
  uint32_t event_flags();
 private:
  /** Dummy constructor to forbid the use. */
  ServerSocket(const Socket&);
  /** Dummy Operator to forbid the use. */
  ServerSocket& operator =(const ServerSocket&);
  /** Opaque pointer. */
  void* opq_;
};


/**
 * I/O event notification.
 */
class Poller {
 public:
  /**
   * Default constructor.
   */
  explicit Poller();
  /**
   * Destructor.
   */
  ~Poller();
  /**
   * Get the last happened error information.
   * @return the last happened error information.
   */
  const char* error();
  /**
   * Open the poller.
   * @return true on success, or false on failure.
   */
  bool open();
  /**
   * Close the poller.
   * @return true on success, or false on failure.
   */
  bool close();
  /**
   * Add a pollable I/O event to the monitored list.
   * @param event the pollable event object.
   * @return true on success, or false on failure.
   */
  bool deposit(Pollable* event);
  /**
   * Remove a pollable I/O from the monitored list.
   * @param event the pollable event object.
   * @return true on success, or false on failure.
   */
  bool withdraw(Pollable* event);
  /**
   * Fetch the next notified I/O event.
   * @return the event object, or NULL on failure.
   */
  Pollable* next();
  /**
   * Enable the next notification of a pollable event.
   * @param event the pollable event object.
   * @return true on success, or false on failure.
   */
  bool undo(Pollable* event);
  /**
   * Wait one or more notifying events.
   * @param timeout the timeout in seconds.  If it is not more than 0, no timeout is specified.
   * @return true on success, or false on failure.
   */
  bool wait(double timeout = -1);
  /**
   * Notify all registered events.
   * @return true on success, or false on failure.
   */
  bool flush();
  /**
   * Get the number of events to watch.
   * @return the number of events to watch, or -1 on failure.
   */
  int64_t count();
  /**
   * Abort the current operation.
   * @return true on success, or false on failure.
   */
  bool abort();
 private:
  /** Dummy constructor to forbid the use. */
  Poller(const Poller&);
  /** Dummy Operator to forbid the use. */
  Poller& operator =(const Poller&);
  /** Opaque pointer. */
  void* opq_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
