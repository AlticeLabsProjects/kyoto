/*************************************************************************************************
 * Threaded Server
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


#ifndef _KTTHSERV_H                      // duplication check
#define _KTTHSERV_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>

namespace kyototycoon {                  // common namespace


/**
 * Threaded TCP Server.
 */
class ThreadedServer {
 public:
  class Logger;
  class Worker;
  class Session;
 private:
  class TaskQueueImpl;
  class SessionTask;
 public:
  /**
   * Interface to log internal information and errors.
   */
  class Logger {
   public:
    /**
     * Event kinds.
     */
    enum Kind {
      DEBUG = 1 << 0,                    ///< normal information
      INFO = 1 << 1,                     ///< normal information
      SYSTEM = 1 << 2,                   ///< system information
      ERROR = 1 << 3                     ///< error
    };
    /**
     * Destructor.
     */
    virtual ~Logger() {
      _assert_(true);
    }
    /**
     * Process a log message.
     * @param kind the kind of the event.  Logger::DEBUG for debugging, Logger::INFO for normal
     * information, Logger::SYSTEM for system information, and Logger::ERROR for fatal error.
     * @param message the log message.
     */
    virtual void log(Kind kind, const char* message) = 0;
  };
  /**
   * Interface to process each request.
   */
  class Worker {
   public:
    /**
     * Destructor.
     */
    virtual ~Worker() {
      _assert_(true);
    }
    /**
     * Process each request.
     * @param serv the server.
     * @param sess the session with the client.
     * @return true to reuse the session, or false to close the session.
     */
    virtual bool process(ThreadedServer* serv, Session* sess) = 0;
    /**
     * Process each idle event.
     * @param serv the server.
     */
    virtual void process_idle(ThreadedServer* serv) {
      _assert_(serv);
    }
    /**
     * Process each timer event.
     * @param serv the server.
     */
    virtual void process_timer(ThreadedServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the starting event.
     * @param serv the server.
     */
    virtual void process_start(ThreadedServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the finishing event.
     * @param serv the server.
     */
    virtual void process_finish(ThreadedServer* serv) {
      _assert_(serv);
    }
  };
  /**
   * Interface to access each session data.
   */
  class Session : public Socket {
    friend class ThreadedServer;
   public:
    class Data;
   public:
    /**
     * Interface of session local data.
     */
    class Data {
     public:
      /**
       * Destructor.
       */
      virtual ~Data() {
        _assert_(true);
      }
    };
    /**
     * Get the ID number of the session.
     * @return the ID number of the session.
     */
    uint64_t id() {
      _assert_(true);
      return id_;
    }
    /**
     * Get the ID number of the worker thread.
     * @return the ID number of the worker thread.  It is from 0 to less than the number of
     * worker threads.
     */
    uint32_t thread_id() {
      _assert_(true);
      return thid_;
    }
    /**
     * Set the session local data.
     * @param data the session local data.  If it is NULL, no data is registered.
     * @note The registered data is destroyed implicitly when the session object is destroyed or
     * this method is called again.
     */
    void set_data(Data* data) {
      _assert_(true);
      delete data_;
      data_ = data;
    }
    /**
     * Get the session local data.
     * @return the session local data, or NULL if no data is registered.
     */
    Data* data() {
      _assert_(true);
      return data_;
    }
   private:
    /**
     * Default Constructor.
     */
    explicit Session(uint64_t id) : id_(id), thid_(0), data_(NULL) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~Session() {
      _assert_(true);
      delete data_;
    }
   private:
    /** The ID number of the session. */
    uint64_t id_;
    /** The ID number of the worker thread. */
    uint32_t thid_;
    /** The session local data. */
    Data* data_;
  };
  /**
   * Default constructor.
   */
  explicit ThreadedServer() :
      run_(false), expr_(), timeout_(0), logger_(NULL), logkinds_(0), worker_(NULL), thnum_(0),
      sock_(), poll_(), queue_(this), sesscnt_(0), idlesem_(0), timersem_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~ThreadedServer() {
    _assert_(true);
  }
  /**
   * Set the network configurations.
   * @param expr an expression of the address and the port of the server.
   * @param timeout the timeout of each network operation in seconds.  If it is not more than 0,
   * no timeout is specified.
   */
  void set_network(const std::string& expr, double timeout = -1) {
    expr_ = expr;
    timeout_ = timeout;
  }
  /**
   * Set the logger to process each log message.
   * @param logger the logger object.
   * @param kinds kinds of logged messages by bitwise-or: Logger::DEBUG for debugging,
   * Logger::INFO for normal information, Logger::SYSTEM for system information, and
   * Logger::ERROR for fatal error.
   */
  void set_logger(Logger* logger, uint32_t kinds = Logger::SYSTEM | Logger::ERROR) {
    _assert_(logger);
    logger_ = logger;
    logkinds_ = kinds;
  }
  /**
   * Set the worker to process each request.
   * @param worker the worker object.
   * @param thnum the number of worker threads.
   */
  void set_worker(Worker* worker, size_t thnum = 1) {
    _assert_(worker && thnum > 0 && thnum < kc::MEMMAXSIZ);
    worker_ = worker;
    thnum_ = thnum;
  }
  /**
   * Start the service.
   * @return true on success, or false on failure.
   * @note This function blocks until the server stops by the ThreadedServer::stop method.
   */
  bool start() {
    log(Logger::SYSTEM, "starting the server: expr=%s", expr_.c_str());
    if (run_) {
      log(Logger::ERROR, "alreadiy running");
      return false;
    }
    if (expr_.empty()) {
      log(Logger::ERROR, "the network configuration is not set");
      return false;
    }
    if (!worker_) {
      log(Logger::ERROR, "the worker is not set");
      return false;
    }
    if (!sock_.open(expr_)) {
      log(Logger::ERROR, "socket error: expr=%s msg=%s", expr_.c_str(), sock_.error());
      return false;
    }
    log(Logger::SYSTEM, "server socket opened: expr=%s timeout=%.1f", expr_.c_str(), timeout_);
    if (!poll_.open()) {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      sock_.close();
      return false;
    }
    log(Logger::SYSTEM, "listening server socket started: fd=%d", sock_.descriptor());
    bool err = false;
    sock_.set_event_flags(Pollable::EVINPUT);
    if (!poll_.deposit(&sock_)) {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      err = true;
    }
    queue_.set_worker(worker_);
    queue_.start(thnum_);
    uint32_t timercnt = 0;
    run_ = true;
    while (run_) {
      if (poll_.wait(0.1)) {
        Pollable* event;
        while ((event = poll_.next()) != NULL) {
          if (event == &sock_) {
            Session* sess = new Session(++sesscnt_);
            if (timeout_ > 0) sess->set_timeout(timeout_);
            if (sock_.accept(sess)) {
              log(Logger::INFO, "connected: expr=%s", sess->expression().c_str());
              sess->set_event_flags(Pollable::EVINPUT);
              if (!poll_.deposit(sess)) {
                log(Logger::ERROR, "poller error: msg=%s", poll_.error());
                err = true;
              }
            } else {
              log(Logger::ERROR, "socket error: msg=%s", sock_.error());
              err = true;
            }
            sock_.set_event_flags(Pollable::EVINPUT);
            if (!poll_.undo(&sock_)) {
              log(Logger::ERROR, "poller error: msg=%s", poll_.error());
              err = true;
            }
          } else {
            Session* sess = (Session*)event;
            SessionTask* task = new SessionTask(sess);
            queue_.add_task(task);
          }
        }
        timercnt++;
      } else {
        if (queue_.count() < 1 && idlesem_.cas(0, 1)) {
          SessionTask* task = new SessionTask(SESSIDLE);
          queue_.add_task(task);
        }
        timercnt += kc::UINT8MAX / 4;
      }
      if (timercnt > kc::UINT8MAX && timersem_.cas(0, 1)) {
        SessionTask* task = new SessionTask(SESSTIMER);
        queue_.add_task(task);
        timercnt = 0;
      }
    }
    log(Logger::SYSTEM, "server stopped");
    if (err) log(Logger::SYSTEM, "one or more errors were detected");
    return !err;
  }
  /**
   * Stop the service.
   * @return true on success, or false on failure.
   */
  bool stop() {
    if (!run_) {
      log(Logger::ERROR, "not running");
      return false;
    }
    run_ = false;
    sock_.abort();
    poll_.abort();
    return true;
  }
  /**
   * Finish the service.
   * @return true on success, or false on failure.
   */
  bool finish() {
    log(Logger::SYSTEM, "finishing the server");
    if (run_) {
      log(Logger::ERROR, "not stopped");
      return false;
    }
    bool err = false;
    queue_.finish();
    if (queue_.error()) {
      log(Logger::SYSTEM, "one or more errors were detected");
      err = true;
    }
    if (poll_.flush()) {
      Pollable* event;
      while ((event = poll_.next()) != NULL) {
        if (event == &sock_) continue;
        Session* sess = (Session*)event;
        log(Logger::INFO, "disconnecting: expr=%s", sess->expression().c_str());
        if (!poll_.withdraw(sess)) {
          log(Logger::ERROR, "poller error: msg=%s", poll_.error());
          err = true;
        }
        if (!sess->close()) {
          log(Logger::ERROR, "socket error: fd=%d msg=%s", sess->descriptor(), sess->error());
          err = true;
        }
        delete sess;
      }
    } else {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      err = true;
    }
    if (!poll_.close()) {
      log(Logger::ERROR, "poller error: msg=%s", poll_.error());
      err = true;
    }
    log(Logger::SYSTEM, "closing the server socket");
    if (!sock_.close()) {
      log(Logger::ERROR, "socket error: fd=%d msg=%s", sock_.descriptor(), sock_.error());
      err = true;
    }
    return !err;
  }
  /**
   * Log a message.
   * @param kind the kind of the event.  Logger::DEBUG for debugging, Logger::INFO for normal
   * information, Logger::SYSTEM for system information, and Logger::ERROR for fatal error.
   * @param format the printf-like format string.  The conversion character `%' can be used with
   * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
   * @param ... used according to the format string.
   */
  void log(Logger::Kind kind, const char* format, ...) {
    _assert_(format);
    if (!logger_ || !(kind & logkinds_)) return;
    std::string msg;
    va_list ap;
    va_start(ap, format);
    kc::vstrprintf(&msg, format, ap);
    va_end(ap);
    logger_->log(kind, msg.c_str());
  }
  /**
   * Log a message.
   * @note Equal to the original Cursor::set_value method except that the last parameters is
   * va_list.
   */
  void log_v(Logger::Kind kind, const char* format, va_list ap) {
    _assert_(format);
    if (!logger_ || !(kind & logkinds_)) return;
    std::string msg;
    kc::vstrprintf(&msg, format, ap);
    logger_->log(kind, msg.c_str());
  }
  /**
   * Get the number of connections.
   * @return the number of connections.
   */
  int64_t connection_count() {
    _assert_(true);
    return poll_.count() - 1;
  }
  /**
   * Get the number of tasks in the queue.
   * @return the number of tasks in the queue.
   */
  int64_t task_count() {
    _assert_(true);
    return queue_.count();
  }
  /**
   * Check whether the thread is to be aborted.
   * @return true if the thread is to be aborted, or false if not.
   */
  bool aborted() {
    _assert_(true);
    return !run_;
  }
 private:
  /** The magic pointer of an idle session. */
  static Session* const SESSIDLE;
  /** The magic pointer of a timer session. */
  static Session* const SESSTIMER;
  /**
   * Task queue implementation.
   */
  class TaskQueueImpl : public kc::TaskQueue {
   public:
    explicit TaskQueueImpl(ThreadedServer* serv) : serv_(serv), worker_(NULL), err_(false) {
      _assert_(true);
    }
    void set_worker(Worker* worker) {
      _assert_(worker);
      worker_ = worker;
    }
    bool error() {
      _assert_(true);
      return err_;
    }
   private:
    void do_task(kc::TaskQueue::Task* task) {
      _assert_(task);
      SessionTask* mytask = (SessionTask*)task;
      Session* sess = mytask->sess_;
      if (sess == SESSIDLE) {
        worker_->process_idle(serv_);
        serv_->idlesem_.set(0);
      } else if (sess == SESSTIMER) {
        worker_->process_timer(serv_);
        serv_->timersem_.set(0);
      } else {
        bool keep = false;
        if (mytask->aborted()) {
          serv_->log(Logger::INFO, "aborted a request: expr=%s", sess->expression().c_str());
        } else {
          sess->thid_ = mytask->thread_id();
          do {
            keep = worker_->process(serv_, sess);
          } while (keep && sess->left_size() > 0);
        }
        if (keep) {
          sess->set_event_flags(Pollable::EVINPUT);
          if (!serv_->poll_.undo(sess)) {
            serv_->log(Logger::ERROR, "poller error: msg=%s", serv_->poll_.error());
            err_ = true;
          }
        } else {
          serv_->log(Logger::INFO, "disconnecting: expr=%s", sess->expression().c_str());
          if (!serv_->poll_.withdraw(sess)) {
            serv_->log(Logger::ERROR, "poller error: msg=%s", serv_->poll_.error());
            err_ = true;
          }
          if (!sess->close()) {
            serv_->log(Logger::ERROR, "socket error: msg=%s", sess->error());
            err_ = true;
          }
          delete sess;
        }
      }
      delete mytask;
    }
    void do_start(const kc::TaskQueue::Task* task) {
      _assert_(task);
      worker_->process_start(serv_);
    }
    void do_finish(const kc::TaskQueue::Task* task) {
      _assert_(task);
      worker_->process_finish(serv_);
    }
    ThreadedServer* serv_;
    Worker* worker_;
    bool err_;
  };
  /**
   * Task with a session.
   */
  class SessionTask : public kc::TaskQueue::Task {
    friend class ThreadedServer;
   public:
    explicit SessionTask(Session* sess) : sess_(sess) {}
   private:
    Session* sess_;
  };
  /** Dummy constructor to forbid the use. */
  ThreadedServer(const ThreadedServer&);
  /** Dummy Operator to forbid the use. */
  ThreadedServer& operator =(const ThreadedServer&);
  /** The flag of running. */
  bool run_;
  /** The expression of the server socket. */
  std::string expr_;
  /** The timeout of each network operation. */
  double timeout_;
  /** The internal logger. */
  Logger* logger_;
  /** The kinds of logged messages. */
  uint32_t logkinds_;
  /** The worker operator. */
  Worker* worker_;
  /** The number of worker threads. */
  size_t thnum_;
  /** The server socket. */
  ServerSocket sock_;
  /** The event poller. */
  Poller poll_;
  /** The task queue. */
  TaskQueueImpl queue_;
  /** The session count. */
  uint64_t sesscnt_;
  /** The idle event semaphore. */
  kc::AtomicInt64 idlesem_;
  /** The timer event semaphore. */
  kc::AtomicInt64 timersem_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
