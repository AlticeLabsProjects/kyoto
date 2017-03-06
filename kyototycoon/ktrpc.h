/*************************************************************************************************
 * RPC utilities
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


#ifndef _KTRPC_H                         // duplication check
#define _KTRPC_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>
#include <kthttp.h>

#define KTRPCPATHPREFIX  "/rpc/"           ///< prefix of the RPC entry
#define KTRPCFORMMTYPE  "application/x-www-form-urlencoded"  ///< MIME type of form data
#define KTRPCTSVMTYPE  "text/tab-separated-values"  ///< MIME type of TSV
#define KTRPCTSVMATTR  "colenc"            ///< encoding attribute of TSV

namespace kyototycoon {                  // common namespace


/**
 * RPC client.
 * @note Although all methods of this class are thread-safe, its instance does not have mutual
 * exclusion mechanism.  So, multiple threads must not share the same instance and they must use
 * their own respective instances.
 */
class RPCClient {
 public:
  /**
   * Return value.
   */
  enum ReturnValue {
    RVSUCCESS,                           ///< success
    RVENOIMPL,                           ///< not implemented
    RVEINVALID,                          ///< invalid operation
    RVELOGIC,                            ///< logical inconsistency
    RVETIMEOUT,                          ///< timeout
    RVEINTERNAL,                         ///< internal error
    RVENETWORK,                          ///< network error
    RVEMISC = 15                         ///< miscellaneous error
  };
  /**
   * Default constructor.
   */
  RPCClient() : ua_(), host_(), port_(0), timeout_(0), open_(false), alive_(false) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~RPCClient() {
    _assert_(true);
    if (open_) close();
  }
  /**
   * Open the connection.
   * @param host the name or the address of the server.  If it is an empty string, the local host
   * is specified.
   * @param port the port number of the server.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& host = "", int32_t port = DEFPORT, double timeout = -1) {
    _assert_(true);
    if (open_ || port < 1) return false;
    if (!ua_.open(host, port, timeout)) return false;
    host_ = host;
    port_ = port;
    timeout_ = timeout;
    open_ = true;
    alive_ = true;
    return true;
  }
  /**
   * Close the connection.
   * @param grace true for graceful shutdown, or false for immediate disconnection.
   * @return true on success, or false on failure.
   */
  bool close(bool grace = true) {
    _assert_(true);
    if (!open_) return false;
    bool err = false;
    if (alive_ && !ua_.close(grace)) err = true;
    return !err;
  }
  /**
   * Call a remote procedure.
   * @param name the name of the procecude.
   * @param inmap a string map which contains the input of the procedure.  If it is NULL, it is
   * ignored.
   * @param outmap a string map to contain the output parameters.  If it is NULL, it is ignored.
   * @return the return value of the procedure.
   */
  ReturnValue call(const std::string& name,
                   const std::map<std::string, std::string>* inmap = NULL,
                   std::map<std::string, std::string>* outmap = NULL) {
    _assert_(true);
    if (outmap) outmap->clear();
    if (!open_) return RVENETWORK;
    if (!alive_ && !ua_.open(host_, port_, timeout_)) return RVENETWORK;
    alive_ = true;
    std::string pathquery = KTRPCPATHPREFIX;
    char* zstr = kc::urlencode(name.data(), name.size());
    pathquery.append(zstr);
    delete[] zstr;
    std::map<std::string, std::string> reqheads;
    std::string reqbody;
    if (inmap) {
      std::map<std::string, std::string> tmap;
      tmap.insert(inmap->begin(), inmap->end());
      int32_t enc = checkmapenc(tmap);
      std::string outtype = KTRPCTSVMTYPE;
      switch (enc) {
        case 'B': kc::strprintf(&outtype, "; %s=B", KTRPCTSVMATTR); break;
        case 'Q': kc::strprintf(&outtype, "; %s=Q", KTRPCTSVMATTR); break;
        case 'U': kc::strprintf(&outtype, "; %s=U", KTRPCTSVMATTR); break;
      }
      reqheads["content-type"] = outtype;
      if (enc != 0) tsvmapencode(&tmap, enc);
      maptotsv(tmap, &reqbody);
    }
    std::map<std::string, std::string> resheads;
    std::string resbody;
    int32_t code = ua_.fetch(pathquery, HTTPClient::MPOST, &resbody, &resheads,
                             &reqbody, &reqheads);
    if (outmap) {
      const char* rp = strmapget(resheads, "content-type");
      if (rp) {
        if (kc::strifwm(rp, KTRPCFORMMTYPE)) {
          wwwformtomap(resbody.c_str(), outmap);
        } else if (kc::strifwm(rp, KTRPCTSVMTYPE)) {
          rp += sizeof(KTRPCTSVMTYPE) - 1;
          int32_t enc = 0;
          while (*rp != '\0') {
            while (*rp == ' ' || *rp == ';') {
              rp++;
            }
            if (kc::strifwm(rp, KTRPCTSVMATTR) && rp[sizeof(KTRPCTSVMATTR)-1] == '=') {
              rp += sizeof(KTRPCTSVMATTR);
              if (*rp == '"') rp++;
              switch (*rp) {
                case 'b': case 'B': enc = 'B'; break;
                case 'q': case 'Q': enc = 'Q'; break;
                case 'u': case 'U': enc = 'U'; break;
              }
            }
            while (*rp != '\0' && *rp != ' ' && *rp != ';') {
              rp++;
            }
          }
          tsvtomap(resbody, outmap);
          if (enc != 0) tsvmapdecode(outmap, enc);
        }
      }
    }
    ReturnValue rv;
    if (code < 1) {
      rv = RVENETWORK;
      ua_.close(false);
      alive_ = false;
    } else if (code >= 200 && code < 300) {
      rv = RVSUCCESS;
    } else if (code >= 400 && code < 500) {
      if (code >= 450) {
        rv = RVELOGIC;
      } else {
        rv = RVEINVALID;
      }
    } else if (code >= 500 && code < 600) {
      if (code == 501) {
        rv = RVENOIMPL;
      } else if (code == 503) {
        rv = RVETIMEOUT;
      } else {
        rv = RVEINTERNAL;
      }
    } else {
      rv = RVEMISC;
    }
    return rv;
  }
  /**
   * Get the expression of the socket.
   * @return the expression of the socket or an empty string on failure.
   */
  const std::string expression() {
    _assert_(true);
    if (!open_) return "";
    std::string expr;
    kc::strprintf(&expr, "%s:%d", host_.c_str(), port_);
    return expr;
  }
  /**
   * Reveal the internal HTTP client.
   * @return the internal HTTP client.
   */
  HTTPClient* reveal_core() {
    _assert_(true);
    return &ua_;
  }
 private:
  /** The HTTP client. */
  HTTPClient ua_;
  /** The host name of the server. */
  std::string host_;
  /** The port numer of the server. */
  uint32_t port_;
  /** The timeout. */
  double timeout_;
  /** The open flag. */
  bool open_;
  /** The alive flag. */
  bool alive_;
};


/**
 * RPC server.
 */
class RPCServer {
 public:
  class Logger;
  class Worker;
  class Session;
 private:
  class WorkerAdapter;
 public:
  /**
   * Interface to log internal information and errors.
   */
  class Logger : public HTTPServer::Logger {
   public:
    /**
     * Destructor.
     */
    virtual ~Logger() {
      _assert_(true);
    }
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
     * Process each request of RPC.
     * @param serv the server.
     * @param sess the session with the client.
     * @param name the name of the procecude.
     * @param inmap a string map which contains the input of the procedure.
     * @param outmap a string map to contain the input parameters.
     * @return the return value of the procedure.
     */
    virtual RPCClient::ReturnValue process(RPCServer* serv, Session* sess,
                                           const std::string& name,
                                           const std::map<std::string, std::string>& inmap,
                                           std::map<std::string, std::string>& outmap) = 0;
    /**
     * Process each request of the others.
     * @param serv the server.
     * @param sess the session with the client.
     * @param path the path of the requested resource.
     * @param method the kind of the request methods.
     * @param reqheads a string map which contains the headers of the request.  Header names are
     * converted into lower cases.  The empty key means the request-line.
     * @param reqbody a string which contains the entity body of the request.
     * @param resheads a string map to contain the headers of the response.
     * @param resbody a string to contain the entity body of the response.
     * @param misc a string map which contains miscellaneous information.  "url" means the
     * absolute URL.  "query" means the query string of the URL.
     * @return the status code of the response.  If it is less than 1, internal server error is
     * sent to the client and the connection is closed.
     */
    virtual int32_t process(HTTPServer* serv, HTTPServer::Session* sess,
                            const std::string& path, HTTPClient::Method method,
                            const std::map<std::string, std::string>& reqheads,
                            const std::string& reqbody,
                            std::map<std::string, std::string>& resheads,
                            std::string& resbody,
                            const std::map<std::string, std::string>& misc) {
      _assert_(serv && sess);
      return 501;
    }
    /**
     * Process each binary request.
     * @param serv the server.
     * @param sess the session with the client.
     * @return true to reuse the session, or false to close the session.
     */
    virtual bool process_binary(ThreadedServer* serv, ThreadedServer::Session* sess) {
      _assert_(serv && sess);
      return false;
    }
    /**
     * Process each idle event.
     * @param serv the server.
     */
    virtual void process_idle(RPCServer* serv) {
      _assert_(serv);
    }
    /**
     * Process each timer event.
     * @param serv the server.
     */
    virtual void process_timer(RPCServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the starting event.
     * @param serv the server.
     */
    virtual void process_start(RPCServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the finishing event.
     * @param serv the server.
     */
    virtual void process_finish(RPCServer* serv) {
      _assert_(serv);
    }
  };
  /**
   * Interface to log internal information and errors.
   */
  class Session {
    friend class RPCServer;
   public:
    /**
     * Interface of session local data.
     */
    class Data : public HTTPServer::Session::Data {
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
      return sess_->id();
    }
    /**
     * Get the ID number of the worker thread.
     * @return the ID number of the worker thread.  It is from 0 to less than the number of
     * worker threads.
     */
    uint32_t thread_id() {
      _assert_(true);
      return sess_->thread_id();
    }
    /**
     * Set the session local data.
     * @param data the session local data.  If it is NULL, no data is registered.
     * @note The registered data is destroyed implicitly when the session object is destroyed or
     * this method is called again.
     */
    void set_data(Data* data) {
      _assert_(true);
      sess_->set_data(data);
    }
    /**
     * Get the session local data.
     * @return the session local data, or NULL if no data is registered.
     */
    Data* data() {
      _assert_(true);
      return (Data*)sess_->data();
    }
    /**
     * Get the expression of the socket.
     * @return the expression of the socket or an empty string on failure.
     */
    const std::string expression() {
      _assert_(true);
      return sess_->expression();
    }
   private:
    /**
     * Constructor.
     */
    explicit Session(HTTPServer::Session* sess) : sess_(sess) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    virtual ~Session() {
      _assert_(true);
    }
   private:
    HTTPServer::Session* sess_;
  };
  /**
   * Default constructor.
   */
  explicit RPCServer() : serv_(), worker_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~RPCServer() {
    _assert_(true);
  }
  /**
   * Set the network configurations.
   * @param expr an expression of the address and the port of the server.
   * @param timeout the timeout of each network operation in seconds.  If it is not more than 0,
   * no timeout is specified.
   */
  void set_network(const std::string& expr, double timeout = -1) {
    _assert_(true);
    serv_.set_network(expr, timeout);
  }
  /**
   * Set the logger to process each log message.
   * @param logger the logger object.
   * @param kinds kinds of logged messages by bitwise-or: Logger::DEBUG for debugging,
   * Logger::INFO for normal information, Logger::SYSTEM for system information, and
   * Logger::ERROR for fatal error.
   */
  void set_logger(Logger* logger, uint32_t kinds = Logger::SYSTEM | Logger::ERROR) {
    _assert_(true);
    serv_.set_logger(logger, kinds);
  }
  /**
   * Set the worker to process each request.
   * @param worker the worker object.
   * @param thnum the number of worker threads.
   */
  void set_worker(Worker* worker, size_t thnum = 1) {
    _assert_(true);
    worker_.serv_ = this;
    worker_.worker_ = worker;
    serv_.set_worker(&worker_, thnum);
  }
  /**
   * Start the service.
   * @return true on success, or false on failure.
   * @note This function blocks until the server stops by the RPCServer::stop method.
   */
  bool start() {
    _assert_(true);
    return serv_.start();
  }
  /**
   * Stop the service.
   * @return true on success, or false on failure.
   */
  bool stop() {
    _assert_(true);
    return serv_.stop();
  }
  /**
   * Finish the service.
   * @return true on success, or false on failure.
   */
  bool finish() {
    _assert_(true);
    return serv_.finish();
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
    va_list ap;
    va_start(ap, format);
    serv_.log_v(kind, format, ap);
    va_end(ap);
  }
  /**
   * Log a message.
   * @note Equal to the original Cursor::set_value method except that the last parameters is
   * va_list.
   */
  void log_v(Logger::Kind kind, const char* format, va_list ap) {
    _assert_(format);
    serv_.log_v(kind, format, ap);
  }
  /**
   * Reveal the internal HTTP server.
   * @return the internal HTTP server.
   */
  HTTPServer* reveal_core() {
    _assert_(true);
    return &serv_;
  }
 private:
  /**
   * Adapter for the worker.
   */
  class WorkerAdapter : public HTTPServer::Worker {
    friend class RPCServer;
   public:
    WorkerAdapter() : serv_(NULL), worker_(NULL) {
      _assert_(true);
    }
   private:
    int32_t process(HTTPServer* serv, HTTPServer::Session* sess,
                    const std::string& path, HTTPClient::Method method,
                    const std::map<std::string, std::string>& reqheads,
                    const std::string& reqbody,
                    std::map<std::string, std::string>& resheads,
                    std::string& resbody,
                    const std::map<std::string, std::string>& misc) {
      const char* name = path.c_str();
      if (!kc::strfwm(name, KTRPCPATHPREFIX))
        return worker_->process(serv, sess, path, method, reqheads, reqbody,
                                resheads, resbody, misc);
      name += sizeof(KTRPCPATHPREFIX) - 1;
      size_t zsiz;
      char* zbuf = kc::urldecode(name, &zsiz);
      std::string rawname(zbuf, zsiz);
      delete[] zbuf;
      std::map<std::string, std::string> inmap;
      const char* rp = strmapget(misc, "query");
      if (rp) wwwformtomap(rp, &inmap);
      rp = strmapget(reqheads, "content-type");
      if (rp) {
        if (kc::strifwm(rp, KTRPCFORMMTYPE)) {
          wwwformtomap(reqbody.c_str(), &inmap);
        } else if (kc::strifwm(rp, KTRPCTSVMTYPE)) {
          rp += sizeof(KTRPCTSVMTYPE) - 1;
          int32_t enc = 0;
          while (*rp != '\0') {
            while (*rp == ' ' || *rp == ';') {
              rp++;
            }
            if (kc::strifwm(rp, KTRPCTSVMATTR) && rp[sizeof(KTRPCTSVMATTR)-1] == '=') {
              rp += sizeof(KTRPCTSVMATTR);
              if (*rp == '"') rp++;
              switch (*rp) {
                case 'b': case 'B': enc = 'B'; break;
                case 'q': case 'Q': enc = 'Q'; break;
                case 'u': case 'U': enc = 'U'; break;
              }
            }
            while (*rp != '\0' && *rp != ' ' && *rp != ';') {
              rp++;
            }
          }
          tsvtomap(reqbody, &inmap);
          if (enc != 0) tsvmapdecode(&inmap, enc);
        }
      }
      std::map<std::string, std::string> outmap;
      Session mysess(sess);
      RPCClient::ReturnValue rv = worker_->process(serv_, &mysess, rawname, inmap, outmap);
      int32_t code = -1;
      switch (rv) {
        case RPCClient::RVSUCCESS: code = 200; break;
        case RPCClient::RVENOIMPL: code = 501; break;
        case RPCClient::RVEINVALID: code = 400; break;
        case RPCClient::RVELOGIC: code = 450; break;
        case RPCClient::RVETIMEOUT: code = 503; break;
        default: code = 500; break;
      }
      int32_t enc = checkmapenc(outmap);
      std::string outtype = KTRPCTSVMTYPE;
      switch (enc) {
        case 'B': kc::strprintf(&outtype, "; %s=B", KTRPCTSVMATTR); break;
        case 'Q': kc::strprintf(&outtype, "; %s=Q", KTRPCTSVMATTR); break;
        case 'U': kc::strprintf(&outtype, "; %s=U", KTRPCTSVMATTR); break;
      }
      resheads["content-type"] = outtype;
      if (enc != 0) tsvmapencode(&outmap, enc);
      maptotsv(outmap, &resbody);
      return code;
    }
    bool process_binary(ThreadedServer* serv, ThreadedServer::Session* sess) {
      return worker_->process_binary(serv, sess);
    }
    void process_idle(HTTPServer* serv) {
      worker_->process_idle(serv_);
    }
    void process_timer(HTTPServer* serv) {
      worker_->process_timer(serv_);
    }
    void process_start(HTTPServer* serv) {
      worker_->process_start(serv_);
    }
    void process_finish(HTTPServer* serv) {
      worker_->process_finish(serv_);
    }
    RPCServer* serv_;
    RPCServer::Worker* worker_;
  };
  /** Dummy constructor to forbid the use. */
  RPCServer(const RPCServer&);
  /** Dummy Operator to forbid the use. */
  RPCServer& operator =(const RPCServer&);
  /** The internal server. */
  HTTPServer serv_;
  /** The adapter for worker. */
  WorkerAdapter worker_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
