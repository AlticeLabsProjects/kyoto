/*************************************************************************************************
 * HTTP utilities
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


#ifndef _KTHTTP_H                        // duplication check
#define _KTHTTP_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>

namespace kyototycoon {                  // common namespace


/**
 * URL accessor.
 */
class URL {
 public:
  /**
   * Default constructor.
   */
  explicit URL() :
      scheme_(""), host_(""), port_(0), authority_(""),
      path_(""), query_(""), fragment_("") {
    _assert_(true);
  }
  /**
   * Constructor.
   * @param expr the string expression of the URL.
   */
  explicit URL(const std::string& expr) :
      scheme_(""), host_(""), port_(0), authority_(""),
      path_(""), query_(""), fragment_("") {
    _assert_(true);
    parse_expression(expr);
  }
  /**
   * Copy constructor.
   * @param src the source object.
   */
  explicit URL(const URL& src) :
      scheme_(src.scheme_), host_(src.host_), port_(src.port_), authority_(src.authority_),
      path_(src.path_), query_(src.query_), fragment_(src.fragment_) {
    _assert_(true);
  }
  /**
   * Destructor
   */
  ~URL() {
    _assert_(true);
  }
  /**
   * Set the string expression of the URL.
   */
  void set_expression(const std::string& expr) {
    _assert_(true);
    parse_expression(expr);
  }
  /**
   * Set the scheme.
   */
  void set_scheme(const std::string& scheme) {
    _assert_(true);
    scheme_ = scheme;
  }
  /**
   * Set the host name.
   */
  void set_host(const std::string& host) {
    _assert_(true);
    host_ = host;
  }
  /**
   * Set the port number.
   */
  void set_port(int32_t port) {
    _assert_(true);
    port_ = port;
  }
  /**
   * Set the authority information.
   */
  void set_authority(const std::string& authority) {
    _assert_(true);
    authority_ = authority;
  }
  /**
   * Set the path.
   */
  void set_path(const std::string& path) {
    _assert_(true);
    path_ = path;
  }
  /**
   * Set the query string.
   */
  void set_query(const std::string& query) {
    _assert_(true);
    query_ = query;
  }
  /**
   * Set the fragment string.
   */
  void set_fragment(const std::string& fragment) {
    _assert_(true);
    fragment_ = fragment;
  }
  /**
   * Get the string expression of the URL.
   * @return the string expression of the URL.
   */
  std::string expression() {
    _assert_(true);
    std::string expr;
    if (!scheme_.empty()) {
      kc::strprintf(&expr, "%s://", scheme_.c_str());
      if (!authority_.empty()) kc::strprintf(&expr, "%s@", authority_.c_str());
      if (!host_.empty()) {
        kc::strprintf(&expr, "%s", host_.c_str());
        if (port_ > 0 && port_ != default_port(scheme_)) kc::strprintf(&expr, ":%d", port_);
      }
    }
    kc::strprintf(&expr, "%s", path_.c_str());
    if (!query_.empty()) kc::strprintf(&expr, "?%s", query_.c_str());
    if (!fragment_.empty()) kc::strprintf(&expr, "#%s", fragment_.c_str());
    return expr;
  }
  /**
   * Get the path and the query string for HTTP request.
   * @return the path and the query string for HTTP request.
   */
  std::string path_query() {
    _assert_(true);
    return path_ + (query_.empty() ? "" : "?") + query_;
  }
  /**
   * Get the scheme.
   * @return the scheme.
   */
  std::string scheme() {
    _assert_(true);
    return scheme_;
  }
  /**
   * Get the host name.
   * @return the host name.
   */
  std::string host() {
    _assert_(true);
    return host_;
  }
  /**
   * Get the port number.
   * @return the port number.
   */
  int32_t port() {
    _assert_(true);
    return port_;
  }
  /**
   * Get the authority information.
   * @return the authority information.
   */
  std::string authority() {
    _assert_(true);
    return authority_;
  }
  /**
   * Get the path.
   * @return the path.
   */
  std::string path() {
    _assert_(true);
    return path_;
  }
  /**
   * Get the query string.
   * @return the query string.
   */
  std::string query() {
    _assert_(true);
    return query_;
  }
  /**
   * Get the fragment string.
   * @return the fragment string.
   */
  std::string fragment() {
    _assert_(true);
    return fragment_;
  }
  /**
   * Assignment operator from the self type.
   * @param right the right operand.
   * @return the reference to itself.
   */
  URL& operator =(const URL& right) {
    _assert_(true);
    if (&right == this) return *this;
    scheme_ = right.scheme_;
    host_ = right.host_;
    port_ = right.port_;
    authority_ = right.authority_;
    path_ = right.path_;
    query_ = right.query_;
    fragment_ = right.fragment_;
    return *this;
  }
 private:
  /**
   * Parse a string expression.
   * @param expr the string expression.
   */
  void parse_expression(const std::string& expr) {
    scheme_ = "";
    host_ = "";
    port_ = 0;
    authority_ = "";
    path_ = "";
    query_ = "";
    fragment_ = "";
    char* trim = kc::strdup(expr.c_str());
    kc::strtrim(trim);
    char* rp = trim;
    char* norm = new char[std::strlen(trim)*3+1];
    char* wp = norm;
    while (*rp != '\0') {
      if (*rp > 0x20 && *rp < 0x7f) {
        *(wp++) = *rp;
      } else {
        *(wp++) = '%';
        int32_t num = *(unsigned char*)rp >> 4;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
        num = *rp & 0x0f;
        if (num < 10) {
          *(wp++) = '0' + num;
        } else {
          *(wp++) = 'a' + num - 10;
        }
      }
      rp++;
    }
    *wp = '\0';
    rp = norm;
    if (kc::strifwm(rp, "http://")) {
      scheme_ = "http";
      rp += 7;
    } else if (kc::strifwm(rp, "https://")) {
      scheme_ = "https";
      rp += 8;
    } else if (kc::strifwm(rp, "ftp://")) {
      scheme_ = "ftp";
      rp += 6;
    } else if (kc::strifwm(rp, "sftp://")) {
      scheme_ = "sftp";
      rp += 7;
    } else if (kc::strifwm(rp, "ftps://")) {
      scheme_ = "ftps";
      rp += 7;
    } else if (kc::strifwm(rp, "tftp://")) {
      scheme_ = "tftp";
      rp += 7;
    } else if (kc::strifwm(rp, "ldap://")) {
      scheme_ = "ldap";
      rp += 7;
    } else if (kc::strifwm(rp, "ldaps://")) {
      scheme_ = "ldaps";
      rp += 8;
    } else if (kc::strifwm(rp, "file://")) {
      scheme_ = "file";
      rp += 7;
    }
    char* ep;
    if ((ep = std::strchr(rp, '#')) != NULL) {
      fragment_ = ep + 1;
      *ep = '\0';
    }
    if ((ep = std::strchr(rp, '?')) != NULL) {
      query_ = ep + 1;
      *ep = '\0';
    }
    if (!scheme_.empty()) {
      if ((ep = std::strchr(rp, '/')) != NULL) {
        path_ = ep;
        *ep = '\0';
      } else {
        path_ = "/";
      }
      if ((ep = std::strchr(rp, '@')) != NULL) {
        *ep = '\0';
        if (rp[0] != '\0') authority_ = rp;
        rp = ep + 1;
      }
      if ((ep = std::strchr(rp, ':')) != NULL) {
        if (ep[1] != '\0') port_ = kc::atoi(ep + 1);
        *ep = '\0';
      }
      if (rp[0] != '\0') host_ = rp;
      if (port_ < 1) port_ = default_port(scheme_);
    } else {
      path_ = rp;
    }
    delete[] norm;
    delete[] trim;
  }
  /**
   * Get the default port of a scheme.
   * @param scheme the scheme.
   */
  int32_t default_port(const std::string& scheme) {
    if (scheme == "http") return 80;
    if (scheme == "https") return 443;
    if (scheme == "ftp") return 21;
    if (scheme == "sftp") return 22;
    if (scheme == "ftps") return 990;
    if (scheme == "tftp") return 69;
    if (scheme == "ldap") return 389;
    if (scheme == "ldaps") return 636;
    return 0;
  }
  /** The scheme. */
  std::string scheme_;
  /** The host name. */
  std::string host_;
  /** The port number. */
  int32_t port_;
  /** The autority information. */
  std::string authority_;
  /** The path. */
  std::string path_;
  /** The query string. */
  std::string query_;
  /** The fragment string. */
  std::string fragment_;
};


/**
 * HTTP client.
 * @note Although all methods of this class are thread-safe, its instance does not have mutual
 * exclusion mechanism.  So, multiple threads must not share the same instance and they must use
 * their own respective instances.
 */
class HTTPClient {
 public:
  /** The size for a line buffer. */
  static const int32_t LINEBUFSIZ = 8192;
  /** The maximum size of received data. */
  static const int32_t RECVMAXSIZ = 1 << 28;
  /**
   * Kinds of HTTP request methods.
   */
  enum Method {
    MGET,                                ///< GET
    MHEAD,                               ///< HEAD
    MPOST,                               ///< POST
    MPUT,                                ///< PUT
    MDELETE,                             ///< DELETE
    MUNKNOWN                             ///< unknown
  };
  /**
   * Default constructor.
   */
  explicit HTTPClient() : sock_(), host_(""), port_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~HTTPClient() {
    _assert_(true);
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
  bool open(const std::string& host = "", int32_t port = 80, double timeout = -1) {
    _assert_(true);
    const std::string& thost = host.empty() ? "localhost" : host;
    const std::string& addr = Socket::get_host_address(thost);
    if (addr.empty() || port < 1) return false;
    std::string expr;
    kc::strprintf(&expr, "%s:%d", addr.c_str(), port);
    if (timeout > 0) sock_.set_timeout(timeout);
    if (!sock_.open(expr)) return false;
    host_ = host;
    port_ = port;
    return true;
  }
  /**
   * Close the connection.
   * @param grace true for graceful shutdown, or false for immediate disconnection.
   * @return true on success, or false on failure.
   */
  bool close(bool grace = true) {
    _assert_(true);
    return sock_.close(grace);
  }
  /**
   * Fetch a resource.
   * @param pathquery the path and the query string of the resource.
   * @param method the kind of the request methods.
   * @param resbody a string to contain the entity body of the response.  If it is NULL, it is
   * ignored.
   * @param resheads a string map to contain the headers of the response.  If it is NULL, it is
   * ignored.  Header names are converted into lower cases.  The empty key means the
   * request-line.
   * @param reqbody a string which contains the entity body of the request.  If it is NULL, it
   * is ignored.
   * @param reqheads a string map which contains the headers of the request.  If it is NULL, it
   * is ignored.
   * @return the status code of the response, or -1 on failure.
   */
  int32_t fetch(const std::string& pathquery, Method method = MGET,
                std::string* resbody = NULL,
                std::map<std::string, std::string>* resheads = NULL,
                const std::string* reqbody = NULL,
                const std::map<std::string, std::string>* reqheads = NULL) {
    _assert_(true);
    if (resbody) resbody->clear();
    if (resheads) resheads->clear();
    if (pathquery.empty() || pathquery[0] != '/') {
      if (resbody) resbody->append("[invalid URL expression]");
      return -1;
    }
    std::string request;
    const char* mstr;
    switch (method) {
      default: mstr = "GET"; break;
      case MHEAD: mstr = "HEAD"; break;
      case MPOST: mstr = "POST"; break;
      case MPUT: mstr = "PUT"; break;
      case MDELETE: mstr = "DELETE"; break;
    }
    kc::strprintf(&request, "%s %s HTTP/1.1\r\n", mstr, pathquery.c_str());
    kc::strprintf(&request, "Host: %s", host_.c_str());
    if (port_ != 80) kc::strprintf(&request, ":%d", port_);
    kc::strprintf(&request, "\r\n");
    if (reqbody) kc::strprintf(&request, "Content-Length: %lld\r\n", (long long)reqbody->size());
    if (reqheads) {
      std::map<std::string, std::string>::const_iterator it = reqheads->begin();
      std::map<std::string, std::string>::const_iterator itend = reqheads->end();
      while (it != itend) {
        char* name = kc::strdup(it->first.c_str());
        char* value = kc::strdup(it->second.c_str());
        kc::strnrmspc(name);
        kc::strtolower(name);
        kc::strnrmspc(value);
        if (*name != '\0' && !std::strchr(name, ':') && !std::strchr(name, ' ')) {
          strcapitalize(name);
          kc::strprintf(&request, "%s: %s\r\n", name, value);
        }
        delete[] value;
        delete[] name;
        ++it;
      }
    }
    kc::strprintf(&request, "\r\n");
    if (reqbody) request.append(*reqbody);
    if (!sock_.send(request)) {
      if (resbody) resbody->append("[sending data failed]");
      return -1;
    }
    char line[LINEBUFSIZ];
    if (!sock_.receive_line(line, sizeof(line))) {
      if (resbody) resbody->append("[receiving data failed]");
      return -1;
    }
    if (!kc::strfwm(line, "HTTP/1.1 ") && !kc::strfwm(line, "HTTP/1.0 ")) {
      if (resbody) resbody->append("[received data was invalid]");
      return -1;
    }
    const char* rp = line + 9;
    int32_t code = kc::atoi(rp);
    if (code < 1) {
      if (resbody) resbody->append("[invalid status code]");
      return -1;
    }
    if (resheads) (*resheads)[""] = line;
    int64_t clen = -1;
    bool chunked = false;
    while (true) {
      if (!sock_.receive_line(line, sizeof(line))) {
        resbody->append("[receiving data failed]");
        return -1;
      }
      if (*line == '\0') break;
      char* pv = std::strchr(line, ':');
      if (pv) {
        *(pv++) = '\0';
        kc::strnrmspc(line);
        kc::strtolower(line);
        if (*line != '\0') {
          while (*pv == ' ') {
            pv++;
          }
          if (!std::strcmp(line, "content-length")) {
            clen = kc::atoi(pv);
          } else if (!std::strcmp(line, "transfer-encoding")) {
            if (!kc::stricmp(pv, "chunked")) chunked = true;
          }
          if (resheads) (*resheads)[line] = pv;
        }
      }
    }
    if (method != MHEAD && code != 304) {
      if (clen >= 0) {
        if (clen > RECVMAXSIZ) {
          if (resbody) resbody->append("[too large response]");
          return -1;
        }
        char* body = new char[clen];
        if (!sock_.receive(body, clen)) {
          if (resbody) resbody->append("[receiving data failed]");
          delete[] body;
          return -1;
        }
        if (resbody) resbody->append(body, clen);
        delete[] body;
      } else if (chunked) {
        int64_t asiz = LINEBUFSIZ;
        char* body = (char*)kc::xmalloc(asiz);
        int64_t bsiz = 0;
        while (true) {
          if (!sock_.receive_line(line, sizeof(line))) {
            if (resbody) resbody->append("[receiving data failed]");
            kc::xfree(body);
            return -1;
          }
          if (*line == '\0') break;
          int64_t csiz = kc::atoih(line);
          if (bsiz + csiz > RECVMAXSIZ) {
            if (resbody) resbody->append("[too large response]");
            kc::xfree(body);
            return -1;
          }
          if (bsiz + csiz > asiz) {
            asiz = bsiz * 2 + csiz;
            body = (char*)kc::xrealloc(body, asiz);
          }
          if (csiz > 0 && !sock_.receive(body + bsiz, csiz)) {
            if (resbody) resbody->append("[receiving data failed]");
            kc::xfree(body);
            return -1;
          }
          if (sock_.receive_byte() != '\r' || sock_.receive_byte() != '\n') {
            if (resbody) resbody->append("[invalid chunk]");
            kc::xfree(body);
            return -1;
          }
          if (csiz < 1) break;
          bsiz += csiz;
        }
        if (resbody) resbody->append(body, bsiz);
        kc::xfree(body);
      } else {
        int64_t asiz = LINEBUFSIZ;
        char* body = (char*)kc::xmalloc(asiz);
        int64_t bsiz = 0;
        while (true) {
          int32_t c = sock_.receive_byte();
          if (c < 0) break;
          if (bsiz > RECVMAXSIZ - 1) {
            if (resbody) resbody->append("[too large response]");
            kc::xfree(body);
            return -1;
          }
          if (bsiz + 1 > asiz) {
            asiz = bsiz * 2;
            body = (char*)kc::xrealloc(body, asiz);
          }
          body[bsiz++] = c;
        }
        if (resbody) resbody->append(body, bsiz);
        kc::xfree(body);
      }
    }
    return code;
  }
  /**
   * Reveal the internal TCP socket.
   * @return the internal TCP socket.
   */
  Socket* reveal_core() {
    _assert_(true);
    return &sock_;
  }
  /**
   * Fetch a resource at once
   * @param url the URL of the resource.
   * @param method the kind of the request methods.
   * @param resbody a string to contain the entity body of the response.  If it is NULL, it is
   * ignored.
   * @param resheads a string map to contain the headers of the response.  If it is NULL, it is
   * ignored.  Header names are converted into lower cases.  The empty key means the
   * request-line.
   * @param reqbody a string which contains the entity body of the request.  If it is NULL, it
   * is ignored.
   * @param reqheads a string map which contains the headers of the request.  If it is NULL, it
   * is ignored.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return the status code of the response, or -1 on failure.
   */
  static int32_t fetch_once(const std::string& url, Method method = MGET,
                            std::string* resbody = NULL,
                            std::map<std::string, std::string>* resheads = NULL,
                            const std::string* reqbody = NULL,
                            const std::map<std::string, std::string>* reqheads = NULL,
                            double timeout = -1) {
    _assert_(true);
    URL uo(url);
    const std::string& scheme = uo.scheme();
    const std::string& host = uo.host();
    uint32_t port = uo.port();
    if (scheme != "http" || host.empty() || port < 1) {
      if (resbody) resbody->append("[invalid URL expression]");
      return -1;
    }
    HTTPClient ua;
    if (!ua.open(host, port, timeout)) {
      if (resbody) resbody->append("[connection refused]");
      return -1;
    }
    std::map<std::string, std::string> rhtmp;
    if (reqheads) rhtmp.insert(reqheads->begin(), reqheads->end());
    rhtmp["connection"] = "close";
    int32_t code = ua.fetch(uo.path_query(), method, resbody, resheads, reqbody, &rhtmp);
    if (!ua.close()) {
      if (resbody) {
        resbody->clear();
        resbody->append("[close failed]");
      }
      return -1;
    }
    return code;
  }
 private:
  /** Dummy constructor to forbid the use. */
  HTTPClient(const HTTPClient&);
  /** Dummy Operator to forbid the use. */
  HTTPClient& operator =(const HTTPClient&);
  /** The socket of connection. */
  Socket sock_;
  /** The host name of the server. */
  std::string host_;
  /** The port number of the server. */
  int32_t port_;
};


/**
 * HTTP server.
 */
class HTTPServer {
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
  class Logger : public ThreadedServer::Logger {
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
     * Process each request.
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
    virtual int32_t process(HTTPServer* serv, Session* sess,
                            const std::string& path, HTTPClient::Method method,
                            const std::map<std::string, std::string>& reqheads,
                            const std::string& reqbody,
                            std::map<std::string, std::string>& resheads,
                            std::string& resbody,
                            const std::map<std::string, std::string>& misc) = 0;
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
    virtual void process_idle(HTTPServer* serv) {
      _assert_(serv);
    }
    /**
     * Process each timer event.
     * @param serv the server.
     */
    virtual void process_timer(HTTPServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the starting event.
     * @param serv the server.
     */
    virtual void process_start(HTTPServer* serv) {
      _assert_(serv);
    }
    /**
     * Process the finishing event.
     * @param serv the server.
     */
    virtual void process_finish(HTTPServer* serv) {
      _assert_(serv);
    }
  };
  /**
   * Interface to access each session data.
   */
  class Session {
    friend class HTTPServer;
   public:
    /**
     * Interface of session local data.
     */
    class Data : public ThreadedServer::Session::Data {
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
    explicit Session(ThreadedServer::Session* sess) : sess_(sess) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    virtual ~Session() {
      _assert_(true);
    }
   private:
    ThreadedServer::Session* sess_;
  };
  /**
   * Default constructor.
   */
  explicit HTTPServer() : serv_(), name_(), worker_() {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~HTTPServer() {
    _assert_(true);
  }
  /**
   * Set the network configurations.
   * @param expr an expression of the address and the port of the server.
   * @param timeout the timeout of each network operation in seconds.  If it is not more than 0,
   * no timeout is specified.
   * @param name the name of the server.  If it is an empty string, the local host is specified.
   */
  void set_network(const std::string& expr, double timeout = -1, const std::string& name = "") {
    _assert_(true);
    if (timeout > 0) serv_.set_network(expr, timeout);
    if (name.empty()) {
      name_ = "localhost";
      const char* rp = std::strrchr(expr.c_str(), ':');
      if (rp) {
        rp++;
        int32_t port = kc::atoi(rp);
        if (port > 0 && port != 80 && !std::strchr(rp, '[') && !std::strchr(rp, ']') &&
            !std::strchr(rp, '/')) kc::strprintf(&name_, ":%d", port);
      }
    } else {
      name_ = name;
    }
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
   * @note This function blocks until the server stops by the HTTPServer::stop method.
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
   * Reveal the internal TCP server.
   * @return the internal TCP server.
   */
  ThreadedServer* reveal_core() {
    _assert_(true);
    return &serv_;
  }
  /**
   * Get the name of a status code.
   * @return the name of a status code.
   */
  static const char* status_name(int32_t code) {
    _assert_(true);
    switch (code) {
      case 100: return "Continue";
      case 101: return "Switching Protocols";
      case 102: return "Processing";
      case 200: return "OK";
      case 201: return "Created";
      case 202: return "Accepted";
      case 203: return "Non-Authoritative Information";
      case 204: return "No Content";
      case 205: return "Reset Content";
      case 206: return "Partial Content";
      case 207: return "Multi-Status";
      case 300: return "Multiple Choices";
      case 301: return "Moved Permanently";
      case 302: return "Found";
      case 303: return "See Other";
      case 304: return "Not Modified";
      case 305: return "Use Proxy";
      case 307: return "Temporary Redirect";
      case 400: return "Bad Request";
      case 401: return "Unauthorized";
      case 402: return "Payment Required";
      case 403: return "Forbidden";
      case 404: return "Not Found";
      case 405: return "Method Not Allowed";
      case 406: return "Not Acceptable";
      case 407: return "Proxy Authentication Required";
      case 408: return "Request Timeout";
      case 409: return "Conflict";
      case 410: return "Gone";
      case 411: return "Length Required";
      case 412: return "Precondition Failed";
      case 413: return "Request Entity Too Large";
      case 414: return "Request-URI Too Long";
      case 415: return "Unsupported Media Type";
      case 416: return "Requested Range Not Satisfiable";
      case 417: return "Expectation Failed";
      case 422: return "Unprocessable Entity";
      case 423: return "Locked";
      case 424: return "Failed Dependency";
      case 426: return "Upgrade Required";
      case 450: return "Logical Inconsistency";
      case 500: return "Internal Server Error";
      case 501: return "Not Implemented";
      case 502: return "Bad Gateway";
      case 503: return "Service Unavailable";
      case 504: return "Gateway Timeout";
      case 505: return "HTTP Version Not Supported";
      case 506: return "Variant Also Negotiates";
      case 507: return "Insufficient Storage";
      case 509: return "Bandwidth Limit Exceeded";
      case 510: return "Not Extended";
    }
    if (code < 100) return "Unknown Status";
    if (code < 200) return "Unknown Informational Status";
    if (code < 300) return "Unknown Success Status";
    if (code < 400) return "Unknown Redirection Status";
    if (code < 500) return "Unknown Client Error Status";
    if (code < 600) return "Unknown Server Error Status";
    return "Unknown Status";
  }
  /**
   * Guess the media type of a URL.
   * @param url the URL.
   * @return the media type string, or NULL on failure.
   */
  static const char* media_type(const std::string& url) {
    static const char* types[] = {
      "txt", "text/plain", "text", "text/plain", "asc", "text/plain",
      "c", "text/plain", "h", "text/plain", "s", "text/plain",
      "cc", "text/plain", "cxx", "text/plain", "cpp", "text/plain",
      "html", "text/html", "htm", "text/html",
      "xml", "application/xml",
      "xhtml", "application/xml+xhtml",
      "tar", "application/x-tar",
      "gz", "application/x-gzip",
      "bz2", "application/x-bzip2",
      "zip", "application/zip",
      "xz", "application/octet-stream", "lzma", "application/octet-stream",
      "lzo", "application/octet-stream", "lzh", "application/octet-stream",
      "o", "application/octet-stream", "so", "application/octet-stream",
      "a", "application/octet-stream", "exe", "application/octet-stream",
      "pdf", "application/pdf",
      "ps", "application/postscript",
      "doc", "application/msword",
      "xls", "application/vnd.ms-excel",
      "ppt", "application/ms-powerpoint",
      "swf", "application/x-shockwave-flash",
      "png", "image/png",
      "jpg", "image/jpeg", "jpeg", "image/jpeg",
      "gif", "image/gif",
      "bmp", "image/bmp",
      "tif", "image/tiff", "tiff", "image/tiff",
      "svg", "image/xml+svg",
      "au", "audio/basic", "snd", "audio/basic",
      "mid", "audio/midi", "midi", "audio/midi",
      "mp3", "audio/mpeg", "mp2", "audio/mpeg",
      "wav", "audio/x-wav",
      "mpg", "video/mpeg", "mpeg", "video/mpeg",
      "mp4", "video/mp4", "mpg4", "video/mp4",
      "mov", "video/quicktime", "qt", "video/quicktime",
      NULL
    };
    const char* rp = url.c_str();
    const char* pv = std::strrchr(rp, '/');
    if (pv) rp = pv + 1;
    pv = std::strrchr(rp, '.');
    if (pv) {
      rp = pv + 1;
      for (int32_t i = 0; types[i]; i += 2) {
        if (!kc::stricmp(rp, types[i])) return types[i+1];
      }
    }
    return NULL;
  }
  /**
   * Convert the path element of a URL into the local path.
   * @param path the path element of a URL.
   * @return the local path.
   */
  static std::string localize_path(const std::string& path) {
    _assert_(true);
    const std::string& npath = path;
    std::vector<std::string> elems, nelems;
    kc::strsplit(npath, '/', &elems);
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    while (it != itend) {
      if (*it == "..") {
        if (!nelems.empty()) nelems.pop_back();
      } else if (!it->empty() && *it != ".") {
        size_t zsiz;
        char* zbuf = kc::urldecode(it->c_str(), &zsiz);
        if (zsiz > 0 && zbuf[0] != '\0' && !std::strchr(zbuf, kc::File::PATHCHR) &&
            std::strcmp(zbuf, kc::File::CDIRSTR) && std::strcmp(zbuf, kc::File::PDIRSTR))
          nelems.push_back(zbuf);
        delete[] zbuf;
      }
      ++it;
    }
    std::string rpath;
    it = nelems.begin();
    itend = nelems.end();
    while (it != itend) {
      if (!rpath.empty()) rpath.append(kc::File::PATHSTR);
      rpath.append(*it);
      ++it;
    }
    return rpath;
  }
 private:
  /**
   * Adapter for the worker.
   */
  class WorkerAdapter : public ThreadedServer::Worker {
    friend class HTTPServer;
   public:
    WorkerAdapter() : serv_(NULL), worker_(NULL) {
      _assert_(true);
    }
   private:
    bool process(ThreadedServer* serv, ThreadedServer::Session* sess) {
      _assert_(true);
      int32_t magic = sess->receive_byte();
      if (magic < 0) return false;
      sess->undo_receive_byte(magic);
      if (magic == 0 || magic >= 0x80) return worker_->process_binary(serv, sess);
      char line[HTTPClient::LINEBUFSIZ];
      if (!sess->receive_line(&line, sizeof(line))) return false;
      std::map<std::string, std::string> misc;
      std::map<std::string, std::string> reqheads;
      reqheads[""] = line;
      char* pv = std::strchr(line, ' ');
      if (!pv) return false;
      *(pv++) = '\0';
      char* tpath = pv;
      pv = std::strchr(tpath, ' ');
      if (!pv) return false;
      *(pv++) = '\0';
      std::string url;
      kc::strprintf(&url, "http://%s%s", serv_->name_.c_str(), tpath);
      misc["url"] = url;
      char* query = std::strchr(tpath, '?');
      if (query) {
        *(query++) = '\0';
        misc["query"] = query;
      }
      if (*tpath == '\0') return false;
      std::string path = tpath;
      int32_t htver;
      if (!std::strcmp(pv, "HTTP/1.0")) {
        htver = 0;
      } else if (!std::strcmp(pv, "HTTP/1.1")) {
        htver = 1;
      } else {
        return false;
      }
      HTTPClient::Method method;
      if (!std::strcmp(line, "GET")) {
        method = HTTPClient::MGET;
      } else if (!std::strcmp(line, "HEAD")) {
        method = HTTPClient::MHEAD;
      } else if (!std::strcmp(line, "POST")) {
        method = HTTPClient::MPOST;
      } else if (!std::strcmp(line, "PUT")) {
        method = HTTPClient::MPUT;
      } else if (!std::strcmp(line, "DELETE")) {
        method = HTTPClient::MDELETE;
      } else {
        method = HTTPClient::MUNKNOWN;
      }
      bool keep = htver >= 1;
      int64_t clen = -1;
      bool chunked = false;
      while (true) {
        if (!sess->receive_line(&line, sizeof(line))) return false;
        if (*line == '\0') break;
        pv = std::strchr(line, ':');
        if (pv) {
          *(pv++) = '\0';
          kc::strnrmspc(line);
          kc::strtolower(line);
          if (*line != '\0') {
            while (*pv == ' ') {
              pv++;
            }
            if (!std::strcmp(line, "connection")) {
              if (!kc::stricmp(pv, "close")) {
                keep = false;
              } else if (!kc::stricmp(pv, "keep-alive")) {
                keep = true;
              }
            } else if (!std::strcmp(line, "content-length")) {
              clen = kc::atoi(pv);
            } else if (!std::strcmp(line, "transfer-encoding")) {
              if (!kc::stricmp(pv, "chunked")) chunked = true;
            }
            reqheads[line] = pv;
          }
        }
      }
      std::string reqbody;
      if (method == HTTPClient::MPOST || method == HTTPClient::MPUT ||
          method == HTTPClient::MUNKNOWN) {
        if (clen >= 0) {
          if (clen > HTTPClient::RECVMAXSIZ) {
            send_error(sess, 413, "request entity too large");
            return false;
          }
          char* body = new char[clen];
          if (!sess->receive(body, clen)) {
            send_error(sess, 400, "receiving data failed");
            delete[] body;
            return false;
          }
          reqbody.append(body, clen);
          delete[] body;
        } else if (chunked) {
          int64_t asiz = HTTPClient::LINEBUFSIZ;
          char* body = (char*)kc::xmalloc(asiz);
          int64_t bsiz = 0;
          while (true) {
            if (!sess->receive_line(line, sizeof(line))) {
              send_error(sess, 400, "receiving data failed");
              kc::xfree(body);
              return false;
            }
            if (*line == '\0') break;
            int64_t csiz = kc::atoih(line);
            if (bsiz + csiz > HTTPClient::RECVMAXSIZ) {
              send_error(sess, 413, "request entity too large");
              kc::xfree(body);
              return false;
            }
            if (bsiz + csiz > asiz) {
              asiz = bsiz * 2 + csiz;
              body = (char*)kc::xrealloc(body, asiz);
            }
            if (csiz > 0 && !sess->receive(body + bsiz, csiz)) {
              send_error(sess, 400, "receiving data failed");
              kc::xfree(body);
              return false;
            }
            if (sess->receive_byte() != '\r' || sess->receive_byte() != '\n') {
              send_error(sess, 400, "invalid chunk");
              kc::xfree(body);
              return false;
            }
            if (csiz < 1) break;
            bsiz += csiz;
          }
          reqbody.append(body, bsiz);
          kc::xfree(body);
        }
      }
      Session mysess(sess);
      std::string resbody;
      std::map<std::string, std::string> resheads;
      int32_t code = worker_->process(serv_, &mysess, path, method, reqheads, reqbody,
                                      resheads, resbody, misc);
      serv->log(Logger::INFO, "(%s): %s: %d",
                sess->expression().c_str(), reqheads[""].c_str(), code);
      if (code > 0) {
        if (!send_result(sess, code, keep, path, method, reqheads, reqbody,
                         resheads, resbody, misc)) keep = false;
      } else {
        send_error(sess, 500, "logic error");
        keep = false;
      }
      return keep;
    }
    void process_idle(ThreadedServer* serv) {
      worker_->process_idle(serv_);
    }
    void process_timer(ThreadedServer* serv) {
      worker_->process_timer(serv_);
    }
    void process_start(ThreadedServer* serv) {
      worker_->process_start(serv_);
    }
    void process_finish(ThreadedServer* serv) {
      worker_->process_finish(serv_);
    }
    void send_error(ThreadedServer::Session* sess, int32_t code, const char* msg) {
      _assert_(sess && code > 0);
      const char* name = status_name(code);
      std::string str;
      kc::strprintf(&str, "%d %s (%s)\n", code, name, msg);
      std::string data;
      kc::strprintf(&data, "HTTP/1.1 %d %s\r\n", code, name);
      append_server_headers(&data);
      kc::strprintf(&data, "Connection: close\r\n");
      kc::strprintf(&data, "Content-Length: %d\r\n", (int)str.size());
      kc::strprintf(&data, "Content-Type: text/plain\r\n");
      kc::strprintf(&data, "\r\n");
      data.append(str);
      sess->send(data.data(), data.size());
    }
    bool send_result(ThreadedServer::Session* sess, int32_t code, bool keep,
                     const std::string& path, HTTPClient::Method method,
                     const std::map<std::string, std::string>& reqheads,
                     const std::string& reqbody,
                     const std::map<std::string, std::string>& resheads,
                     const std::string& resbody,
                     const std::map<std::string, std::string>& misc) {
      const char* name = status_name(code);
      bool body = true;
      if (method == HTTPClient::MHEAD || code == 304) body = false;
      std::string data;
      kc::strprintf(&data, "HTTP/1.1 %d %s\r\n", code, name);
      append_server_headers(&data);
      if (!keep) kc::strprintf(&data, "Connection: close\r\n");
      if (body) kc::strprintf(&data, "Content-Length: %lld\r\n", (long long)resbody.size());
      std::map<std::string, std::string>::const_iterator it = resheads.begin();
      std::map<std::string, std::string>::const_iterator itend = resheads.end();
      while (it != itend) {
        char* name = kc::strdup(it->first.c_str());
        char* value = kc::strdup(it->second.c_str());
        kc::strnrmspc(name);
        kc::strtolower(name);
        kc::strnrmspc(value);
        if (*name != '\0' && !std::strchr(name, ':') && !std::strchr(name, ' ')) {
          strcapitalize(name);
          kc::strprintf(&data, "%s: %s\r\n", name, value);
        }
        delete[] value;
        delete[] name;
        ++it;
      }
      kc::strprintf(&data, "\r\n");
      if (body) data.append(resbody);
      return sess->send(data.data(), data.size());
    }
    void append_server_headers(std::string* str) {
      _assert_(str);
      kc::strprintf(str, "Server: KyotoTycoon/%s\r\n", VERSION);
      char buf[48];
      datestrhttp(kc::INT64MAX, 0, buf);
      kc::strprintf(str, "Date: %s\r\n", buf);
    }
    HTTPServer* serv_;
    HTTPServer::Worker* worker_;
  };
  /** Dummy constructor to forbid the use. */
  HTTPServer(const HTTPServer&);
  /** Dummy Operator to forbid the use. */
  HTTPServer& operator =(const HTTPServer&);
  /** The internal server. */
  ThreadedServer serv_;
  /** The server name. */
  std::string name_;
  /** The adapter for worker. */
  WorkerAdapter worker_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
