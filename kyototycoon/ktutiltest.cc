/*************************************************************************************************
 * The test cases of the utility functions
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


#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name
uint32_t g_randseed;                     // random seed
int64_t g_memusage;                      // memory usage


// function prototypes
int main(int argc, char** argv);
static void usage();
static void errprint(int32_t line, const char* format, ...);
static int32_t runhttp(int32_t argc, char** argv);
static int32_t runrpc(int32_t argc, char** argv);
static int32_t runulog(int32_t argc, char** argv);
static int32_t prochttp(const char* url, int64_t rnum, int32_t thnum,
                        kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries, double tout, bool ka);
static int32_t procrpc(const char* proc, int64_t rnum,
                       std::map<std::string, std::string>* params, int32_t thnum,
                       const char* host, int32_t port, double tout);
static int32_t proculog(const char* path, int64_t rnum, int32_t thnum, int64_t ulim);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  const char* ebuf = kc::getenv("KTRNDSEED");
  g_randseed = ebuf ? (uint32_t)kc::atoi(ebuf) : (uint32_t)(kc::time() * 1000);
  mysrand(g_randseed);
  g_memusage = memusage();
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "http")) {
    rv = runhttp(argc, argv);
  } else if (!std::strcmp(argv[1], "rpc")) {
    rv = runrpc(argc, argv);
  } else if (!std::strcmp(argv[1], "ulog")) {
    rv = runulog(argc, argv);
  } else {
    usage();
  }
  if (rv != 0) {
    oprintf("FAILED: KTRNDSEED=%u PID=%ld", g_randseed, (long)kc::getpid());
    for (int32_t i = 0; i < argc; i++) {
      oprintf(" %s", argv[i]);
    }
    oprintf("\n\n");
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: test cases of the utility functions of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s http [-th num] [-get|-head|-post|-put|-delete] [-body file] [-ah name value]"
          " [-qs name value] [-tout num] [-ka] url rnum\n", g_progname);
  eprintf("  %s rpc [-th num] [-host str] [-port num] [-tout num] proc rnum [name value ...]\n",
          g_progname);
  eprintf("  %s ulog [-th num] [-ulim num] path rnum\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print formatted error information string and flush the buffer
static void errprint(int32_t line, const char* format, ...) {
  std::string msg;
  kc::strprintf(&msg, "%s: %d: ", g_progname, line);
  va_list ap;
  va_start(ap, format);
  kc::vstrprintf(&msg, format, ap);
  va_end(ap);
  kc::strprintf(&msg, "\n");
  std::cout << msg;
  std::cout.flush();
}


// parse arguments of http command
static int32_t runhttp(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* url = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  kt::HTTPClient::Method meth = kt::HTTPClient::MUNKNOWN;
  const char* body = NULL;
  std::map<std::string, std::string> reqheads;
  std::map<std::string, std::string> queries;
  double tout = 0;
  bool ka = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-get")) {
        meth = kt::HTTPClient::MGET;
      } else if (!std::strcmp(argv[i], "-head")) {
        meth = kt::HTTPClient::MHEAD;
      } else if (!std::strcmp(argv[i], "-post")) {
        meth = kt::HTTPClient::MPOST;
      } else if (!std::strcmp(argv[i], "-put")) {
        meth = kt::HTTPClient::MPUT;
      } else if (!std::strcmp(argv[i], "-delete")) {
        meth = kt::HTTPClient::MDELETE;
      } else if (!std::strcmp(argv[i], "-body")) {
        if (++i >= argc) usage();
        body = argv[i];
      } else if (!std::strcmp(argv[i], "-ah")) {
        if ((i += 2) >= argc) usage();
        char* name = kc::strdup(argv[i-1]);
        kc::strnrmspc(name);
        kc::strtolower(name);
        reqheads[name] = argv[i];
        delete[] name;
      } else if (!std::strcmp(argv[i], "-qs")) {
        if ((i += 2) >= argc) usage();
        queries[argv[i-1]] = argv[i];
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ka")) {
        ka = true;
      } else {
        usage();
      }
    } else if (!url) {
      argbrk = false;
      url = argv[i];
    } else if (!rstr) {
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!url || !rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = prochttp(url, rnum, thnum, meth, body, &reqheads, &queries, tout, ka);
  return rv;
}


// parse arguments of rpc command
static int32_t runrpc(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* proc = NULL;
  const char* rstr = NULL;
  std::map<std::string, std::string> params;
  int32_t thnum = 1;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else {
        usage();
      }
    } else if (!proc) {
      argbrk = false;
      proc = argv[i];
    } else if (!rstr) {
      rstr = argv[i];
    } else {
      if (++i >= argc) usage();
      params[argv[i-1]] = argv[i];
    }
  }
  if (!proc || !rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1 || port < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procrpc(proc, rnum, &params, thnum, host, port, tout);
  return rv;
}


// parse arguments of ulog command
static int32_t runulog(int32_t argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int64_t ulim = -1;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = false;
      path = argv[i];
    } else if (!rstr) {
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = proculog(path, rnum, thnum, ulim);
  return rv;
}


// perform http command
static int32_t prochttp(const char* url, int64_t rnum, int32_t thnum,
                        kt::HTTPClient::Method meth, const char* body,
                        std::map<std::string, std::string>* reqheads,
                        std::map<std::string, std::string>* queries, double tout, bool ka) {
  oprintf("<HTTP Test>\n  seed=%u  url=%s  rnum=%lld  thnum=%d  meth=%d  tout=%.3f  ka=%d\n\n",
          g_randseed, url, (long long)rnum, thnum, (int)meth, tout, ka);
  const char* istr = body && *body == '@' ? body + 1 : NULL;
  std::istream *is;
  std::ifstream ifs;
  std::istringstream iss(istr ? istr : "");
  if (body) {
    if (istr) {
      is = &iss;
    } else {
      ifs.open(body, std::ios_base::in | std::ios_base::binary);
      if (!ifs) {
        errprint(__LINE__, "%s: open error", body);
        return 1;
      }
      is = &ifs;
    }
  } else {
    is = &std::cin;
  }
  std::string urlstr = url;
  std::ostringstream oss;
  bool isbody = body || meth == kt::HTTPClient::MPOST || meth == kt::HTTPClient::MPUT;
  if (isbody) {
    if (queries->empty()) {
      char c;
      while (is->get(c)) {
        oss.put(c);
      }
    } else {
      std::map<std::string, std::string>::const_iterator it = queries->begin();
      std::map<std::string, std::string>::const_iterator itend = queries->end();
      bool first = true;
      while (it != itend) {
        char* name = kc::urlencode(it->first.data(), it->first.size());
        char* value = kc::urlencode(it->second.data(), it->second.size());
        if (first) {
          first = false;
        } else {
          oss << "&";
        }
        oss << name << "=" << value;
        delete[] value;
        delete[] name;
        ++it;
      }
      (*reqheads)["content-type"] = "application/x-www-form-urlencoded";
    }
  } else if (!queries->empty()) {
    std::map<std::string, std::string>::const_iterator it = queries->begin();
    std::map<std::string, std::string>::const_iterator itend = queries->end();
    bool first = !std::strchr(url, '?');
    while (it != itend) {
      char* name = kc::urlencode(it->first.data(), it->first.size());
      char* value = kc::urlencode(it->second.data(), it->second.size());
      if (first) {
        first = false;
        urlstr.append("?");
      } else {
        urlstr.append("&");
      }
      urlstr.append(name);
      urlstr.append("=");
      urlstr.append(value);
      delete[] value;
      delete[] name;
      ++it;
    }
  }
  if (!kt::strmapget(*reqheads, "user-agent")) {
    std::string uastr;
    kc::strprintf(&uastr, "KyotoTycoon/%s", kt::VERSION);
    (*reqheads)["user-agent"] = uastr;
  }
  if (!kt::strmapget(*reqheads, "accept")) (*reqheads)["accept"] = "*/*";
  const std::string& ostr = oss.str();
  const std::string* reqbody = isbody ? &ostr : NULL;
  if (meth == kt::HTTPClient::MUNKNOWN)
    meth = isbody ? kt::HTTPClient::MPOST : kt::HTTPClient::MGET;
  bool err = false;
  class Worker : public kc::Thread {
   public:
    Worker() :
        id_(0), rnum_(0), url_(NULL), meth_(), reqheads_(NULL), reqbody_(NULL), tout_(0),
        err_(false), okcnt_(0) {}
    void setparams(int32_t id, int64_t rnum, const char* url, kt::HTTPClient::Method meth,
                   const std::map<std::string, std::string>* reqheads,
                   const std::string* reqbody, double tout, bool ka) {
      id_ = id;
      rnum_ = rnum;
      url_ = url;
      meth_ = meth;
      reqheads_ = reqheads;
      reqbody_ = reqbody;
      tout_ = tout;
      ka_ = ka;
    }
    bool error() {
      return err_;
    }
    int64_t okcnt() {
      return okcnt_;
    }
   private:
    void run() {
      kt::URL u(url_);
      kt::HTTPClient ua;
      bool open = false;
      std::map<std::string, std::string> resheads;
      for (int64_t i = 1; i <= rnum_; i++) {
        if (!open) {
          if (!ua.open(u.host(), u.port(), tout_)) {
            err_ = true;
            break;
          }
          open = true;
        }
        int32_t code = ua.fetch(u.path_query(), meth_, NULL, &resheads, reqbody_, reqheads_);
        if (code >= 200 && code < 300) okcnt_++;
        const char* conn = kt::strmapget(resheads, "connection");
        if (!ka_ || code < 0 || (conn && !kc::stricmp(conn, "close"))) {
          if (!ua.close(false)) err_ = true;
          open = false;
        }
        if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
          oputchar('.');
          if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
        }
      }
      if (open && !ua.close()) err_ = true;
    }
    int32_t id_;
    int64_t rnum_;
    const char* url_;
    kt::HTTPClient::Method meth_;
    const std::map<std::string, std::string>* reqheads_;
    const std::string* reqbody_;
    double tout_;
    bool ka_;
    bool err_;
    int64_t okcnt_;
  };
  Worker* workers = new Worker[thnum];
  double stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].setparams(i, rnum, url, meth, reqheads, reqbody, tout, ka);
    workers[i].start();
  }
  int64_t okcnt = 0;
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].join();
    if (workers[i].error()) err = true;
    okcnt += workers[i].okcnt();
  }
  double etime = kc::time();
  oprintf("OK count: %lld\n", (long long)okcnt);
  oprintf("NG count: %lld\n", (long long)(rnum * thnum - okcnt));
  oprintf("time: %.3f\n", etime - stime);
  oprintf("throughput: %.3f req/s\n", okcnt / (etime - stime));
  delete[] workers;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform rpc command
static int32_t procrpc(const char* proc, int64_t rnum,
                       std::map<std::string, std::string>* params, int32_t thnum,
                       const char* host, int32_t port, double tout) {
  oprintf("<RPC Test>\n  seed=%u  proc=%s  rnum=%lld  thnum=%d  host=%s  port=%d  tout=%.3f\n\n",
          g_randseed, proc, (long long)rnum, thnum, host ? host : "-", port, tout);
  if (!host) host = "localhost";
  bool err = false;
  class Worker : public kc::Thread {
   public:
    Worker() :
        id_(0), rnum_(0), proc_(NULL), host_(NULL), port_(0), tout_(0), params_(NULL),
        err_(false), okcnt_(0) {}
    void setparams(int32_t id, int64_t rnum, const char* proc, const char* host, int32_t port,
                   double tout, const std::map<std::string, std::string>* params) {
      id_ = id;
      rnum_ = rnum;
      proc_ = proc;
      host_ = host;
      port_ = port;
      tout_ = tout;
      params_ = params;
    }
    bool error() {
      return err_;
    }
    int64_t okcnt() {
      return okcnt_;
    }
   private:
    void run() {
      kt::RPCClient rpc;
      if (!rpc.open(host_, port_, tout_)) {
        err_ = true;
        return;
      }
      std::map<std::string, std::string> outmap;
      for (int64_t i = 1; i <= rnum_; i++) {
        kt::RPCClient::ReturnValue rv = rpc.call(proc_, params_, &outmap);
        if (rv == kt::RPCClient::RVSUCCESS) {
          okcnt_++;
        } else {
          err_ = true;
        }
        if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
          oputchar('.');
          if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
        }
      }
      if (!rpc.close()) err_ = true;
    }
    int32_t id_;
    int64_t rnum_;
    const char* proc_;
    const char* host_;
    int32_t port_;
    double tout_;
    const std::map<std::string, std::string>* params_;
    bool err_;
    int64_t okcnt_;
  };
  Worker* workers = new Worker[thnum];
  double stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].setparams(i, rnum, proc, host, port, tout, params);
    workers[i].start();
  }
  int64_t okcnt = 0;
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].join();
    if (workers[i].error()) err = true;
    okcnt += workers[i].okcnt();
  }
  double etime = kc::time();
  oprintf("OK count: %lld\n", (long long)okcnt);
  oprintf("NG count: %lld\n", (long long)(rnum * thnum - okcnt));
  oprintf("time: %.3f\n", etime - stime);
  oprintf("throughput: %.3f req/s\n", okcnt / (etime - stime));
  delete[] workers;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform ulog command
static int32_t proculog(const char* path, int64_t rnum, int32_t thnum, int64_t ulim) {
  oprintf("<Update Logging Test>\n  seed=%u  path=%s  rnum=%lld  thnum=%d  ulim=%lld\n\n",
          g_randseed, path, (long long)rnum, thnum, (long long)ulim);
  bool err = false;
  bool init = !kc::File::status(path);
  kt::UpdateLogger ulog;
  if (!ulog.open(path, ulim)) {
    errprint(__LINE__, "opening the logger failed");
    return false;
  }
  class Writer : public kc::Thread {
   public:
    Writer() : ulog_(NULL), rnum_(0), err_(false) {}
    void setparams(int32_t id, kt::UpdateLogger* ulog, int64_t rnum) {
      id_ = id;
      ulog_ = ulog;
      rnum_ = rnum;
    }
    bool error() {
      return err_;
    }
   private:
    void run() {
      for (int64_t i = 1; i <= rnum_; i++) {
        char rbuf[RECBUFSIZ];
        size_t rsiz = std::sprintf(rbuf, "%lld", (long long)i);
        if (!ulog_->write(rbuf, rsiz)) {
          errprint(__LINE__, "writing a log failed");
          err_ = true;
        }
        if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
          oputchar('.');
          if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
        }
      }
    }
    int32_t id_;
    kt::UpdateLogger* ulog_;
    int64_t rnum_;
    bool err_;
  };
  class Reader : public kc::Thread {
   public:
    Reader() : ulog_(NULL), rnum_(0), alive_(true), cnt_(0), err_(false) {}
    void setparams(int32_t id, kt::UpdateLogger* ulog, int64_t rnum) {
      id_ = id;
      ulog_ = ulog;
      rnum_ = rnum;
    }
    void stop() {
      alive_ = false;
    }
    int64_t count() {
      return cnt_;
    }
    bool error() {
      return err_;
    }
   private:
    void run() {
      kt::UpdateLogger::Reader ulrd;
      if (!ulrd.open(ulog_, 0)) {
        errprint(__LINE__, "opening a reader failed");
        err_ = true;
      }
      while (alive_) {
        const char* mbuf;
        size_t msiz;
        uint64_t mts;
        while ((mbuf = ulrd.read(&msiz, &mts)) != NULL) {
          cnt_ += 1;
          delete[] mbuf;
        }
        kc::Thread::sleep(0.1);
      }
      if (!ulrd.close()) {
        errprint(__LINE__, "opening a reader failed");
        err_ = true;
      }
    }
    int32_t id_;
    kt::UpdateLogger* ulog_;
    int64_t rnum_;
    bool alive_;
    kc::AtomicInt64 cnt_;
    bool err_;
  };
  double stime = kc::time();
  Reader readers[THREADMAX];
  for (int32_t i = 0; i < thnum; i++) {
    readers[i].setparams(i, &ulog, rnum);
    readers[i].start();
  }
  Writer writers[THREADMAX];
  for (int32_t i = 0; i < thnum; i++) {
    writers[i].setparams(i, &ulog, rnum);
    writers[i].start();
  }
  for (int32_t i = 0; i < thnum; i++) {
    writers[i].join();
    if (writers[i].error()) {
      errprint(__LINE__, "writing logs failed");
      err = true;
    }
  }
  for (int32_t i = 0; i < thnum; i++) {
    for (int32_t j = 0; j < 100; j++) {
      if (readers[i].count() >= rnum * thnum) break;
      kc::Thread::sleep(0.1);
    }
    readers[i].stop();
    readers[i].join();
    if (readers[i].error()) {
      errprint(__LINE__, "reading logs failed");
      err = true;
    }
    if (init && readers[i].count() != rnum * thnum) {
      errprint(__LINE__, "reading logs failed");
      err = true;
    }
  }
  if (!ulog.close()) {
    errprint(__LINE__, "closing the logger failed");
    err = true;
  }
  double etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
