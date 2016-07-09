/*************************************************************************************************
 * The testing implementations using the server tool kit
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
kt::ServerSocket *g_servsock;            // running server socket
kt::Poller *g_poller;                    // running poller
kt::ThreadedServer* g_thserv;            // running threaded server
kt::HTTPServer* g_httpserv;              // running HTTP server
kt::RPCServer* g_rpcserv;                // running RPC server


// function prototypes
int main(int argc, char** argv);
static void usage();
static void killserver(int signum);
static int32_t runecho(int argc, char** argv);
static int32_t runmtecho(int argc, char** argv);
static int32_t runhttp(int argc, char** argv);
static int32_t runrpc(int argc, char** argv);
static int32_t procecho(const char* host, int32_t port, double tout);
static int32_t procmtecho(const char* host, int32_t port, double tout, int32_t thnum,
                          uint32_t logkinds);
static int32_t prochttp(const char* base,
                        const char* host, int32_t port, double tout, int32_t thnum,
                        uint32_t logkinds);
static int32_t procrpc(const char* host, int32_t port, double tout, int32_t thnum,
                       uint32_t logkinds);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  kt::setkillsignalhandler(killserver);
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "echo")) {
    rv = runecho(argc, argv);
  } else if (!std::strcmp(argv[1], "mtecho")) {
    rv = runmtecho(argc, argv);
  } else if (!std::strcmp(argv[1], "http")) {
    rv = runhttp(argc, argv);
  } else if (!std::strcmp(argv[1], "rpc")) {
    rv = runrpc(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
    rv = 0;
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: testing implementations using the server tool kit\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s echo [-host str] [-port num] [-tout num]\n", g_progname);
  eprintf("  %s mtecho [-host str] [-port num] [-tout num] [-th num] [-li|-ls|-le|-lz]\n",
          g_progname);
  eprintf("  %s http [-host str] [-port num] [-tout num] [-th num] [-li|-ls|-le|-lz]"
          " [basedir]\n", g_progname);
  eprintf("  %s rpc [-host str] [-port num] [-tout num] [-th num] [-li|-ls|-le|-lz]\n",
          g_progname);
  eprintf("\n");
  std::exit(1);
}


// kill the running server
static void killserver(int signum) {
  oprintf("%s: catched the signal %d\n", g_progname, signum);
  if (g_servsock) {
    g_servsock->abort();
    g_servsock = NULL;
  }
  if (g_poller) {
    g_poller->abort();
    g_poller = NULL;
  }
  if (g_thserv) {
    g_thserv->stop();
    g_thserv = NULL;
  }
  if (g_httpserv) {
    g_httpserv->stop();
    g_httpserv = NULL;
  }
  if (g_rpcserv) {
    g_rpcserv->stop();
    g_rpcserv = NULL;
  }
}


// parse arguments of echo command
static int32_t runecho(int argc, char** argv) {
  bool argbrk = false;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = DEFTOUT;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procecho(host, port, tout);
  return rv;
}


// parse arguments of mtecho command
static int32_t runmtecho(int argc, char** argv) {
  bool argbrk = false;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = DEFTOUT;
  int32_t thnum = DEFTHNUM;
  uint32_t logkinds = kc::UINT32MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-li")) {
        logkinds = kt::ThreadedServer::Logger::INFO | kt::ThreadedServer::Logger::SYSTEM |
            kt::ThreadedServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-ls")) {
        logkinds = kt::ThreadedServer::Logger::SYSTEM | kt::ThreadedServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-le")) {
        logkinds = kt::ThreadedServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-lz")) {
        logkinds = 0;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procmtecho(host, port, tout, thnum, logkinds);
  return rv;
}


// parse arguments of http command
static int32_t runhttp(int argc, char** argv) {
  bool argbrk = false;
  const char* host = NULL;
  const char* base = NULL;
  int32_t port = kt::DEFPORT;
  double tout = DEFTOUT;
  int32_t thnum = DEFTHNUM;
  uint32_t logkinds = kc::UINT32MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-li")) {
        logkinds = kt::HTTPServer::Logger::INFO | kt::HTTPServer::Logger::SYSTEM |
            kt::HTTPServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-ls")) {
        logkinds = kt::HTTPServer::Logger::SYSTEM | kt::HTTPServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-le")) {
        logkinds = kt::HTTPServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-lz")) {
        logkinds = 0;
      } else {
        usage();
      }
    } else if (!base) {
      argbrk = true;
      base = argv[i];
    } else {
      usage();
    }
  }
  if (port < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = prochttp(base, host, port, tout, thnum, logkinds);
  return rv;
}


// parse arguments of rpc command
static int32_t runrpc(int argc, char** argv) {
  bool argbrk = false;
  const char* host = NULL;
  int32_t port = kt::DEFPORT;
  double tout = DEFTOUT;
  int32_t thnum = DEFTHNUM;
  uint32_t logkinds = kc::UINT32MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-li")) {
        logkinds = kt::RPCServer::Logger::INFO | kt::RPCServer::Logger::SYSTEM |
            kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-ls")) {
        logkinds = kt::RPCServer::Logger::SYSTEM | kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-le")) {
        logkinds = kt::RPCServer::Logger::ERROR;
      } else if (!std::strcmp(argv[i], "-lz")) {
        logkinds = 0;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1 || thnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procrpc(host, port, tout, thnum, logkinds);
  return rv;
}


// perform echo command
static int32_t procecho(const char* host, int32_t port, double tout) {
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  kt::ServerSocket serv;
  if (!serv.open(expr)) {
    eprintf("%s: server: open error: %s\n", g_progname, serv.error());
    return 1;
  }
  bool err = false;
  kt::Poller poll;
  if (!poll.open()) {
    eprintf("%s: poller: open error: %s\n", g_progname, poll.error());
    err = true;
  }
  g_servsock = &serv;
  g_poller = &poll;
  oprintf("%s: started: %s\n", g_progname, serv.expression().c_str());
  serv.set_event_flags(kt::Pollable::EVINPUT);
  if (!poll.deposit(&serv)) {
    eprintf("%s: poller: deposit error: %s\n", g_progname, poll.error());
    err = true;
  }
  while (g_servsock) {
    if (poll.wait()) {
      kt::Pollable* event;
      while ((event = poll.next()) != NULL) {
        if (event == &serv) {
          kt::Socket* sock = new kt::Socket;
          sock->set_timeout(tout);
          if (serv.accept(sock)) {
            oprintf("%s: connected: %s\n", g_progname, sock->expression().c_str());
            sock->set_event_flags(kt::Pollable::EVINPUT);
            if (!poll.deposit(sock)) {
              eprintf("%s: poller: deposit error: %s\n", g_progname, poll.error());
              err = true;
            }
          } else {
            eprintf("%s: server: accept error: %s\n", g_progname, serv.error());
            err = true;
            delete sock;
          }
          serv.set_event_flags(kt::Pollable::EVINPUT);
          if (!poll.undo(&serv)) {
            eprintf("%s: poller: undo error: %s\n", g_progname, poll.error());
            err = true;
          }
        } else {
          kt::Socket* sock = (kt::Socket*)event;
          char line[LINEBUFSIZ];
          if (sock->receive_line(line, sizeof(line))) {
            oprintf("%s: (%s): %s\n", g_progname, sock->expression().c_str(), line);
            if (!kc::stricmp(line, "/quit")) {
              if (!sock->printf("> Bye!\n")) {
                eprintf("%s: socket: printf error: %s\n", g_progname, sock->error());
                err = true;
              }
              oprintf("%s: closing: %s\n", g_progname, sock->expression().c_str());
              if (!poll.withdraw(sock)) {
                eprintf("%s: poller: withdraw error: %s\n", g_progname, poll.error());
                err = true;
              }
              if (!sock->close()) {
                eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
                err = true;
              }
              delete sock;
            } else {
              if (!sock->printf("> %s\n", line)) {
                eprintf("%s: socket: printf error: %s\n", g_progname, sock->error());
                err = true;
              }
              serv.set_event_flags(kt::Pollable::EVINPUT);
              if (!poll.undo(sock)) {
                eprintf("%s: poller: undo error: %s\n", g_progname, poll.error());
                err = true;
              }
            }
          } else {
            oprintf("%s: closed: %s\n", g_progname, sock->expression().c_str());
            if (!poll.withdraw(sock)) {
              eprintf("%s: poller: withdraw error: %s\n", g_progname, poll.error());
              err = true;
            }
            if (!sock->close()) {
              eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
              err = true;
            }
            delete sock;
          }
        }
      }
    } else {
      eprintf("%s: poller: wait error: %s\n", g_progname, poll.error());
      err = true;
    }
  }
  g_poller = NULL;
  if (poll.flush()) {
    kt::Pollable* event;
    while ((event = poll.next()) != NULL) {
      if (event != &serv) {
        kt::Socket* sock = (kt::Socket*)event;
        oprintf("%s: discarded: %s\n", g_progname, sock->expression().c_str());
        if (!poll.withdraw(sock)) {
          eprintf("%s: poller: withdraw error: %s\n", g_progname, poll.error());
          err = true;
        }
        if (!sock->close()) {
          eprintf("%s: socket: close error: %s\n", g_progname, poll.error());
          err = true;
        }
        delete sock;
      }
    }
  } else {
    eprintf("%s: poller: flush error: %s\n", g_progname, poll.error());
    err = true;
  }
  oprintf("%s: finished: %s\n", g_progname, serv.expression().c_str());
  if (!poll.close()) {
    eprintf("%s: poller: close error: %s\n", g_progname, poll.error());
    err = true;
  }
  if (!serv.close()) {
    eprintf("%s: server: close error: %s\n", g_progname, serv.error());
    err = true;
  }
  return err ? 1 : 0;
}


// perform mtecho command
static int32_t procmtecho(const char* host, int32_t port, double tout, int32_t thnum,
                          uint32_t logkinds) {
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  kt::ThreadedServer serv;
  kt::ThreadedServer::Logger* logger = stdlogger(g_progname, &std::cout);
  class Worker : public kt::ThreadedServer::Worker {
   private:
    bool process(kt::ThreadedServer* serv, kt::ThreadedServer::Session* sess) {
      bool keep = false;
      char line[LINEBUFSIZ];
      if (sess->receive_line(line, sizeof(line))) {
        if (!kc::stricmp(line, "/quit")) {
          sess->printf("> Bye!\n");
        } else {
          class Data : public kt::ThreadedServer::Session::Data {
           public:
            time_t t;
          };
          Data* data = (Data*)sess->data();
          if (!data) {
            data = new Data;
            sess->set_data(data);
            data->t = kc::time();
          }
          sess->printf("> %s\n", line);
          serv->log(kt::ThreadedServer::Logger::INFO, "(%s): id=%llu thid=%u time=%lld msg=%s",
                    sess->expression().c_str(), (unsigned long long)sess->id(),
                    (unsigned)sess->thread_id(), (long long)(kc::time() - data->t), line);
          keep = true;
        }
      }
      return keep;
    }
  };
  Worker worker;
  bool err = false;
  serv.set_network(expr, tout);
  serv.set_logger(logger, logkinds);
  serv.set_worker(&worker, thnum);
  g_thserv = &serv;
  serv.log(kt::ThreadedServer::Logger::SYSTEM, "================ [START]");
  if (serv.start()) {
    if (serv.finish()) err = true;
  } else {
    err = true;
  }
  serv.log(kt::ThreadedServer::Logger::SYSTEM, "================ [FINISH]");
  return err ? 1 : 0;
}


// perform http command
static int32_t prochttp(const char* base,
                        const char* host, int32_t port, double tout, int32_t thnum,
                        uint32_t logkinds) {
  if (!base) base = kc::File::CDIRSTR;
  std::string baseabs = kc::File::absolute_path(base);
  if (baseabs.empty()) {
    eprintf("%s: %s: unknown directory\n", g_progname, base);
    return 1;
  }
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  kt::HTTPServer serv;
  kt::HTTPServer::Logger* logger = stdlogger(g_progname, &std::cout);
  class Worker : public kt::HTTPServer::Worker {
   public:
    explicit Worker(const std::string& base) : base_(base) {}
   private:
    int32_t process(kt::HTTPServer* serv, kt::HTTPServer::Session* sess,
                    const std::string& path, kt::HTTPClient::Method method,
                    const std::map<std::string, std::string>& reqheads,
                    const std::string& reqbody,
                    std::map<std::string, std::string>& resheads,
                    std::string& resbody,
                    const std::map<std::string, std::string>& misc) {
      const char* url = kt::strmapget(misc, "url");
      int32_t code = -1;
      const std::string& lpath = kt::HTTPServer::localize_path(path);
      std::string apath = base_ + (lpath.empty() ? "" : kc::File::PATHSTR) + lpath;
      bool dir = kc::strbwm(path.c_str(), "/");
      if (method == kt::HTTPClient::MGET) {
        kc::File::Status sbuf;
        if (kc::File::status(apath, &sbuf)) {
          if (dir && sbuf.isdir) {
            const std::string& ipath = apath + kc::File::PATHSTR + "index.html";
            if (kc::File::status(ipath)) {
              apath = ipath;
              sbuf.isdir = false;
            }
          }
          if (sbuf.isdir) {
            if (dir) {
              std::vector<std::string> files;
              if (kc::File::read_directory(apath, &files)) {
                code = 200;
                resheads["content-type"] = "text/html";
                kc::strprintf(&resbody, "<html>\n");
                kc::strprintf(&resbody, "<body>\n");
                kc::strprintf(&resbody, "<ul>\n");
                kc::strprintf(&resbody, "<li><a href=\"./\">./</a></li>\n");
                kc::strprintf(&resbody, "<li><a href=\"../\">../</a></li>\n");
                kc::strprintf(&resbody, "</ul>\n");
                kc::strprintf(&resbody, "<ul>\n");
                std::sort(files.begin(), files.end());
                std::vector<std::string>::iterator it = files.begin();
                std::vector<std::string>::iterator itend = files.end();
                while (it != itend) {
                  if (*it != kc::File::CDIRSTR && *it != kc::File::PDIRSTR) {
                    std::string cpath = apath + kc::File::PATHSTR + *it;
                    if (kc::File::status(cpath, &sbuf)) {
                      char* ubuf = kc::urlencode(it->data(), it->size());
                      char* xstr = kt::xmlescape(it->c_str());
                      const char* dsuf = sbuf.isdir ? "/" : "";
                      kc::strprintf(&resbody, "<li><a href=\"%s%s\">%s%s</a></li>\n",
                                    ubuf, dsuf, xstr, dsuf);
                      delete[] xstr;
                      delete[] ubuf;
                    }
                  }
                  ++it;
                }
                kc::strprintf(&resbody, "</ul>\n");
                kc::strprintf(&resbody, "</body>\n");
                kc::strprintf(&resbody, "</html>\n");
              } else {
                code = 403;
                resheads["content-type"] = "text/plain";
                kc::strprintf(&resbody, "%s\n", kt::HTTPServer::status_name(code));
              }
            } else {
              code = 301;
              resheads["content-type"] = "text/plain";
              resheads["location"] = (url ? url : path) + '/';
              kc::strprintf(&resbody, "%s\n", kt::HTTPServer::status_name(code));
            }
          } else {
            int64_t size;
            char* buf = kc::File::read_file(apath, &size, 256LL << 20);
            if (buf) {
              code = 200;
              const char* type = kt::HTTPServer::media_type(path);
              if (type) resheads["content-type"] = type;
              resbody.append(buf, size);
              delete[] buf;
            } else {
              code = 403;
              resheads["content-type"] = "text/plain";
              kc::strprintf(&resbody, "%s\n", kt::HTTPServer::status_name(code));
            }
          }
        } else {
          code = 404;
          resheads["content-type"] = "text/plain";
          kc::strprintf(&resbody, "%s\n", kt::HTTPServer::status_name(code));
        }
      } else {
        code = 403;
        resheads["content-type"] = "text/plain";
        kc::strprintf(&resbody, "%s\n", kt::HTTPServer::status_name(code));
      }
      return code;
    }
    std::string base_;
  };
  Worker worker(baseabs);
  bool err = false;
  serv.set_network(expr, tout);
  serv.set_logger(logger, logkinds);
  serv.set_worker(&worker, thnum);
  g_httpserv = &serv;
  serv.log(kt::HTTPServer::Logger::SYSTEM, "================ [START]");
  if (serv.start()) {
    if (serv.finish()) err = true;
  } else {
    err = true;
  }
  serv.log(kt::HTTPServer::Logger::SYSTEM, "================ [FINISH]");
  return err ? 1 : 0;
}


// perform rpc command
static int32_t procrpc(const char* host, int32_t port, double tout, int32_t thnum,
                       uint32_t logkinds) {
  std::string addr = "";
  if (host) {
    addr = kt::Socket::get_host_address(host);
    if (addr.empty()) {
      eprintf("%s: %s: unknown host\n", g_progname, host);
      return 1;
    }
  }
  std::string expr = kc::strprintf("%s:%d", addr.c_str(), port);
  kt::RPCServer serv;
  kt::RPCServer::Logger* logger = stdlogger(g_progname, &std::cout);
  class Worker : public kt::RPCServer::Worker {
   private:
    kt::RPCClient::ReturnValue process(kt::RPCServer* serv, kt::RPCServer::Session* sess,
                                       const std::string& name,
                                       const std::map<std::string, std::string>& inmap,
                                       std::map<std::string, std::string>& outmap) {
      outmap.insert(inmap.begin(), inmap.end());
      return kt::RPCClient::RVSUCCESS;
    }
  };
  Worker worker;
  bool err = false;
  serv.set_network(expr, tout);
  serv.set_logger(logger, logkinds);
  serv.set_worker(&worker, thnum);
  g_rpcserv = &serv;
  serv.log(kt::RPCServer::Logger::SYSTEM, "================ [START]");
  if (serv.start()) {
    if (serv.finish()) err = true;
  } else {
    err = true;
  }
  serv.log(kt::RPCServer::Logger::SYSTEM, "================ [FINISH]");
  return err ? 1 : 0;
}



// END OF FILE
