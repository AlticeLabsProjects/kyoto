/*************************************************************************************************
 * The test cases of the remote database
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


#include <ktremotedb.h>
#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name
uint32_t g_randseed;                     // random seed
int64_t g_memusage;                      // memory usage


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::RemoteDB* db, int32_t line, const char* func);
static void dbmetaprint(kt::RemoteDB* db, bool verbose);
static int32_t runorder(int argc, char** argv);
static int32_t runbulk(int argc, char** argv);
static int32_t runwicked(int argc, char** argv);
static int32_t runusual(int argc, char** argv);
static int32_t procorder(int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         const char* host, int32_t port, double tout);
static int32_t procbulk(int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t mode,
                        int32_t bulk, const char* host, int32_t port, double tout,
                        int32_t bopts);
static int32_t procwicked(int64_t rnum, int32_t thnum, int32_t itnum,
                          const char* host, int32_t port, double tout);
static int32_t procusual(int64_t rnum, int32_t thnum, int32_t itnum,
                         const char* host, int32_t port, double tout,
                         int64_t kp, int64_t vs, int64_t xt, double iv);


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
  if (!std::strcmp(argv[1], "order")) {
    rv = runorder(argc, argv);
  } else if (!std::strcmp(argv[1], "bulk")) {
    rv = runbulk(argc, argv);
  } else if (!std::strcmp(argv[1], "wicked")) {
    rv = runwicked(argc, argv);
  } else if (!std::strcmp(argv[1], "usual")) {
    rv = runusual(argc, argv);
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
  eprintf("%s: test cases of the remote database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s order [-th num] [-rnd] [-set|-get|-rem|-etc]"
          " [-host str] [-port num] [-tout num] rnum\n", g_progname);
  eprintf("  %s bulk [-th num] [-bin] [-rnd] [-set|-get|-rem|-etc] [-bulk num]"
          " [-host str] [-port num] [-tout num] [-bnr] rnum\n", g_progname);
  eprintf("  %s wicked [-th num] [-it num] [-host str] [-port num] [-tout num] rnum\n",
          g_progname);
  eprintf("  %s usual [-th num] [-host str] [-port num] [-tout num]"
          " [-kp num] [-vs num] [-xt num] [-iv num] rnum\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print the error message of a database
static void dberrprint(kt::RemoteDB* db, int32_t line, const char* func) {
  const kt::RemoteDB::Error& err = db->error();
  oprintf("%s: %d: %s: %s: %d: %s: %s\n",
          g_progname, line, func, db->expression().c_str(),
          err.code(), err.name(), err.message());
}


// print members of a database
static void dbmetaprint(kt::RemoteDB* db, bool verbose) {
  if (verbose) {
    std::map<std::string, std::string> status;
    if (db->status(&status)) {
      std::map<std::string, std::string>::iterator it = status.begin();
      std::map<std::string, std::string>::iterator itend = status.end();
      while (it != itend) {
        oprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
        ++it;
      }
    }
  } else {
    oprintf("count: %lld\n", (long long)db->count());
    oprintf("size: %lld\n", (long long)db->size());
  }
  int64_t musage = memusage();
  if (musage > 0) oprintf("memory: %lld\n", (long long)(musage - g_memusage));
}


// parse arguments of order command
static int32_t runorder(int argc, char** argv) {
  bool argbrk = false;
  const char* rstr = NULL;
  int32_t thnum = 1;
  bool rnd = false;
  int32_t mode = 0;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-rnd")) {
        rnd = true;
      } else if (!std::strcmp(argv[i], "-set")) {
        mode = 's';
      } else if (!std::strcmp(argv[i], "-get")) {
        mode = 'g';
      } else if (!std::strcmp(argv[i], "-rem")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-etc")) {
        mode = 'e';
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
    } else if (!rstr) {
      argbrk = false;
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1 || port < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procorder(rnum, thnum, rnd, mode, host, port, tout);
  return rv;
}


// parse arguments of bulk command
static int32_t runbulk(int argc, char** argv) {
  bool argbrk = false;
  const char* rstr = NULL;
  int32_t thnum = 1;
  bool bin = false;
  bool rnd = false;
  int32_t mode = 0;
  int32_t bulk = 1;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  int32_t bopts = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-bin")) {
        bin = true;
      } else if (!std::strcmp(argv[i], "-rnd")) {
        rnd = true;
      } else if (!std::strcmp(argv[i], "-set")) {
        mode = 's';
      } else if (!std::strcmp(argv[i], "-get")) {
        mode = 'g';
      } else if (!std::strcmp(argv[i], "-rem")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-etc")) {
        mode = 'e';
      } else if (!std::strcmp(argv[i], "-bulk")) {
        if (++i >= argc) usage();
        bulk = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-bnr")) {
        bopts |= kt::RemoteDB::BONOREPLY;
      } else {
        usage();
      }
    } else if (!rstr) {
      argbrk = false;
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1 || bulk < 1 || port < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procbulk(rnum, thnum, bin, rnd, mode, bulk, host, port, tout, bopts);
  return rv;
}


// parse arguments of wicked command
static int32_t runwicked(int argc, char** argv) {
  bool argbrk = false;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int32_t itnum = 1;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-it")) {
        if (++i >= argc) usage();
        itnum = kc::atoix(argv[i]);
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
    } else if (!rstr) {
      argbrk = false;
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1 || port < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procwicked(rnum, thnum, itnum, host, port, tout);
  return rv;
}


// parse arguments of usual command
static int32_t runusual(int argc, char** argv) {
  bool argbrk = false;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int32_t itnum = 1;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  int64_t kp = 0;
  int64_t vs = 0;
  int64_t xt = 0;
  double iv = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-th")) {
        if (++i >= argc) usage();
        thnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-it")) {
        if (++i >= argc) usage();
        itnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoi(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-kp")) {
        if (++i >= argc) usage();
        kp = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-vs")) {
        if (++i >= argc) usage();
        vs = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-iv")) {
        if (++i >= argc) usage();
        iv = kc::atof(argv[i]);
      } else {
        usage();
      }
    } else if (!rstr) {
      argbrk = false;
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1 || thnum < 1 || port < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  if (kp < 1) kp = rnum * thnum;
  int32_t rv = procusual(rnum, thnum, itnum, host, port, tout, kp, vs, xt, iv);
  return rv;
}


// perform order command
static int32_t procorder(int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         const char* host, int32_t port, double tout) {
  oprintf("<In-order Test>\n  seed=%u  rnum=%lld  thnum=%d  rnd=%d  mode=%d  host=%s  port=%d"
          "  tout=%f\n\n", g_randseed, (long long)rnum, thnum, rnd, mode, host, port, tout);
  bool err = false;
  oprintf("opening the database:\n");
  double stime = kc::time();
  kt::RemoteDB* dbs = new kt::RemoteDB[thnum];
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].open(host, port, tout)) {
      dberrprint(dbs + i, __LINE__, "DB::open");
      err = true;
    }
  }
  if (mode != 'g' && mode != 'r' && !dbs[0].clear()) {
    dberrprint(dbs, __LINE__, "DB::clear");
    err = true;
  }
  double etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  if (mode == 0 || mode == 's' || mode == 'e') {
    oprintf("setting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->set(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 's');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("adding records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->add(kbuf, ksiz, kbuf, ksiz, xt) &&
              db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::add");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("appending records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->append(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'g' || mode == 'e') {
    oprintf("geting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          size_t vsiz;
          char* vbuf = db_->get(kbuf, ksiz, &vsiz);
          if (vbuf) {
            if (vsiz < ksiz || std::memcmp(vbuf, kbuf, ksiz)) {
              dberrprint(db_, __LINE__, "DB::get");
              err_ = true;
            }
            delete[] vbuf;
          } else if (!rnd_ || db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::get");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'g');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("traversing the database by the outer cursor:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        kt::RemoteDB::Cursor* cur = db_->cursor();
        if (!cur->jump() && cur->error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(db_, __LINE__, "Cursor::jump");
          err_ = true;
        }
        int64_t cnt = 0;
        const char* kbuf;
        size_t ksiz;
        while ((kbuf = cur->get_key(&ksiz)) != NULL) {
          cnt++;
          if (rnd_) {
            switch (myrand(5)) {
              case 0: {
                char vbuf[RECBUFSIZ];
                size_t vsiz = std::sprintf(vbuf, "%lld", (long long)cnt);
                if (!cur->set_value(vbuf, vsiz, myrand(600) + 1, myrand(2) == 0) &&
                    cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::set_value");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!cur->remove() && cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::remove");
                  err_ = true;
                }
                break;
              }
              case 2: {
                size_t rsiz, vsiz;
                const char* vbuf;
                int64_t xt;
                char* rbuf = cur->get(&rsiz, &vbuf, &vsiz, &xt, myrand(2) == 0);
                if (rbuf ) {
                  delete[] rbuf;
                } else if (cur->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "Cursor::et");
                  err_ = true;
                }
                break;
              }
            }
          } else {
            size_t vsiz;
            char* vbuf = cur->get_value(&vsiz);
            if (vbuf) {
              delete[] vbuf;
            } else {
              dberrprint(db_, __LINE__, "Cursor::get_value");
              err_ = true;
            }
          }
          delete[] kbuf;
          if (!cur->step() && cur->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "Cursor::step");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && cnt % (rnum_ / 250) == 0) {
            oputchar('.');
            if (cnt == rnum_ || cnt % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)cnt);
          }
        }
        if (cur->error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(db_, __LINE__, "Cursor::get_key");
          err_ = true;
        }
        if (!rnd_ && cnt != db_->count()) {
          dberrprint(db_, __LINE__, "Cursor::get_key");
          err_ = true;
        }
        if (id_ < 1) oprintf(" (end)\n");
        delete cur;
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'r' || mode == 'e') {
    oprintf("removing records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), rnd_(false), mode_(0), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                     kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        mode_ = mode;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          if (!db_->remove(kbuf, ksiz) &&
              ((!rnd_ && mode_ != 'e') || db_->error() != kt::RemoteDB::Error::LOGIC)) {
            dberrprint(db_, __LINE__, "DB::remove");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      int32_t mode_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, rnd, mode, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'r' || mode == 'e');
    oprintf("time: %.3f\n", etime - stime);
  }
  oprintf("closing the database:\n");
  stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].close()) {
      dberrprint(dbs + i, __LINE__, "DB::close");
      err = true;
    }
  }
  etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  delete[] dbs;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform bulk command
static int32_t procbulk(int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t mode,
                        int32_t bulk, const char* host, int32_t port, double tout,
                        int32_t bopts) {
  oprintf("<Bulk Test>\n  seed=%u  rnum=%lld  thnum=%d  bin=%d  rnd=%d  mode=%d  bulk=%d"
          "  host=%s  port=%d  tout=%f  bopts=%d\n\n",
          g_randseed, (long long)rnum, thnum, bin, rnd, mode, bulk, host, port, tout, bopts);
  bool err = false;
  oprintf("opening the database:\n");
  double stime = kc::time();
  kt::RemoteDB* dbs = new kt::RemoteDB[thnum];
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].open(host, port, tout)) {
      dberrprint(dbs + i, __LINE__, "DB::open");
      err = true;
    }
  }
  if (mode != 'g' && mode != 'r' && !dbs[0].clear()) {
    dberrprint(dbs, __LINE__, "DB::clear");
    err = true;
  }
  double etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  if (mode == 0 || mode == 's') {
    oprintf("setting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), bin_(false), rnd_(false), bulk_(0),
                 bopts_(0), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t bulk,
                     int32_t bopts, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        bin_ = bin;
        rnd_ = rnd;
        bulk_ = bulk;
        bopts_ = bopts;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        std::map<std::string, std::string> recs;
        std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          std::string key(kbuf, ksiz);
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (bin_) {
            kt::RemoteDB::BulkRecord rec = { 0, key, key, xt };
            bulkrecs.push_back(rec);
            if (bulkrecs.size() >= (size_t)bulk_) {
              if (db_->set_bulk_binary(bulkrecs, bopts_) < 0) {
                dberrprint(db_, __LINE__, "DB::set_bulk_binary");
                err_ = true;
              }
              bulkrecs.clear();
            }
          } else {
            recs[key] = key;
            if (recs.size() >= (size_t)bulk_) {
              if (db_->set_bulk(recs, xt) != (int64_t)recs.size()) {
                dberrprint(db_, __LINE__, "DB::set_bulk");
                err_ = true;
              }
              recs.clear();
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        if (bulkrecs.size() > 0) {
          if (db_->set_bulk_binary(bulkrecs, bopts_) < 0) {
            dberrprint(db_, __LINE__, "DB::set_bulk_binary");
            err_ = true;
          }
          bulkrecs.clear();
        }
        if (recs.size() > 0) {
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (db_->set_bulk(recs, xt) != (int64_t)recs.size()) {
            dberrprint(db_, __LINE__, "DB::set_bulk");
            err_ = true;
          }
          recs.clear();
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool bin_;
      bool rnd_;
      int32_t bulk_;
      int32_t bopts_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, bin, rnd, bulk, bopts, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 's');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'g') {
    oprintf("getting records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), bin_(false), rnd_(false), bulk_(0),
                 db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t bulk,
                     int32_t bopts, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        bin_ = bin;
        rnd_ = rnd;
        bulk_ = bulk;
        bopts_ = bopts;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        std::vector<std::string> keys;
        std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          std::string key(kbuf, ksiz);
          if (bin_) {
            kt::RemoteDB::BulkRecord rec = { 0, key, "", 0 };
            bulkrecs.push_back(rec);
            if (bulkrecs.size() >= (size_t)bulk_) {
              if (db_->get_bulk_binary(&bulkrecs) < 0) {
                dberrprint(db_, __LINE__, "DB::get_bulk_binary");
                err_ = true;
              }
              bulkrecs.clear();
            }
          } else {
            keys.push_back(key);
            if (keys.size() >= (size_t)bulk_) {
              std::map<std::string, std::string> recs;
              if (db_->get_bulk(keys, &recs) < 0) {
                dberrprint(db_, __LINE__, "DB::get_bulk");
                err_ = true;
              }
              keys.clear();
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        if (bulkrecs.size() > 0) {
          if (db_->get_bulk_binary(&bulkrecs) < 0) {
            dberrprint(db_, __LINE__, "DB::get_bulk_binary");
            err_ = true;
          }
          bulkrecs.clear();
        }
        if (keys.size() > 0) {
          std::map<std::string, std::string> recs;
          if (db_->get_bulk(keys, &recs) < 0) {
            dberrprint(db_, __LINE__, "DB::get_bulk");
            err_ = true;
          }
          keys.clear();
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool bin_;
      bool rnd_;
      int32_t bulk_;
      int32_t bopts_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, bin, rnd, bulk, bopts, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'g');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'r') {
    oprintf("removing records:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), bin_(false), rnd_(false), bulk_(0),
                 db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t bulk,
                     int32_t bopts, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        bin_ = bin;
        rnd_ = rnd;
        bulk_ = bulk;
        bopts_ = bopts;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        std::vector<std::string> keys;
        std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          std::string key(kbuf, ksiz);
          if (bin_) {
            kt::RemoteDB::BulkRecord rec = { 0, key, "", 0 };
            bulkrecs.push_back(rec);
            if (bulkrecs.size() >= (size_t)bulk_) {
              if (db_->remove_bulk_binary(bulkrecs, bopts_) < 0) {
                dberrprint(db_, __LINE__, "DB::remove_bulk_binary");
                err_ = true;
              }
              bulkrecs.clear();
            }
          } else {
            keys.push_back(key);
            if (keys.size() >= (size_t)bulk_) {
              if (db_->remove_bulk(keys) < 0) {
                dberrprint(db_, __LINE__, "DB::remove_bulk");
                err_ = true;
              }
              keys.clear();
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        if (bulkrecs.size() > 0) {
          if (db_->remove_bulk_binary(bulkrecs, bopts_) < 0) {
            dberrprint(db_, __LINE__, "DB::remove_bulk_binary");
            err_ = true;
          }
          bulkrecs.clear();
        }
        if (keys.size() > 0) {
          if (db_->remove_bulk(keys) < 0) {
            dberrprint(db_, __LINE__, "DB::remove_bulk");
            err_ = true;
          }
          keys.clear();
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool bin_;
      bool rnd_;
      int32_t bulk_;
      int32_t bopts_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, bin, rnd, bulk, bopts, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'r');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'e') {
    oprintf("performing mixed operations:\n");
    stime = kc::time();
    class Worker : public kc::Thread {
     public:
      Worker() : id_(0), rnum_(0), thnum_(0), bin_(false), rnd_(false), bulk_(0),
                 bopts_(0), db_(NULL), err_(false) {}
      void setparams(int32_t id, int64_t rnum, int32_t thnum, bool bin, bool rnd, int32_t bulk,
                     int32_t bopts, kt::RemoteDB* db) {
        id_ = id;
        rnum_ = rnum;
        thnum_ = thnum;
        bin_ = bin;
        rnd_ = rnd;
        bulk_ = bulk;
        bopts_ = bopts;
        db_ = db;
      }
      bool error() {
        return err_;
      }
     private:
      void run() {
        std::map<std::string, std::string> recs;
        std::vector<std::string> keys;
        std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(rnum_) + 1 : i));
          std::string key(kbuf, ksiz);
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          int32_t cmd;
          if (rnd_) {
            cmd = myrand(100);
          } else {
            cmd = i + id_;
            cmd = kc::hashmurmur(&cmd, sizeof(cmd)) % 100;
          }
          if (bin_) {
            kt::RemoteDB::BulkRecord rec = { 0, key, key, xt };
            bulkrecs.push_back(rec);
            if (bulkrecs.size() >= (size_t)bulk_) {
              if (cmd < 50) {
                if (db_->get_bulk_binary(&bulkrecs) < 0) {
                  dberrprint(db_, __LINE__, "DB::get_bulk_binary");
                  err_ = true;
                }
              } else if (cmd < 90) {
                if (db_->set_bulk_binary(bulkrecs, bopts_) < 0) {
                  dberrprint(db_, __LINE__, "DB::set_bulk_binary");
                  err_ = true;
                }
              } else if (cmd < 98) {
                if (db_->remove_bulk_binary(bulkrecs, bopts_) < 0) {
                  dberrprint(db_, __LINE__, "DB::remove_bulk_binary");
                  err_ = true;
                }
              } else {
                std::map<std::string, std::string> params, result;
                params[key] = key;
                if (!db_->play_script_binary("echo", params, &result, bopts_) &&
                    db_->error() != kt::RemoteDB::Error::NOIMPL &&
                    db_->error() != kt::RemoteDB::Error::LOGIC &&
                    db_->error() != kt::RemoteDB::Error::INTERNAL) {
                  dberrprint(db_, __LINE__, "DB::play_script_binary");
                  err_ = true;
                }
              }
              bulkrecs.clear();
            }
          } else {
            recs[key] = key;
            keys.push_back(key);
            if (keys.size() >= (size_t)bulk_) {
              if (cmd < 50) {
                std::map<std::string, std::string> recs;
                if (db_->get_bulk(keys, &recs) < 0) {
                  dberrprint(db_, __LINE__, "DB::get_bulk");
                  err_ = true;
                }
              } else if (cmd < 90) {
                if (db_->set_bulk(recs, xt) != (int64_t)recs.size()) {
                  dberrprint(db_, __LINE__, "DB::set_bulk");
                  err_ = true;
                }
              } else if (cmd < 98) {
                if (db_->remove_bulk(keys) < 0) {
                  dberrprint(db_, __LINE__, "DB::remove_bulk");
                  err_ = true;
                }
              } else {
                std::map<std::string, std::string> params, result;
                params[key] = key;
                if (!db_->play_script("echo", params, &result) &&
                    db_->error() != kt::RemoteDB::Error::NOIMPL &&
                    db_->error() != kt::RemoteDB::Error::LOGIC &&
                    db_->error() != kt::RemoteDB::Error::INTERNAL) {
                  dberrprint(db_, __LINE__, "DB::play_script_binary");
                  err_ = true;
                }
              }
              recs.clear();
              keys.clear();
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        if (bulkrecs.size() > 0) {
          if (db_->set_bulk_binary(bulkrecs, bopts_) < 0) {
            dberrprint(db_, __LINE__, "DB::set_bulk_binary");
            err_ = true;
          }
          bulkrecs.clear();
        }
        if (recs.size() > 0) {
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (db_->set_bulk(recs, xt) != (int64_t)recs.size()) {
            dberrprint(db_, __LINE__, "DB::set_bulk");
            err_ = true;
          }
          recs.clear();
        }
      }
      int32_t id_;
      int64_t rnum_;
      int32_t thnum_;
      bool bin_;
      bool rnd_;
      int32_t bulk_;
      int32_t bopts_;
      kt::RemoteDB* db_;
      bool err_;
    };
    Worker workers[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].setparams(i, rnum, thnum, bin, rnd, bulk, bopts, dbs + i);
      workers[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      workers[i].join();
      if (workers[i].error()) err = true;
    }
    etime = kc::time();
    dbmetaprint(dbs, mode == 'e');
    oprintf("time: %.3f\n", etime - stime);
  }
  oprintf("closing the database:\n");
  stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].close()) {
      dberrprint(dbs + i, __LINE__, "DB::close");
      err = true;
    }
  }
  etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  delete[] dbs;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform wicked command
static int32_t procwicked(int64_t rnum, int32_t thnum, int32_t itnum,
                          const char* host, int32_t port, double tout) {
  oprintf("<Wicked Test>\n  seed=%u  rnum=%lld  thnum=%d  itnum=%d  host=%s  port=%d"
          "  tout=%f\n\n", g_randseed, (long long)rnum, thnum, itnum, host, port, tout);
  bool err = false;
  for (int32_t itcnt = 1; itcnt <= itnum; itcnt++) {
    if (itnum > 1) oprintf("iteration %d:\n", itcnt);
    double stime = kc::time();
    kt::RemoteDB* dbs = new kt::RemoteDB[thnum];
    for (int32_t i = 0; i < thnum; i++) {
      if (!dbs[i].open(host, port, tout)) {
        dberrprint(dbs + i, __LINE__, "DB::open");
        err = true;
      }
    }
    class ThreadWicked : public kc::Thread {
     public:
      void setparams(int32_t id, kt::RemoteDB* db, int64_t rnum, int32_t thnum,
                     const char* lbuf) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        lbuf_ = lbuf;
        err_ = false;
      }
      bool error() {
        return err_;
      }
      void run() {
        kt::RemoteDB::Cursor* cur = db_->cursor();
        int64_t range = rnum_ * thnum_ / 2;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
          if (myrand(1000) == 0) {
            ksiz = myrand(RECBUFSIZ) + 1;
            if (myrand(2) == 0) {
              for (size_t j = 0; j < ksiz; j++) {
                kbuf[j] = j;
              }
            } else {
              for (size_t j = 0; j < ksiz; j++) {
                kbuf[j] = myrand(256);
              }
            }
          }
          const char* vbuf = kbuf;
          size_t vsiz = ksiz;
          if (myrand(10) == 0) {
            vbuf = lbuf_;
            vsiz = myrand(RECBUFSIZL) / (myrand(5) + 1);
          }
          int64_t xt = myrand(600);
          if (myrand(100) == 0) db_->set_signal_sending(kbuf, myrand(10) == 0);
          do {
            switch (myrand(16)) {
              case 0: {
                if (!db_->set(kbuf, ksiz, vbuf, vsiz, xt)) {
                  dberrprint(db_, __LINE__, "DB::set");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!db_->add(kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "DB::add");
                  err_ = true;
                }
                break;
              }
              case 2: {
                if (!db_->replace(kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "DB::replace");
                  err_ = true;
                }
                break;
              }
              case 3: {
                if (!db_->append(kbuf, ksiz, vbuf, vsiz, xt)) {
                  dberrprint(db_, __LINE__, "DB::append");
                  err_ = true;
                }
                break;
              }
              case 4: {
                if (myrand(2) == 0) {
                  int64_t num = myrand(rnum_);
                  int64_t orig = myrand(10) == 0 ? kc::INT64MIN : myrand(rnum_);
                  if (myrand(10) == 0) orig = orig == kc::INT64MIN ? kc::INT64MAX : -orig;
                  if (db_->increment(kbuf, ksiz, num, orig, xt) == kc::INT64MIN &&
                      db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::increment");
                    err_ = true;
                  }
                } else {
                  double num = myrand(rnum_ * 10) / (myrand(rnum_) + 1.0);
                  double orig = myrand(10) == 0 ? -kc::inf() : myrand(rnum_);
                  if (myrand(10) == 0) orig = -orig;
                  if (kc::chknan(db_->increment_double(kbuf, ksiz, num, orig, xt)) &&
                      db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::increment_double");
                    err_ = true;
                  }
                }
                break;
              }
              case 5: {
                if (!db_->cas(kbuf, ksiz, kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "DB::cas");
                  err_ = true;
                }
                break;
              }
              case 6: {
                if (!db_->remove(kbuf, ksiz) &&
                    db_->error() != kt::RemoteDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "DB::remove");
                  err_ = true;
                }
                break;
              }
              case 7: {
                if (myrand(2) == 0) {
                  int32_t vsiz = db_->check(kbuf, ksiz);
                  if (vsiz < 0 && db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::check");
                    err_ = true;
                  }
                } else {
                  size_t rsiz;
                  char* rbuf = db_->seize(kbuf, ksiz, &rsiz);
                  if (rbuf) {
                    delete[] rbuf;
                  } else if (db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::seize");
                    err_ = true;
                  }
                }
                break;
              }
              case 8: {
                if (myrand(10) == 0) {
                  if (myrand(4) == 0) {
                    if (!cur->jump_back(kbuf, ksiz) &&
                        db_->error() != kt::RemoteDB::Error::NOIMPL &&
                        db_->error() != kt::RemoteDB::Error::LOGIC) {
                      dberrprint(db_, __LINE__, "Cursor::jump_back");
                      err_ = true;
                    }
                  } else {
                    if (!cur->jump(kbuf, ksiz) &&
                        db_->error() != kt::RemoteDB::Error::LOGIC) {
                      dberrprint(db_, __LINE__, "Cursor::jump");
                      err_ = true;
                    }
                  }
                } else {
                  switch (myrand(3)) {
                    case 0: {
                      size_t vsiz = myrand(RECBUFSIZL) / (myrand(5) + 1);
                      if (!cur->set_value(lbuf_, vsiz, xt, myrand(2) == 0) &&
                          db_->error() != kt::RemoteDB::Error::LOGIC) {
                        dberrprint(db_, __LINE__, "Cursor::set_value");
                        err_ = true;
                      }
                      break;
                    }
                    case 1: {
                      if (!cur->remove() && db_->error() != kt::RemoteDB::Error::LOGIC) {
                        dberrprint(db_, __LINE__, "Cursor::remove");
                        err_ = true;
                      }
                      break;
                    }
                  }
                  if (myrand(5) > 0 && !cur->step() &&
                      db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "Cursor::step");
                    err_ = true;
                  }
                }
                if (myrand(rnum_ / 50 + 1) == 0) {
                  std::vector<std::string> keys;
                  std::string prefix(kbuf, ksiz > 0 ? ksiz - 1 : 0);
                  if (db_->match_prefix(prefix, &keys, myrand(10)) == -1) {
                    dberrprint(db_, __LINE__, "DB::match_prefix");
                    err_ = true;
                  }
                }
                if (myrand(rnum_ / 50 + 1) == 0) {
                  std::vector<std::string> keys;
                  std::string regex(kbuf, ksiz > 0 ? ksiz - 1 : 0);
                  if (db_->match_regex(regex, &keys, myrand(10)) == -1 &&
                      db_->error() != kt::RemoteDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::match_regex");
                    err_ = true;
                  }
                }
                if (myrand(rnum_ / 50 + 1) == 0) {
                  std::vector<std::string> keys;
                  std::string origin(kbuf, ksiz > 0 ? ksiz - 1 : 0);
                  if (db_->match_similar(origin, 3, myrand(2) == 0, &keys, myrand(10)) == -1) {
                    dberrprint(db_, __LINE__, "DB::match_similar");
                    err_ = true;
                  }
                }
                break;
              }
              case 9: {
                std::map<std::string, std::string> recs;
                int32_t num = myrand(4);
                for (int32_t i = 0; i < num; i++) {
                  ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
                  std::string key(kbuf, ksiz);
                  recs[key] = std::string(vbuf, vsiz);
                }
                if (db_->set_bulk(recs, xt) != (int64_t)recs.size()) {
                  dberrprint(db_, __LINE__, "DB::set_bulk");
                  err_ = true;
                }
                break;
              }
              case 10: {
                std::vector<std::string> keys;
                int32_t num = myrand(4);
                for (int32_t i = 0; i < num; i++) {
                  ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
                  keys.push_back(std::string(kbuf, ksiz));
                }
                if (db_->remove_bulk(keys) < 0) {
                  dberrprint(db_, __LINE__, "DB::remove_bulk");
                  err_ = true;
                }
                break;
              }
              case 11: {
                std::vector<std::string> keys;
                int32_t num = myrand(4);
                for (int32_t i = 0; i < num; i++) {
                  ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
                  keys.push_back(std::string(kbuf, ksiz));
                }
                std::map<std::string, std::string> recs;
                if (db_->get_bulk(keys, &recs) < 0) {
                  dberrprint(db_, __LINE__, "DB::get_bulk");
                  err_ = true;
                }
                break;
              }
              default: {
                if (myrand(100) == 0) db_->set_signal_waiting(kbuf, 1.0 / (myrand(1000) + 1));
                size_t rsiz;
                char* rbuf = db_->get(kbuf, ksiz, &rsiz);
                if (rbuf) {
                  delete[] rbuf;
                } else if (db_->error() != kt::RemoteDB::Error::LOGIC &&
                           db_->error() != kt::RemoteDB::Error::TIMEOUT) {
                  dberrprint(db_, __LINE__, "DB::get");
                  err_ = true;
                }
                break;
              }
            }
          } while (myrand(100) == 0);
          if (i == rnum_ / 2) {
            if (myrand(thnum_ * 4) == 0 && !db_->clear()) {
              dberrprint(db_, __LINE__, "DB::clear");
              err_ = true;
            } else {
              if (!db_->synchronize(false)) {
                dberrprint(db_, __LINE__, "DB::synchronize");
                err_ = true;
              }
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        delete cur;
      }
     private:
      int32_t id_;
      kt::RemoteDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      const char* lbuf_;
      bool err_;
    };
    char lbuf[RECBUFSIZL];
    std::memset(lbuf, '*', sizeof(lbuf));
    ThreadWicked threads[THREADMAX];
    for (int32_t i = 0; i < thnum; i++) {
      threads[i].setparams(i, dbs + i, rnum, thnum, lbuf);
      threads[i].start();
    }
    for (int32_t i = 0; i < thnum; i++) {
      threads[i].join();
      if (threads[i].error()) err = true;
    }
    dbmetaprint(dbs, itcnt == itnum);
    for (int32_t i = 0; i < thnum; i++) {
      if (!dbs[i].close()) {
        dberrprint(dbs + i, __LINE__, "DB::close");
        err = true;
      }
    }
    delete[] dbs;
    oprintf("time: %.3f\n", kc::time() - stime);
  }
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform usual command
static int32_t procusual(int64_t rnum, int32_t thnum, int32_t itnum,
                         const char* host, int32_t port, double tout,
                         int64_t kp, int64_t vs, int64_t xt, double iv) {
  oprintf("<Usual Test>\n  seed=%u  rnum=%lld  thnum=%d  itnum=%d  host=%s  port=%d"
          "  tout=%f  kp=%lld  vs=%lld  xt=%lld  iv=%f\n\n",
          g_randseed, (long long)rnum, thnum, itnum, host, port, tout, kp, vs, xt, iv);
  bool err = false;
  oprintf("opening the database:\n");
  double stime = kc::time();
  kt::RemoteDB* dbs = new kt::RemoteDB[thnum];
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].open(host, port, tout)) {
      dberrprint(dbs + i, __LINE__, "DB::open");
      err = true;
    }
  }
  double etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  oprintf("performing mixed operations:\n");
  stime = kc::time();
  class Worker : public kc::Thread {
   public:
    Worker() : id_(0), rnum_(0), thnum_(0), db_(NULL), err_(false) {}
    void setparams(int32_t id, int64_t rnum, int32_t thnum,
                   int64_t kp, int64_t vs, int64_t xt, double iv, kt::RemoteDB* db) {
      id_ = id;
      rnum_ = rnum;
      thnum_ = thnum;
      kp_ = kp;
      vs_ = vs;
      xt_ = xt;
      iv_ = iv;
      db_ = db;
    }
    bool error() {
      return err_;
    }
   private:
    void run() {
      size_t vsmax = vs_ > kc::INT8MAX ? vs_ : kc::INT8MAX;
      char* vbuf = new char[vsmax];
      for (size_t i = 0; i < vsmax; i++) {
        vbuf[i] = 'A' + myrand('Z' - 'A');
      }
      double slptime = 0;
      for (int64_t i = 1; !err_ && i <= rnum_; i++) {
        char kbuf[RECBUFSIZ];
        size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)myrand(kp_));
        std::memcpy(vbuf, kbuf, ksiz);
        int32_t cmd = myrand(100);
        if (cmd < 50) {
          size_t rsiz;
          char* rbuf = db_->get(kbuf, ksiz, &rsiz);
          if (rbuf) {
            delete[] rbuf;
          } else if (db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::get");
            err_ = true;
          }
        } else if (cmd < 60) {
          if (!db_->remove(kbuf, ksiz) && db_->error() != kt::RemoteDB::Error::LOGIC) {
            dberrprint(db_, __LINE__, "DB::remove");
            err_ = true;
          }
        } else {
          size_t vsiz = vs_ > 0 ? vs_ : myrand(kc::INT8MAX);
          int64_t xt = xt_ == 0 ? kc::UINT32MAX : xt_ < 0 ? myrand(-xt_) + 1 : xt_;
          if (!db_->set(kbuf, ksiz, vbuf, vsiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
        }
        if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
          oputchar('.');
          if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
        }
        if (i % kc::INT8MAX == 0) {
          for (size_t i = 0; i < vsmax; i++) {
            vbuf[i] = 'A' + myrand('Z' - 'A');
          }
        }
        if (iv_ > 0) {
          slptime += iv_;
          if (slptime >= 100) {
            Thread::sleep(slptime / 1000);
            slptime = 0;
          }
        }
      }
      delete[] vbuf;
    }
    int32_t id_;
    int64_t rnum_;
    int32_t thnum_;
    int64_t kp_;
    int64_t vs_;
    int64_t xt_;
    double iv_;
    kt::RemoteDB* db_;
    bool err_;
  };
  Worker workers[THREADMAX];
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].setparams(i, rnum, thnum, kp, vs, xt, iv, dbs + i);
    workers[i].start();
  }
  for (int32_t i = 0; i < thnum; i++) {
    workers[i].join();
    if (workers[i].error()) err = true;
  }
  etime = kc::time();
  dbmetaprint(dbs, true);
  oprintf("time: %.3f\n", etime - stime);
  oprintf("closing the database:\n");
  stime = kc::time();
  for (int32_t i = 0; i < thnum; i++) {
    if (!dbs[i].close()) {
      dberrprint(dbs + i, __LINE__, "DB::close");
      err = true;
    }
  }
  etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  delete[] dbs;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
