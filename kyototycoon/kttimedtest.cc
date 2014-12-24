/*************************************************************************************************
 * The test cases of the timed database
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


#include <kttimeddb.h>
#include "cmdcommon.h"


// global variables
const char* g_progname;                  // program name
uint32_t g_randseed;                     // random seed
int64_t g_memusage;                      // memory usage


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::TimedDB* db, int32_t line, const char* func);
static void dbmetaprint(kt::TimedDB* db, bool verbose);
static int32_t runorder(int argc, char** argv);
static int32_t runqueue(int argc, char** argv);
static int32_t runwicked(int argc, char** argv);
static int32_t runtran(int argc, char** argv);
static int32_t runmapred(int argc, char** argv);
static int32_t runmisc(int argc, char** argv);
static int32_t procorder(const char* path, int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         bool tran, int32_t oflags, const char* ulogpath, int64_t ulim,
                         uint16_t sid, uint16_t dbid, bool lv);
static int32_t procqueue(const char* path, int64_t rnum, int32_t thnum, int32_t itnum, bool rnd,
                         int32_t oflags, const char* ulogpath, int64_t ulim,
                         uint16_t sid, uint16_t dbid, bool lv);
static int32_t procwicked(const char* path, int64_t rnum, int32_t thnum, int32_t itnum,
                          int32_t oflags, const char* ulogpath, int64_t ulim,
                          uint16_t sid, uint16_t dbid, bool lv);
static int32_t proctran(const char* path, int64_t rnum, int32_t thnum, int32_t itnum, bool hard,
                        int32_t oflags, const char* ulogpath, int64_t ulim, uint16_t sid,
                        uint16_t dbid, bool lv);
static int32_t procmapred(const char* path, int64_t rnum, bool rnd, bool ru, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool lv, const char* tmpdir, int64_t dbnum,
                          int64_t clim, int64_t cbnum, int32_t opts);
static int32_t procmisc(const char* path);


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
  } else if (!std::strcmp(argv[1], "queue")) {
    rv = runqueue(argc, argv);
  } else if (!std::strcmp(argv[1], "wicked")) {
    rv = runwicked(argc, argv);
  } else if (!std::strcmp(argv[1], "tran")) {
    rv = runtran(argc, argv);
  } else if (!std::strcmp(argv[1], "mapred")) {
    rv = runmapred(argc, argv);
  } else if (!std::strcmp(argv[1], "misc")) {
    rv = runmisc(argc, argv);
  } else {
    usage();
  }
  if (rv != 0) {
    oprintf("FAILED: KCRNDSEED=%u PID=%ld", g_randseed, (long)kc::getpid());
    for (int32_t i = 0; i < argc; i++) {
      oprintf(" %s", argv[i]);
    }
    oprintf("\n\n");
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: test cases of the timed database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s order [-th num] [-rnd] [-set|-get|-getw|-rem|-etc] [-tran]"
          " [-oat|-oas|-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-lv] path rnum\n", g_progname);
  eprintf("  %s queue [-th num] [-it num] [-rnd] [-oat|-oas|-onl|-otl|-onr]"
          " [-ulog str] [-ulim num] [-sid num] [-dbid num] [-lv] path rnum\n", g_progname);
  eprintf("  %s wicked [-th num] [-it num] [-oat|-oas|-onl|-otl|-onr]"
          " [-ulog str] [-ulim num] [-sid num] [-dbid num] [-lv] path rnum\n", g_progname);
  eprintf("  %s tran [-th num] [-it num] [-hard] [-oat|-oas|-onl|-otl|-onr]"
          " [-ulog str] [-ulim num] [-sid num] [-dbid num] [-lv] path rnum\n", g_progname);
  eprintf("  %s mapred [-rnd] [-oat|-oas|-onl|-otl|-onr]"
          " [-ulog str] [-ulim num] [-sid num] [-dbid num] [-lv] [-tmp str]"
          " [-dbnum num] [-clim num] [-cbnum num] [-xnl] [-xpm] [-xpr] [-xpf] [-xnc]"
          " path rnum\n", g_progname);
  eprintf("  %s misc path\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print the error message of a database
static void dberrprint(kt::TimedDB* db, int32_t line, const char* func) {
  const kc::BasicDB::Error& err = db->error();
  oprintf("%s: %d: %s: %s: %d: %s: %s\n",
          g_progname, line, func, db->path().c_str(), err.code(), err.name(), err.message());
}


// print members of a database
static void dbmetaprint(kt::TimedDB* db, bool verbose) {
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
  const char* path = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  bool rnd = false;
  int32_t mode = 0;
  bool tran = false;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool lv = false;
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
      } else if (!std::strcmp(argv[i], "-getw")) {
        mode = 'w';
      } else if (!std::strcmp(argv[i], "-rem")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-etc")) {
        mode = 'e';
      } else if (!std::strcmp(argv[i], "-tran")) {
        tran = true;
      } else if (!std::strcmp(argv[i], "-oat")) {
        oflags |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        oflags |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-ulog")) {
        if (++i >= argc) usage();
        ulogpath = argv[i];
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-dbid")) {
        if (++i >= argc) usage();
        dbid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-lv")) {
        lv = true;
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
  int32_t rv = procorder(path, rnum, thnum, rnd, mode, tran, oflags,
                         ulogpath, ulim, sid, dbid, lv);
  return rv;
}


// parse arguments of queue command
static int32_t runqueue(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int32_t itnum = 1;
  bool rnd = false;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool lv = false;
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
      } else if (!std::strcmp(argv[i], "-rnd")) {
        rnd = true;
      } else if (!std::strcmp(argv[i], "-oat")) {
        oflags |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        oflags |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-ulog")) {
        if (++i >= argc) usage();
        ulogpath = argv[i];
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-dbid")) {
        if (++i >= argc) usage();
        dbid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-lv")) {
        lv = true;
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
  if (rnum < 1 || thnum < 1 || itnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procqueue(path, rnum, thnum, itnum, rnd, oflags, ulogpath, ulim, sid, dbid, lv);
  return rv;
}


// parse arguments of wicked command
static int32_t runwicked(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int32_t itnum = 1;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool lv = false;
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
      } else if (!std::strcmp(argv[i], "-oat")) {
        oflags |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        oflags |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-ulog")) {
        if (++i >= argc) usage();
        ulogpath = argv[i];
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-dbid")) {
        if (++i >= argc) usage();
        dbid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-lv")) {
        lv = true;
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
  if (rnum < 1 || thnum < 1 || itnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = procwicked(path, rnum, thnum, itnum, oflags, ulogpath, ulim, sid, dbid, lv);
  return rv;
}


// parse arguments of tran command
static int32_t runtran(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* rstr = NULL;
  int32_t thnum = 1;
  int32_t itnum = 1;
  bool hard = false;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool lv = false;
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
      } else if (!std::strcmp(argv[i], "-hard")) {
        hard = true;
      } else if (!std::strcmp(argv[i], "-oat")) {
        oflags |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        oflags |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-ulog")) {
        if (++i >= argc) usage();
        ulogpath = argv[i];
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-dbid")) {
        if (++i >= argc) usage();
        dbid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-lv")) {
        lv = true;
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
  if (rnum < 1 || thnum < 1 || itnum < 1) usage();
  if (thnum > THREADMAX) thnum = THREADMAX;
  int32_t rv = proctran(path, rnum, thnum, itnum, hard, oflags, ulogpath, ulim, sid, dbid, lv);
  return rv;
}


// parse arguments of mapred command
static int32_t runmapred(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* rstr = NULL;
  bool rnd = false;
  bool ru = false;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool lv = false;
  const char* tmpdir = "";
  int64_t dbnum = -1;
  int64_t clim = -1;
  int64_t cbnum = -1;
  int32_t opts = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-rnd")) {
        rnd = true;
      } else if (!std::strcmp(argv[i], "-ru")) {
        ru = true;
      } else if (!std::strcmp(argv[i], "-oat")) {
        oflags |= kc::BasicDB::OAUTOTRAN;
      } else if (!std::strcmp(argv[i], "-oas")) {
        oflags |= kc::BasicDB::OAUTOSYNC;
      } else if (!std::strcmp(argv[i], "-onl")) {
        oflags |= kc::BasicDB::ONOLOCK;
      } else if (!std::strcmp(argv[i], "-otl")) {
        oflags |= kc::BasicDB::OTRYLOCK;
      } else if (!std::strcmp(argv[i], "-onr")) {
        oflags |= kc::BasicDB::ONOREPAIR;
      } else if (!std::strcmp(argv[i], "-ulog")) {
        if (++i >= argc) usage();
        ulogpath = argv[i];
      } else if (!std::strcmp(argv[i], "-ulim")) {
        if (++i >= argc) usage();
        ulim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-dbid")) {
        if (++i >= argc) usage();
        dbid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-lv")) {
        lv = true;
      } else if (!std::strcmp(argv[i], "-tmp")) {
        if (++i >= argc) usage();
        tmpdir = argv[i];
      } else if (!std::strcmp(argv[i], "-dbnum")) {
        if (++i >= argc) usage();
        dbnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-clim")) {
        if (++i >= argc) usage();
        clim = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-cbnum")) {
        if (++i >= argc) usage();
        cbnum = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-xnl")) {
        opts |= kt::MapReduce::XNOLOCK;
      } else if (!std::strcmp(argv[i], "-xpm")) {
        opts |= kt::MapReduce::XPARAMAP;
      } else if (!std::strcmp(argv[i], "-xpr")) {
        opts |= kt::MapReduce::XPARARED;
      } else if (!std::strcmp(argv[i], "-xpf")) {
        opts |= kt::MapReduce::XPARAFLS;
      } else if (!std::strcmp(argv[i], "-xnc")) {
        opts |= kt::MapReduce::XNOCOMP;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!rstr) {
      rstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !rstr) usage();
  int64_t rnum = kc::atoix(rstr);
  if (rnum < 1) usage();
  int32_t rv = procmapred(path, rnum, rnd, ru, oflags, ulogpath, ulim, sid, dbid,
                          lv, tmpdir, dbnum, clim, cbnum, opts);
  return rv;
}


// parse arguments of misc command
static int32_t runmisc(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else      usage();
    } else if (!path) {
      argbrk = false;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procmisc(path);
  return rv;
}


// perform order command
static int32_t procorder(const char* path, int64_t rnum, int32_t thnum, bool rnd, int32_t mode,
                         bool tran, int32_t oflags, const char* ulogpath, int64_t ulim,
                         uint16_t sid, uint16_t dbid, bool lv) {
  oprintf("<In-order Test>\n  seed=%u  path=%s  rnum=%lld  thnum=%d  rnd=%d  mode=%d  tran=%d"
          "  oflags=%d  ulog=%s  ulim=%lld  sid=%u  dbid=%u  lv=%d\n\n",
          g_randseed, path, (long long)rnum, thnum, rnd, mode, tran, oflags,
          ulogpath ? ulogpath : "", (long long)ulim, sid, dbid, lv);
  bool err = false;
  kt::TimedDB db;
  oprintf("opening the database:\n");
  double stime = kc::time();
  db.tune_logger(stddblogger(g_progname, &std::cout),
                 lv ? kc::UINT32MAX : kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, __LINE__, "DB::tune_update_trigger");
        err = true;
      }
    } else {
      dberrprint(&db, __LINE__, "UpdateLogger::open");
      err = true;
    }
  }
  uint32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | kc::BasicDB::OTRUNCATE;
  if (mode == 'r') {
    omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
  } else if (mode == 'g' || mode == 'w') {
    omode = kc::BasicDB::OREADER;
  }
  if (!db.open(path, omode | oflags)) {
    dberrprint(&db, __LINE__, "DB::open");
    err = true;
  }
  double etime = kc::time();
  dbmetaprint(&db, false);
  oprintf("time: %.3f\n", etime - stime);
  if (mode == 0 || mode == 's' || mode == 'e') {
    oprintf("setting records:\n");
    stime = kc::time();
    class ThreadSet : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->set(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (rnd_ && i % 8 == 0) {
            switch (myrand(8)) {
              case 0: {
                if (!db_->set(kbuf, ksiz, kbuf, ksiz)) {
                  dberrprint(db_, __LINE__, "DB::set");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!db_->append(kbuf, ksiz, kbuf, ksiz)) {
                  dberrprint(db_, __LINE__, "DB::append");
                  err_ = true;
                }
                break;
              }
              case 2: {
                if (!db_->remove(kbuf, ksiz) && db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::remove");
                  err_ = true;
                }
                break;
              }
              case 3: {
                kt::TimedDB::Cursor* cur = db_->cursor();
                if (cur->jump(kbuf, ksiz)) {
                  switch (myrand(8)) {
                    default: {
                      size_t rsiz;
                      char* rbuf = cur->get_key(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_key");
                        err_ = true;
                      }
                      break;
                    }
                    case 1: {
                      size_t rsiz;
                      char* rbuf = cur->get_value(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_value");
                        err_ = true;
                      }
                      break;
                    }
                    case 2: {
                      size_t rksiz;
                      const char* rvbuf;
                      size_t rvsiz;
                      char* rkbuf = cur->get(&rksiz, &rvbuf, &rvsiz, NULL, myrand(10) == 0);
                      if (rkbuf) {
                        delete[] rkbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 3: {
                      std::string key, value;
                      if (!cur->get(&key, &value, NULL, myrand(10) == 0) &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 4: {
                      if (myrand(8) == 0 && !cur->remove() &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::remove");
                        err_ = true;
                      }
                      break;
                    }
                  }
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "Cursor::jump");
                  err_ = true;
                }
                delete cur;
                break;
              }
              default: {
                size_t vsiz;
                char* vbuf = db_->get(kbuf, ksiz, &vsiz);
                if (vbuf) {
                  delete[] vbuf;
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::get");
                  err_ = true;
                }
                break;
              }
            }
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      bool tran_;
    };
    ThreadSet threadsets[THREADMAX];
    if (thnum < 2) {
      threadsets[0].setparams(0, &db, rnum, thnum, rnd, tran);
      threadsets[0].run();
      if (threadsets[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadsets[i].setparams(i, &db, rnum, thnum, rnd, tran);
        threadsets[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadsets[i].join();
        if (threadsets[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, mode == 's');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("adding records:\n");
    stime = kc::time();
    class ThreadAdd : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->add(kbuf, ksiz, kbuf, ksiz, xt) &&
              db_->error() != kc::BasicDB::Error::DUPREC) {
            dberrprint(db_, __LINE__, "DB::add");
            err_ = true;
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      bool tran_;
    };
    ThreadAdd threadadds[THREADMAX];
    if (thnum < 2) {
      threadadds[0].setparams(0, &db, rnum, thnum, rnd, tran);
      threadadds[0].run();
      if (threadadds[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadadds[i].setparams(i, &db, rnum, thnum, rnd, tran);
        threadadds[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadadds[i].join();
        if (threadadds[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("appending records:\n");
    stime = kc::time();
    class ThreadAppend : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          int64_t xt = rnd_ ? myrand(600) + 1 : kc::INT64MAX;
          if (!db_->append(kbuf, ksiz, kbuf, ksiz, xt)) {
            dberrprint(db_, __LINE__, "DB::append");
            err_ = true;
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      bool tran_;
    };
    ThreadAppend threadappends[THREADMAX];
    if (thnum < 2) {
      threadappends[0].setparams(0, &db, rnum, thnum, rnd, tran);
      threadappends[0].run();
      if (threadappends[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadappends[i].setparams(i, &db, rnum, thnum, rnd, tran);
        threadappends[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadappends[i].join();
        if (threadappends[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'g' || mode == 'e') {
    oprintf("getting records:\n");
    stime = kc::time();
    class ThreadGet : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
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
          } else if (!rnd_ || db_->error() != kc::BasicDB::Error::NOREC) {
            dberrprint(db_, __LINE__, "DB::get");
            err_ = true;
          }
          if (rnd_ && i % 8 == 0) {
            switch (myrand(8)) {
              case 0: {
                if (!db_->set(kbuf, ksiz, kbuf, ksiz) &&
                    db_->error() != kc::BasicDB::Error::NOPERM) {
                  dberrprint(db_, __LINE__, "DB::set");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!db_->append(kbuf, ksiz, kbuf, ksiz) &&
                    db_->error() != kc::BasicDB::Error::NOPERM) {
                  dberrprint(db_, __LINE__, "DB::append");
                  err_ = true;
                }
                break;
              }
              case 2: {
                if (!db_->remove(kbuf, ksiz) &&
                    db_->error() != kc::BasicDB::Error::NOPERM &&
                    db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::remove");
                  err_ = true;
                }
                break;
              }
              case 3: {
                kt::TimedDB::Cursor* cur = db_->cursor();
                if (cur->jump(kbuf, ksiz)) {
                  switch (myrand(8)) {
                    default: {
                      size_t rsiz;
                      char* rbuf = cur->get_key(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_key");
                        err_ = true;
                      }
                      break;
                    }
                    case 1: {
                      size_t rsiz;
                      char* rbuf = cur->get_value(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_value");
                        err_ = true;
                      }
                      break;
                    }
                    case 2: {
                      size_t rksiz;
                      const char* rvbuf;
                      size_t rvsiz;
                      char* rkbuf = cur->get(&rksiz, &rvbuf, &rvsiz, NULL, myrand(10) == 0);
                      if (rkbuf) {
                        delete[] rkbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 3: {
                      std::string key, value;
                      if (!cur->get(&key, &value, NULL, myrand(10) == 0) &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 4: {
                      if (myrand(8) == 0 && !cur->remove() &&
                          db_->error() != kc::BasicDB::Error::NOPERM &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::remove");
                        err_ = true;
                      }
                      break;
                    }
                  }
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "Cursor::jump");
                  err_ = true;
                }
                delete cur;
                break;
              }
              default: {
                size_t vsiz;
                char* vbuf = db_->get(kbuf, ksiz, &vsiz);
                if (vbuf) {
                  delete[] vbuf;
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::get");
                  err_ = true;
                }
                break;
              }
            }
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      bool tran_;
    };
    ThreadGet threadgets[THREADMAX];
    if (thnum < 2) {
      threadgets[0].setparams(0, &db, rnum, thnum, rnd, tran);
      threadgets[0].run();
      if (threadgets[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadgets[i].setparams(i, &db, rnum, thnum, rnd, tran);
        threadgets[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadgets[i].join();
        if (threadgets[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, mode == 'g');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'w' || mode == 'e') {
    oprintf("getting records with a buffer:\n");
    stime = kc::time();
    class ThreadGetBuffer : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          char vbuf[RECBUFSIZ];
          int32_t vsiz = db_->get(kbuf, ksiz, vbuf, sizeof(vbuf));
          if (vsiz >= 0) {
            if (vsiz < (int32_t)ksiz || std::memcmp(vbuf, kbuf, ksiz)) {
              dberrprint(db_, __LINE__, "DB::get");
              err_ = true;
            }
          } else if (!rnd_ || db_->error() != kc::BasicDB::Error::NOREC) {
            dberrprint(db_, __LINE__, "DB::get");
            err_ = true;
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      bool tran_;
    };
    ThreadGetBuffer threadgetbuffers[THREADMAX];
    if (thnum < 2) {
      threadgetbuffers[0].setparams(0, &db, rnum, thnum, rnd, tran);
      threadgetbuffers[0].run();
      if (threadgetbuffers[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadgetbuffers[i].setparams(i, &db, rnum, thnum, rnd, tran);
        threadgetbuffers[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadgetbuffers[i].join();
        if (threadgetbuffers[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, mode == 'w');
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("traversing the database by the inner iterator:\n");
    stime = kc::time();
    int64_t cnt = db.count();
    class VisitorIterator : public kt::TimedDB::Visitor {
     public:
      explicit VisitorIterator(int64_t rnum, bool rnd) :
          rnum_(rnum), rnd_(rnd), cnt_(0), rbuf_() {
        std::memset(rbuf_, '+', sizeof(rbuf_));
      }
      int64_t cnt() {
        return cnt_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        cnt_++;
        const char* rv = NOP;
        switch (rnd_ ? myrand(7) : cnt_ % 7) {
          case 0: {
            rv = rbuf_;
            *sp = rnd_ ? myrand(sizeof(rbuf_)) : sizeof(rbuf_) / (cnt_ % 5 + 1);
            *xtp = rnd_ ? myrand(100) + 1 : kc::INT64MAX;
            break;
          }
          case 1: {
            rv = REMOVE;
            break;
          }
        }
        if (rnum_ > 250 && cnt_ % (rnum_ / 250) == 0) {
          oputchar('.');
          if (cnt_ == rnum_ || cnt_ % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)cnt_);
        }
        return rv;
      }
      int64_t rnum_;
      bool rnd_;
      int64_t cnt_;
      char rbuf_[RECBUFSIZ];
    } visitoriterator(rnum, rnd);
    if (tran && !db.begin_transaction(false)) {
      dberrprint(&db, __LINE__, "DB::begin_transaction");
      err = true;
    }
    if (!db.iterate(&visitoriterator, true)) {
      dberrprint(&db, __LINE__, "DB::iterate");
      err = true;
    }
    if (rnd) oprintf(" (end)\n");
    if (tran && !db.end_transaction(true)) {
      dberrprint(&db, __LINE__, "DB::end_transaction");
      err = true;
    }
    if (!rnd && visitoriterator.cnt() != cnt) {
      dberrprint(&db, __LINE__, "DB::iterate");
      err = true;
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("traversing the database by the outer cursor:\n");
    stime = kc::time();
    int64_t cnt = db.count();
    class VisitorCursor : public kt::TimedDB::Visitor {
     public:
      explicit VisitorCursor(int64_t rnum, bool rnd) :
          rnum_(rnum), rnd_(rnd), cnt_(0), rbuf_() {
        std::memset(rbuf_, '-', sizeof(rbuf_));
      }
      int64_t cnt() {
        return cnt_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        cnt_++;
        const char* rv = NOP;
        switch (rnd_ ? myrand(7) : cnt_ % 7) {
          case 0: {
            rv = rbuf_;
            *sp = rnd_ ? myrand(sizeof(rbuf_)) : sizeof(rbuf_) / (cnt_ % 5 + 1);
            *xtp = rnd_ ? myrand(100) + 1 : kc::INT64MAX;
            break;
          }
          case 1: {
            rv = REMOVE;
            break;
          }
        }
        if (rnum_ > 250 && cnt_ % (rnum_ / 250) == 0) {
          oputchar('.');
          if (cnt_ == rnum_ || cnt_ % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)cnt_);
        }
        return rv;
      }
      int64_t rnum_;
      bool rnd_;
      int64_t cnt_;
      char rbuf_[RECBUFSIZ];
    } visitorcursor(rnum, rnd);
    if (tran && !db.begin_transaction(false)) {
      dberrprint(&db, __LINE__, "DB::begin_transaction");
      err = true;
    }
    kt::TimedDB::Cursor cur(&db);
    if (!cur.jump() && db.error() != kc::BasicDB::Error::NOREC) {
      dberrprint(&db, __LINE__, "Cursor::jump");
      err = true;
    }
    kt::TimedDB::Cursor* paracur = db.cursor();
    int64_t range = rnum * thnum;
    while (!err && cur.accept(&visitorcursor, true, !rnd)) {
      if (rnd) {
        char kbuf[RECBUFSIZ];
        size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)myrand(range));
        switch (myrand(3)) {
          case 0: {
            if (!db.remove(kbuf, ksiz) && db.error() != kc::BasicDB::Error::NOREC) {
              dberrprint(&db, __LINE__, "DB::remove");
              err = true;
            }
            break;
          }
          case 1: {
            if (!paracur->jump(kbuf, ksiz) && db.error() != kc::BasicDB::Error::NOREC) {
              dberrprint(&db, __LINE__, "Cursor::jump");
              err = true;
            }
            break;
          }
          default: {
            if (!cur.step() && db.error() != kc::BasicDB::Error::NOREC) {
              dberrprint(&db, __LINE__, "Cursor::step");
              err = true;
            }
            break;
          }
        }
      }
    }
    if (db.error() != kc::BasicDB::Error::NOREC) {
      dberrprint(&db, __LINE__, "Cursor::accept");
      err = true;
    }
    oprintf(" (end)\n");
    delete paracur;
    if (tran && !db.end_transaction(true)) {
      dberrprint(&db, __LINE__, "DB::end_transaction");
      err = true;
    }
    if (!rnd && visitorcursor.cnt() != cnt) {
      dberrprint(&db, __LINE__, "Cursor::accept");
      err = true;
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e') {
    oprintf("synchronizing the database:\n");
    stime = kc::time();
    if (!db.synchronize(false, NULL)) {
      dberrprint(&db, __LINE__, "DB::synchronize");
      err = true;
    }
    class SyncProcessor : public kc::BasicDB::FileProcessor {
     public:
      explicit SyncProcessor(int64_t size) : size_(size) {}
     private:
      bool process(const std::string& path, int64_t count, int64_t size) {
        if (size != size_) return false;
        return true;
      }
      int64_t rnum_;
      bool rnd_;
      int64_t size_;
      int64_t msiz_;
    } syncprocessor(db.size());
    if (!db.synchronize(false, &syncprocessor)) {
      dberrprint(&db, __LINE__, "DB::synchronize");
      err = true;
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 'e' && db.size() < (256LL << 20)) {
    oprintf("dumping records into snapshot:\n");
    stime = kc::time();
    std::ostringstream ostrm;
    if (!db.dump_snapshot(&ostrm)) {
      dberrprint(&db, __LINE__, "DB::dump_snapshot");
      err = true;
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
    oprintf("loading records from snapshot:\n");
    stime = kc::time();
    int64_t cnt = db.count();
    if (rnd && myrand(2) == 0 && !db.clear()) {
      dberrprint(&db, __LINE__, "DB::clear");
      err = true;
    }
    const std::string& str = ostrm.str();
    std::istringstream istrm(str);
    if (!db.load_snapshot(&istrm) || db.count() != cnt) {
      dberrprint(&db, __LINE__, "DB::load_snapshot");
      err = true;
    }
    etime = kc::time();
    dbmetaprint(&db, false);
    oprintf("time: %.3f\n", etime - stime);
  }
  if (mode == 0 || mode == 'r' || mode == 'e') {
    oprintf("removing records:\n");
    stime = kc::time();
    class ThreadRemove : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
                     bool rnd, int32_t mode, bool tran) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        err_ = false;
        rnd_ = rnd;
        mode_ = mode;
        tran_ = tran;
      }
      bool error() {
        return err_;
      }
      void run() {
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          if (tran_ && !db_->begin_transaction(false)) {
            dberrprint(db_, __LINE__, "DB::begin_transaction");
            err_ = true;
          }
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%08lld",
                                     (long long)(rnd_ ? myrand(range) + 1 : base + i));
          if (!db_->remove(kbuf, ksiz) &&
              ((!rnd_ && mode_ != 'e') || db_->error() != kc::BasicDB::Error::NOREC)) {
            dberrprint(db_, __LINE__, "DB::remove");
            err_ = true;
          }
          if (rnd_ && i % 8 == 0) {
            switch (myrand(8)) {
              case 0: {
                if (!db_->set(kbuf, ksiz, kbuf, ksiz)) {
                  dberrprint(db_, __LINE__, "DB::set");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!db_->append(kbuf, ksiz, kbuf, ksiz)) {
                  dberrprint(db_, __LINE__, "DB::append");
                  err_ = true;
                }
                break;
              }
              case 2: {
                if (!db_->remove(kbuf, ksiz) &&
                    db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::remove");
                  err_ = true;
                }
                break;
              }
              case 3: {
                kt::TimedDB::Cursor* cur = db_->cursor();
                if (cur->jump(kbuf, ksiz)) {
                  switch (myrand(8)) {
                    default: {
                      size_t rsiz;
                      char* rbuf = cur->get_key(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_key");
                        err_ = true;
                      }
                      break;
                    }
                    case 1: {
                      size_t rsiz;
                      char* rbuf = cur->get_value(&rsiz, myrand(10) == 0);
                      if (rbuf) {
                        delete[] rbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get_value");
                        err_ = true;
                      }
                      break;
                    }
                    case 2: {
                      size_t rksiz;
                      const char* rvbuf;
                      size_t rvsiz;
                      char* rkbuf = cur->get(&rksiz, &rvbuf, &rvsiz, NULL, myrand(10) == 0);
                      if (rkbuf) {
                        delete[] rkbuf;
                      } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 3: {
                      std::string key, value;
                      if (!cur->get(&key, &value, NULL, myrand(10) == 0) &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::get");
                        err_ = true;
                      }
                      break;
                    }
                    case 4: {
                      if (myrand(8) == 0 && !cur->remove() &&
                          db_->error() != kc::BasicDB::Error::NOREC) {
                        dberrprint(db_, __LINE__, "Cursor::remove");
                        err_ = true;
                      }
                      break;
                    }
                  }
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "Cursor::jump");
                  err_ = true;
                }
                delete cur;
                break;
              }
              default: {
                size_t vsiz;
                char* vbuf = db_->get(kbuf, ksiz, &vsiz);
                if (vbuf) {
                  delete[] vbuf;
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::get");
                  err_ = true;
                }
                break;
              }
            }
          }
          if (tran_ && !db_->end_transaction(true)) {
            dberrprint(db_, __LINE__, "DB::end_transaction");
            err_ = true;
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool err_;
      bool rnd_;
      int32_t mode_;
      bool tran_;
    };
    ThreadRemove threadremoves[THREADMAX];
    if (thnum < 2) {
      threadremoves[0].setparams(0, &db, rnum, thnum, rnd, mode, tran);
      threadremoves[0].run();
      if (threadremoves[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threadremoves[i].setparams(i, &db, rnum, thnum, rnd, mode, tran);
        threadremoves[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threadremoves[i].join();
        if (threadremoves[i].error()) err = true;
      }
    }
    etime = kc::time();
    dbmetaprint(&db, mode == 'r' || mode == 'e');
    oprintf("time: %.3f\n", etime - stime);
  }
  oprintf("closing the database:\n");
  stime = kc::time();
  if (!db.close()) {
    dberrprint(&db, __LINE__, "DB::close");
    err = true;
  }
  if (ulogpath && !ulog.close()) {
    dberrprint(&db, __LINE__, "UpdateLogger::close");
    err = true;
  }
  etime = kc::time();
  oprintf("time: %.3f\n", etime - stime);
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform queue command
static int32_t procqueue(const char* path, int64_t rnum, int32_t thnum, int32_t itnum, bool rnd,
                         int32_t oflags, const char* ulogpath, int64_t ulim,
                         uint16_t sid, uint16_t dbid, bool lv) {
  oprintf("<Queue Test>\n  seed=%u  path=%s  rnum=%lld  thnum=%d  itnum=%d  rnd=%d"
          "  oflags=%d  ulog=%s  ulim=%lld  sid=%u  dbid=%u  lv=%d\n\n",
          g_randseed, path, (long long)rnum, thnum, itnum, rnd, oflags,
          ulogpath ? ulogpath : "", (long long)ulim, sid, dbid, lv);
  bool err = false;
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cout),
                 lv ? kc::UINT32MAX : kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, __LINE__, "DB::tune_update_trigger");
        err = true;
      }
    } else {
      dberrprint(&db, __LINE__, "UpdateLogger::open");
      err = true;
    }
  }
  for (int32_t itcnt = 1; itcnt <= itnum; itcnt++) {
    if (itnum > 1) oprintf("iteration %d:\n", itcnt);
    double stime = kc::time();
    uint32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
    if (itcnt == 1) omode |= kc::BasicDB::OTRUNCATE;
    if (!db.open(path, omode)) {
      dberrprint(&db, __LINE__, "DB::open");
      err = true;
    }
    class ThreadQueue : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum, bool rnd,
                     int64_t width) {
        id_ = id;
        db_ = db;
        rnum_ = rnum;
        thnum_ = thnum;
        rnd_ = rnd;
        width_ = width;
        err_ = false;
      }
      bool error() {
        return err_;
      }
      void run() {
        kt::TimedDB::Cursor* cur = db_->cursor();
        int64_t base = id_ * rnum_;
        int64_t range = rnum_ * thnum_;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          char kbuf[RECBUFSIZ];
          size_t ksiz = std::sprintf(kbuf, "%010lld", (long long)(base + i));
          if (!db_->set(kbuf, ksiz, kbuf, ksiz)) {
            dberrprint(db_, __LINE__, "DB::set");
            err_ = true;
          }
          if (rnd_) {
            if (myrand(width_ / 2) == 0) {
              if (!cur->jump() && db_->error() != kc::BasicDB::Error::NOREC) {
                dberrprint(db_, __LINE__, "Cursor::jump");
                err_ = true;
              }
              ksiz = std::sprintf(kbuf, "%010lld", (long long)myrand(range) + 1);
              switch (myrand(10)) {
                case 0: {
                  if (!db_->set(kbuf, ksiz, kbuf, ksiz)) {
                    dberrprint(db_, __LINE__, "DB::set");
                    err_ = true;
                  }
                  break;
                }
                case 1: {
                  if (!db_->append(kbuf, ksiz, kbuf, ksiz)) {
                    dberrprint(db_, __LINE__, "DB::append");
                    err_ = true;
                  }
                  break;
                }
                case 2: {
                  if (!db_->remove(kbuf, ksiz) &&
                      db_->error() != kc::BasicDB::Error::NOREC) {
                    dberrprint(db_, __LINE__, "DB::remove");
                    err_ = true;
                  }
                  break;
                }
              }
              int64_t dnum = myrand(width_) + 2;
              for (int64_t j = 0; j < dnum; j++) {
                if (!cur->remove() && db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "Cursor::remove");
                  err_ = true;
                }
              }
            }
          } else {
            if (i > width_) {
              if (!cur->jump() && db_->error() != kc::BasicDB::Error::NOREC) {
                dberrprint(db_, __LINE__, "Cursor::jump");
                err_ = true;
              }
              if (!cur->remove() && db_->error() != kc::BasicDB::Error::NOREC) {
                dberrprint(db_, __LINE__, "Cursor::remove");
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
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      bool rnd_;
      int64_t width_;
      bool err_;
    };
    int64_t width = rnum / 10;
    ThreadQueue threads[THREADMAX];
    if (thnum < 2) {
      threads[0].setparams(0, &db, rnum, thnum, rnd, width);
      threads[0].run();
      if (threads[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].setparams(i, &db, rnum, thnum, rnd, width);
        threads[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].join();
        if (threads[i].error()) err = true;
      }
    }
    int64_t count = db.count();
    if (!rnd && itcnt == 1 && count != width * thnum) {
      dberrprint(&db, __LINE__, "DB::count");
      err = true;
    }
    if ((rnd ? (myrand(2) == 0) : itcnt == itnum) && count > 0) {
      kt::TimedDB::Cursor* cur = db.cursor();
      if (!cur->jump()) {
        dberrprint(&db, __LINE__, "Cursor::jump");
        err = true;
      }
      for (int64_t i = 1; i <= count; i++) {
        if (!cur->remove()) {
          dberrprint(&db, __LINE__, "Cursor::jump");
          err = true;
        }
        if (rnum > 250 && i % (rnum / 250) == 0) {
          oputchar('.');
          if (i == rnum || i % (rnum / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
        }
      }
      if (rnd) oprintf(" (end)\n");
      delete cur;
      if (db.count() != 0) {
        dberrprint(&db, __LINE__, "DB::count");
        err = true;
      }
    }
    dbmetaprint(&db, itcnt == itnum);
    if (!db.close()) {
      dberrprint(&db, __LINE__, "DB::close");
      err = true;
    }
    oprintf("time: %.3f\n", kc::time() - stime);
  }
  if (ulogpath && !ulog.close()) {
    dberrprint(&db, __LINE__, "UpdateLogger::close");
    err = true;
  }
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform wicked command
static int32_t procwicked(const char* path, int64_t rnum, int32_t thnum, int32_t itnum,
                          int32_t oflags, const char* ulogpath, int64_t ulim,
                          uint16_t sid, uint16_t dbid, bool lv) {
  oprintf("<Wicked Test>\n  seed=%u  path=%s  rnum=%lld  thnum=%d  itnum=%d"
          "  oflags=%d  ulog=%s  ulim=%lld  sid=%u  dbid=%u  lv=%d\n\n",
          g_randseed, path, (long long)rnum, thnum, itnum, oflags,
          ulogpath ? ulogpath : "", (long long)ulim, sid, dbid, lv);
  bool err = false;
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cout),
                 lv ? kc::UINT32MAX : kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, __LINE__, "DB::tune_update_trigger");
        err = true;
      }
    } else {
      dberrprint(&db, __LINE__, "UpdateLogger::open");
      err = true;
    }
  }
  for (int32_t itcnt = 1; itcnt <= itnum; itcnt++) {
    if (itnum > 1) oprintf("iteration %d:\n", itcnt);
    double stime = kc::time();
    uint32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
    if (itcnt == 1) omode |= kc::BasicDB::OTRUNCATE;
    if (!db.open(path, omode | oflags)) {
      dberrprint(&db, __LINE__, "DB::open");
      err = true;
    }
    class ThreadWicked : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, int64_t rnum, int32_t thnum,
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
        kt::TimedDB::Cursor* cur = db_->cursor();
        int64_t range = rnum_ * thnum_ / 2;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          bool tran = myrand(100) == 0;
          if (tran) {
            if (myrand(2) == 0) {
              if (!db_->begin_transaction(myrand(rnum_) == 0)) {
                if (db_->error() != kc::BasicDB::Error::NOIMPL) {
                  dberrprint(db_, __LINE__, "DB::begin_transaction");
                  tran = false;
                  err_ = true;
                }
                tran = false;
              }
            } else {
              if (!db_->begin_transaction_try(myrand(rnum_) == 0)) {
                if (db_->error() != kc::BasicDB::Error::LOGIC &&
                    db_->error() != kc::BasicDB::Error::NOIMPL) {
                  dberrprint(db_, __LINE__, "DB::begin_transaction_try");
                  err_ = true;
                }
                tran = false;
              }
            }
          }
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
          do {
            switch (myrand(10)) {
              case 0: {
                if (!db_->set(kbuf, ksiz, vbuf, vsiz, xt)) {
                  dberrprint(db_, __LINE__, "DB::set");
                  err_ = true;
                }
                break;
              }
              case 1: {
                if (!db_->add(kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kc::BasicDB::Error::DUPREC) {
                  dberrprint(db_, __LINE__, "DB::add");
                  err_ = true;
                }
                break;
              }
              case 2: {
                if (!db_->replace(kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kc::BasicDB::Error::NOREC) {
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
                      db_->error() != kc::BasicDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::increment");
                    err_ = true;
                  }
                } else {
                  double num = myrand(rnum_ * 10) / (myrand(rnum_) + 1.0);
                  double orig = myrand(10) == 0 ? -kc::inf() : myrand(rnum_);
                  if (myrand(10) == 0) orig = -orig;
                  if (kc::chknan(db_->increment_double(kbuf, ksiz, num, orig, xt)) &&
                      db_->error() != kc::BasicDB::Error::LOGIC) {
                    dberrprint(db_, __LINE__, "DB::increment_double");
                    err_ = true;
                  }
                }
                break;
              }
              case 5: {
                if (!db_->cas(kbuf, ksiz, kbuf, ksiz, vbuf, vsiz, xt) &&
                    db_->error() != kc::BasicDB::Error::LOGIC) {
                  dberrprint(db_, __LINE__, "DB::cas");
                  err_ = true;
                }
                break;
              }
              case 6: {
                if (!db_->remove(kbuf, ksiz) &&
                    db_->error() != kc::BasicDB::Error::NOREC) {
                  dberrprint(db_, __LINE__, "DB::remove");
                  err_ = true;
                }
                break;
              }
              case 7: {
                if (myrand(2) == 0) {
                  int32_t vsiz = db_->check(kbuf, ksiz);
                  if (vsiz < 0 && db_->error() != kc::BasicDB::Error::NOREC) {
                    dberrprint(db_, __LINE__, "DB::check");
                    err_ = true;
                  }
                } else {
                  size_t rsiz;
                  char* rbuf = db_->seize(kbuf, ksiz, &rsiz);
                  if (rbuf) {
                    delete[] rbuf;
                  } else if (db_->error() != kc::BasicDB::Error::NOREC) {
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
                        db_->error() != kc::BasicDB::Error::NOIMPL &&
                        db_->error() != kc::BasicDB::Error::NOREC) {
                      dberrprint(db_, __LINE__, "Cursor::jump_back");
                      err_ = true;
                    }
                  } else {
                    if (!cur->jump(kbuf, ksiz) &&
                        db_->error() != kc::BasicDB::Error::NOREC) {
                      dberrprint(db_, __LINE__, "Cursor::jump");
                      err_ = true;
                    }
                  }
                } else {
                  class VisitorImpl : public kt::TimedDB::Visitor {
                   public:
                    explicit VisitorImpl(const char* lbuf) : lbuf_(lbuf) {}
                   private:
                    const char* visit_full(const char* kbuf, size_t ksiz,
                                           const char* vbuf, size_t vsiz,
                                           size_t* sp, int64_t* xtp) {
                      const char* rv = NOP;
                      switch (myrand(3)) {
                        case 0: {
                          rv = lbuf_;
                          *sp = myrand(RECBUFSIZL) / (myrand(5) + 1);
                          *xtp = myrand(100) + 1;
                          break;
                        }
                        case 1: {
                          rv = REMOVE;
                          break;
                        }
                      }
                      return rv;
                    }
                    const char* lbuf_;
                  } visitor(lbuf_);
                  if (!cur->accept(&visitor, true, myrand(2) == 0) &&
                      db_->error() != kc::BasicDB::Error::NOREC) {
                    dberrprint(db_, __LINE__, "Cursor::accept");
                    err_ = true;
                  }
                  if (myrand(5) > 0 && !cur->step() &&
                      db_->error() != kc::BasicDB::Error::NOREC) {
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
                      db_->error() != kc::BasicDB::Error::LOGIC) {
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
              default: {
                size_t rsiz;
                char* rbuf = db_->get(kbuf, ksiz, &rsiz);
                if (rbuf) {
                  delete[] rbuf;
                } else if (db_->error() != kc::BasicDB::Error::NOREC) {
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
              class SyncProcessor : public kc::BasicDB::FileProcessor {
               private:
                bool process(const std::string& path, int64_t count, int64_t size) {
                  yield();
                  return true;
                }
              } syncprocessor;
              if (!db_->synchronize(false, &syncprocessor)) {
                dberrprint(db_, __LINE__, "DB::synchronize");
                err_ = true;
              }
            }
          }
          if (tran) {
            yield();
            if (!db_->end_transaction(myrand(10) > 0)) {
              dberrprint(db_, __LINE__, "DB::end_transactin");
              err_ = true;
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
      kt::TimedDB* db_;
      int64_t rnum_;
      int32_t thnum_;
      const char* lbuf_;
      bool err_;
    };
    char lbuf[RECBUFSIZL];
    std::memset(lbuf, '*', sizeof(lbuf));
    ThreadWicked threads[THREADMAX];
    if (thnum < 2) {
      threads[0].setparams(0, &db, rnum, thnum, lbuf);
      threads[0].run();
      if (threads[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].setparams(i, &db, rnum, thnum, lbuf);
        threads[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].join();
        if (threads[i].error()) err = true;
      }
    }
    dbmetaprint(&db, itcnt == itnum);
    if (!db.close()) {
      dberrprint(&db, __LINE__, "DB::close");
      err = true;
    }
    oprintf("time: %.3f\n", kc::time() - stime);
  }
  if (ulogpath && !ulog.close()) {
    dberrprint(&db, __LINE__, "UpdateLogger::close");
    err = true;
  }
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform tran command
static int32_t proctran(const char* path, int64_t rnum, int32_t thnum, int32_t itnum, bool hard,
                        int32_t oflags, const char* ulogpath, int64_t ulim, uint16_t sid,
                        uint16_t dbid, bool lv) {
  oprintf("<Transaction Test>\n  seed=%u  path=%s  rnum=%lld  thnum=%d  itnum=%d  hard=%d"
          "  oflags=%d  ulog=%s  ulim=%lld  sid=%u  dbid=%u  lv=%d\n\n",
          g_randseed, path, (long long)rnum, thnum, itnum, hard, oflags,
          ulogpath ? ulogpath : "", (long long)ulim, sid, dbid, lv);
  bool err = false;
  kt::TimedDB db;
  kt::TimedDB paradb;
  db.tune_logger(stddblogger(g_progname, &std::cout),
                 lv ? kc::UINT32MAX : kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  paradb.tune_logger(stddblogger(g_progname, &std::cout), lv ? kc::UINT32MAX :
                     kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, __LINE__, "DB::tune_update_trigger");
        err = true;
      }
    } else {
      dberrprint(&db, __LINE__, "UpdateLogger::open");
      err = true;
    }
  }
  for (int32_t itcnt = 1; itcnt <= itnum; itcnt++) {
    oprintf("iteration %d updating:\n", itcnt);
    double stime = kc::time();
    uint32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE;
    if (itcnt == 1) omode |= kc::BasicDB::OTRUNCATE;
    if (!db.open(path, omode | oflags)) {
      dberrprint(&db, __LINE__, "DB::open");
      err = true;
    }
    std::string parapath = db.path() + "-para.kch";
    if (!paradb.open(parapath, omode)) {
      dberrprint(&paradb, __LINE__, "DB::open");
      err = true;
    }
    class ThreadTran : public kc::Thread {
     public:
      void setparams(int32_t id, kt::TimedDB* db, kt::TimedDB* paradb, int64_t rnum,
                     int32_t thnum, bool hard, const char* lbuf) {
        id_ = id;
        db_ = db;
        paradb_ = paradb;
        rnum_ = rnum;
        thnum_ = thnum;
        hard_ = hard;
        lbuf_ = lbuf;
        err_ = false;
      }
      bool error() {
        return err_;
      }
      void run() {
        kt::TimedDB::Cursor* cur = db_->cursor();
        int64_t range = rnum_ * thnum_;
        char kbuf[RECBUFSIZ];
        size_t ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
        if (!cur->jump(kbuf, ksiz) && db_->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db_, __LINE__, "Cursor::jump");
          err_ = true;
        }
        bool tran = true;
        if (!db_->begin_transaction(hard_)) {
          dberrprint(db_, __LINE__, "DB::begin_transaction");
          tran = false;
          err_ = true;
        }
        bool commit = myrand(10) > 0;
        for (int64_t i = 1; !err_ && i <= rnum_; i++) {
          ksiz = std::sprintf(kbuf, "%lld", (long long)(myrand(range) + 1));
          const char* vbuf = kbuf;
          size_t vsiz = ksiz;
          if (myrand(10) == 0) {
            vbuf = lbuf_;
            vsiz = myrand(RECBUFSIZL) / (myrand(5) + 1);
          }
          class VisitorImpl : public kt::TimedDB::Visitor {
           public:
            explicit VisitorImpl(const char* vbuf, size_t vsiz, kt::TimedDB* paradb) :
                vbuf_(vbuf), vsiz_(vsiz), paradb_(paradb) {}
           private:
            const char* visit_full(const char* kbuf, size_t ksiz,
                                   const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
              return visit_empty(kbuf, ksiz, sp, xtp);
            }
            const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
              const char* rv = NOP;
              switch (myrand(3)) {
                case 0: {
                  rv = vbuf_;
                  *sp = vsiz_;
                  *xtp = kc::INT64MAX;
                  if (paradb_) paradb_->set(kbuf, ksiz, vbuf_, vsiz_);
                  break;
                }
                case 1: {
                  rv = REMOVE;
                  if (paradb_) paradb_->remove(kbuf, ksiz);
                  break;
                }
              }
              return rv;
            }
            const char* vbuf_;
            size_t vsiz_;
            kt::TimedDB* paradb_;
          } visitor(vbuf, vsiz, !tran || commit ? paradb_ : NULL);
          if (myrand(4) == 0) {
            if (!cur->accept(&visitor, true, myrand(2) == 0) &&
                db_->error() != kc::BasicDB::Error::NOREC) {
              dberrprint(db_, __LINE__, "Cursor::accept");
              err_ = true;
            }
          } else {
            if (!db_->accept(kbuf, ksiz, &visitor, true)) {
              dberrprint(db_, __LINE__, "DB::accept");
              err_ = true;
            }
          }
          if (tran && myrand(100) == 0) {
            if (db_->end_transaction(commit)) {
              yield();
              if (!db_->begin_transaction(hard_)) {
                dberrprint(db_, __LINE__, "DB::begin_transaction");
                tran = false;
                err_ = true;
              }
            } else {
              dberrprint(db_, __LINE__, "DB::end_transaction");
              err_ = true;
            }
          }
          if (id_ < 1 && rnum_ > 250 && i % (rnum_ / 250) == 0) {
            oputchar('.');
            if (i == rnum_ || i % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)i);
          }
        }
        if (tran && !db_->end_transaction(commit)) {
          dberrprint(db_, __LINE__, "DB::end_transaction");
          err_ = true;
        }
        delete cur;
      }
     private:
      int32_t id_;
      kt::TimedDB* db_;
      kt::TimedDB* paradb_;
      int64_t rnum_;
      int32_t thnum_;
      bool hard_;
      const char* lbuf_;
      bool err_;
    };
    char lbuf[RECBUFSIZL];
    std::memset(lbuf, '*', sizeof(lbuf));
    ThreadTran threads[THREADMAX];
    if (thnum < 2) {
      threads[0].setparams(0, &db, &paradb, rnum, thnum, hard, lbuf);
      threads[0].run();
      if (threads[0].error()) err = true;
    } else {
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].setparams(i, &db, &paradb, rnum, thnum, hard, lbuf);
        threads[i].start();
      }
      for (int32_t i = 0; i < thnum; i++) {
        threads[i].join();
        if (threads[i].error()) err = true;
      }
    }
    oprintf("iteration %d checking:\n", itcnt);
    if (db.count() != paradb.count()) {
      dberrprint(&db, __LINE__, "DB::count");
      err = true;
    }
    class VisitorImpl : public kt::TimedDB::Visitor {
     public:
      explicit VisitorImpl(int64_t rnum, kt::TimedDB* paradb) :
          rnum_(rnum), paradb_(paradb), err_(false), cnt_(0) {}
      bool error() {
        return err_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        cnt_++;
        size_t rsiz;
        char* rbuf = paradb_->get(kbuf, ksiz, &rsiz);
        if (rbuf) {
          delete[] rbuf;
        } else {
          dberrprint(paradb_, __LINE__, "DB::get");
          err_ = true;
        }
        if (rnum_ > 250 && cnt_ % (rnum_ / 250) == 0) {
          oputchar('.');
          if (cnt_ == rnum_ || cnt_ % (rnum_ / 10) == 0) oprintf(" (%08lld)\n", (long long)cnt_);
        }
        return NOP;
      }
      int64_t rnum_;
      kt::TimedDB* paradb_;
      bool err_;
      int64_t cnt_;
    } visitor(rnum, &paradb), paravisitor(rnum, &db);
    if (!db.iterate(&visitor, false)) {
      dberrprint(&db, __LINE__, "DB::iterate");
      err = true;
    }
    oprintf(" (end)\n");
    if (visitor.error()) err = true;
    if (!paradb.iterate(&paravisitor, false)) {
      dberrprint(&db, __LINE__, "DB::iterate");
      err = true;
    }
    oprintf(" (end)\n");
    if (paravisitor.error()) err = true;
    if (!paradb.close()) {
      dberrprint(&paradb, __LINE__, "DB::close");
      err = true;
    }
    dbmetaprint(&db, itcnt == itnum);
    if (!db.close()) {
      dberrprint(&db, __LINE__, "DB::close");
      err = true;
    }
    oprintf("time: %.3f\n", kc::time() - stime);
  }
  if (ulogpath && !ulog.close()) {
    dberrprint(&db, __LINE__, "UpdateLogger::close");
    err = true;
  }
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform mapred command
static int32_t procmapred(const char* path, int64_t rnum, bool rnd, bool ru, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool lv, const char* tmpdir, int64_t dbnum,
                          int64_t clim, int64_t cbnum, int32_t opts) {
  oprintf("<MapReduce Test>\n  seed=%u  path=%s  rnum=%lld  rnd=%d  ru=%d  oflags=%d"
          "  ulog=%s  ulim=%lld  sid=%u  dbid=%u  lv=%d  tmp=%s  dbnum=%lld"
          "  clim=%lld  cbnum=%lld  opts=%d\n\n",
          g_randseed, path, (long long)rnum, rnd, ru, oflags,
          ulogpath ? ulogpath : "", (long long)ulim, sid, dbid, lv,
          tmpdir, (long long)dbnum, (long long)clim, (long long)cbnum, opts);
  bool err = false;
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cout),
                 lv ? kc::UINT32MAX : kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR);
  double stime = kc::time();
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, __LINE__, "DB::tune_update_trigger");
        err = true;
      }
    } else {
      dberrprint(&db, __LINE__, "UpdateLogger::open");
      err = true;
    }
  }
  uint32_t omode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | kc::BasicDB::OTRUNCATE;
  if (ru) omode = kc::PolyDB::OREADER;
  if (!db.open(path, omode | oflags)) {
    dberrprint(&db, __LINE__, "DB::open");
    err = true;
  }
  class MapReduceImpl : public kt::MapReduce {
   public:
    MapReduceImpl() : mapcnt_(0), redcnt_(0), lock_() {}
    bool map(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
      mapcnt_.add(1);
      return emit(vbuf, vsiz, kbuf, ksiz);
    }
    bool reduce(const char* kbuf, size_t ksiz, ValueIterator* iter) {
      const char* vbuf;
      size_t vsiz;
      while ((vbuf = iter->next(&vsiz)) != NULL) {
        redcnt_.add(1);
      }
      return true;
    }
    bool preprocess() {
      oprintf("preprocessing:\n");
      if (!emit("pre", 3, "process", 7)) return false;
      if (!emit("PROCESS", 7, "PRE", 3)) return false;
      return true;
    }
    bool midprocess() {
      oprintf("midprocessing:\n");
      if (!emit("mid", 3, "process", 7)) return false;
      if (!emit("PROCESS", 7, "MID", 3)) return false;
      return true;
    }
    bool postprocess() {
      oprintf("postprocessing:\n");
      return true;
    }
    bool log(const char* name, const char* message) {
      kc::ScopedMutex lock(&lock_);
      oprintf("%s: %s\n", name, message);
      return true;
    }
    int64_t mapcnt() {
      return mapcnt_;
    }
    int64_t redcnt() {
      return redcnt_;
    }
   private:
    kc::AtomicInt64 mapcnt_;
    kc::AtomicInt64 redcnt_;
    kc::Mutex lock_;
  };
  MapReduceImpl mr;
  mr.tune_storage(dbnum, clim, cbnum);
  if (!ru) {
    mr.log("misc", "setting records");
    int64_t pnum = rnum / 100;
    if (pnum < 1) pnum = 1;
    for (int64_t i = 1; !err && i <= rnum; i++) {
      char kbuf[RECBUFSIZ];
      size_t ksiz = std::sprintf(kbuf, "%lld", (long long)(rnd ? myrand(rnum) + 1 : i));
      char vbuf[RECBUFSIZ];
      size_t vsiz = std::sprintf(vbuf, "%lld", (long long)(rnd ? myrand(pnum) + 1 : i % pnum));
      if (!db.append(kbuf, ksiz, vbuf, vsiz)) {
        dberrprint(&db, __LINE__, "DB::append");
        err = true;
      }
    }
  }
  if (!mr.execute(&db, tmpdir, opts)) {
    dberrprint(&db, __LINE__, "MapReduce::execute");
    err = true;
  }
  if (!rnd && mr.mapcnt() != rnum) {
    dberrprint(&db, __LINE__, "MapReduce::mapcnt");
    err = true;
  }
  if (!rnd && rnum % 100 == 0 && mr.redcnt() != rnum + 4) {
    dberrprint(&db, __LINE__, "MapReduce::redcnt");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, __LINE__, "DB::close");
    err = true;
  }
  if (ulogpath && !ulog.close()) {
    dberrprint(&db, __LINE__, "UpdateLogger::close");
    err = true;
  }
  oprintf("time: %.3f\n", kc::time() - stime);
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}


// perform misc command
static int32_t procmisc(const char* path) {
  oprintf("<Miscellaneous Test>\n  seed=%u  path=%s\n\n", g_randseed, path);
  bool err = false;
  oprintf("opening the database:\n");
  kt::TimedDB* db = new kt::TimedDB;
  db->open(path, kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | kc::BasicDB::OTRUNCATE);
  oprintf("setting records:\n");
  int64_t rnum = 10000;
  for (int64_t i = 1; !err && i <= rnum; i++) {
    char kbuf[RECBUFSIZ];
    size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)i);
    if (!db->set(kbuf, ksiz, kbuf, ksiz)) {
      dberrprint(db, __LINE__, "DB::set");
      err = true;
    }
  }
  oprintf("deploying cursors:\n");
  const int32_t cnum = 100;
  kt::TimedDB::Cursor* curs[cnum];
  for (int32_t i = 0; i < cnum; i++) {
    curs[i] = db->cursor();
    char kbuf[RECBUFSIZ];
    size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)i + 1);
    if (!curs[i]->jump(kbuf, ksiz)) {
      dberrprint(db, __LINE__, "Cursor::jump");
      err = true;
    }
  }
  oprintf("accessing records:\n");
  for (int64_t i = 1; !err && i <= rnum; i++) {
    char kbuf[RECBUFSIZ];
    size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)i);
    if (i % 3 == 0) {
      size_t vsiz;
      char* vbuf = db->get(kbuf, ksiz, &vsiz);
      if (vbuf) {
        delete[] vbuf;
      } else if (db->error() != kc::BasicDB::Error::NOREC) {
        dberrprint(db, __LINE__, "DB::get");
        err = true;
      }
    } else {
      if (!db->remove(kbuf, ksiz)) {
        dberrprint(db, __LINE__, "DB::remove");
        err = true;
      }
    }
  }
  oprintf("updating records with cursors:\n");
  for (int32_t i = 0; i < cnum; i++) {
    kt::TimedDB::Cursor* cur = curs[i];
    switch (i % 7) {
      default: {
        size_t ksiz;
        char* kbuf = cur->get_key(&ksiz, i % 3 == 0);
        if (kbuf) {
          delete[] kbuf;
        } else if (cur->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db, __LINE__, "Cursor::get_key");
          err = true;
        }
        break;
      }
      case 1: {
        size_t vsiz;
        char* vbuf = cur->get_key(&vsiz, i % 3 == 0);
        if (vbuf) {
          delete[] vbuf;
        } else if (cur->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db, __LINE__, "Cursor::get_key");
          err = true;
        }
        break;
      }
      case 2: {
        size_t ksiz;
        const char* vbuf;
        size_t vsiz;
        char* kbuf = cur->get(&ksiz, &vbuf, &vsiz, NULL, i % 3 == 0);
        if (kbuf) {
          delete[] kbuf;
        } else if (cur->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db, __LINE__, "Cursor::get");
          err = true;
        }
        break;
      }
      case 3: {
        char vbuf[RECBUFSIZ];
        size_t vsiz = std::sprintf(vbuf, "kyoto:%d", i);
        if (!cur->set_value(vbuf, vsiz) && cur->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db, __LINE__, "Cursor::set_value");
          err = true;
        }
        break;
      }
      case 4: {
        if (!cur->remove() && cur->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db, __LINE__, "Cursor::remove");
          err = true;
        }
        break;
      }
    }
  }
  oprintf("bulk operations:\n");
  class VisitorBulk : public kt::TimedDB::Visitor {
   public:
    VisitorBulk() : before_(0), after_(0) {}
    int64_t before() {
      return before_;
    }
    int64_t after() {
      return after_;
    }
   private:
    void visit_before() {
      before_++;
    }
    void visit_after() {
      after_++;
    }
    int64_t before_;
    int64_t after_;
  };
  std::vector<std::string> keys;
  for (int64_t i = 1; !err && i <= rnum; i++) {
    char kbuf[RECBUFSIZ];
    size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)i);
    keys.push_back(std::string(kbuf, ksiz));
    if (i % 10 == 0) {
      VisitorBulk visitor;
      if (db->accept_bulk(keys, &visitor, i % 3 > 0)) {
        if (visitor.before() != 1 || visitor.after() != 1) {
          dberrprint(db, __LINE__, "DB::accept_bulk");
          err = true;
        }
      } else {
        dberrprint(db, __LINE__, "DB::accept_bulk");
        err = true;
      }
      keys.clear();
    }
  }
  oprintf("scanning in parallel:\n");
  class VisitorCount : public kt::TimedDB::Visitor {
   public:
    explicit VisitorCount() : cnt_(0) {}
    int64_t cnt() {
      return cnt_;
    }
   private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
      cnt_.add(1);
      return NOP;
    }
    kc::AtomicInt64 cnt_;
  } visitorcount;
  class ProgressCheckerCount : public kc::BasicDB::ProgressChecker {
   public:
    explicit ProgressCheckerCount() : cnt_(0) {}
    int64_t cnt() {
      return cnt_;
    }
   private:
    bool check(const char* name, const char* message, int64_t curcnt, int64_t allcnt) {
      cnt_.add(1);
      return true;
    }
    kc::AtomicInt64 cnt_;
  } checkercount;
  class ThreadWicked : public kc::Thread {
   public:
    explicit ThreadWicked(kt::TimedDB* db, int64_t rnum) :
        db_(db), rnum_(rnum), err_(false) {}
    bool error() {
      return err_;
    }
   private:
    void run() {
      for (int64_t i = 0; i < rnum_; i++) {
        char kbuf[RECBUFSIZ];
        size_t ksiz = std::sprintf(kbuf, "%08lld", (long long)i);
        size_t vsiz;
        char* vbuf = db_->get(kbuf, ksiz, &vsiz);
        if (vbuf) {
          delete[] vbuf;
        } else if (db_->error() != kc::BasicDB::Error::NOREC) {
          dberrprint(db_, __LINE__, "DB::set");
          err_ = true;
        }
      }
    }
    kt::TimedDB* db_;
    int64_t rnum_;
    bool err_;
  } threadwicked(db, rnum);
  threadwicked.start();
  if (!db->scan_parallel(&visitorcount, 4, &checkercount)) {
    dberrprint(db, __LINE__, "DB::scan_parallel");
    err = true;
  }
  threadwicked.join();
  if (threadwicked.error()) err = true;
  if (visitorcount.cnt() != db->count()) {
    dberrprint(db, __LINE__, "DB::scan_parallel");
    err = true;
  }
  if (checkercount.cnt() < db->count() + 2) {
    dberrprint(db, __LINE__, "DB::scan_parallel ss");
    err = true;
  }
  oprintf("deleting the database object:\n");
  delete db;
  oprintf("deleting the cursor objects:\n");
  for (int32_t i = 0; i < cnum; i++) {
    delete curs[i];
  }
  oprintf("re-opening the database object with a external database:\n");
  kt::TimedDB* tdb = new kt::TimedDB;
  if (!tdb->set_internal_db(new kc::PolyDB)) {
    dberrprint(tdb, __LINE__, "DB::set_internal_db");
    err = true;
  }
  if (!tdb->open(path, kc::BasicDB::OREADER)) {
    dberrprint(tdb, __LINE__, "DB::open");
    err = true;
  }
  oprintf("deleting the database object:\n");
  delete tdb;
  oprintf("%s\n\n", err ? "error" : "ok");
  return err ? 1 : 0;
}



// END OF FILE
