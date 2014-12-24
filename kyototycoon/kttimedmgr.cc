/*************************************************************************************************
 * The command line utility of the timed database
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


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::TimedDB* db, const char* info);
static int32_t runcreate(int argc, char** argv);
static int32_t runinform(int argc, char** argv);
static int32_t runset(int argc, char** argv);
static int32_t runremove(int argc, char** argv);
static int32_t runget(int argc, char** argv);
static int32_t runlist(int argc, char** argv);
static int32_t runclear(int argc, char** argv);
static int32_t runimport(int argc, char** argv);
static int32_t runcopy(int argc, char** argv);
static int32_t rundump(int argc, char** argv);
static int32_t runload(int argc, char** argv);
static int32_t runvacuum(int argc, char** argv);
static int32_t runrecover(int argc, char** argv);
static int32_t runmerge(int argc, char** argv);
static int32_t runcheck(int argc, char** argv);
static int32_t runbgsinform(int argc, char** argv);
static int32_t proccreate(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procinform(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool st);
static int32_t procset(const char* path, const char* kbuf, size_t ksiz,
                       const char* vbuf, size_t vsiz, int32_t oflags,
                       const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                       int32_t mode, int64_t xt);
static int32_t procremove(const char* path, const char* kbuf, size_t ksiz, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procget(const char* path, const char* kbuf, size_t ksiz, int32_t oflags,
                       const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                       bool rm, bool px, bool pt, bool pz);
static int32_t proclist(const char* path, const char*kbuf, size_t ksiz, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                        bool des, int64_t max, bool rm, bool pv, bool px, bool pt);
static int32_t procclear(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procimport(const char* path, const char* file, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool sx, int64_t xt);
static int32_t proccopy(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procdump(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procload(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procvacuum(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procrecover(const char* path, const char* dir, int32_t oflags,
                           const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                           uint64_t ts);
static int32_t procmerge(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                         kt::TimedDB::MergeMode mode,
                         const std::vector<std::string>& srcpaths);
static int32_t proccheck(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid);
static int32_t procbgsinform(const char* bgspath);


// main routine
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "create")) {
    rv = runcreate(argc, argv);
  } else if (!std::strcmp(argv[1], "inform")) {
    rv = runinform(argc, argv);
  } else if (!std::strcmp(argv[1], "set")) {
    rv = runset(argc, argv);
  } else if (!std::strcmp(argv[1], "remove")) {
    rv = runremove(argc, argv);
  } else if (!std::strcmp(argv[1], "get")) {
    rv = runget(argc, argv);
  } else if (!std::strcmp(argv[1], "list")) {
    rv = runlist(argc, argv);
  } else if (!std::strcmp(argv[1], "clear")) {
    rv = runclear(argc, argv);
  } else if (!std::strcmp(argv[1], "import")) {
    rv = runimport(argc, argv);
  } else if (!std::strcmp(argv[1], "copy")) {
    rv = runcopy(argc, argv);
  } else if (!std::strcmp(argv[1], "dump")) {
    rv = rundump(argc, argv);
  } else if (!std::strcmp(argv[1], "load")) {
    rv = runload(argc, argv);
  } else if (!std::strcmp(argv[1], "vacuum")) {
    rv = runvacuum(argc, argv);
  } else if (!std::strcmp(argv[1], "recover")) {
    rv = runrecover(argc, argv);
  } else if (!std::strcmp(argv[1], "merge")) {
    rv = runmerge(argc, argv);
  } else if (!std::strcmp(argv[1], "check")) {
    rv = runcheck(argc, argv);
  } else if (!std::strcmp(argv[1], "bgsinform")) {
    rv = runbgsinform(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: the command line utility of the timed database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s create [-otr] [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path\n", g_progname);
  eprintf("  %s inform [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-st] path\n", g_progname);
  eprintf("  %s set [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-add|-rep|-app|-inci|-incd] [-sx] [-xt num] path key value\n", g_progname);
  eprintf("  %s remove [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-sx] path key\n", g_progname);
  eprintf("  %s get [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-rm] [-sx] [-px] [-pt] [-pz] path key\n", g_progname);
  eprintf("  %s list [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-des] [-max num] [-rm] [-sx] [-pv] [-px] [-pt] path [key]\n", g_progname);
  eprintf("  %s clear [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path\n", g_progname);
  eprintf("  %s import [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-sx] [-xt num] path [file]\n", g_progname);
  eprintf("  %s copy [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path file\n", g_progname);
  eprintf("  %s dump [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path [file]\n", g_progname);
  eprintf("  %s load [-otr] [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path [file]\n", g_progname);
  eprintf("  %s vacuum [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path\n", g_progname);
  eprintf("  %s recover [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-ts num] [-sid num] [-dbid num] [-dbid num] path dir\n", g_progname);
  eprintf("  %s merge [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " [-add|-rep|-app] path src...\n", g_progname);
  eprintf("  %s check [-onl|-otl|-onr] [-ulog str] [-ulim num] [-sid num] [-dbid num]"
          " path\n", g_progname);
  eprintf("  %s bgsinform file\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print error message of database
static void dberrprint(kt::TimedDB* db, const char* info) {
  const kc::BasicDB::Error& err = db->error();
  eprintf("%s: %s: %s: %d: %s: %s\n",
          g_progname, info, db->path().c_str(), err.code(), err.name(), err.message());
}


// parse arguments of create command
static int32_t runcreate(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-otr")) {
        oflags |= kc::BasicDB::OTRUNCATE;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = proccreate(path, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of inform command
static int32_t runinform(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool st = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-st")) {
        st = true;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procinform(path, oflags, ulogpath, ulim, sid, dbid, st);
  return rv;
}


// parse arguments of set command
static int32_t runset(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* kstr = NULL;
  const char* vstr = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  int32_t mode = 0;
  bool sx = false;
  int64_t xt = kc::INT64MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-add")) {
        mode = 'a';
      } else if (!std::strcmp(argv[i], "-rep")) {
        mode = 'r';
      } else if (!std::strcmp(argv[i], "-app")) {
        mode = 'c';
      } else if (!std::strcmp(argv[i], "-inci")) {
        mode = 'i';
      } else if (!std::strcmp(argv[i], "-incd")) {
        mode = 'd';
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else if (!vstr) {
      vstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr || !vstr) usage();
  char* kbuf;
  size_t ksiz;
  char* vbuf;
  size_t vsiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
    vbuf = kc::hexdecode(vstr, &vsiz);
    vstr = vbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
    vsiz = std::strlen(vstr);
    vbuf = NULL;
  }
  int32_t rv = procset(path, kstr, ksiz, vstr, vsiz, oflags,
                       ulogpath, ulim, sid, dbid, mode, xt);
  delete[] kbuf;
  delete[] vbuf;
  return rv;
}


// parse arguments of remove command
static int32_t runremove(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procremove(path, kstr, ksiz, oflags, ulogpath, ulim, sid, dbid);
  delete[] kbuf;
  return rv;
}


// parse arguments of get command
static int32_t runget(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool rm = false;
  bool sx = false;
  bool px = false;
  bool pt = false;
  bool pz = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-rm")) {
        rm = true;
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-pt")) {
        pt = true;
      } else if (!std::strcmp(argv[i], "-pz")) {
        pz = true;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  int32_t rv = procget(path, kstr, ksiz, oflags, ulogpath, ulim, sid, dbid, rm, px, pt, pz);
  delete[] kbuf;
  return rv;
}


// parse arguments of list command
static int32_t runlist(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* kstr = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool des = false;
  int64_t max = -1;
  bool rm = false;
  bool sx = false;
  bool pv = false;
  bool px = false;
  bool pt = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-des")) {
        des = true;
      } else if (!std::strcmp(argv[i], "-max")) {
        if (++i >= argc) usage();
        max = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-rm")) {
        rm = true;
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-pv")) {
        pv = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-pt")) {
        pt = true;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!kstr) {
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  char* kbuf = NULL;
  size_t ksiz = 0;
  if (kstr) {
    if (sx) {
      kbuf = kc::hexdecode(kstr, &ksiz);
      kstr = kbuf;
    } else {
      ksiz = std::strlen(kstr);
      kbuf = new char[ksiz+1];
      std::memcpy(kbuf, kstr, ksiz);
      kbuf[ksiz] = '\0';
    }
  }
  int32_t rv = proclist(path, kbuf, ksiz, oflags,
                        ulogpath, ulim, sid, dbid, des, max, rm, pv, px, pt);
  delete[] kbuf;
  return rv;
}


// parse arguments of clear command
static int32_t runclear(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procclear(path, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of import command
static int32_t runimport(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  bool sx = false;
  int64_t xt = kc::INT64MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procimport(path, file, oflags, ulogpath, ulim, sid, dbid, sx, xt);
  return rv;
}


// parse arguments of copy command
static int32_t runcopy(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !file) usage();
  int32_t rv = proccopy(path, file, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of dump command
static int32_t rundump(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procdump(path, file, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of load command
static int32_t runload(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* file = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-otr")) {
        oflags |= kc::BasicDB::OTRUNCATE;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!file) {
      file = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procload(path, file, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of vacuum command
static int32_t runvacuum(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = procvacuum(path, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of recover command
static int32_t runrecover(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  const char* dir = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  uint64_t ts = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-ts")) {
        if (++i >= argc) usage();
        if (!std::strcmp(argv[i], "now") || !std::strcmp(argv[i], "-")) {
          ts = kt::UpdateLogger::clock_pure();
        } else {
          ts = kc::atoix(argv[i]);
        }
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else if (!dir) {
      dir = argv[i];
    } else {
      usage();
    }
  }
  if (!path || !dir) usage();
  int32_t rv = procrecover(path, dir, oflags, ulogpath, ulim, sid, dbid, ts);
  return rv;
}


// parse arguments of merge command
static int32_t runmerge(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  kt::TimedDB::MergeMode mode = kt::TimedDB::MSET;
  std::vector<std::string> srcpaths;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else if (!std::strcmp(argv[i], "-add")) {
        mode = kt::TimedDB::MADD;
      } else if (!std::strcmp(argv[i], "-rep")) {
        mode = kt::TimedDB::MREPLACE;
      } else if (!std::strcmp(argv[i], "-app")) {
        mode = kt::TimedDB::MAPPEND;
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      srcpaths.push_back(argv[i]);
    }
  }
  if (!path && srcpaths.size() < 1) usage();
  int32_t rv = procmerge(path, oflags, ulogpath, ulim, sid, dbid, mode, srcpaths);
  return rv;
}


// parse arguments of check command
static int32_t runcheck(int argc, char** argv) {
  bool argbrk = false;
  const char* path = NULL;
  int32_t oflags = 0;
  const char* ulogpath = NULL;
  int64_t ulim = DEFULIM;
  uint16_t sid = 0;
  uint16_t dbid = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
      } else {
        usage();
      }
    } else if (!path) {
      argbrk = true;
      path = argv[i];
    } else {
      usage();
    }
  }
  if (!path) usage();
  int32_t rv = proccheck(path, oflags, ulogpath, ulim, sid, dbid);
  return rv;
}


// parse arguments of bgsinform command
static int32_t runbgsinform(int argc, char** argv) {
  bool argbrk = false;
  const char* bgspath = NULL;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else {
        usage();
      }
    } else if (!bgspath) {
      argbrk = true;
      bgspath = argv[i];
    } else {
      usage();
    }
  }
  if (!bgspath) usage();
  int32_t rv = procbgsinform(bgspath);
  return rv;
}


// perform create command
static int32_t proccreate(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform inform command
static int32_t procinform(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool st) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OREADER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (st) {
    std::map<std::string, std::string> status;
    if (db.status(&status)) {
      std::map<std::string, std::string>::iterator it = status.begin();
      std::map<std::string, std::string>::iterator itend = status.end();
      while (it != itend) {
        oprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
        ++it;
      }
    } else {
      dberrprint(&db, "DB::status failed");
      err = true;
    }
  } else {
    oprintf("count: %lld\n", (long long)db.count());
    oprintf("size: %lld\n", (long long)db.size());
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform set command
static int32_t procset(const char* path, const char* kbuf, size_t ksiz,
                       const char* vbuf, size_t vsiz, int32_t oflags,
                       const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                       int32_t mode, int64_t xt) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  switch (mode) {
    default: {
      if (!db.set(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::set failed");
        err = true;
      }
      break;
    }
    case 'a': {
      if (!db.add(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::add failed");
        err = true;
      }
      break;
    }
    case 'r': {
      if (!db.replace(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::replace failed");
        err = true;
      }
      break;
    }
    case 'c': {
      if (!db.append(kbuf, ksiz, vbuf, vsiz, xt)) {
        dberrprint(&db, "DB::append failed");
        err = true;
      }
      break;
    }
    case 'i': {
      int64_t onum = db.increment(kbuf, ksiz, kc::atoi(vbuf), 0, xt);
      if (onum == kc::INT64MIN) {
        dberrprint(&db, "DB::increment failed");
        err = true;
      } else {
        oprintf("%lld\n", (long long)onum);
      }
      break;
    }
    case 'd': {
      double onum = db.increment_double(kbuf, ksiz, kc::atof(vbuf), 0, xt);
      if (kc::chknan(onum)) {
        dberrprint(&db, "DB::increment_double failed");
        err = true;
      } else {
        oprintf("%f\n", onum);
      }
      break;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform remove command
static int32_t procremove(const char* path, const char* kbuf, size_t ksiz, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (!db.remove(kbuf, ksiz)) {
    dberrprint(&db, "DB::remove failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform get command
static int32_t procget(const char* path, const char* kbuf, size_t ksiz, int32_t oflags,
                       const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                       bool rm, bool px, bool pt, bool pz) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  uint32_t omode = rm ? kc::BasicDB::OWRITER : kc::BasicDB::OREADER;
  if (!db.open(path, omode | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  char* vbuf;
  size_t vsiz;
  int64_t xt;
  if (rm) {
    vbuf = db.seize(kbuf, ksiz, &vsiz, &xt);
  } else {
    vbuf = db.get(kbuf, ksiz, &vsiz, &xt);
  }
  if (vbuf) {
    printdata(vbuf, vsiz, px);
    if (pt) oprintf("\t%lld", (long long)xt);
    if (!pz) oprintf("\n");
    delete[] vbuf;
  } else {
    dberrprint(&db, "DB::get failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform list command
static int32_t proclist(const char* path, const char*kbuf, size_t ksiz, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                        bool des, int64_t max, bool rm, bool pv, bool px, bool pt) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  uint32_t omode = rm ? kc::BasicDB::OWRITER : kc::BasicDB::OREADER;
  if (!db.open(path, omode | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  class VisitorImpl : public kt::TimedDB::Visitor {
   public:
    explicit VisitorImpl(bool rm, bool pv, bool px, bool pt) :
        rm_(rm), pv_(pv), px_(px), pt_(pt) {}
   private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
      printdata(kbuf, ksiz, px_);
      if (pv_) {
        oprintf("\t");
        printdata(vbuf, vsiz, px_);
      }
      if (pt_) oprintf("\t%lld", (long long)*xtp);
      oprintf("\n");
      return rm_ ? REMOVE : NOP;
    }
    bool rm_;
    bool pv_;
    bool px_;
    bool pt_;
  } visitor(rm, pv, px, pt);
  if (kbuf || des || max >= 0) {
    if (max < 0) max = kc::INT64MAX;
    kt::TimedDB::Cursor cur(&db);
    if (des) {
      if (kbuf) {
        if (!cur.jump_back(kbuf, ksiz) && db.error() != kc::BasicDB::Error::NOREC) {
          dberrprint(&db, "Cursor::jump failed");
          err = true;
        }
      } else {
        if (!cur.jump_back() && db.error() != kc::BasicDB::Error::NOREC) {
          dberrprint(&db, "Cursor::jump failed");
          err = true;
        }
      }
      while (!err && max > 0) {
        if (!cur.accept(&visitor, rm, true)) {
          if (db.error() != kc::BasicDB::Error::NOREC) {
            dberrprint(&db, "Cursor::accept failed");
            err = true;
          }
          break;
        }
        max--;
      }
    } else {
      if (kbuf) {
        if (!cur.jump(kbuf, ksiz) && db.error() != kc::BasicDB::Error::NOREC) {
          dberrprint(&db, "Cursor::jump failed");
          err = true;
        }
      } else {
        if (!cur.jump() && db.error() != kc::BasicDB::Error::NOREC) {
          dberrprint(&db, "Cursor::jump failed");
          err = true;
        }
      }
      while (!err && max > 0) {
        if (!cur.accept(&visitor, rm, true)) {
          if (db.error() != kc::BasicDB::Error::NOREC) {
            dberrprint(&db, "Cursor::accept failed");
            err = true;
          }
          break;
        }
        max--;
      }
    }
  } else {
    if (!db.iterate(&visitor, rm)) {
      dberrprint(&db, "DB::iterate failed");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform clear command
static int32_t procclear(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (!db.clear()) {
    dberrprint(&db, "DB::clear failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform import command
static int32_t procimport(const char* path, const char* file, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                          bool sx, int64_t xt) {
  std::istream *is = &std::cin;
  std::ifstream ifs;
  if (file) {
    ifs.open(file, std::ios_base::in | std::ios_base::binary);
    if (!ifs) {
      eprintf("%s: %s: open error\n", g_progname, file);
      return 1;
    }
    is = &ifs;
  }
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  int64_t cnt = 0;
  std::string line;
  std::vector<std::string> fields;
  while (!err && mygetline(is, &line)) {
    cnt++;
    kc::strsplit(line, '\t', &fields);
    if (sx) {
      std::vector<std::string>::iterator it = fields.begin();
      std::vector<std::string>::iterator itend = fields.end();
      while (it != itend) {
        size_t esiz;
        char* ebuf = kc::hexdecode(it->c_str(), &esiz);
        it->clear();
        it->append(ebuf, esiz);
        delete[] ebuf;
        ++it;
      }
    }
    switch (fields.size()) {
      case 2: {
        if (!db.set(fields[0], fields[1], xt)) {
          dberrprint(&db, "DB::set failed");
          err = true;
        }
        break;
      }
      case 1: {
        if (!db.remove(fields[0]) && db.error() != kc::BasicDB::Error::NOREC) {
          dberrprint(&db, "DB::remove failed");
          err = true;
        }
        break;
      }
    }
    oputchar('.');
    if (cnt % 50 == 0) oprintf(" (%d)\n", cnt);
  }
  if (cnt % 50 > 0) oprintf(" (%d)\n", cnt);
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform copy command
static int32_t proccopy(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OREADER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  DotChecker checker(&std::cout, -100);
  if (!db.copy(file, &checker)) {
    dberrprint(&db, "DB::copy failed");
    err = true;
  }
  oprintf(" (end)\n");
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  if (!err) oprintf("%lld blocks were merged successfully\n", (long long)checker.count());
  return err ? 1 : 0;
}


// perform dump command
static int32_t procdump(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OREADER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (file) {
    DotChecker checker(&std::cout, 1000);
    if (!db.dump_snapshot(file, &checker)) {
      dberrprint(&db, "DB::dump_snapshot");
      err = true;
    }
    oprintf(" (end)\n");
    if (!err) oprintf("%lld records were dumped successfully\n", (long long)checker.count());
  } else {
    if (!db.dump_snapshot(&std::cout)) {
      dberrprint(&db, "DB::dump_snapshot");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform load command
static int32_t procload(const char* path, const char* file, int32_t oflags,
                        const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (file) {
    DotChecker checker(&std::cout, -1000);
    if (!db.load_snapshot(file, &checker)) {
      dberrprint(&db, "DB::load_snapshot");
      err = true;
    }
    oprintf(" (end)\n");
    if (!err) oprintf("%lld records were loaded successfully\n", (long long)checker.count());
  } else {
    DotChecker checker(&std::cout, -1000);
    if (!db.load_snapshot(&std::cin)) {
      dberrprint(&db, "DB::load_snapshot");
      err = true;
    }
    oprintf(" (end)\n");
    if (!err) oprintf("%lld records were loaded successfully\n", (long long)checker.count());
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform vacuum command
static int32_t procvacuum(const char* path, int32_t oflags,
                          const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (!db.vacuum()) {
    dberrprint(&db, "DB::vacuum failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform recover command
static int32_t procrecover(const char* path, const char* dir, int32_t oflags,
                           const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                           uint64_t ts) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  kt::UpdateLogger ulogsrc;
  if (!ulogsrc.open(dir, kc::INT64MIN)) {
    dberrprint(&db, "UpdateLogger::open failed");
    return 1;
  }
  bool err = false;
  kt::UpdateLogger::Reader ulrd;
  if (!ulrd.open(&ulogsrc, ts)) {
    dberrprint(&db, "UpdateLogger::Reader::open failed");
    err = true;
  }
  int64_t cnt = 0;
  while (true) {
    size_t msiz;
    uint64_t mts;
    char* mbuf = ulrd.read(&msiz, &mts);
    if (mbuf) {
      size_t rsiz;
      uint16_t rsid, rdbid;
      const char* rbuf = DBUpdateLogger::parse(mbuf, msiz, &rsiz, &rsid, &rdbid);
      if (rbuf) {
        ulogdb.set_rsid(rsid);
        if (sid != rsid && dbid == rdbid && !db.recover(rbuf, rsiz)) {
          dberrprint(&db, "DB::recover failed");
          err = true;
        }
        ulogdb.clear_rsid();
      } else {
        dberrprint(&db, "DBUpdateLogger::parse failed");
        err = true;
      }
      delete[] mbuf;
    } else {
      break;
    }
    cnt++;
    oputchar('.');
    if (cnt % 50 == 0) oprintf(" (%d)\n", cnt);
  }
  if (cnt % 50 > 0) oprintf(" (%d)\n", cnt);
  if (!ulrd.close()) {
    dberrprint(&db, "DBUpdateLogger::Reader::close failed");
    err = true;
  }
  if (!ulogsrc.close()) {
    dberrprint(&db, "DBUpdateLogger::close failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform merge command
static int32_t procmerge(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid,
                         kt::TimedDB::MergeMode mode,
                         const std::vector<std::string>& srcpaths) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OWRITER | kc::BasicDB::OCREATE | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  kt::TimedDB** srcary = new kt::TimedDB*[srcpaths.size()];
  size_t srcnum = 0;
  std::vector<std::string>::const_iterator it = srcpaths.begin();
  std::vector<std::string>::const_iterator itend = srcpaths.end();
  while (it != itend) {
    const std::string srcpath = *it;
    kt::TimedDB* srcdb = new kt::TimedDB;
    if (srcdb->open(srcpath, kc::BasicDB::OREADER | oflags)) {
      srcary[srcnum++] = srcdb;
    } else {
      dberrprint(srcdb, "DB::open failed");
      err = true;
      delete srcdb;
    }
    ++it;
  }
  DotChecker checker(&std::cout, 1000);
  if (!db.merge(srcary, srcnum, mode, &checker)) {
    dberrprint(&db, "DB::merge failed");
    err = true;
  }
  oprintf(" (end)\n");
  for (size_t i = 0; i < srcnum; i++) {
    kt::TimedDB* srcdb = srcary[i];
    if (!srcdb->close()) {
      dberrprint(srcdb, "DB::close failed");
      err = true;
    }
    delete srcdb;
  }
  delete[] srcary;
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  if (!err) oprintf("%lld records were merged successfully\n", (long long)checker.count());
  return err ? 1 : 0;
}


// perform check command
static int32_t proccheck(const char* path, int32_t oflags,
                         const char* ulogpath, int64_t ulim, uint16_t sid, uint16_t dbid) {
  kt::TimedDB db;
  db.tune_logger(stddblogger(g_progname, &std::cerr));
  kt::UpdateLogger ulog;
  DBUpdateLogger ulogdb;
  if (ulogpath) {
    if (ulog.open(ulogpath, ulim)) {
      ulogdb.initialize(&ulog, sid, dbid);
      if (!db.tune_update_trigger(&ulogdb)) {
        dberrprint(&db, "DB::tune_update_trigger failed");
        return 1;
      }
    } else {
      dberrprint(&db, "UpdateLogger::open failed");
      return 1;
    }
  }
  if (!db.open(path, kc::BasicDB::OREADER | oflags)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  kt::TimedDB::Cursor cur(&db);
  if (!cur.jump() && db.error() != kc::BasicDB::Error::NOREC) {
    dberrprint(&db, "DB::jump failed");
    err = true;
  }
  int64_t cnt = 0;
  while (!err) {
    size_t ksiz;
    const char* vbuf;
    size_t vsiz;
    char* kbuf = cur.get(&ksiz, &vbuf, &vsiz);
    if (kbuf) {
      cnt++;
      size_t rsiz;
      char* rbuf = db.get(kbuf, ksiz, &rsiz);
      if (rbuf) {
        if (rsiz != vsiz || std::memcmp(rbuf, vbuf, rsiz)) {
          dberrprint(&db, "DB::get failed");
          err = true;
        }
        delete[] rbuf;
      } else {
        dberrprint(&db, "DB::get failed");
        err = true;
      }
      delete[] kbuf;
      if (cnt % 1000 == 0) {
        oputchar('.');
        if (cnt % 50000 == 0) oprintf(" (%lld)\n", (long long)cnt);
      }
    } else {
      if (db.error() != kc::BasicDB::Error::NOREC) {
        dberrprint(&db, "Cursor::get failed xx");
        err = true;
      }
      break;
    }
    if (!cur.step() && db.error() != kc::BasicDB::Error::NOREC) {
      dberrprint(&db, "Cursor::step failed");
      err = true;
    }
  }
  oprintf(" (end)\n");
  kc::File::Status sbuf;
  if (kc::File::status(path, &sbuf)) {
    if (!sbuf.isdir && db.size() != sbuf.size) {
      dberrprint(&db, "DB::size failed");
      err = true;
    }
  } else {
    dberrprint(&db, "File::status failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  if (ulogpath) {
    if (!ulog.close()) {
      dberrprint(&db, "UpdateLogger::close failed");
      err = true;
    }
  }
  if (!err) oprintf("%lld records were checked successfully\n", (long long)cnt);
  return err ? 1 : 0;
}


// perform bgsinform command
static int32_t procbgsinform(const char* bgspath) {
  kc::File::Status sbuf;
  if (!kc::File::status(bgspath, &sbuf)) {
    eprintf("%s: %s: no such file or directory\n", g_progname, bgspath);
    return 1;
  }
  if (sbuf.isdir) {
    kc::DirStream dir;
    if (dir.open(bgspath)) {
      std::string name;
      while (dir.read(&name)) {
        const char* nstr = name.c_str();
        const char* pv = std::strrchr(nstr, kc::File::EXTCHR);
        if (*nstr >= '0' && *nstr <= '9' && pv && !kc::stricmp(pv + 1, BGSPATHEXT)) {
          std::string path;
          kc::strprintf(&path, "%s%c%s", bgspath, kc::File::PATHCHR, nstr);
          uint64_t ssts;
          int64_t sscount, sssize;
          if (kt::TimedDB::status_snapshot_atomic(path, &ssts, &sscount, &sssize))
            oprintf("%d\t%llu\t%lld\t%lld\n", (int)kc::atoi(nstr),
                    (unsigned long long)ssts, (long long)sscount, (long long)sssize);
        }
      }
      dir.close();
    } else {
      eprintf("%s: %s: could not open the directory\n", g_progname, bgspath);
      return 1;
    }
  } else {
    uint64_t ssts;
    int64_t sscount, sssize;
    if (kt::TimedDB::status_snapshot_atomic(bgspath, &ssts, &sscount, &sssize)) {
      const char* nstr = std::strrchr(bgspath, kc::File::PATHCHR);
      if (!nstr) nstr = bgspath;
      oprintf("%d\t%llu\t%lld\t%lld\n", (int)kc::atoi(nstr),
              (unsigned long long)ssts, (long long)sscount, (long long)sssize);
    } else {
      eprintf("%s: %s: could not open the file\n", g_progname, bgspath);
      return 1;
    }
  }
  return 0;
}



// END OF FILE
