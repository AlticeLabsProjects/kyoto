/*************************************************************************************************
 * The command line utility of the remote database
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


// function prototypes
int main(int argc, char** argv);
static void usage();
static void dberrprint(kt::RemoteDB* db, const char* info);
static int32_t runreport(int argc, char** argv);
static int32_t runscript(int argc, char** argv);
static int32_t runtunerepl(int argc, char** argv);
static int32_t runinform(int argc, char** argv);
static int32_t runclear(int argc, char** argv);
static int32_t runsync(int argc, char** argv);
static int32_t runset(int argc, char** argv);
static int32_t runremove(int argc, char** argv);
static int32_t runget(int argc, char** argv);
static int32_t runlist(int argc, char** argv);
static int32_t runimport(int argc, char** argv);
static int32_t runvacuum(int argc, char** argv);
static int32_t runslave(int argc, char** argv);
static int32_t runsetbulk(int argc, char** argv);
static int32_t runremovebulk(int argc, char** argv);
static int32_t rungetbulk(int argc, char** argv);
static int32_t runmatch(int argc, char** argv);
static int32_t runregex(int argc, char** argv);
static int32_t procreport(const char* host, int32_t port, double tout);
static int32_t procscript(const char* proc, const char* host, int32_t port, double tout,
                          bool bin, const char* swname, double swtime,
                          const char* ssname, bool ssbrd,
                          const std::map<std::string, std::string>& params);
static int32_t proctunerepl(const char* mhost, const char* host, int32_t port, double tout,
                            int32_t mport, uint64_t ts, double iv);
static int32_t procinform(const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr, bool st);
static int32_t procclear(const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr);
static int32_t procsync(const char* host, int32_t port, double tout,
                        const char* swname, double swtime, const char* ssname, bool ssbrd,
                        const char* dbexpr, bool hard, const char* cmd);
static int32_t procset(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                       const char* host, int32_t port, double tout,
                       const char* swname, double swtime, const char* ssname, bool ssbrd,
                       const char* dbexpr, int32_t mode, int64_t xt);
static int32_t procremove(const char* kbuf, size_t ksiz,
                          const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr);
static int32_t procget(const char* kbuf, size_t ksiz,
                       const char* host, int32_t port, double tout,
                       const char* swname, double swtime, const char* ssname, bool ssbrd,
                       const char* dbexpr, bool rm, bool px, bool pt, bool pz);
static int32_t proclist(const char* kbuf, size_t ksiz,
                        const char* host, int32_t port, double tout,
                        const char* swname, double swtime, const char* ssname, bool ssbrd,
                        const char* dbexpr, bool des, int64_t max,
                        bool rm, bool pv, bool px, bool pt);
static int32_t procimport(const char* file, const char* host, int32_t port, double tout,
                          const char* dbexpr, bool sx, int64_t xt);
static int32_t procvacuum(const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr, int64_t step);
static int32_t procslave(const char* host, int32_t port, double tout,
                         uint64_t ts, int32_t sid, int32_t opts, bool uw, bool uf, bool ur);
static int32_t procsetbulk(const std::map<std::string, std::string>& recs,
                           const char* host, int32_t port, double tout, bool bin,
                           const char* swname, double swtime, const char* ssname, bool ssbrd,
                           const char* dbexpr, int64_t xt);
static int32_t procremovebulk(const std::vector<std::string>& keys,
                              const char* host, int32_t port, double tout, bool bin,
                              const char* swname, double swtime, const char* ssname, bool ssbrd,
                              const char* dbexpr);
static int32_t procgetbulk(const std::vector<std::string>& keys,
                           const char* host, int32_t port, double tout, bool bin,
                           const char* swname, double swtime, const char* ssname, bool ssbrd,
                           const char* dbexpr, bool px);
static int32_t procmatch(const std::vector<std::string>& keys,
                         const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr, bool px, int32_t limit);
static int32_t procregex(const std::vector<std::string>& regexes,
                         const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr, bool px, int32_t limit);


// print the usage and exit
int main(int argc, char** argv) {
  g_progname = argv[0];
  kc::setstdiobin();
  if (argc < 2) usage();
  int32_t rv = 0;
  if (!std::strcmp(argv[1], "report")) {
    rv = runreport(argc, argv);
  } else if (!std::strcmp(argv[1], "script")) {
    rv = runscript(argc, argv);
  } else if (!std::strcmp(argv[1], "tunerepl")) {
    rv = runtunerepl(argc, argv);
  } else if (!std::strcmp(argv[1], "inform")) {
    rv = runinform(argc, argv);
  } else if (!std::strcmp(argv[1], "clear")) {
    rv = runclear(argc, argv);
  } else if (!std::strcmp(argv[1], "sync")) {
    rv = runsync(argc, argv);
  } else if (!std::strcmp(argv[1], "set")) {
    rv = runset(argc, argv);
  } else if (!std::strcmp(argv[1], "remove")) {
    rv = runremove(argc, argv);
  } else if (!std::strcmp(argv[1], "get")) {
    rv = runget(argc, argv);
  } else if (!std::strcmp(argv[1], "list")) {
    rv = runlist(argc, argv);
  } else if (!std::strcmp(argv[1], "import")) {
    rv = runimport(argc, argv);
  } else if (!std::strcmp(argv[1], "vacuum")) {
    rv = runvacuum(argc, argv);
  } else if (!std::strcmp(argv[1], "slave")) {
    rv = runslave(argc, argv);
  } else if (!std::strcmp(argv[1], "setbulk")) {
    rv = runsetbulk(argc, argv);
  } else if (!std::strcmp(argv[1], "removebulk")) {
    rv = runremovebulk(argc, argv);
  } else if (!std::strcmp(argv[1], "getbulk")) {
    rv = rungetbulk(argc, argv);
  } else if (!std::strcmp(argv[1], "match")) {
    rv = runmatch(argc, argv);
  } else if (!std::strcmp(argv[1], "regex")) {
    rv = runregex(argc, argv);
  } else if (!std::strcmp(argv[1], "version") || !std::strcmp(argv[1], "--version")) {
    printversion();
  } else {
    usage();
  }
  return rv;
}


// print the usage and exit
static void usage() {
  eprintf("%s: the command line utility of the remote database of Kyoto Tycoon\n", g_progname);
  eprintf("\n");
  eprintf("usage:\n");
  eprintf("  %s report [-host str] [-port num] [-tout num]\n", g_progname);
  eprintf("  %s script [-host str] [-port num] [-tout num] [-bin]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] proc [args...]\n", g_progname);
  eprintf("  %s tunerepl [-host str] [-port num] [-tout num] [-mport num] [-ts num] [-iv num]"
          " [mhost]\n", g_progname);
  eprintf("  %s inform [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-st]\n", g_progname);
  eprintf("  %s clear [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str]\n", g_progname);
  eprintf("  %s sync [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-hard] [-cmd str]\n",
          g_progname);
  eprintf("  %s set [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str]"
          " [-add|-rep|-app|-inci|-incd] [-sx] [-xt num] key value\n", g_progname);
  eprintf("  %s remove [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx] key\n",
          g_progname);
  eprintf("  %s get [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str]"
          " [-rm] [-sx] [-px] [-pt] [-pz] key\n", g_progname);
  eprintf("  %s list [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str]"
          " [-des] [-max num] [-rm] [-sx] [-pv] [-px] [-pt] [key]\n", g_progname);
  eprintf("  %s import [-host str] [-port num] [-tout num] [-db str] [-sx] [-xt num] [file]\n",
          g_progname);
  eprintf("  %s vacuum [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-step num]\n",
          g_progname);
  eprintf("  %s slave [-host str] [-port num] [-tout num] [-ts num] [-sid num]"
          " [-ux] [-uw] [-uf] [-ur]\n", g_progname);
  eprintf("  %s setbulk [-host str] [-port num] [-tout num] [-bin]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx] [-xt num]"
          " key value ...\n", g_progname);
  eprintf("  %s removebulk [-host str] [-port num] [-tout num] [-bin]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx]"
          " key ...\n", g_progname);
  eprintf("  %s getbulk [-host str] [-port num] [-tout num] [-bin]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx] [-px]"
          " key ...\n", g_progname);
  eprintf("  %s match [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx] [-px]"
          " [-limit num] prefix ...\n", g_progname);
  eprintf("  %s regex [-host str] [-port num] [-tout num]"
          " [-swname str] [-swtime num] [-ssname str] [-ssbrd] [-db str] [-sx] [-px]"
          " [-limit num] regex ...\n", g_progname);
  eprintf("\n");
  std::exit(1);
}


// print error message of database
static void dberrprint(kt::RemoteDB* db, const char* info) {
  const kt::RemoteDB::Error& err = db->error();
  eprintf("%s: %s: %s: %d: %s: %s\n",
          g_progname, info, db->expression().c_str(), err.code(), err.name(), err.message());
}


// parse arguments of report command
static int32_t runreport(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
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
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procreport(host, port, tout);
  return rv;
}


// parse arguments of script command
static int32_t runscript(int argc, char** argv) {
  bool argbrk = false;
  const char* proc = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  bool bin = false;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  std::map<std::string, std::string> params;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-bin")) {
        bin = true;
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else {
        usage();
      }
    } else if (!proc) {
      argbrk = true;
      proc = argv[i];
    } else {
      if (++i >= argc) usage();
      params[argv[i-1]] = argv[i];
    }
  }
  if (!proc || port < 1) usage();
  int32_t rv = procscript(proc, host, port, tout, bin, swname, swtime, ssname, ssbrd, params);
  return rv;
}


// parse arguments of tunerepl command
static int32_t runtunerepl(int argc, char** argv) {
  bool argbrk = false;
  const char* mhost = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  int32_t mport = kt::DEFPORT;
  uint64_t ts = kc::UINT64MAX;
  double iv = -1;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-mport")) {
        if (++i >= argc) usage();
        mport = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-ts")) {
        if (++i >= argc) usage();
        if (!std::strcmp(argv[i], "now") || !std::strcmp(argv[i], "-")) {
          ts = kc::UINT64MAX - 1;
        } else {
          ts = kc::atoix(argv[i]);
        }
      } else if (!std::strcmp(argv[i], "-iv")) {
        if (++i >= argc) usage();
        iv = kc::atof(argv[i]);
      } else {
        usage();
      }
    } else if (!mhost) {
      argbrk = true;
      mhost = argv[i];
    } else {
      usage();
    }
  }
  if (port < 1 || mport < 1) usage();
  int32_t rv = proctunerepl(mhost, host, port, tout, mport, ts, iv);
  return rv;
}


// parse arguments of inform command
static int32_t runinform(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool st = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-st")) {
        st = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procinform(host, port, tout, swname, swtime, ssname, ssbrd, dbexpr, st);
  return rv;
}


// parse arguments of clear command
static int32_t runclear(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procclear(host, port, tout, swname, swtime, ssname, ssbrd, dbexpr);
  return rv;
}


// parse arguments of sync command
static int32_t runsync(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool hard = false;
  const char* cmd = "";
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-hard")) {
        hard = true;
      } else if (!std::strcmp(argv[i], "-cmd")) {
        if (++i >= argc) usage();
        cmd = argv[i];
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procsync(host, port, tout, swname, swtime, ssname, ssbrd, dbexpr, hard, cmd);
  return rv;
}


// parse arguments of set command
static int32_t runset(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* vstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  int32_t mode = 0;
  bool sx = false;
  int64_t xt = kc::INT64MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
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
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else if (!vstr) {
      vstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr || !vstr) usage();
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
  if (port < 1) {
    delete[] kbuf;
    delete[] vbuf;
    usage();
  }
  int32_t rv = procset(kstr, ksiz, vstr, vsiz, host, port, tout, swname, swtime, ssname, ssbrd,
                       dbexpr, mode, xt);
  delete[] kbuf;
  delete[] vbuf;
  return rv;
}


// parse arguments of remove command
static int32_t runremove(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  if (port < 1) {
    delete[] kbuf;
    usage();
  }
  int32_t rv = procremove(kstr, ksiz, host, port, tout, swname, swtime, ssname, ssbrd, dbexpr);
  delete[] kbuf;
  return rv;
}


// parse arguments of get command
static int32_t runget(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool rm = false;
  bool sx = false;
  bool px = false;
  bool pt = false;
  bool pz = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
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
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
  if (!kstr) usage();
  char* kbuf;
  size_t ksiz;
  if (sx) {
    kbuf = kc::hexdecode(kstr, &ksiz);
    kstr = kbuf;
  } else {
    ksiz = std::strlen(kstr);
    kbuf = NULL;
  }
  if (port < 1) {
    delete[] kbuf;
    usage();
  }
  int32_t rv = procget(kstr, ksiz, host, port, tout, swname, swtime, ssname, ssbrd,
                       dbexpr, rm, px, pt, pz);
  delete[] kbuf;
  return rv;
}


// parse arguments of list command
static int32_t runlist(int argc, char** argv) {
  bool argbrk = false;
  const char* kstr = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
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
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
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
    } else if (!kstr) {
      argbrk = true;
      kstr = argv[i];
    } else {
      usage();
    }
  }
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
  if (port < 1) {
    delete[] kbuf;
    usage();
  }
  int32_t rv = proclist(kbuf, ksiz, host, port, tout, swname, swtime, ssname, ssbrd,
                        dbexpr, des, max, rm, pv, px, pt);
  delete[] kbuf;
  return rv;
}


// parse arguments of import command
static int32_t runimport(int argc, char** argv) {
  bool argbrk = false;
  const char* file = NULL;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* dbexpr = NULL;
  bool sx = false;
  int64_t xt = kc::INT64MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else if (!file) {
      argbrk = true;
      file = argv[i];
    } else {
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procimport(file, host, port, tout, dbexpr, sx, xt);
  return rv;
}


// parse arguments of vacuum command
static int32_t runvacuum(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  int64_t step = 0;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-step")) {
        if (++i >= argc) usage();
        step = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procvacuum(host, port, tout, swname, swtime, ssname, ssbrd, dbexpr, step);
  return rv;
}


// parse arguments of slave command
static int32_t runslave(int argc, char** argv) {
  bool argbrk = false;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  uint64_t ts = 0;
  int32_t sid = 0;
  bool uw = false;
  int32_t opts = 0;
  bool uf = false;
  bool ur = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ts")) {
        if (++i >= argc) usage();
        if (!std::strcmp(argv[i], "now") || !std::strcmp(argv[i], "-")) {
          ts = kt::UpdateLogger::clock_pure();
        } else {
          ts = kc::atoix(argv[i]);
        }
      } else if (!std::strcmp(argv[i], "-sid")) {
        if (++i >= argc) usage();
        sid = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-ux")) {
        opts |= kt::ReplicationClient::WHITESID;
      } else if (!std::strcmp(argv[i], "-uw")) {
        uw = true;
      } else if (!std::strcmp(argv[i], "-uf")) {
        uf = true;
      } else if (!std::strcmp(argv[i], "-ur")) {
        ur = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      usage();
    }
  }
  if (port < 1) usage();
  int32_t rv = procslave(host, port, tout, ts, sid, opts, uw, uf, ur);
  return rv;
}


// parse arguments of setbulk command
static int32_t runsetbulk(int argc, char** argv) {
  bool argbrk = false;
  std::map<std::string, std::string> recs;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  bool bin = false;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  int64_t xt = kc::INT64MAX;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-bin")) {
        bin = true;
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-xt")) {
        if (++i >= argc) usage();
        xt = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else {
      argbrk = true;
      const char* kstr = argv[i];
      if (++i >= argc) usage();
      const char* vstr = argv[i];
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
      std::string key(kstr, ksiz);
      std::string value(vstr, vsiz);
      recs[key] = value;
      delete[] kbuf;
      delete[] vbuf;
    }
  }
  int32_t rv = procsetbulk(recs, host, port, tout, bin, swname, swtime, ssname, ssbrd,
                           dbexpr, xt);
  return rv;
}


// parse arguments of removebulk command
static int32_t runremovebulk(int argc, char** argv) {
  bool argbrk = false;
  std::vector<std::string> keys;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  bool bin = false;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-bin")) {
        bin = true;
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      const char* kstr = argv[i];
      char* kbuf;
      size_t ksiz;
      if (sx) {
        kbuf = kc::hexdecode(kstr, &ksiz);
        kstr = kbuf;
      } else {
        ksiz = std::strlen(kstr);
        kbuf = NULL;
      }
      std::string key(kstr, ksiz);
      keys.push_back(key);
      delete[] kbuf;
    }
  }
  int32_t rv = procremovebulk(keys, host, port, tout, bin, swname, swtime, ssname, ssbrd,
                              dbexpr);
  return rv;
}


// parse arguments of getbulk command
static int32_t rungetbulk(int argc, char** argv) {
  bool argbrk = false;
  std::vector<std::string> keys;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  bool bin = false;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  bool px = false;
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-bin")) {
        bin = true;
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else {
        usage();
      }
    } else {
      argbrk = true;
      const char* kstr = argv[i];
      char* kbuf;
      size_t ksiz;
      if (sx) {
        kbuf = kc::hexdecode(kstr, &ksiz);
        kstr = kbuf;
      } else {
        ksiz = std::strlen(kstr);
        kbuf = NULL;
      }
      std::string key(kstr, ksiz);
      keys.push_back(key);
      delete[] kbuf;
    }
  }
  int32_t rv = procgetbulk(keys, host, port, tout, bin, swname, swtime, ssname, ssbrd,
                           dbexpr, px);
  return rv;
}


// parse arguments of match command
static int32_t runmatch(int argc, char** argv) {
  bool argbrk = false;
  std::vector<std::string> keys;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  bool px = false;
  int32_t limit = -1;  // unlimited
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-limit")) {
        if (++i >= argc) usage();
        limit = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else {
      argbrk = true;
      const char* kstr = argv[i];
      char* kbuf;
      size_t ksiz;
      if (sx) {
        kbuf = kc::hexdecode(kstr, &ksiz);
        kstr = kbuf;
      } else {
        ksiz = std::strlen(kstr);
        kbuf = NULL;
      }
      std::string key(kstr, ksiz);
      keys.push_back(key);
      delete[] kbuf;
    }
  }
  int32_t rv = procmatch(keys, host, port, tout, swname, swtime, ssname, ssbrd,
                         dbexpr, px, limit);
  return rv;
}


// parse arguments of regex command
static int32_t runregex(int argc, char** argv) {
  bool argbrk = false;
  std::vector<std::string> regexes;
  const char* host = "";
  int32_t port = kt::DEFPORT;
  double tout = 0;
  const char* swname = NULL;
  double swtime = 0;
  const char* ssname = NULL;
  bool ssbrd = false;
  const char* dbexpr = NULL;
  bool sx = false;
  bool px = false;
  int32_t limit = -1;  // unlimited
  for (int32_t i = 2; i < argc; i++) {
    if (!argbrk && argv[i][0] == '-') {
      if (!std::strcmp(argv[i], "--")) {
        argbrk = true;
      } else if (!std::strcmp(argv[i], "-host")) {
        if (++i >= argc) usage();
        host = argv[i];
      } else if (!std::strcmp(argv[i], "-port")) {
        if (++i >= argc) usage();
        port = kc::atoix(argv[i]);
      } else if (!std::strcmp(argv[i], "-tout")) {
        if (++i >= argc) usage();
        tout = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-swname")) {
        if (++i >= argc) usage();
        swname = argv[i];
      } else if (!std::strcmp(argv[i], "-swtime")) {
        if (++i >= argc) usage();
        swtime = kc::atof(argv[i]);
      } else if (!std::strcmp(argv[i], "-ssname")) {
        if (++i >= argc) usage();
        ssname = argv[i];
      } else if (!std::strcmp(argv[i], "-ssbrd")) {
        ssbrd = false;
      } else if (!std::strcmp(argv[i], "-db")) {
        if (++i >= argc) usage();
        dbexpr = argv[i];
      } else if (!std::strcmp(argv[i], "-sx")) {
        sx = true;
      } else if (!std::strcmp(argv[i], "-px")) {
        px = true;
      } else if (!std::strcmp(argv[i], "-limit")) {
        if (++i >= argc) usage();
        limit = kc::atoix(argv[i]);
      } else {
        usage();
      }
    } else {
      argbrk = true;
      const char* rstr = argv[i];
      char* rbuf;
      size_t rsiz;
      if (sx) {
        rbuf = kc::hexdecode(rstr, &rsiz);
        rstr = rbuf;
      } else {
        rsiz = std::strlen(rstr);
        rbuf = NULL;
      }
      std::string regex(rstr, rsiz);
      regexes.push_back(regex);
      delete[] rbuf;
    }
  }
  int32_t rv = procregex(regexes, host, port, tout, swname, swtime, ssname, ssbrd,
                         dbexpr, px, limit);
  return rv;
}


// perform report command
static int32_t procreport(const char* host, int32_t port, double tout) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  std::map<std::string, std::string> status;
  if (db.report(&status)) {
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
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform script command
static int32_t procscript(const char* proc, const char* host, int32_t port, double tout,
                          bool bin, const char* swname, double swtime,
                          const char* ssname, bool ssbrd,
                          const std::map<std::string, std::string>& params) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (bin) {
    std::map<std::string, std::string> result;
    if (db.play_script_binary(proc, params, &result)) {
      std::map<std::string, std::string>::iterator it = result.begin();
      std::map<std::string, std::string>::iterator itend = result.end();
      while (it != itend) {
        oprintf("%s\t%s\n", it->first.c_str(), it->second.c_str());
        ++it;
      }
    } else {
      dberrprint(&db, "DB::play_script_binary failed");
      err = true;
    }
  } else {
    std::map<std::string, std::string> result;
    if (db.play_script(proc, params, &result)) {
      std::map<std::string, std::string>::iterator it = result.begin();
      std::map<std::string, std::string>::iterator itend = result.end();
      while (it != itend) {
        oprintf("%s\t%s\n", it->first.c_str(), it->second.c_str());
        ++it;
      }
    } else {
      dberrprint(&db, "DB::play_script failed");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform tunerepl command
static int32_t proctunerepl(const char* mhost, const char* host, int32_t port, double tout,
                            int32_t mport, uint64_t ts, double iv) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  bool err = false;
  if (!db.tune_replication(mhost ? mhost : "", mport, ts, iv)) {
    dberrprint(&db, "DB::tune_replication failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform inform command
static int32_t procinform(const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr, bool st) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  std::map<std::string, std::string> status;
  if (db.status(&status)) {
    if (st) {
      std::map<std::string, std::string>::iterator it = status.begin();
      std::map<std::string, std::string>::iterator itend = status.end();
      while (it != itend) {
        oprintf("%s: %s\n", it->first.c_str(), it->second.c_str());
        ++it;
      }
    } else {
      oprintf("count: %s\n", status["count"].c_str());
      oprintf("size: %s\n", status["size"].c_str());
    }
  } else {
    dberrprint(&db, "DB::status failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform clear command
static int32_t procclear(const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.clear()) {
    dberrprint(&db, "DB::clear failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform sync command
static int32_t procsync(const char* host, int32_t port, double tout,
                        const char* swname, double swtime, const char* ssname, bool ssbrd,
                        const char* dbexpr, bool hard, const char* cmd) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.synchronize(hard, cmd)) {
    dberrprint(&db, "DB::synchronize failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform set command
static int32_t procset(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
                       const char* host, int32_t port, double tout,
                       const char* swname, double swtime, const char* ssname, bool ssbrd,
                       const char* dbexpr, int32_t mode, int64_t xt) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
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
  return err ? 1 : 0;
}


// perform remove command
static int32_t procremove(const char* kbuf, size_t ksiz,
                          const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.remove(kbuf, ksiz)) {
    dberrprint(&db, "DB::remove failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform get command
static int32_t procget(const char* kbuf, size_t ksiz,
                       const char* host, int32_t port, double tout,
                       const char* swname, double swtime, const char* ssname, bool ssbrd,
                       const char* dbexpr, bool rm, bool px, bool pt, bool pz) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
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
  return err ? 1 : 0;
}


// perform list command
static int32_t proclist(const char* kbuf, size_t ksiz,
                        const char* host, int32_t port, double tout,
                        const char* swname, double swtime, const char* ssname, bool ssbrd,
                        const char* dbexpr, bool des, int64_t max,
                        bool rm, bool pv, bool px, bool pt) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (max < 0) max = kc::INT64MAX;
  kt::RemoteDB::Cursor cur(&db);
  if (des) {
    if (kbuf) {
      if (!cur.jump_back(kbuf, ksiz) && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    } else {
      if (!cur.jump_back() && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    }
    while (!err && max > 0) {
      char* kbuf;
      size_t ksiz, vsiz;
      const char* vbuf;
      int64_t xt;
      if (rm) {
        kbuf = cur.seize(&ksiz, &vbuf, &vsiz, &xt);
      } else {
        kbuf = cur.get(&ksiz, &vbuf, &vsiz, &xt);
      }
      if (kbuf) {
        printdata(kbuf, ksiz, px);
        if (pv) {
          oprintf("\t");
          printdata(vbuf, vsiz, px);
        }
        if (pt) oprintf("\t%lld", (long long)xt);
        oprintf("\n");
        delete[] kbuf;
      } else {
        if (db.error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(&db, "Cursor::get failed");
          err = true;
        }
        break;
      }
      cur.step_back();
      max--;
    }
  } else {
    if (kbuf) {
      if (!cur.jump(kbuf, ksiz) && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    } else {
      if (!cur.jump() && db.error() != kt::RemoteDB::Error::LOGIC) {
        dberrprint(&db, "Cursor::jump failed");
        err = true;
      }
    }
    while (!err && max > 0) {
      char* kbuf;
      size_t ksiz, vsiz;
      const char* vbuf;
      int64_t xt;
      if (rm) {
        kbuf = cur.seize(&ksiz, &vbuf, &vsiz, &xt);
      } else {
        kbuf = cur.get(&ksiz, &vbuf, &vsiz, &xt, true);
      }
      if (kbuf) {
        printdata(kbuf, ksiz, px);
        if (pv) {
          oprintf("\t");
          printdata(vbuf, vsiz, px);
        }
        if (pt) oprintf("\t%lld", (long long)xt);
        oprintf("\n");
        delete[] kbuf;
      } else {
        if (db.error() != kt::RemoteDB::Error::LOGIC) {
          dberrprint(&db, "Cursor::get failed");
          err = true;
        }
        break;
      }
      max--;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform import command
static int32_t procimport(const char* file, const char* host, int32_t port, double tout,
                          const char* dbexpr, bool sx, int64_t xt) {
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
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (dbexpr) db.set_target(dbexpr);
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
        if (!db.remove(fields[0]) && db.error() != kt::RemoteDB::Error::LOGIC) {
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
  return err ? 1 : 0;
}


// perform vacuum command
static int32_t procvacuum(const char* host, int32_t port, double tout,
                          const char* swname, double swtime, const char* ssname, bool ssbrd,
                          const char* dbexpr, int64_t step) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  if (dbexpr) db.set_target(dbexpr);
  bool err = false;
  if (!db.vacuum(step)) {
    dberrprint(&db, "DB::vacuum failed");
    err = true;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform slave command
static int32_t procslave(const char* host, int32_t port, double tout,
                         uint64_t ts, int32_t sid, int32_t opts, bool uw, bool uf, bool ur) {
  bool err = false;
  if (uf || ur) {
    kt::RemoteDB db;
    if (!db.open(host, port, tout)) {
      dberrprint(&db, "DB::open failed");
      return 1;
    }
    if (ur) {
      if (ts < 1) ts = kc::UINT64MAX;
      if (!db.ulog_remove(ts)) {
        dberrprint(&db, "DB::ulog_remove failed");
        err = true;
      }
    } else {
      std::vector<kt::UpdateLogger::FileStatus> files;
      if (db.ulog_list(&files)) {
        std::vector<kt::UpdateLogger::FileStatus>::iterator it = files.begin();
        std::vector<kt::UpdateLogger::FileStatus>::iterator itend = files.end();
        while (it != itend) {
          oprintf("%s\t%llu\t%llu\n",
                  it->path.c_str(), (unsigned long long)it->size, (unsigned long long)it->ts);
          ++it;
        }
      } else {
        dberrprint(&db, "DB::ulog_list failed");
        err = true;
      }
    }
    if (!db.close()) {
      dberrprint(&db, "DB::close failed");
      err = true;
    }
  } else {
    kt::ReplicationClient rc;
    if (!rc.open(host, port, tout, ts, sid, opts)) {
      eprintf("%s: %s:%d: open error\n", g_progname, host, port);
      return 1;
    }
    while (true) {
      size_t msiz;
      uint64_t mts;
      char* mbuf = rc.read(&msiz, &mts);
      if (mbuf) {
        if (msiz > 0) {
          size_t rsiz;
          uint16_t rsid, rdbid;
          const char* rbuf = DBUpdateLogger::parse(mbuf, msiz, &rsiz, &rsid, &rdbid);
          if (rbuf) {
            std::vector<std::string> tokens;
            kt::TimedDB::tokenize_update_log(rbuf, rsiz, &tokens);
            if (!tokens.empty()) {
              const std::string& name = tokens.front();
              oprintf("%llu\t%u\t%u\t%s", (unsigned long long)mts, rsid, rdbid, name.c_str());
              std::vector<std::string>::iterator it = tokens.begin() + 1;
              std::vector<std::string>::iterator itend = tokens.end();
              while (it != itend) {
                char* str = kc::baseencode(it->data(), it->size());
                oprintf("\t%s", str);
                delete[] str;
                ++it;
              }
              oprintf("\n");
            }
          } else {
            eprintf("%s: parsing a message failed\n", g_progname);
            err = true;
          }
        }
        delete[] mbuf;
      } else if (!rc.alive() || !uw) {
        break;
      }
    }
    if (!rc.close()) {
      eprintf("%s: close error\n", g_progname);
      err = true;
    }
  }
  return err ? 1 : 0;
}


// perform setbulk command
static int32_t procsetbulk(const std::map<std::string, std::string>& recs,
                           const char* host, int32_t port, double tout, bool bin,
                           const char* swname, double swtime, const char* ssname, bool ssbrd,
                           const char* dbexpr, int64_t xt) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (bin) {
    std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
    uint16_t dbidx = dbexpr ? kc::atoi(dbexpr) : 0;
    std::map<std::string, std::string>::const_iterator it = recs.begin();
    std::map<std::string, std::string>::const_iterator itend = recs.end();
    while (it != itend) {
      kt::RemoteDB::BulkRecord rec = { dbidx, it->first, it->second, xt };
      bulkrecs.push_back(rec);
      ++it;
    }
    if (db.set_bulk_binary(bulkrecs) != (int64_t)recs.size()) {
      dberrprint(&db, "DB::set_bulk_binary failed");
      err = true;
    }
  } else {
    if (dbexpr) db.set_target(dbexpr);
    if (db.set_bulk(recs, xt) != (int64_t)recs.size()) {
      dberrprint(&db, "DB::set_bulk failed");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform removebulk command
static int32_t procremovebulk(const std::vector<std::string>& keys,
                              const char* host, int32_t port, double tout, bool bin,
                              const char* swname, double swtime, const char* ssname, bool ssbrd,
                              const char* dbexpr) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (bin) {
    std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
    uint16_t dbidx = dbexpr ? kc::atoi(dbexpr) : 0;
    std::vector<std::string>::const_iterator it = keys.begin();
    std::vector<std::string>::const_iterator itend = keys.end();
    while (it != itend) {
      kt::RemoteDB::BulkRecord rec = { dbidx, *it, "", 0 };
      bulkrecs.push_back(rec);
      ++it;
    }
    if (db.remove_bulk_binary(bulkrecs) < 0) {
      dberrprint(&db, "DB::remove_bulk_binary failed");
      err = true;
    }
  } else {
    if (dbexpr) db.set_target(dbexpr);
    if (db.remove_bulk(keys) < 0) {
      dberrprint(&db, "DB::remove_bulk failed");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform getbulk command
static int32_t procgetbulk(const std::vector<std::string>& keys,
                           const char* host, int32_t port, double tout, bool bin,
                           const char* swname, double swtime, const char* ssname, bool ssbrd,
                           const char* dbexpr, bool px) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (bin) {
    std::vector<kt::RemoteDB::BulkRecord> bulkrecs;
    uint16_t dbidx = dbexpr ? kc::atoi(dbexpr) : 0;
    std::vector<std::string>::const_iterator it = keys.begin();
    std::vector<std::string>::const_iterator itend = keys.end();
    while (it != itend) {
      kt::RemoteDB::BulkRecord rec = { dbidx, *it, "", 0 };
      bulkrecs.push_back(rec);
      ++it;
    }
    if (db.get_bulk_binary(&bulkrecs) >= 0) {
      std::vector<kt::RemoteDB::BulkRecord>::iterator rit = bulkrecs.begin();
      std::vector<kt::RemoteDB::BulkRecord>::iterator ritend = bulkrecs.end();
      while (rit != ritend) {
        if (rit->xt > 0) {
          printdata(rit->key.data(), rit->key.size(), px);
          oprintf("\t");
          printdata(rit->value.data(), rit->value.size(), px);
          oprintf("\n");
        }
        ++rit;
      }
    } else {
      dberrprint(&db, "DB::get_bulk_binary failed");
      err = true;
    }
  } else {
    if (dbexpr) db.set_target(dbexpr);
    std::map<std::string, std::string> recs;
    if (db.get_bulk(keys, &recs) >= 0) {
      std::map<std::string, std::string>::iterator it = recs.begin();
      std::map<std::string, std::string>::iterator itend = recs.end();
      while (it != itend) {
        printdata(it->first.data(), it->first.size(), px);
        oprintf("\t");
        printdata(it->second.data(), it->second.size(), px);
        oprintf("\n");
        ++it;
      }
    } else {
      dberrprint(&db, "DB::get_bulk failed");
      err = true;
    }
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform match command
static int32_t procmatch(const std::vector<std::string>& keys,
                         const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr, bool px, int32_t limit) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (dbexpr) db.set_target(dbexpr);
  std::vector<std::string>::const_iterator keys_it = keys.begin();
  std::vector<std::string>::const_iterator keys_end = keys.end();
  while (keys_it != keys_end) {
    std::vector<std::string> found;
    int64_t rc = db.match_prefix(keys_it->c_str(), &found, limit);
    if (rc == -1) {
      dberrprint(&db, "DB::match_prefix failed");
      err = true;
      break;
    }
    if (rc > 0) {
      std::vector<std::string>::iterator found_it = found.begin();
      std::vector<std::string>::iterator found_end = found.end();
      while (found_it != found_end) {
        printf("%s\n", found_it->c_str());
        ++found_it;
      }
    }
    ++keys_it;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}


// perform regex command
static int32_t procregex(const std::vector<std::string>& regexes,
                         const char* host, int32_t port, double tout,
                         const char* swname, double swtime, const char* ssname, bool ssbrd,
                         const char* dbexpr, bool px, int32_t limit) {
  kt::RemoteDB db;
  if (!db.open(host, port, tout)) {
    dberrprint(&db, "DB::open failed");
    return 1;
  }
  if (swname) db.set_signal_waiting(swname, swtime);
  if (ssname) db.set_signal_sending(ssname, ssbrd);
  bool err = false;
  if (dbexpr) db.set_target(dbexpr);
  std::vector<std::string>::const_iterator regexes_it = regexes.begin();
  std::vector<std::string>::const_iterator regexes_end = regexes.end();
  while (regexes_it != regexes_end) {
    std::vector<std::string> found;
    int64_t rc = db.match_regex(regexes_it->c_str(), &found, limit);
    if (rc == -1) {
      dberrprint(&db, "DB::match_regex failed");
      err = true;
      break;
    }
    if (rc > 0) {
      std::vector<std::string>::iterator found_it = found.begin();
      std::vector<std::string>::iterator found_end = found.end();
      while (found_it != found_end) {
        printf("%s\n", found_it->c_str());
        ++found_it;
      }
    }
    ++regexes_it;
  }
  if (!db.close()) {
    dberrprint(&db, "DB::close failed");
    err = true;
  }
  return err ? 1 : 0;
}

// END OF FILE
