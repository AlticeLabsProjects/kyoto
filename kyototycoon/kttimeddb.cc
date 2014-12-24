/*************************************************************************************************
 * Timed database
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


#include "kttimeddb.h"
#include "myconf.h"

namespace kyototycoon {                  // common namespace


/**
 * Constants for implementation.
 */
namespace {
const char SSMAGICDATA[] = "KTSS\n";     ///< magic data of the snapshot file
const uint8_t SSRECMAGIC = 0xcc;         ///< magic data for record in the snapshot
const int32_t SSIOUNIT = 1 * (1<<20);    ///< unit size of the snapshot IO
}


/** Special pointer for no operation. */
const char* const TimedDB::Visitor::NOP = (const char*)0;

/** Special pointer to remove the record. */
const char* const TimedDB::Visitor::REMOVE = (const char*)1;


/**
 * Create a child process.
 * @return the process ID of the child process to the paremnt process, or 0 for the child
 * process, or -1 on failure.
 */
static int64_t fork_impl();


/**
 * Wait for a process to terminate.
 * @param pid the process ID of the process.
 * @param stauts the pointer to a variable to indicate the stauts code.
 * @param timeout the time to wait in seconds.  If it is not more than 0, no timeout is
 * specified.
 * @return 0 on success, or -1 on failure, or 1 on timeout.
 */
static int32_t wait_impl(int64_t pid, int32_t* status, double timeout);


/**
 * Kill a termination signal to a process.
 * @param pid the process ID of the process.
 * @param crit true for critical signal, or false for normal termination.
 * @return true on success, or false on failure.
 */
static bool kill_impl(int64_t pid, bool crit);


/**
 * Change the process priority.
 * @param inc the increment of the nice value of the process priority.
 * @return true on success, or false on failure.
 */
static bool nice_impl(int32_t inc);


/**
 * The terminate the calling process.
 * @param the status code.
 */
static void exit_impl(int32_t status);


/**
 * Dump records atomically into a file.
 */
bool TimedDB::dump_snapshot_atomic(const std::string& dest, kc::Compressor* zcomp,
                                   kc::BasicDB::ProgressChecker* checker) {
  _assert_(true);
  bool forkable = false;
  kc::BasicDB* idb = db_.reveal_inner_db();
  if (idb) {
    const std::type_info& info = typeid(*idb);
    if (info == typeid(kc::ProtoHashDB) || info == typeid(kc::ProtoTreeDB) ||
        info == typeid(kc::StashDB) || info == typeid(kc::CacheDB) ||
        info == typeid(kc::GrassDB)) forkable = true;
  }
  int64_t cpid = -1;
  if (forkable) {
    class Forker : public kc::BasicDB::FileProcessor {
     public:
      explicit Forker() : cpid_(-1) {}
      int64_t cpid() {
        return cpid_;
      }
     private:
      bool process(const std::string& path, int64_t count, int64_t size) {
        cpid_ = fork_impl();
        return true;
      }
      int64_t cpid_;
    };
    Forker forker;
    db_.occupy(true, &forker);
    cpid = forker.cpid();
  };
  if (cpid > 0) {
    int64_t osiz = 0;
    int32_t cnt = 0;
    while (true) {
      cnt++;
      int32_t status;
      int32_t rv = wait_impl(cpid, &status, 1);
      if (rv == 0) return status == 0;
      if (rv < 0) {
        kill_impl(cpid, true);
        wait_impl(cpid, &status, 1);
        break;
      }
      int64_t nsiz = 0;
      kc::File::Status sbuf;
      if (kc::File::status(dest, &sbuf)) nsiz = sbuf.size;
      if (nsiz > osiz) {
        osiz = nsiz;
        cnt = 0;
      }
      if (cnt >= 10) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::LOGIC, "hanging");
        kill_impl(cpid, true);
        wait_impl(cpid, &status, 0);
        break;
      }
    }
    return false;
  } else if (cpid == 0) {
    nice_impl(1);
  }
  kc::File file;
  if (!file.open(dest, kc::File::OWRITER | kc::File::OCREATE | kc::File::OTRUNCATE)) {
    if (cpid != 0) db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  uint32_t chksum = 0;
  if (zcomp) {
    size_t zsiz;
    char* zbuf = zcomp->compress(SSMAGICDATA, sizeof(SSMAGICDATA), &zsiz);
    if (zbuf) {
      chksum = kc::hashmurmur(zbuf, zsiz);
      delete[] zbuf;
    }
  }
  uint64_t ts = UpdateLogger::clock_pure();
  uint64_t dbcount = db_.count();
  uint64_t dbsize = db_.size();
  char head[sizeof(chksum)+sizeof(ts)+sizeof(dbcount)+sizeof(dbsize)];
  char* wp = head;
  kc::writefixnum(wp, chksum, sizeof(chksum));
  wp += sizeof(chksum);
  kc::writefixnum(wp, ts, sizeof(ts));
  wp += sizeof(ts);
  kc::writefixnum(wp, dbcount, sizeof(dbcount));
  wp += sizeof(dbcount);
  kc::writefixnum(wp, dbsize, sizeof(dbsize));
  wp += sizeof(dbsize);
  if (!file.append(SSMAGICDATA, sizeof(SSMAGICDATA)) ||
      !file.append(head, sizeof(head))) {
    if (cpid != 0) db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  class Dumper : public kc::BasicDB::Visitor {
   public:
    explicit Dumper(kc::File* file, kc::Compressor* zcomp) :
        file_(file), zcomp_(zcomp), emsg_(NULL), buf_("") {}
    void flush() {
      if (buf_.empty()) return;
      if (zcomp_) {
        size_t zsiz;
        char* zbuf = zcomp_->compress(buf_.data(), buf_.size(), &zsiz);
        if (zbuf) {
          uint32_t num = kc::hton32(zsiz);
          if (!file_->append((char*)&num, sizeof(num)) || !file_->append(zbuf, zsiz))
            emsg_ = file_->error();
        } else {
          emsg_ = "compression failed";
        }
        delete[] zbuf;
      } else {
        if (!file_->append(buf_.data(), buf_.size())) emsg_ = file_->error();
      }
      buf_.clear();
    }
    const char* emsg() {
      return emsg_;
    }
   private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      char numbuf[kc::NUMBUFSIZ];
      char* wp = numbuf;
      *(wp++) = SSRECMAGIC;
      wp += kc::writevarnum(wp, ksiz);
      wp += kc::writevarnum(wp, vsiz);
      buf_.append(numbuf, wp - numbuf);
      buf_.append(kbuf, ksiz);
      buf_.append(vbuf, vsiz);
      if ((int32_t)buf_.size() >= SSIOUNIT) flush();
      return NOP;
    }
    kc::File* file_;
    kc::Compressor* zcomp_;
    const char* emsg_;
    std::string buf_;
  };
  Dumper dumper(&file, zcomp);
  bool err = false;
  if (!db_.iterate(&dumper, false, checker)) err = true;
  dumper.flush();
  const char* emsg = dumper.emsg();
  if (emsg) {
    if (cpid != 0) db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, emsg);
    err = true;
  }
  if (!file.close()) {
    if (cpid != 0) db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  if (cpid == 0) exit_impl(0);
  return !err;
}


/**
 * Load records atomically from a file.
 */
bool TimedDB::load_snapshot_atomic(const std::string& src, kc::Compressor* zcomp,
                                   kc::BasicDB::ProgressChecker* checker) {
  _assert_(true);
  kc::File file;
  if (!file.open(src, kc::File::OREADER)) {
    db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  uint32_t chksum = 0;
  if (zcomp) {
    size_t zsiz;
    char* zbuf = zcomp->compress(SSMAGICDATA, sizeof(SSMAGICDATA), &zsiz);
    if (zbuf) {
      chksum = kc::hashmurmur(zbuf, zsiz);
      delete[] zbuf;
    }
  }
  char head[sizeof(SSMAGICDATA)+sizeof(chksum)+sizeof(uint64_t)*3];
  if (!file.read(0, head, sizeof(head))) {
    db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  const char* rp = head;
  if (std::memcmp(rp, SSMAGICDATA, sizeof(SSMAGICDATA))) {
    db_.set_error(_KCCODELINE_, kc::BasicDB::Error::BROKEN, "invalid magic data");
    return false;
  }
  rp += sizeof(SSMAGICDATA);
  if (kc::readfixnum(rp, sizeof(chksum)) != chksum) {
    db_.set_error(_KCCODELINE_, kc::BasicDB::Error::BROKEN, "invalid check sum");
    return false;
  }
  if (zcomp) {
    int64_t off = sizeof(head);
    int64_t size = file.size() - off;
    while (size > (int64_t)sizeof(uint32_t)) {
      uint32_t zsiz;
      if (!file.read(off, (char*)&zsiz, sizeof(zsiz))) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
        return false;
      }
      zsiz = kc::ntoh32(zsiz);
      off += sizeof(uint32_t);
      size -= sizeof(uint32_t);
      if (zsiz < 1 || zsiz > size) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "too short region");
        return false;
      }
      char* zbuf = new char[zsiz];
      if (!file.read(off, zbuf, zsiz)) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
        delete[] zbuf;
        return false;
      }
      off += zsiz;
      size -= zsiz;
      size_t rsiz;
      char* rbuf = zcomp->decompress(zbuf, zsiz, &rsiz);
      if (!rbuf) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "decompression failed");
        delete[] zbuf;
        return false;
      }
      delete[] zbuf;
      const char* rp = rbuf;
      while (rsiz >= 3) {
        if (*(uint8_t*)rp != SSRECMAGIC) {
          db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "invalid magic data");
          delete[] rbuf;
          return false;
        }
        rp++;
        rsiz--;
        uint64_t ksiz;
        size_t step = kc::readvarnum(rp, rsiz, &ksiz);
        rp += step;
        rsiz -= step;
        uint64_t vsiz;
        step = kc::readvarnum(rp, rsiz, &vsiz);
        rp += step;
        rsiz -= step;
        if (!db_.set(rp, ksiz, rp + ksiz, vsiz)) {
          delete[] rbuf;
          return false;
        }
        rp += ksiz + vsiz;
        rsiz -= ksiz + vsiz;
      }
      delete[] rbuf;
    }
  } else {
    int64_t off = sizeof(head);
    int64_t size = file.size() - off;
    char stack[SSIOUNIT];
    while (size >= 3) {
      int64_t rsiz = size;
      if (rsiz > 1 + (int64_t)sizeof(uint32_t) * 2) rsiz = 1 + sizeof(uint32_t) * 2;
      if (!file.read(off, stack, rsiz)) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
        return false;
      }
      if (*(uint8_t*)stack != SSRECMAGIC) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "invalid magic data");
        return false;
      }
      off++;
      size--;
      char* rp = stack + 1;
      uint64_t ksiz;
      size_t step = kc::readvarnum(rp, size, &ksiz);
      off += step;
      rp += step;
      size -= step;
      uint64_t vsiz;
      step = kc::readvarnum(rp, size, &vsiz);
      off += step;
      rp += step;
      size -= step;
      rsiz = ksiz + vsiz;
      if (size < rsiz) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "too short region");
        return false;
      }
      char* rbuf = rsiz > (int64_t)sizeof(stack) ? new char[rsiz] : stack;
      if (!file.read(off, rbuf, rsiz)) {
        db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
        if (rbuf != stack) delete[] rbuf;
        return false;
      }
      if (!db_.set(rbuf, ksiz, rbuf + ksiz, vsiz)) {
        if (rbuf != stack) delete[] rbuf;
        return false;
      }
      if (rbuf != stack) delete[] rbuf;
      off += rsiz;
      size -= rsiz;
    }
    if (size != 0) {
      db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, "too long region");
      return false;
    }
  }
  if (!file.close()) {
    db_.set_error(_KCCODELINE_, kc::BasicDB::Error::SYSTEM, file.error());
    return false;
  }
  return true;
}


/**
 * Get status of an atomic snapshot file.
 */
bool TimedDB::status_snapshot_atomic(const std::string& src, uint64_t* tsp,
                                     int64_t* cntp, int64_t* sizp) {
  _assert_(true);
  kc::File file;
  if (!file.open(src, kc::File::OREADER)) return false;
  char head[sizeof(SSMAGICDATA)+sizeof(uint32_t)+sizeof(uint64_t)*3];
  if (!file.read(0, head, sizeof(head))) return false;
  if (!file.close()) return false;
  if (std::memcmp(head, SSMAGICDATA, sizeof(SSMAGICDATA))) return false;
  const char* rp = head + sizeof(SSMAGICDATA) + sizeof(uint32_t);
  uint64_t ts = kc::readfixnum(rp, sizeof(ts));
  rp += sizeof(ts);
  int64_t dbcount = kc::readfixnum(rp, sizeof(dbcount));
  rp += sizeof(dbcount);
  int64_t dbsize = kc::readfixnum(rp, sizeof(dbsize));
  rp += sizeof(dbcount);
  if (tsp) *tsp = ts;
  if (cntp) *cntp = dbcount;
  if (sizp) *sizp = dbsize;
  return true;
}


/**
 * Create a child process.
 */
static int64_t fork_impl() {
#if defined(_SYS_CYGWIN_) || defined(_SYS_MINGW_)
  _assert_(true);
  return -1;
#else
  _assert_(true);
  return (int64_t)::fork();
#endif
}


/**
 * Wait for a process to terminate.
 */
static int32_t wait_impl(int64_t pid, int32_t* status, double timeout) {
#if defined(_SYS_CYGWIN_) || defined(_SYS_MINGW_)
  _assert_(status);
  return false;
#else
  _assert_(status);
  if (timeout > 0) {
    double etime = kc::time() + timeout;
    while (true) {
      int code;
      ::pid_t rid = ::waitpid(pid, &code, WNOHANG);
      if (rid > 0) {
        *status = code;
        return 0;
      }
      if (rid != 0 && errno != EINTR) break;
      kc::Thread::sleep(0.1);
      if (kc::time() > etime) return 1;
    }
    return -1;
  }
  while (true) {
    int code;
    ::pid_t rid = ::waitpid(pid, &code, 0);
    if (rid > 0) {
      *status = code;
      return 0;
    }
    if (rid != 0 && errno != EINTR) break;
  }
  return -1;
#endif
}


/**
 * Kill a termination signal to a process.
 */
static bool kill_impl(int64_t pid, bool crit) {
#if defined(_SYS_CYGWIN_) || defined(_SYS_MINGW_)
  return false;
#else
  int signum = crit ? SIGKILL : SIGTERM;
  return ::kill(pid, signum) == 0;
#endif
}


/**
 * Change the process priority.
 */
static bool nice_impl(int32_t inc) {
#if defined(_SYS_CYGWIN_) || defined(_SYS_MINGW_)
  return true;
#else
  return ::nice(inc) != -1;
#endif
}


/**
 * The terminate the calling process.
 */
static void exit_impl(int32_t status) {
#if defined(_SYS_CYGWIN_) || defined(_SYS_MINGW_)
  _assert_(true);
  std::exit(status);
#else
  _assert_(true);
  ::_exit(status);
#endif
}


}                                        // common namespace

// END OF FILE
