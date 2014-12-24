/*************************************************************************************************
 * Update logger
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


#ifndef _KTULOG_H                        // duplication check
#define _KTULOG_H

#include <ktcommon.h>
#include <ktutil.h>

#define KTULPATHEXT  "ulog"              ///< extension of each file

namespace kyototycoon {                  // common namespace


/**
 * Update logger.
 */
class UpdateLogger {
 public:
  class Reader;
  struct FileStatus;
 private:
  struct Log;
  /** An alias of cached logs. */
  typedef std::vector<Log> Cache;
  /* The maximum size of cached logs. */
  static const size_t CACHEMAX = 65536;
  /* The magic data for meta data. */
  static const uint8_t METAMAGIC = 0xa0;
  /* The magic data for beginning mark. */
  static const uint8_t BEGMAGIC = 0xa1;
  /* The magic data for ending mark. */
  static const uint8_t ENDMAGIC = 0xa2;
  /* The accuracy of wall clock time stamp. */
  static const uint64_t TSWACC = 1000;
  /* The accuracy of logical time stamp. */
  static const uint64_t TSLACC = 1000 * 1000;
  /* The waiting seconds of auto flush. */
  static const double FLUSHWAIT = 0.1;
 public:
  /**
   * Reader of update logs.
   */
  class Reader {
   public:
    /**
     * Default constructor.
     */
    explicit Reader() : ulog_(NULL), ts_(0), id_(0), file_(), off_(0) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~Reader() {
      _assert_(true);
      if (ulog_) close();
    }
    /**
     * Open the reader.
     * @param ulog the update logger.
     * @param ts the maximum time stamp of already read logs.
     * @return true on success, or false on failure.
     */
    bool open(UpdateLogger* ulog, uint64_t ts = 0) {
      _assert_(ulog);
      if (ulog_) return false;
      ulog_ = ulog;
      ts_ = ts;
      id_ = 0;
      std::vector<std::string> names;
      kc::File::read_directory(ulog_->path_, &names);
      std::sort(names.begin(), names.end(), std::greater<std::string>());
      std::vector<std::string>::iterator it = names.begin();
      std::vector<std::string>::iterator itend = names.end();
      uint32_t lid = 0;
      std::string ext;
      kc::strprintf(&ext, "%c%s", kc::File::EXTCHR, KTULPATHEXT);
      while (it != itend && id_ < 1) {
        const std::string& path = ulog_->path_ + kc::File::PATHCHR + *it;
        if (kc::strbwm(path, ext) &&
            file_.open(path, kc::File::OREADER | kc::File::ONOLOCK, 0)) {
          ulog_->flock_.lock_reader();
          uint32_t cid = kc::atoi(it->c_str());
          if (file_.refresh()) {
            int64_t fsiz;
            uint64_t fts;
            if (read_meta(&fsiz, &fts) && fts + TSWACC * TSLACC < ts) id_ = lid;
          }
          ulog_->flock_.unlock();
          file_.close();
          lid = cid;
        }
        ++it;
      }
      if (id_ < 1) id_ = lid > 0 ? lid : 1;
      ulog_->flock_.lock_reader();
      const std::string& path = ulog_->generate_path(id_);
      if (!file_.open(path, kc::File::OREADER | kc::File::ONOLOCK, 0)) {
        ulog_->flock_.unlock();
        ulog_ = NULL;
        return false;
      }
      int64_t fsiz;
      uint64_t fts;
      if (!read_meta(&fsiz, &fts)) {
        file_.close();
        ulog_->flock_.unlock();
        ulog_ = NULL;
        return false;
      }
      ulog_->flock_.unlock();
      read_skip(ts);
      return true;
    }
    /**
     * Close the reader.
     */
    bool close() {
      _assert_(true);
      if (!ulog_) return false;
      bool err = false;
      if (!file_.close()) err = true;
      ulog_ = NULL;
      return !err;
    }
    /**
     * Read the next message.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param tsp the pointer to the variable into which the time stamp is assigned.
     * @return the pointer to the region of the message, or NULL on failure.  Because the region
     * of the return value is allocated with the the new[] operator, it should be released with
     * the delete[] operator when it is no longer in use.
     */
    char* read(size_t* sp, uint64_t* tsp) {
      _assert_(sp && tsp);
      if (!ulog_) return NULL;
      ulog_->flock_.lock_reader();
      char* mbuf = read_impl(sp, tsp);
      ulog_->flock_.unlock();
      if (!mbuf) return NULL;
      if (*tsp <= ts_) {
        delete[] mbuf;
        return NULL;
      }
      return mbuf;
    }
   private:
    /**
     * Read the meta data.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param tsp the pointer to the variable into which the time stamp is assigned.
     * @return true on success, or false on failure.
     */
    bool read_meta(int64_t* sp, uint64_t* tsp) {
      _assert_(sp && tsp);
      char hbuf[1+sizeof(uint64_t)+sizeof(uint64_t)];
      int64_t psiz = file_.size();
      if (psiz < (int64_t)sizeof(hbuf) || !file_.read(0, hbuf, sizeof(hbuf))) return false;
      const char* rp = hbuf;
      if (*(uint8_t*)(rp++) != METAMAGIC) return false;
      int64_t fsiz = kc::readfixnum(rp, sizeof(uint64_t));
      rp += sizeof(uint64_t);
      uint64_t fts = kc::readfixnum(rp, sizeof(uint64_t));
      if (psiz < fsiz) return false;
      *sp = fsiz;
      *tsp = fts;
      off_ = sizeof(hbuf);
      return true;
    }
    /**
     * Read the next message.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param tsp the pointer to the variable into which the time stamp is assigned.
     * @return the pointer to the region of the message, or NULL on failure.
     */
    char* read_impl(size_t* sp, uint64_t* tsp) {
      _assert_(sp && tsp);
      *sp = 0;
      *tsp = 0;
      char buf[1+sizeof(uint64_t)+sizeof(uint32_t)];
      if (!file_.refresh()) return NULL;
      if (!file_.read(off_, buf, sizeof(buf))) {
        bool ok = false;
        int64_t nid = id_ + 1;
        while (nid <= ulog_->id_) {
          const std::string& path = ulog_->generate_path(nid);
          if (kc::File::status(path)) {
            if (!file_.close()) return NULL;
            if (!file_.open(path, kc::File::OREADER | kc::File::ONOLOCK, 0)) return NULL;
            int64_t fsiz;
            uint64_t fts;
            if (!read_meta(&fsiz, &fts)) return NULL;
            id_ = nid;
            if (!file_.read(off_, buf, sizeof(buf))) return NULL;
            ok = true;
            break;
          }
          nid++;
        }
        if (!ok) return NULL;
      }
      int64_t noff = off_ + sizeof(buf);
      const char* rp = buf;
      if (*(uint8_t*)rp != BEGMAGIC) return NULL;
      rp++;
      uint64_t ts = kc::readfixnum(rp, sizeof(uint64_t));
      rp += sizeof(uint64_t);
      size_t msiz = kc::readfixnum(rp, sizeof(uint32_t));
      char* mbuf = new char[msiz+1];
      if (!file_.read(noff, mbuf, msiz + 1) || ((uint8_t*)mbuf)[msiz] != ENDMAGIC) {
        delete[] mbuf;
        return NULL;
      }
      off_ = noff + msiz + 1;
      *sp = msiz;
      *tsp = ts;
      return mbuf;
    }
    /**
     * Read and skip messages until a time stamp.
     * @param ts the time stamp.
     */
    void read_skip(uint64_t ts) {
      _assert_(true);
      while (true) {
        uint32_t oldid = id_;
        int64_t oldoff = off_;
        ulog_->flock_.lock_reader();
        size_t msiz;
        uint64_t mts;
        char* mbuf = read_impl(&msiz, &mts);
        ulog_->flock_.unlock();
        if (mbuf) {
          delete[] mbuf;
          if (mts > ts) {
            if (id_ == oldid) {
              off_ = oldoff;
            } else {
              off_ = 1 + sizeof(uint64_t) + sizeof(uint64_t);
            }
            break;
          }
        } else {
          break;
        }
      }
    }
    /** The update logger. */
    UpdateLogger* ulog_;
    /** The current time stamp. */
    uint64_t ts_;
    /** The current ID number. */
    uint32_t id_;
    /** The current file. */
    kc::File file_;
    /** The current offset. */
    int64_t off_;
  };
  /**
   * Status of each log file.
   */
  struct FileStatus {
    std::string path;                    ///< path
    uint64_t size;                       ///< file size
    uint64_t ts;                         ///< maximum time stamp
  };
  /**
   * Default constructor.
   */
  explicit UpdateLogger() :
      path_(), limsiz_(0), asi_(0), id_(0), file_(),
      cache_(), csiz_(0), cts_(0), clock_(), flock_(), tslock_(),
      flusher_(this), tswall_(0), tslogic_(0) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  ~UpdateLogger() {
    _assert_(true);
    if (!path_.empty()) close();
  }
  /**
   * Open the logger.
   * @param path the path of the base directory.
   * @param limsiz the limit size of each log file.  If it is not more than 0, no limit is
   * specified.  If it is kyotocabinet::INT64MIN, the logger is opened as reader.
   * @param asi the interval of auto synchronization.  If it is not more than 0, auto
   * synchronization is not performed.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& path, int64_t limsiz = -1, double asi = -1) {
    _assert_(true);
    if (!path_.empty()) return false;
    size_t psiz = path.size();
    while (psiz > 0 && path[psiz-1] == kc::File::PATHCHR) {
      psiz--;
    }
    const std::string& cpath = path.substr(0, psiz);
    kc::File::Status sbuf;
    if (kc::File::status(cpath, &sbuf)) {
      if (!sbuf.isdir) return false;
    } else {
      if (limsiz == kc::INT64MIN) return false;
      if (!kc::File::make_directory(cpath)) return false;
    }
    kc::DirStream dir;
    if (!dir.open(cpath)) return false;
    uint32_t id = 0;
    std::string name;
    while (dir.read(&name)) {
      const char* nstr = name.c_str();
      if (check_name(nstr)) {
        int64_t num = kc::atoi(nstr);
        if (num > id) id = num;
      }
    }
    path_ = cpath;
    limsiz_ = limsiz > 0 ? limsiz : kc::INT64MAX;
    asi_ = asi;
    id_ = id > 0 ? id : 1;
    const std::string& tpath = generate_path(id_);
    if (limsiz == kc::INT64MIN) {
      if (!file_.open(tpath, kc::File::OREADER | kc::File::ONOLOCK, 0)) {
        path_.clear();
        return false;
      }
    } else {
      if (!file_.open(tpath, kc::File::OWRITER | kc::File::OCREATE, 0)) {
        path_.clear();
        return false;
      }
      if (file_.size() < 1 && !write_meta()) {
        file_.close();
        path_.clear();
        return false;
      }
      if (!validate_meta()) {
        file_.close();
        path_.clear();
        return false;
      }
    }
    flusher_.start();
    return true;
  }
  /**
   * Close the logger.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    if (path_.empty()) return false;
    bool err = false;
    flusher_.stop();
    flusher_.join();
    if (flusher_.error()) err = true;
    if (csiz_ > 0 && !flush()) err = true;
    if (!file_.close()) err = true;
    path_.clear();
    return !err;
  }
  /**
   * Write a log message.
   * @param mbuf the pointer to the message region.
   * @param msiz the size of the message region.
   * @param ts the time stamp of the message.  If it is not more than 0, the current time stamp
   * is specified.
   * @return true on success, or false on failure.
   */
  bool write(const char* mbuf, size_t msiz, uint64_t ts = 0) {
    _assert_(mbuf && msiz <= kc::MEMMAXSIZ);
    char* nbuf = new char[msiz];
    std::memcpy(nbuf, mbuf, msiz);
    return write_volatile(nbuf, msiz, ts);
  }
  /**
   * Write a log message with a volatile buffer.
   * @param mbuf the pointer to the message region which is allocated by the new[] operator.
   * @param msiz the size of the message region.
   * @param ts the time stamp of the message.  If it is not more than 0, the current time stamp
   * is specified.
   * @return true on success, or false on failure.
   * @note the message region is to be deleted inside this object implicitly.
   */
  bool write_volatile(char* mbuf, size_t msiz, uint64_t ts = 0) {
    _assert_(mbuf && msiz <= kc::MEMMAXSIZ);
    if (path_.empty()) return false;
    kc::ScopedSpinLock lock(&clock_);
    if (ts < 1) ts = clock_impl();
    bool err = false;
    Log log = { mbuf, msiz, ts };
    cache_.push_back(log);
    csiz_ += 2 + sizeof(uint64_t) + sizeof(uint32_t) + msiz;
    if (ts > cts_) cts_ = ts;
    if (csiz_ > CACHEMAX && !flush()) err = true;
    return !err;
  }
  /**
   * Write multiple log messages at once.
   * @param mvec a string vector of log messages.
   * @param ts the time stamp of the message.  If it is not more than 0, the current time stamp
   * is specified.
   * @return true on success, or false on failure.
   */
  bool write_bulk(const std::vector<std::string>& mvec, uint64_t ts = 0) {
    if (path_.empty()) return false;
    kc::ScopedSpinLock lock(&clock_);
    bool err = false;
    std::vector<std::string>::const_iterator it = mvec.begin();
    std::vector<std::string>::const_iterator itend = mvec.end();
    while (it != itend) {
      size_t msiz = it->size();
      char* mbuf = new char[msiz];
      std::memcpy(mbuf, it->data(), msiz);
      uint64_t mts = ts > 0 ? ts : clock_impl();
      Log log = { mbuf, msiz, mts };
      cache_.push_back(log);
      csiz_ += 2 + sizeof(uint64_t) + sizeof(uint32_t) + msiz;
      if (mts > cts_) cts_ = mts;
      ++it;
    }
    if (csiz_ > CACHEMAX && !flush()) err = true;
    return !err;
  }
  /**
   * Get the current clock data for time stamp.
   * @return the current clock data for time stamp.
   */
  uint64_t clock() {
    _assert_(true);
    kc::ScopedSpinLock lock(&tslock_);
    return clock_impl();
  }
  /**
   * Get status of each log files.
   * @param fstvec a vector to store status structures of each log files.
   */
  void list_files(std::vector<FileStatus>* fstvec) {
    _assert_(fstvec);
    if (path_.empty()) return;
    fstvec->clear();
    std::vector<std::string> names;
    kc::File::read_directory(path_, &names);
    std::sort(names.begin(), names.end());
    std::vector<std::string>::iterator it = names.begin();
    std::vector<std::string>::iterator itend = names.end();
    while (it != itend) {
      const std::string& path = path_ + kc::File::PATHCHR + *it;
      kc::File file;
      if (file.open(path, kc::File::OREADER | kc::File::ONOLOCK, 0)) {
        flock_.lock_reader();
        if (file.refresh()) {
          char hbuf[1+sizeof(uint64_t)+sizeof(uint64_t)];
          int64_t psiz = file.size();
          if (psiz >= (int64_t)sizeof(hbuf) && file.read(0, hbuf, sizeof(hbuf))) {
            const char* rp = hbuf;
            if (*(uint8_t*)(rp++) == METAMAGIC) {
              uint64_t fsiz = kc::readfixnum(rp, sizeof(uint64_t));
              rp += sizeof(uint64_t);
              uint64_t fts = kc::readfixnum(rp, sizeof(uint64_t));
              FileStatus fs = { path, fsiz, fts };
              fstvec->push_back(fs);
            }
          }
        }
        flock_.unlock();
        file.close();
      }
      ++it;
    }
  }
  /**
   * Get the current pure clock data for time stamp.
   * @return the current pure clock data for time stamp.
   */
  static uint64_t clock_pure() {
    return (uint64_t)(kc::time() * TSWACC) * TSLACC;
  }
 private:
  /**
   * Log message.
   */
  struct Log {
    char* mbuf;                          ///< pointer to the message
    size_t msiz;                         ///< size of the message
    uint64_t ts;                         ///< time stamp
  };
  /**
   * Automatic flusher of cacheed logs.
   */
  class AutoFlusher : public kc::Thread {
   public:
    AutoFlusher(UpdateLogger* ulog) : ulog_(ulog), alive_(true), error_(false) {}
    void run() {
      double asnext = 0;
      while (alive_ && !error_) {
        kc::Thread::sleep(FLUSHWAIT);
        if (ulog_->clock_.lock_try()) {
          if (ulog_->csiz_ > 0 && !ulog_->flush()) error_ = true;
          ulog_->clock_.unlock();
        }
        if (ulog_->asi_ > 0 && kc::time() >= asnext) {
          ulog_->clock_.lock();
          ulog_->file_.synchronize(true);
          ulog_->clock_.unlock();
          asnext = kc::time() + ulog_->asi_;
        }
      }
    }
    void stop() {
      alive_ = false;
    }
    bool error() {
      return error_;
    }
   private:
    UpdateLogger* ulog_;
    bool alive_;
    bool error_;
  };
  /**
   * Check whether a file name is for update log.
   * @param name the file name.
   * @return true for update log, or false if not.
   */
  bool check_name(const char* name) {
    _assert_(name);
    const char* pv = std::strchr(name, kc::File::PATHCHR);
    if (pv) name = pv + 1;
    const char* bp = name;
    while (*name != '\0') {
      if (*name == kc::File::EXTCHR) {
        if (name - bp != 10) return false;
        return !std::strcmp(name + 1, KTULPATHEXT);
      }
      if (*name < '0' || *name > '9') return false;
      name++;
    }
    return false;
  }
  /**
   * Generate the path of a update log file.
   * @param id the ID number of the file.
   * @return the path of the file.
   */
  std::string generate_path(uint32_t id) {
    _assert_(true);
    return kc::strprintf("%s%c%010u%c%s", path_.c_str(), kc::File::PATHCHR,
                         id, kc::File::EXTCHR, KTULPATHEXT);
  }
  /**
   * Write meta data.
   * @return true on success, or false on failure.
   */
  bool write_meta() {
    _assert_(true);
    char hbuf[1+sizeof(uint64_t)+sizeof(uint64_t)];
    char* wp = hbuf;
    *(wp++) = METAMAGIC;
    int64_t psiz = file_.size();
    if (psiz < (int64_t)sizeof(hbuf)) psiz = sizeof(hbuf);
    kc::writefixnum(wp, psiz, sizeof(uint64_t));
    wp += sizeof(uint64_t);
    kc::writefixnum(wp, cts_, sizeof(uint64_t));
    return file_.write(0, hbuf, sizeof(hbuf));
  }
  /**
   * Validate the meta data.
   * @return true on success, or false on failure.
   */
  bool validate_meta() {
    _assert_(true);
    char hbuf[1+sizeof(uint64_t)+sizeof(uint64_t)];
    int64_t psiz = file_.size();
    if (psiz < (int64_t)sizeof(hbuf) || !file_.read(0, hbuf, sizeof(hbuf))) return false;
    const char* rp = hbuf;
    if (*(uint8_t*)(rp++) != METAMAGIC) return false;
    int64_t fsiz = kc::readfixnum(rp, sizeof(uint64_t));
    rp += sizeof(uint64_t);
    uint64_t fts = kc::readfixnum(rp, sizeof(uint64_t));
    if (psiz < fsiz || fsiz < (int64_t)sizeof(hbuf)) return false;
    if (psiz > fsiz && !file_.truncate(fsiz)) return false;
    tswall_ = fts / TSLACC + 1;
    return true;
  }
  /**
   * Flush cached logs into a file.
   * @return true on success, or false on failure.
   */
  bool flush() {
    _assert_(true);
    bool err = false;
    flock_.lock_writer();
    if (file_.size() >= limsiz_) {
      if (asi_ > 0 && !file_.synchronize(true)) err = true;
      if (!file_.close()) err = true;
      id_++;
      const std::string& tpath = generate_path(id_);
      if (!file_.open(tpath, kc::File::OWRITER | kc::File::OCREATE | kc::File::OTRUNCATE, 0))
        err = true;
      if (!write_meta()) err = true;
    }
    char* cbuf = new char[csiz_];
    char* wp = cbuf;
    Cache::iterator it = cache_.begin();
    Cache::iterator itend = cache_.end();
    while (it != itend) {
      Log log = *it;
      *(wp++) = BEGMAGIC;
      kc::writefixnum(wp, log.ts, sizeof(uint64_t));
      wp += sizeof(uint64_t);
      kc::writefixnum(wp, log.msiz, sizeof(uint32_t));
      wp += sizeof(uint32_t);
      std::memcpy(wp, log.mbuf, log.msiz);
      wp += log.msiz;
      *(wp++) = ENDMAGIC;
      delete[] log.mbuf;
      ++it;
    }
    if (!file_.append(cbuf, csiz_)) err = true;
    if (!err && !write_meta()) err = true;
    delete[] cbuf;
    cache_.clear();
    csiz_ = 0;
    flock_.unlock();
    return !err;
  }
  /**
   * Get the current clock data for time stamp.
   * @return the current clock data for time stamp.
   */
  uint64_t clock_impl() {
    _assert_(true);
    uint64_t ct = kc::time() * TSWACC;
    if (ct > tswall_) {
      tswall_ = ct;
      tslogic_ = 0;
    } else {
      tslogic_++;
    }
    return tswall_ * TSLACC + tslogic_;
  }
  /** Dummy constructor to forbid the use. */
  UpdateLogger(const UpdateLogger&);
  /** Dummy Operator to forbid the use. */
  UpdateLogger& operator =(const UpdateLogger&);
  /** The path of the base directory. */
  std::string path_;
  /** The limit size of each file. */
  int64_t limsiz_;
  /** The inverval of auto synchronization. */
  double asi_;
  /** The ID number of the current file. */
  uint32_t id_;
  /** The current file. */
  kc::File file_;
  /** The cached logs. */
  Cache cache_;
  /** The size of the cache. */
  size_t csiz_;
  /** The last time stamp in the cache. */
  uint64_t cts_;
  /** The cache lock. */
  kc::SpinLock clock_;
  /** The file lock. */
  kc::SpinRWLock flock_;
  /** The time stamp lock. */
  kc::SpinLock tslock_;
  /** The automatic flusher. */
  AutoFlusher flusher_;
  /** The wall clock time stamp. */
  uint64_t tswall_;
  /** The logical time stamp. */
  uint64_t tslogic_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
