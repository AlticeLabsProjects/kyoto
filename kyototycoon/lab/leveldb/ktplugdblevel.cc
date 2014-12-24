/*************************************************************************************************
 * A pluggable database for LevelDB
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


#include <ktplugdb.h>
#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/comparator.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include <leveldb/slice.h>
#include <leveldb/status.h>
#include <leveldb/env.h>

namespace kc = kyotocabinet;
namespace kt = kyototycoon;
namespace lv = leveldb;


// pluggable database for LevelDB
class LevelDB : public kt::PluggableDB {
 public:
  class Cursor;
 private:
  class ScopedVisitor;
  typedef std::list<Cursor*> CursorList;
 public:
  // cursor to indicate a record
  class Cursor : public BasicDB::Cursor {
    friend class LevelDB;
   public:
    // constructor
    explicit Cursor(LevelDB* db) : db_(db), it_(NULL) {
      _assert_(db);
      kc::ScopedRWLock lock(&db_->mlock_, true);
      db_ = db;
      it_ = db_->db_->NewIterator(lv::ReadOptions());
      db_->curs_.push_back(this);
    }
    // destructor
    virtual ~Cursor() {
      _assert_(true);
      if (!db_) return;
      kc::ScopedRWLock lock(&db_->mlock_, true);
      db_->curs_.remove(this);
      delete it_;
    }
    // accept a visitor to the current record
    bool accept(Visitor* visitor, bool writable = true, bool step = false) {
      _assert_(visitor);
      kc::ScopedRWLock lock(&db_->mlock_, writable);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      bool err = false;
      if (!accept_impl(visitor, step)) err = true;
      return !err;
    }
    // jump the cursor to the first record for forward scan
    bool jump() {
      _assert_(true);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      bool err = false;
      it_->SeekToFirst();
      if (it_->status().ok()) {
        if (!it_->Valid()) {
          db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
          err = true;
        }
      } else {
        db_->set_error(_KCCODELINE_, Error::SYSTEM, "Iterator failed");
        err = true;
      }
      return !err;
    }
    // jump the cursor to a record for forward scan
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      bool err = false;
      std::string key(kbuf, ksiz);
      it_->Seek(key);
      if (!it_->status().ok()) {
        db_->set_error(_KCCODELINE_, Error::SYSTEM, "Iterator failed");
        err = true;
      }
      if (!it_->Valid()) {
        db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
        err = true;
      }
      return !err;
    }
    // jump the cursor to a record for forward scan
    bool jump(const std::string& key) {
      _assert_(true);
      return jump(key.data(), key.size());
    }
    // jump the cursor to the last record for backward scan
    bool jump_back() {
      _assert_(true);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      db_->set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
      return false;
    }
    // jump the cursor to a record for backward scan
    bool jump_back(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      db_->set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
      return false;
    }
    // jump the cursor to a record for backward scan
    bool jump_back(const std::string& key) {
      _assert_(true);
      return jump_back(key.data(), key.size());
    }
    // step the cursor to the next record
    bool step() {
      _assert_(true);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      bool err = true;
      if (it_->Valid()) {
        it_->Next();
        if (it_->status().ok()) {
          if (!it_->Valid()) {
            db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
            err = true;
          }
        } else {
          db_->set_error(_KCCODELINE_, Error::SYSTEM, "Iterator failed");
          err = true;
        }
      } else {
        db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
        err = true;
      }
      return !err;
    }
    // step the cursor to the previous record
    bool step_back() {
      _assert_(true);
      kc::ScopedRWLock lock(&db_->mlock_, false);
      if (!db_->db_) {
        db_->set_error(_KCCODELINE_, Error::INVALID, "not opened");
        return false;
      }
      db_->set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
      return false;
    }
    // get the database object
    LevelDB* db() {
      _assert_(true);
      return db_;
    }
   private:
    // accept a visitor to the current record
    bool accept_impl(Visitor* visitor, bool step) {
      bool err = false;
      if (it_->Valid()) {
        const std::string& key = it_->key().ToString();
        const std::string& value = it_->value().ToString();
        size_t rsiz;
        const char* rbuf = visitor->visit_full(key.data(), key.size(),
                                               value.data(), value.size(), &rsiz);
        if (rbuf == kc::BasicDB::Visitor::REMOVE) {
          lv::WriteOptions wopts;
          if (db_->autosync_) wopts.sync = true;
          lv::Status status = db_->db_->Delete(wopts, key);
          if (!status.ok()) {
            db_->set_error(_KCCODELINE_, Error::SYSTEM, "DB::Delete failed");
            err = true;
          }
        } else if (rbuf != kc::BasicDB::Visitor::NOP) {
          lv::WriteOptions wopts;
          if (db_->autosync_) wopts.sync = true;
          std::string rvalue(rbuf, rsiz);
          lv::Status status = db_->db_->Put(wopts, key, rvalue);
          if (!status.ok()) {
            db_->set_error(_KCCODELINE_, Error::SYSTEM, "DB::Put failed");
            err = true;
          }
        }
        if (step && it_->Valid()) it_->Next();
      } else {
        db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
        err = true;
      }
      return !err;
    }
    Cursor(const Cursor&);
    Cursor& operator =(const Cursor&);
    LevelDB* db_;
    lv::Iterator* it_;
  };
  // default constructor
  explicit LevelDB() :
      mlock_(), rlock_(RLOCKSLOT), error_(), logger_(NULL), logkinds_(0),
      mtrigger_(NULL), path_(""), db_(NULL), autosync_(false) {
    _assert_(true);
  }
  // destructor
  ~LevelDB() {
    _assert_(true);
    if (db_) close();
  }
  // get the last happened error
  Error error() const {
    _assert_(true);
    return error_;
  }
  // set the error information
  void set_error(const char* file, int32_t line, const char* func,
                 Error::Code code, const char* message) {
    _assert_(file && line > 0 && func && message);
    error_->set(code, message);
    if (logger_) {
      Logger::Kind kind = code == Error::BROKEN || code == Error::SYSTEM ?
          Logger::ERROR : Logger::INFO;
      if (kind & logkinds_)
        report(file, line, func, kind, "%d: %s: %s", code, Error::codename(code), message);
    }
  }
  // open a database file
  bool open(const std::string& path, uint32_t mode = OWRITER | OCREATE) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (db_) {
      set_error(_KCCODELINE_, Error::INVALID, "already opened");
      return false;
    }
    std::vector<std::string> elems;
    kc::strsplit(path, '#', &elems);
    std::string fpath;
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    if (it != itend) {
      fpath = *it;
      ++it;
    }
    if (fpath.empty()) {
      set_error(_KCCODELINE_, Error::INVALID, "invalid path");
      return false;
    }
    if ((mode & OWRITER) && (mode & OTRUNCATE)) lv::DestroyDB(path, lv::Options());
    lv::Options options;
    if ((mode & OWRITER) && (mode & OCREATE)) options.create_if_missing = true;
    lv::Status status = lv::DB::Open(options, fpath, &db_);
    if (!status.ok()) {
      set_error(_KCCODELINE_, Error::SYSTEM, "DB::Open failed");
      return false;
    }
    path_.append(fpath);
    if ((mode & OWRITER) && ((mode & OAUTOTRAN) || (mode & OAUTOSYNC))) autosync_ = true;
    trigger_meta(MetaTrigger::OPEN, "open");
    return true;
  }
  // close the database file
  bool close() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    if (!curs_.empty()) {
      CursorList::const_iterator cit = curs_.begin();
      CursorList::const_iterator citend = curs_.end();
      while (cit != citend) {
        Cursor* cur = *cit;
        cur->db_ = NULL;
        delete cur->it_;
        ++cit;
      }
    }
    delete db_;
    db_ = NULL;
    path_.clear();
    trigger_meta(MetaTrigger::CLOSE, "close");
    return true;
  }
  // accept a visitor to a record
  bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable = true) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && visitor);
    kc::ScopedRWLock lock(&mlock_, false);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    return accept_impl(kbuf, ksiz, visitor, writable);
  }
  // accept a visitor to multiple records at once
  bool accept_bulk(const std::vector<std::string>& keys, Visitor* visitor,
                   bool writable = true) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, writable);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    ScopedVisitor svis(visitor);
    bool err = false;
    std::vector<std::string>::const_iterator kit = keys.begin();
    std::vector<std::string>::const_iterator kitend = keys.end();
    while (kit != kitend) {
      if (!accept_impl(kit->data(), kit->size(), visitor, writable)) err = true;
      ++kit;
    }
    return !err;
  }
  // iterate to accept a visitor for each record
  bool iterate(Visitor *visitor, bool writable = true, ProgressChecker* checker = NULL) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    ScopedVisitor svis(visitor);
    bool err = false;
    if (!iterate_impl(visitor, checker)) err = true;
    trigger_meta(MetaTrigger::ITERATE, "iterate");
    return !err;
  }
  // scan each record in parallel
  bool scan_parallel(Visitor *visitor, size_t thnum, ProgressChecker* checker = NULL) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, false);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    ScopedVisitor svis(visitor);
    bool err = true;
    if (!scan_parallel_impl(visitor, thnum, checker)) err = true;
    trigger_meta(MetaTrigger::ITERATE, "scan_parallel");
    return !err;
  }
  // synchronize updated contents with the file and the device
  bool synchronize(bool hard = false, FileProcessor* proc = NULL,
                   ProgressChecker* checker = NULL) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    bool err = false;
    if (proc && !proc->process(path_, 0, 0)) {
      set_error(_KCCODELINE_, Error::LOGIC, "postprocessing failed");
      err = true;
    }
    trigger_meta(MetaTrigger::SYNCHRONIZE, "synchronize");
    return !err;
  }
  // occupy database by locking and do something meanwhile
  bool occupy(bool writable = true, FileProcessor* proc = NULL) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, writable);
    bool err = false;
    if (proc && !proc->process(path_, 0, 0)) {
      set_error(_KCCODELINE_, Error::LOGIC, "processing failed");
      err = true;
    }
    trigger_meta(MetaTrigger::OCCUPY, "occupy");
    return !err;
  }
  // begin transaction
  bool begin_transaction(bool hard = false) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
    return false;
  }
  // try to begin transaction
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
    return false;
  }
  // end transaction
  bool end_transaction(bool commit = true) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
    return false;
  }
  // remove all records
  bool clear() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    if (!curs_.empty()) {
      CursorList::const_iterator cit = curs_.begin();
      CursorList::const_iterator citend = curs_.end();
      while (cit != citend) {
        Cursor* cur = *cit;
        cur->db_ = NULL;
        delete cur->it_;
        cur->it_ = NULL;
        ++cit;
      }
    }
    delete db_;
    bool err = false;
    lv::DestroyDB(path_, lv::Options());
    lv::Options options;
    options.create_if_missing = true;
    lv::Status status = lv::DB::Open(options, path_, &db_);
    if (status.ok()) {
      if (!curs_.empty()) {
        CursorList::const_iterator cit = curs_.begin();
        CursorList::const_iterator citend = curs_.end();
        while (cit != citend) {
          Cursor* cur = *cit;
          cur->db_ = this;
          cur->it_ = db_->NewIterator(lv::ReadOptions());
          ++cit;
        }
      }
    } else {
      set_error(_KCCODELINE_, Error::SYSTEM, "DB::Open failed");
      db_ = NULL;
      err = true;
    }
    trigger_meta(MetaTrigger::CLEAR, "clear");
    return true;
  }
  // get the number of records
  int64_t count() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
    return -1;
  }
  // get the size of the database file
  int64_t size() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    if (!db_) {
      set_error(_KCCODELINE_, Error::INVALID, "not opened");
      return false;
    }
    set_error(_KCCODELINE_, Error::NOIMPL, "not implemented");
    return -1;
  }
  // get the path of the database file
  std::string path() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    return path_;
  }
  // get the miscellaneous status information
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    kc::ScopedRWLock lock(&mlock_, true);
    (*strmap)["type"] = kc::strprintf("%u", (unsigned)TYPEMISC);
    (*strmap)["path"] = path_;
    return true;
  }
  // create a cursor object
  Cursor* cursor() {
    _assert_(true);
    return new Cursor(this);
  }
  // write a log message
  void log(const char* file, int32_t line, const char* func, Logger::Kind kind,
           const char* message) {
    _assert_(file && line > 0 && func && message);
    kc::ScopedRWLock lock(&mlock_, false);
    if (!logger_) return;
    logger_->log(file, line, func, kind, message);
  }
  // set the internal logger
  bool tune_logger(Logger* logger, uint32_t kinds = Logger::WARN | Logger::ERROR) {
    _assert_(logger);
    kc::ScopedRWLock lock(&mlock_, true);
    logger_ = logger;
    logkinds_ = kinds;
    return true;
  }
  // set the internal meta operation trigger
  bool tune_meta_trigger(MetaTrigger* trigger) {
    _assert_(trigger);
    kc::ScopedRWLock lock(&mlock_, true);
    mtrigger_ = trigger;
    return true;
  }
 protected:
  // report a message for debugging
  void report(const char* file, int32_t line, const char* func, Logger::Kind kind,
              const char* format, ...) {
    _assert_(file && line > 0 && func && format);
    if (!logger_ || !(kind & logkinds_)) return;
    std::string message;
    kc::strprintf(&message, "%s: ", path_.empty() ? "-" : path_.c_str());
    va_list ap;
    va_start(ap, format);
    kc::vstrprintf(&message, format, ap);
    va_end(ap);
    logger_->log(file, line, func, kind, message.c_str());
  }
  // trigger a meta database operation
  void trigger_meta(MetaTrigger::Kind kind, const char* message) {
    _assert_(message);
    if (mtrigger_) mtrigger_->trigger(kind, message);
  }
 private:
  // scoped visitor
  class ScopedVisitor {
   public:
    explicit ScopedVisitor(Visitor* visitor) : visitor_(visitor) {
      _assert_(visitor);
      visitor_->visit_before();
    }
    ~ScopedVisitor() {
      _assert_(true);
      visitor_->visit_after();
    }
   private:
    Visitor* visitor_;
  };
  // accept a visitor to a record
  bool accept_impl(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable) {
    size_t lidx = kc::hashmurmur(kbuf, ksiz) % RLOCKSLOT;
    if (writable) {
      rlock_.lock_writer(lidx);
    } else {
      rlock_.lock_reader(lidx);
    }
    std::string key(kbuf, ksiz);
    std::string value;
    lv::Status status = db_->Get(lv::ReadOptions(), key, &value);
    const char* rbuf;
    size_t rsiz;
    if (status.ok()) {
      rbuf = visitor->visit_full(kbuf, ksiz, value.data(), value.size(), &rsiz);
    } else {
      rbuf = visitor->visit_empty(kbuf, ksiz, &rsiz);
    }
    bool err = false;
    if (rbuf == kc::BasicDB::Visitor::REMOVE) {
      lv::WriteOptions wopts;
      if (autosync_) wopts.sync = true;
      status = db_->Delete(wopts, key);
      if (!status.ok()) {
        set_error(_KCCODELINE_, Error::SYSTEM, "DB::Delete failed");
        err = true;
      }
    } else if (rbuf != kc::BasicDB::Visitor::NOP) {
      lv::WriteOptions wopts;
      if (autosync_) wopts.sync = true;
      std::string rvalue(rbuf, rsiz);
      status = db_->Put(wopts, key, rvalue);
      if (!status.ok()) {
        set_error(_KCCODELINE_, Error::SYSTEM, "DB::Put failed");
        err = true;
      }
    }
    rlock_.unlock(lidx);
    return !err;
  }
  // iterate to accept a visitor for each record.
  bool iterate_impl(Visitor* visitor, ProgressChecker* checker) {
    bool err = false;
    lv::Iterator* it = db_->NewIterator(lv::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const std::string& key = it->key().ToString();
      const std::string& value = it->value().ToString();
      size_t rsiz;
      const char* rbuf = visitor->visit_full(key.data(), key.size(),
                                             value.data(), value.size(), &rsiz);
      if (rbuf == kc::BasicDB::Visitor::REMOVE) {
        lv::WriteOptions wopts;
        if (autosync_) wopts.sync = true;
        lv::Status status = db_->Delete(wopts, key);
        if (!status.ok()) {
          set_error(_KCCODELINE_, Error::SYSTEM, "DB::Delete failed");
          err = true;
        }
        if (!it->Valid()) break;
      } else if (rbuf != kc::BasicDB::Visitor::NOP) {
        lv::WriteOptions wopts;
        if (autosync_) wopts.sync = true;
        std::string rvalue(rbuf, rsiz);
        lv::Status status = db_->Put(wopts, key, rvalue);
        if (!status.ok()) {
          set_error(_KCCODELINE_, Error::SYSTEM, "DB::Put failed");
          err = true;
        }
        if (!it->Valid()) break;
      }
    }
    if (!it->status().ok()) {
      set_error(_KCCODELINE_, Error::SYSTEM, "Iterator failed");
      err = true;
    }
    return !err;
  }
  // scan each record in parallel
  bool scan_parallel_impl(Visitor *visitor, size_t thnum, ProgressChecker* checker) {
    bool err = false;
    lv::Iterator* it = db_->NewIterator(lv::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const std::string& key = it->key().ToString();
      const std::string& value = it->value().ToString();
      size_t rsiz;
      visitor->visit_full(key.data(), key.size(), value.data(), value.size(), &rsiz);
    }
    if (!it->status().ok()) {
      set_error(_KCCODELINE_, Error::SYSTEM, "Iterator failed");
      err = true;
    }
    return !err;
  }
  static const int32_t RLOCKSLOT = 1024;
  LevelDB(const LevelDB&);
  LevelDB& operator =(const LevelDB&);
  kc::RWLock mlock_;
  kc::SlottedRWLock rlock_;
  kc::TSD<kc::BasicDB::Error> error_;
  kc::BasicDB::Logger* logger_;
  uint32_t logkinds_;
  MetaTrigger* mtrigger_;
  CursorList curs_;
  std::string path_;
  lv::DB* db_;
  bool autosync_;
};


// initializer called by the main server
extern "C" void* ktdbinit() {
  return new LevelDB;
}



// END OF FILE
