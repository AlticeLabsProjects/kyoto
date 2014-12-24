/*************************************************************************************************
 * A pluggable database of no operation
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

namespace kc = kyotocabinet;
namespace kt = kyototycoon;


// pluggable database of no operation
class VoidDB : public kt::PluggableDB {
 public:
  class Cursor;
 private:
  class ScopedVisitor;
 public:
  // cursor to indicate a record
  class Cursor : public BasicDB::Cursor {
    friend class VoidDB;
   public:
    // constructor
    explicit Cursor(VoidDB* db) : db_(db) {
      _assert_(db);
    }
    // destructor
    virtual ~Cursor() {
      _assert_(true);
    }
    // accept a visitor to the current record
    bool accept(Visitor* visitor, bool writable = true, bool step = false) {
      _assert_(visitor);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to the first record for forward scan
    bool jump() {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to a record for forward scan
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to a record for forward scan
    bool jump(const std::string& key) {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to the last record for backward scan
    bool jump_back() {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to a record for backward scan
    bool jump_back(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // jump the cursor to a record for backward scan
    bool jump_back(const std::string& key) {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // step the cursor to the next record
    bool step() {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // step the cursor to the previous record
    bool step_back() {
      _assert_(true);
      db_->set_error(_KCCODELINE_, Error::NOREC, "no record");
      return false;
    }
    // get the database object
    VoidDB* db() {
      _assert_(true);
      return db_;
    }
   private:
    Cursor(const Cursor&);
    Cursor& operator =(const Cursor&);
    VoidDB* db_;
  };
  // default constructor
  explicit VoidDB() :
      mlock_(), error_(), logger_(NULL), logkinds_(0), mtrigger_(NULL), path_("") {
    _assert_(true);
  }
  // destructor
  ~VoidDB() {
    _assert_(true);
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
    path_.append(path);
    trigger_meta(MetaTrigger::OPEN, "open");
    return true;
  }
  // close the database file
  bool close() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    path_.clear();
    trigger_meta(MetaTrigger::CLOSE, "close");
    return true;
  }
  // accept a visitor to a record
  bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable = true) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && visitor);
    kc::ScopedRWLock lock(&mlock_, false);
    size_t vsiz;
    visitor->visit_empty(kbuf, ksiz, &vsiz);
    return true;
  }
  // accept a visitor to multiple records at once
  bool accept_bulk(const std::vector<std::string>& keys, Visitor* visitor,
                   bool writable = true) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, writable);
    ScopedVisitor svis(visitor);
    std::vector<std::string>::const_iterator kit = keys.begin();
    std::vector<std::string>::const_iterator kitend = keys.end();
    while (kit != kitend) {
      size_t vsiz;
      visitor->visit_empty(kit->data(), kit->size(), &vsiz);
      ++kit;
    }
    return true;
  }
  // iterate to accept a visitor for each record
  bool iterate(Visitor *visitor, bool writable = true, ProgressChecker* checker = NULL) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, true);
    ScopedVisitor svis(visitor);
    trigger_meta(MetaTrigger::ITERATE, "iterate");
    return true;
  }
  // scan each record in parallel
  bool scan_parallel(Visitor *visitor, size_t thnum, ProgressChecker* checker = NULL) {
    _assert_(visitor);
    kc::ScopedRWLock lock(&mlock_, false);
    ScopedVisitor svis(visitor);
    trigger_meta(MetaTrigger::ITERATE, "scan_parallel");
    return true;
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
    trigger_meta(MetaTrigger::BEGINTRAN, "begin_transaction");
    return true;
  }
  // try to begin transaction
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    trigger_meta(MetaTrigger::BEGINTRAN, "begin_transaction_try");
    return true;
  }
  // end transaction
  bool end_transaction(bool commit = true) {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    trigger_meta(commit ? MetaTrigger::COMMITTRAN : MetaTrigger::ABORTTRAN, "end_transaction");
    return true;
  }
  // remove all records
  bool clear() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, true);
    trigger_meta(MetaTrigger::CLEAR, "clear");
    return true;
  }
  // get the number of records
  int64_t count() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    return 0;
  }
  // get the size of the database file
  int64_t size() {
    _assert_(true);
    kc::ScopedRWLock lock(&mlock_, false);
    return 0;
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
    (*strmap)["count"] = "0";
    (*strmap)["size"] = "0";
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
  VoidDB(const VoidDB&);
  VoidDB& operator =(const VoidDB&);
  kc::RWLock mlock_;
  kc::TSD<kc::BasicDB::Error> error_;
  kc::BasicDB::Logger* logger_;
  uint32_t logkinds_;
  MetaTrigger* mtrigger_;
  std::string path_;
};


// initializer called by the main server
extern "C" void* ktdbinit() {
  return new VoidDB;
}



// END OF FILE
