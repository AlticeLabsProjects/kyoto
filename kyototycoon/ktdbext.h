/*************************************************************************************************
 * Database extension
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


#ifndef _KTDBEXT_H                       // duplication check
#define _KTDBEXT_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktulog.h>
#include <ktshlib.h>
#include <kttimeddb.h>

namespace kyototycoon {                  // common namespace


/**
 * MapReduce framework.
 * @note Although this framework is not distributed or concurrent, it is useful for aggregate
 * calculation with less CPU loading and less memory usage.
 */
class MapReduce {
 public:
  class ValueIterator;
 private:
  class FlushThread;
  class ReduceTaskQueue;
  class MapVisitor;
  struct MergeLine;
  /** An alias of vector of loaded values. */
  typedef std::vector<std::string> Values;
  /** The default number of temporary databases. */
  static const size_t DEFDBNUM = 8;
  /** The maxinum number of temporary databases. */
  static const size_t MAXDBNUM = 256;
  /** The default cache limit. */
  static const int64_t DEFCLIM = 512LL << 20;
  /** The default cache bucket numer. */
  static const int64_t DEFCBNUM = 1048583LL;
  /** The bucket number of temprary databases. */
  static const int64_t DBBNUM = 512LL << 10;
  /** The page size of temprary databases. */
  static const int32_t DBPSIZ = 32768;
  /** The mapped size of temprary databases. */
  static const int64_t DBMSIZ = 516LL * 4096;
  /** The page cache capacity of temprary databases. */
  static const int64_t DBPCCAP = 16LL << 20;
  /** The default number of threads in parallel mode. */
  static const size_t DEFTHNUM = 8;
  /** The number of slots of the record lock. */
  static const int32_t RLOCKSLOT = 256;
 public:
  /**
   * Value iterator for the reducer.
   */
  class ValueIterator {
    friend class MapReduce;
   public:
    /**
     * Get the next value.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @return the pointer to the next value region, or NULL if no value remains.
     */
    const char* next(size_t* sp) {
      _assert_(sp);
      if (!vptr_) {
        if (vit_ == vend_) return NULL;
        vptr_ = vit_->data();
        vsiz_ = vit_->size();
        vit_++;
      }
      uint64_t vsiz;
      size_t step = kc::readvarnum(vptr_, vsiz_, &vsiz);
      vptr_ += step;
      vsiz_ -= step;
      const char* vbuf = vptr_;
      *sp = vsiz;
      vptr_ += vsiz;
      vsiz_ -= vsiz;
      if (vsiz_ < 1) vptr_ = NULL;
      return vbuf;
    }
   private:
    /**
     * Default constructor.
     */
    explicit ValueIterator(Values::const_iterator vit, Values::const_iterator vend) :
        vit_(vit), vend_(vend), vptr_(NULL), vsiz_(0) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~ValueIterator() {
      _assert_(true);
    }
    /** Dummy constructor to forbid the use. */
    ValueIterator(const ValueIterator&);
    /** Dummy Operator to forbid the use. */
    ValueIterator& operator =(const ValueIterator&);
    /** The current iterator of loaded values. */
    Values::const_iterator vit_;
    /** The ending iterator of loaded values. */
    Values::const_iterator vend_;
    /** The pointer of the current value. */
    const char* vptr_;
    /** The size of the current value. */
    size_t vsiz_;
  };
  /**
   * Execution options.
   */
  enum Option {
    XNOLOCK = 1 << 0,                    ///< avoid locking against update operations
    XPARAMAP = 1 << 1,                   ///< run mappers in parallel
    XPARARED = 1 << 2,                   ///< run reducers in parallel
    XPARAFLS = 1 << 3,                   ///< run cache flushers in parallel
    XNOCOMP = 1 << 8                     ///< avoid compression of temporary databases
  };
  /**
   * Default constructor.
   */
  explicit MapReduce() :
      rcomp_(NULL), tmpdbs_(NULL), dbnum_(DEFDBNUM), dbclock_(0),
      mapthnum_(DEFTHNUM), redthnum_(DEFTHNUM), flsthnum_(DEFTHNUM),
      cache_(NULL), csiz_(0), clim_(DEFCLIM), cbnum_(DEFCBNUM), flsths_(NULL),
      redtasks_(NULL), redaborted_(false), rlocks_(NULL) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~MapReduce() {
    _assert_(true);
  }
  /**
   * Map a record data.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @return true on success, or false on failure.
   * @note This function can call the MapReduce::emit method to emit a record.  To avoid
   * deadlock, any explicit database operation must not be performed in this function.
   */
  virtual bool map(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) = 0;
  /**
   * Reduce a record data.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param iter the iterator to get the values.
   * @return true on success, or false on failure.
   * @note To avoid deadlock, any explicit database operation must not be performed in this
   * function.
   */
  virtual bool reduce(const char* kbuf, size_t ksiz, ValueIterator* iter) = 0;
  /**
   * Preprocess the map operations.
   * @return true on success, or false on failure.
   * @note This function can call the MapReduce::emit method to emit a record.  To avoid
   * deadlock, any explicit database operation must not be performed in this function.
   */
  virtual bool preprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Mediate between the map and the reduce phases.
   * @return true on success, or false on failure.
   * @note This function can call the MapReduce::emit method to emit a record.  To avoid
   * deadlock, any explicit database operation must not be performed in this function.
   */
  virtual bool midprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Postprocess the reduce operations.
   * @return true on success, or false on failure.
   * @note To avoid deadlock, any explicit database operation must not be performed in this
   * function.
   */
  virtual bool postprocess() {
    _assert_(true);
    return true;
  }
  /**
   * Process a log message.
   * @param name the name of the event.
   * @param message a supplement message.
   * @return true on success, or false on failure.
   */
  virtual bool log(const char* name, const char* message) {
    _assert_(name && message);
    return true;
  }
  /**
   * Execute the MapReduce process about a database.
   * @param db the source database.
   * @param tmppath the path of a directory for the temporary data storage.  If it is an empty
   * string, temporary data are handled on memory.
   * @param opts the optional features by bitwise-or: MapReduce::XNOLOCK to avoid locking
   * against update operations by other threads, MapReduce::XNOCOMP to avoid compression of
   * temporary databases.
   * @return true on success, or false on failure.
   */
  bool execute(TimedDB* db, const std::string& tmppath = "", uint32_t opts = 0) {
    int64_t count = db->count();
    if (count < 0) {
      if (db->error() != kc::BasicDB::Error::NOIMPL) return false;
      count = 0;
    }
    bool err = false;
    double stime, etime;
    db_ = db;
    rcomp_ = kc::LEXICALCOMP;
    kc::BasicDB* idb = db->reveal_inner_db();
    const std::type_info& info = typeid(*idb);
    if (info == typeid(kc::GrassDB)) {
      kc::GrassDB* gdb = (kc::GrassDB*)idb;
      rcomp_ = gdb->rcomp();
    } else if (info == typeid(kc::TreeDB)) {
      kc::TreeDB* tdb = (kc::TreeDB*)idb;
      rcomp_ = tdb->rcomp();
    } else if (info == typeid(kc::ForestDB)) {
      kc::ForestDB* fdb = (kc::ForestDB*)idb;
      rcomp_ = fdb->rcomp();
    }
    tmpdbs_ = new kc::BasicDB*[dbnum_];
    if (tmppath.empty()) {
      if (!logf("prepare", "started to open temporary databases on memory")) err = true;
      stime = kc::time();
      for (size_t i = 0; i < dbnum_; i++) {
        kc::GrassDB* gdb = new kc::GrassDB;
        int32_t myopts = 0;
        if (!(opts & XNOCOMP)) myopts |= kc::GrassDB::TCOMPRESS;
        gdb->tune_options(myopts);
        gdb->tune_buckets(DBBNUM / 2);
        gdb->tune_page(DBPSIZ);
        gdb->tune_page_cache(DBPCCAP);
        gdb->tune_comparator(rcomp_);
        gdb->open("%", kc::GrassDB::OWRITER | kc::GrassDB::OCREATE | kc::GrassDB::OTRUNCATE);
        tmpdbs_[i] = gdb;
      }
      etime = kc::time();
      if (!logf("prepare", "opening temporary databases finished: time=%.6f", etime - stime))
        err = true;
      if (err) {
        delete[] tmpdbs_;
        return false;
      }
    } else {
      kc::File::Status sbuf;
      if (!kc::File::status(tmppath, &sbuf) || !sbuf.isdir) {
        db->set_error(kc::BasicDB::Error::NOREPOS, "no such directory");
        delete[] tmpdbs_;
        return false;
      }
      if (!logf("prepare", "started to open temporary databases under %s", tmppath.c_str()))
        err = true;
      stime = kc::time();
      uint32_t pid = kc::getpid() & kc::UINT16MAX;
      uint32_t tid = kc::Thread::hash() & kc::UINT16MAX;
      uint32_t ts = kc::time() * 1000;
      for (size_t i = 0; i < dbnum_; i++) {
        std::string childpath =
            kc::strprintf("%s%cmr-%04x-%04x-%08x-%03d%ckct", tmppath.c_str(),
                          kc::File::PATHCHR, pid, tid, ts, (int)(i + 1), kc::File::EXTCHR);
        kc::TreeDB* tdb = new kc::TreeDB;
        int32_t myopts = kc::TreeDB::TSMALL | kc::TreeDB::TLINEAR;
        if (!(opts & XNOCOMP)) myopts |= kc::TreeDB::TCOMPRESS;
        tdb->tune_options(myopts);
        tdb->tune_buckets(DBBNUM);
        tdb->tune_page(DBPSIZ);
        tdb->tune_map(DBMSIZ);
        tdb->tune_page_cache(DBPCCAP);
        tdb->tune_comparator(rcomp_);
        if (!tdb->open(childpath,
                       kc::TreeDB::OWRITER | kc::TreeDB::OCREATE | kc::TreeDB::OTRUNCATE)) {
          const kc::BasicDB::Error& e = tdb->error();
          db->set_error(e.code(), e.message());
          err = true;
        }
        tmpdbs_[i] = tdb;
      }
      etime = kc::time();
      if (!logf("prepare", "opening temporary databases finished: time=%.6f", etime - stime))
        err = true;
      if (err) {
        for (size_t i = 0; i < dbnum_; i++) {
          delete tmpdbs_[i];
        }
        delete[] tmpdbs_;
        return false;
      }
    }
    if (opts & XPARARED) redtasks_ = new ReduceTaskQueue;
    if (opts & XPARAFLS) flsths_ = new std::deque<FlushThread*>;
    if (opts & XNOLOCK) {
      MapChecker mapchecker;
      MapVisitor mapvisitor(this, &mapchecker, count);
      mapvisitor.visit_before();
      if (!err) {
        TimedDB::Cursor* cur = db->cursor();
        if (!cur->jump() && cur->error() != kc::BasicDB::Error::NOREC) err = true;
        while (!err) {
          if (!cur->accept(&mapvisitor, false, true)) {
            if (cur->error() != kc::BasicDB::Error::NOREC) err = true;
            break;
          }
        }
        delete cur;
      }
      if (mapvisitor.error()) err = true;
      mapvisitor.visit_after();
    } else if (opts & XPARAMAP) {
      MapChecker mapchecker;
      MapVisitor mapvisitor(this, &mapchecker, count);
      rlocks_ = new kc::SlottedMutex(RLOCKSLOT);
      if (!err && !db->scan_parallel(&mapvisitor, mapthnum_, &mapchecker)) {
        db_->set_error(kc::BasicDB::Error::LOGIC, "mapper failed");
        err = true;
      }
      delete rlocks_;
      rlocks_ = NULL;
      if (mapvisitor.error()) err = true;
    } else {
      MapChecker mapchecker;
      MapVisitor mapvisitor(this, &mapchecker, count);
      if (!err && !db->iterate(&mapvisitor, false, &mapchecker)) err = true;
      if (mapvisitor.error()) err = true;
    }
    if (flsths_) {
      delete flsths_;
      flsths_ = NULL;
    }
    if (redtasks_) {
      delete redtasks_;
      redtasks_ = NULL;
    }
    if (!logf("clean", "closing the temporary databases")) err = true;
    stime = kc::time();
    for (size_t i = 0; i < dbnum_; i++) {
      std::string path = tmpdbs_[i]->path();
      if (!tmpdbs_[i]->clear()) {
        const kc::BasicDB::Error& e = tmpdbs_[i]->error();
        db->set_error(e.code(), e.message());
        err = true;
      }
      if (!tmpdbs_[i]->close()) {
        const kc::BasicDB::Error& e = tmpdbs_[i]->error();
        db->set_error(e.code(), e.message());
        err = true;
      }
      if (!tmppath.empty()) kc::File::remove(path);
      delete tmpdbs_[i];
    }
    etime = kc::time();
    if (!logf("clean", "closing the temporary databases finished: time=%.6f",
              etime - stime)) err = true;
    delete[] tmpdbs_;
    return !err;
  }
  /**
   * Set the storage configurations.
   * @param dbnum the number of temporary databases.
   * @param clim the limit size of the internal cache.
   * @param cbnum the bucket number of the internal cache.
   */
  void tune_storage(int32_t dbnum, int64_t clim, int64_t cbnum) {
    _assert_(true);
    dbnum_ = dbnum > 0 ? dbnum : DEFDBNUM;
    if (dbnum_ > MAXDBNUM) dbnum_ = MAXDBNUM;
    clim_ = clim > 0 ? clim : DEFCLIM;
    cbnum_ = cbnum > 0 ? cbnum : DEFCBNUM;
    if (cbnum_ > kc::INT16MAX) cbnum_ = kc::nearbyprime(cbnum_);
  }
  /**
   * Set the thread configurations.
   * @param mapthnum the number of threads for the mapper.
   * @param redthnum the number of threads for the reducer.
   * @param flsthnum the number of threads for the internal flusher.
   */
  void tune_thread(int32_t mapthnum, int32_t redthnum, int32_t flsthnum) {
    _assert_(true);
    mapthnum_ = mapthnum > 0 ? mapthnum : DEFTHNUM;
    redthnum_ = redthnum > 0 ? redthnum : DEFTHNUM;
    flsthnum_ = flsthnum > 0 ? flsthnum : DEFTHNUM;
  }
 protected:
  /**
   * Emit a record from the mapper.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @return true on success, or false on failure.
   */
  bool emit(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    bool err = false;
    size_t rsiz = kc::sizevarnum(vsiz) + vsiz;
    char stack[kc::NUMBUFSIZ*4];
    char* rbuf = rsiz > sizeof(stack) ? new char[rsiz] : stack;
    char* wp = rbuf;
    wp += kc::writevarnum(rbuf, vsiz);
    std::memcpy(wp, vbuf, vsiz);
    if (rlocks_) {
      size_t bidx = kc::TinyHashMap::hash_record(kbuf, ksiz) % cbnum_;
      size_t lidx = bidx % RLOCKSLOT;
      rlocks_->lock(lidx);
      cache_->append(kbuf, ksiz, rbuf, rsiz);
      rlocks_->unlock(lidx);
    } else {
      cache_->append(kbuf, ksiz, rbuf, rsiz);
    }
    if (rbuf != stack) delete[] rbuf;
    csiz_ += kc::sizevarnum(ksiz) + ksiz + rsiz;
    return !err;
  }
 private:
  /**
   * Cache flusher.
   */
  class FlushThread : public kc::Thread {
   public:
    /** constructor */
    explicit FlushThread(MapReduce* mr, kc::BasicDB* tmpdb,
                         kc::TinyHashMap* cache, size_t csiz, bool cown) :
        mr_(mr), tmpdb_(tmpdb), cache_(cache), csiz_(csiz), cown_(cown), err_(false) {}
    /** perform the concrete process */
    void run() {
      if (!mr_->logf("map", "started to flushing the cache: count=%lld size=%lld",
                     (long long)cache_->count(), (long long)csiz_)) err_ = true;
      double stime = kc::time();
      kc::BasicDB* tmpdb = tmpdb_;
      kc::TinyHashMap* cache = cache_;
      bool cown = cown_;
      kc::TinyHashMap::Sorter sorter(cache);
      const char* kbuf, *vbuf;
      size_t ksiz, vsiz;
      while ((kbuf = sorter.get(&ksiz, &vbuf, &vsiz)) != NULL) {
        if (!tmpdb->append(kbuf, ksiz, vbuf, vsiz)) {
          const kc::BasicDB::Error& e = tmpdb->error();
          mr_->db_->set_error(e.code(), e.message());
          err_ = true;
        }
        sorter.step();
        if (cown) cache->remove(kbuf, ksiz);
      }
      double etime = kc::time();
      if (!mr_->logf("map", "flushing the cache finished: time=%.6f", etime - stime))
        err_ = true;
      if (cown) delete cache;
    }
    /** check the error flag. */
    bool error() {
      return err_;
    }
   private:
    MapReduce* mr_;                          ///< driver
    kc::BasicDB* tmpdb_;                     ///< temprary database
    kc::TinyHashMap* cache_;                 ///< cache for emitter
    size_t csiz_;                            ///< current cache size
    bool cown_;                              ///< cache ownership flag
    bool err_;                               ///< error flag
  };
  /**
   * Task queue for parallel reducer.
   */
  class ReduceTaskQueue : public kc::TaskQueue {
   public:
    /**
     * Task for parallel reducer.
     */
    class ReduceTask : public Task {
      friend class ReduceTaskQueue;
     public:
      /** constructor */
      explicit ReduceTask(MapReduce* mr, const char* kbuf, size_t ksiz, const Values& values) :
          mr_(mr), key_(kbuf, ksiz), values_(values) {}
     private:
      MapReduce* mr_;                    ///< driver
      std::string key_;                  ///< key
      Values values_;                    ///< values
    };
    /** constructor */
    explicit ReduceTaskQueue() {}
   private:
    /** process a task */
    void do_task(Task* task) {
      ReduceTask* rtask = (ReduceTask*)task;
      ValueIterator iter(rtask->values_.begin(), rtask->values_.end());
      if (!rtask->mr_->reduce(rtask->key_.data(), rtask->key_.size(), &iter))
        rtask->mr_->redaborted_ = true;
      delete rtask;
    }
  };
  /**
   * Checker for the map process.
   */
  class MapChecker : public kc::BasicDB::ProgressChecker {
   public:
    /** constructor */
    explicit MapChecker() : stop_(false) {}
    /** stop the process */
    void stop() {
      stop_ = true;
    }
    /** check whether stopped */
    bool stopped() {
      return stop_;
    }
   private:
    /** check whether stopped */
    bool check(const char* name, const char* message, int64_t curcnt, int64_t allcnt) {
      return !stop_;
    }
    bool stop_;                          ///< flag for stop
  };
  /**
   * Visitor for the map process.
   */
  class MapVisitor : public TimedDB::Visitor {
   public:
    /** constructor */
    explicit MapVisitor(MapReduce* mr, MapChecker* checker, int64_t scale) :
        mr_(mr), checker_(checker), scale_(scale), stime_(0), err_(false) {}
    /** get the error flag */
    bool error() {
      return err_;
    }
    /** preprocess the mappter */
    void visit_before() {
      mr_->dbclock_ = 0;
      mr_->cache_ = new kc::TinyHashMap(mr_->cbnum_);
      mr_->csiz_ = 0;
      if (!mr_->preprocess()) err_ = true;
      if (mr_->cache_->count() > 0 && !mr_->flush_cache()) err_ = true;
      if (!mr_->logf("map", "started the map process: scale=%lld", (long long)scale_))
        err_ = true;
      stime_ = kc::time();
    }
    /** postprocess the mappter and call the reducer */
    void visit_after() {
      if (mr_->cache_->count() > 0 && !mr_->flush_cache()) err_ = true;
      double etime = kc::time();
      if (!mr_->logf("map", "the map process finished: time=%.6f", etime - stime_))
        err_ = true;
      if (!mr_->midprocess()) err_ = true;
      if (mr_->cache_->count() > 0 && !mr_->flush_cache()) err_ = true;
      delete mr_->cache_;
      if (mr_->flsths_ && !mr_->flsths_->empty()) {
        std::deque<FlushThread*>::iterator flthit = mr_->flsths_->begin();
        std::deque<FlushThread*>::iterator flthitend = mr_->flsths_->end();
        while (flthit != flthitend) {
          FlushThread* flth = *flthit;
          flth->join();
          if (flth->error()) err_ = true;
          delete flth;
          ++flthit;
        }
      }
      if (!err_ && !mr_->execute_reduce()) err_ = true;
      if (!mr_->postprocess()) err_ = true;
    }
   private:
    /** visit a record */
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
      if (!mr_->map(kbuf, ksiz, vbuf, vsiz)) {
        checker_->stop();
        err_ = true;
      }
      if (mr_->rlocks_) {
        if (mr_->csiz_ >= mr_->clim_) {
          mr_->rlocks_->lock_all();
          if (mr_->csiz_ >= mr_->clim_ && !mr_->flush_cache()) {
            checker_->stop();
            err_ = true;
          }
          mr_->rlocks_->unlock_all();
        }
      } else {
        if (mr_->csiz_ >= mr_->clim_ && !mr_->flush_cache()) {
          checker_->stop();
          err_ = true;
        }
      }
      return NOP;
    }
    MapReduce* mr_;                      ///< driver
    MapChecker* checker_;                ///< checker
    int64_t scale_;                      ///< number of records
    double stime_;                       ///< start time
    bool err_;                           ///< error flag
  };
  /**
   * Front line of a merging list.
   */
  struct MergeLine {
    kc::BasicDB::Cursor* cur;            ///< cursor
    kc::Comparator* rcomp;               ///< record comparator
    char* kbuf;                          ///< pointer to the key
    size_t ksiz;                         ///< size of the key
    const char* vbuf;                    ///< pointer to the value
    size_t vsiz;                         ///< size of the value
    /** comparing operator */
    bool operator <(const MergeLine& right) const {
      return rcomp->compare(kbuf, ksiz, right.kbuf, right.ksiz) > 0;
    }
  };
  /**
   * Process a log message.
   * @param name the name of the event.
   * @param format the printf-like format string.
   * @param ... used according to the format string.
   * @return true on success, or false on failure.
   */
  bool logf(const char* name, const char* format, ...) {
    _assert_(name && format);
    va_list ap;
    va_start(ap, format);
    std::string message;
    kc::vstrprintf(&message, format, ap);
    va_end(ap);
    return log(name, message.c_str());
  }
  /**
   * Flush all cache records.
   * @return true on success, or false on failure.
   */
  bool flush_cache() {
    _assert_(true);
    bool err = false;
    kc::BasicDB* tmpdb = tmpdbs_[dbclock_];
    dbclock_ = (dbclock_ + 1) % dbnum_;
    if (flsths_) {
      size_t num = flsths_->size();
      if (num >= flsthnum_ || num >= dbnum_) {
        FlushThread* flth = flsths_->front();
        flsths_->pop_front();
        flth->join();
        if (flth->error()) err = true;
        delete flth;
      }
      FlushThread* flth = new FlushThread(this, tmpdb, cache_, csiz_, true);
      cache_ = new kc::TinyHashMap(cbnum_);
      csiz_ = 0;
      flth->start();
      flsths_->push_back(flth);
    } else {
      FlushThread flth(this, tmpdb, cache_, csiz_, false);
      flth.run();
      if (flth.error()) err = true;
      cache_->clear();
      csiz_ = 0;
    }
    return !err;
  }
  /**
   * Execute the reduce part.
   * @return true on success, or false on failure.
   */
  bool execute_reduce() {
    bool err = false;
    int64_t scale = 0;
    for (size_t i = 0; i < dbnum_; i++) {
      scale += tmpdbs_[i]->count();
    }
    if (!logf("reduce", "started the reduce process: scale=%lld", (long long)scale)) err = true;
    double stime = kc::time();
    if (redtasks_) redtasks_->start(redthnum_);
    std::priority_queue<MergeLine> lines;
    for (size_t i = 0; i < dbnum_; i++) {
      MergeLine line;
      line.cur = tmpdbs_[i]->cursor();
      line.rcomp = rcomp_;
      line.cur->jump();
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
    }
    char* lkbuf = NULL;
    size_t lksiz = 0;
    Values values;
    while (!err && !lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      if (lkbuf && (lksiz != line.ksiz || std::memcmp(lkbuf, line.kbuf, lksiz))) {
        if (!call_reducer(lkbuf, lksiz, values)) err = true;
        values.clear();
      }
      values.push_back(std::string(line.vbuf, line.vsiz));
      delete[] lkbuf;
      lkbuf = line.kbuf;
      lksiz = line.ksiz;
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
    }
    if (lkbuf) {
      if (!err && !call_reducer(lkbuf, lksiz, values)) err = true;
      values.clear();
      delete[] lkbuf;
    }
    while (!lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      delete[] line.kbuf;
      delete line.cur;
    }
    if (redtasks_) redtasks_->finish();
    double etime = kc::time();
    if (!logf("reduce", "the reduce process finished: time=%.6f", etime - stime)) err = true;
    return !err;
  }
  /**
   * Call the reducer.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param values a vector of the values.
   * @return true on success, or false on failure.
   */
  bool call_reducer(const char* kbuf, size_t ksiz, const Values& values) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    if (redtasks_) {
      if (redaborted_) return false;
      ReduceTaskQueue::ReduceTask* task =
          new ReduceTaskQueue::ReduceTask(this, kbuf, ksiz, values);
      redtasks_->add_task(task);
      return true;
    }
    bool err = false;
    ValueIterator iter(values.begin(), values.end());
    if (!reduce(kbuf, ksiz, &iter)) err = true;
    return !err;
  }
  /** Dummy constructor to forbid the use. */
  MapReduce(const MapReduce&);
  /** Dummy Operator to forbid the use. */
  MapReduce& operator =(const MapReduce&);
  /** The internal database. */
  TimedDB* db_;
  /** The record comparator. */
  kc::Comparator* rcomp_;
  /** The temporary databases. */
  kc::BasicDB** tmpdbs_;
  /** The number of temporary databases. */
  size_t dbnum_;
  /** The logical clock for temporary databases. */
  int64_t dbclock_;
  /** The number of the mapper threads. */
  size_t mapthnum_;
  /** The number of the reducer threads. */
  size_t redthnum_;
  /** The number of the flusher threads. */
  size_t flsthnum_;
  /** The cache for emitter. */
  kc::TinyHashMap* cache_;
  /** The current size of the cache for emitter. */
  int64_t csiz_;
  /** The limit size of the cache for emitter. */
  int64_t clim_;
  /** The bucket number of the cache for emitter. */
  int64_t cbnum_;
  /** The flush threads. */
  std::deque<FlushThread*>* flsths_;
  /** The task queue for parallel reducer. */
  kc::TaskQueue* redtasks_;
  /** The flag whether aborted. */
  bool redaborted_;
  /** The whole lock. */
  kc::SlottedMutex* rlocks_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
