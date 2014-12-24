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


#ifndef _KTTIMEDDB_H                     // duplication check
#define _KTTIMEDDB_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktulog.h>
#include <ktshlib.h>

namespace kyototycoon {                  // common namespace


/**
 * Timed database.
 * @note This class is a concrete class of a wrapper for the polymorphic database to add
 * expiration features.  This class can be inherited but overwriting methods is forbidden.
 * Before every database operation, it is necessary to call the TimedDB::open method in order to
 * open a database file and connect the database object to it.  To avoid data missing or
 * corruption, it is important to close every database file by the TimedDB::close method when
 * the database is no longer in use.  It is forbidden for multible database objects in a process
 * to open the same database at the same time.
 */
class TimedDB {
 public:
  class Cursor;
  class Visitor;
  class UpdateTrigger;
  /** The width of expiration time. */
  static const int32_t XTWIDTH = 5;
  /** The maximum number of expiration time. */
  static const int64_t XTMAX = (1LL << (XTWIDTH * 8)) - 1;
 private:
  class TimedVisitor;
  class TimedMetaTrigger;
  struct MergeLine;
  /* The magic data of the database type. */
  static const uint8_t MAGICDATA = 0xbb;
  /* The score unit of expiratoin. */
  static const int64_t XTSCUNIT = 256;
  /* The inverse frequency of reading expiration. */
  static const int64_t XTREADFREQ = 8;
  /* The inverse frequency of iterating expiration. */
  static const int64_t XTITERFREQ = 4;
  /* The unit step number of expiration. */
  static const int64_t XTUNIT = 8;
  /* The size of the logging buffer. */
  static const size_t LOGBUFSIZ = 1024;
 public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor {
    friend class TimedDB;
   public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(TimedDB* db) : db_(db), cur_(NULL), back_(false) {
      _assert_(db);
      cur_ = db_->db_.cursor();
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      delete cur_;
    }
    /**
     * Jump the cursor to the first record for forward scan.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      if (!cur_->jump()) return false;
      back_ = false;
      return true;
    }
    /**
     * Jump the cursor to a record for forward scan.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     */
    bool jump(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      if (!cur_->jump(kbuf, ksiz)) return false;
      back_ = false;
      return true;
    }
    /**
     * Jump the cursor to a record for forward scan.
     * @note Equal to the original Cursor::jump method except that the parameter is std::string.
     */
    bool jump(const std::string& key) {
      _assert_(true);
      return jump(key.c_str(), key.size());
    }
    /**
     * Jump the cursor to the last record for backward scan.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, may provide a dummy implementation.
     */
    bool jump_back() {
      _assert_(true);
      if (!cur_->jump_back()) return false;
      back_ = true;
      return true;
    }
    /**
     * Jump the cursor to a record for backward scan.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, will provide a dummy implementation.
     */
    bool jump_back(const char* kbuf, size_t ksiz) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
      if (!cur_->jump_back(kbuf, ksiz)) return false;
      back_ = true;
      return true;
    }
    /**
     * Jump the cursor to a record for backward scan.
     * @note Equal to the original Cursor::jump_back method except that the parameter is
     * std::string.
     */
    bool jump_back(const std::string& key) {
      _assert_(true);
      return jump_back(key.c_str(), key.size());
    }
    /**
     * Step the cursor to the next record.
     * @return true on success, or false on failure.
     */
    bool step() {
      _assert_(true);
      if (!cur_->step()) return false;
      back_ = false;
      return true;
    }
    /**
     * Step the cursor to the previous record.
     * @return true on success, or false on failure.
     * @note This method is dedicated to tree databases.  Some database types, especially hash
     * databases, may provide a dummy implementation.
     */
    bool step_back() {
      _assert_(true);
      if (!cur_->step_back()) return false;
      back_ = true;
      return true;
    }
    /**
     * Accept a visitor to the current record.
     * @param visitor a visitor object.
     * @param writable true for writable operation, or false for read-only operation.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     * @note the operation for each record is performed atomically and other threads accessing
     * the same record are blocked.
     */
    bool accept(Visitor* visitor, bool writable = true, bool step = false) {
      _assert_(visitor);
      bool err = false;
      int64_t ct = std::time(NULL);
      while (true) {
        TimedVisitor myvisitor(db_, visitor, ct, true);
        if (!cur_->accept(&myvisitor, writable, step)) {
          err = true;
          break;
        }
        if (myvisitor.again()) {
          if (!step && !(back_ ? cur_->step_back() : cur_->step())) {
            err = true;
            break;
          }
          continue;
        }
        break;
      }
      if (db_->xcur_) {
        int64_t xtsc = writable ? XTSCUNIT : XTSCUNIT / XTREADFREQ;
        if (!db_->expire_records(xtsc)) err = true;
      }
      return !err;
    }
    /**
     * Set the value of the current record.
     * @param vbuf the pointer to the value region.
     * @param vsiz the size of the value region.
     * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
     * is treated as the epoch time.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return true on success, or false on failure.
     */
    bool set_value(const char* vbuf, size_t vsiz, int64_t xt = kc::INT64MAX, bool step = false) {
      _assert_(vbuf && vsiz <= kc::MEMMAXSIZ);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
            vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
        bool ok() const {
          return ok_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          ok_ = true;
          *sp = vsiz_;
          *xtp = xt_;
          return vbuf_;
        }
        const char* vbuf_;
        size_t vsiz_;
        int64_t xt_;
        bool ok_;
      };
      VisitorImpl visitor(vbuf, vsiz, xt);
      if (!accept(&visitor, true, step)) return false;
      if (!visitor.ok()) return false;
      return true;
    }
    /**
     * Set the value of the current record.
     * @note Equal to the original Cursor::set_value method except that the parameter is
     * std::string.
     */
    bool set_value_str(const std::string& value, int64_t xt = kc::INT64MAX, bool step = false) {
      _assert_(true);
      return set_value(value.c_str(), value.size(), xt, step);
    }
    /**
     * Remove the current record.
     * @return true on success, or false on failure.
     * @note If no record corresponds to the key, false is returned.  The cursor is moved to the
     * next record implicitly.
     */
    bool remove() {
      _assert_(true);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : ok_(false) {}
        bool ok() const {
          return ok_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          ok_ = true;
          return REMOVE;
        }
        bool ok_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, true, false)) return false;
      if (!visitor.ok()) return false;
      return true;
    }
    /**
     * Get the key of the current record.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the key region of the current record, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero
     * code is appended at the end of the region of the return value, the return value can be
     * treated as a C-style string.  Because the region of the return value is allocated with the
     * the new[] operator, it should be released with the delete[] operator when it is no longer
     * in use.
     */
    char* get_key(size_t* sp, bool step = false) {
      _assert_(sp);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : kbuf_(NULL), ksiz_(0) {}
        char* pop(size_t* sp) {
          *sp = ksiz_;
          return kbuf_;
        }
        void clear() {
          delete[] kbuf_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          kbuf_ = new char[ksiz+1];
          std::memcpy(kbuf_, kbuf, ksiz);
          kbuf_[ksiz] = '\0';
          ksiz_ = ksiz;
          return NOP;
        }
        char* kbuf_;
        size_t ksiz_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *sp = 0;
        return NULL;
      }
      size_t ksiz;
      char* kbuf = visitor.pop(&ksiz);
      if (!kbuf) {
        *sp = 0;
        return NULL;
      }
      *sp = ksiz;
      return kbuf;
    }
    /**
     * Get the key of the current record.
     * @note Equal to the original Cursor::get_key method except that a parameter is a string to
     * contain the result and the return value is bool for success.
     */
    bool get_key(std::string* key, bool step = false) {
      _assert_(key);
      size_t ksiz;
      char* kbuf = get_key(&ksiz, step);
      if (!kbuf) return false;
      key->clear();
      key->append(kbuf, ksiz);
      delete[] kbuf;
      return true;
    }
    /**
     * Get the value of the current record.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the value region of the current record, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero
     * code is appended at the end of the region of the return value, the return value can be
     * treated as a C-style string.  Because the region of the return value is allocated with the
     * the new[] operator, it should be released with the delete[] operator when it is no longer
     * in use.
     */
    char* get_value(size_t* sp, bool step = false) {
      _assert_(sp);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : vbuf_(NULL), vsiz_(0) {}
        char* pop(size_t* sp) {
          *sp = vsiz_;
          return vbuf_;
        }
        void clear() {
          delete[] vbuf_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          vbuf_ = new char[vsiz+1];
          std::memcpy(vbuf_, vbuf, vsiz);
          vbuf_[vsiz] = '\0';
          vsiz_ = vsiz;
          return NOP;
        }
        char* vbuf_;
        size_t vsiz_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *sp = 0;
        return NULL;
      }
      size_t vsiz;
      char* vbuf = visitor.pop(&vsiz);
      if (!vbuf) {
        *sp = 0;
        return NULL;
      }
      *sp = vsiz;
      return vbuf;
    }
    /**
     * Get the value of the current record.
     * @note Equal to the original Cursor::get_value method except that a parameter is a string
     * to contain the result and the return value is bool for success.
     */
    bool get_value(std::string* value, bool step = false) {
      _assert_(value);
      size_t vsiz;
      char* vbuf = get_value(&vsiz, step);
      if (!vbuf) return false;
      value->clear();
      value->append(vbuf, vsiz);
      delete[] vbuf;
      return true;
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @param ksp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param vbp the pointer to the variable into which the pointer to the value region is
     * assigned.
     * @param vsp the pointer to the variable into which the size of the value region is
     * assigned.
     * @param xtp the pointer to the variable into which the absolute expiration time is
     * assigned.  If it is NULL, it is ignored.
     * @param step true to move the cursor to the next record, or false for no move.
     * @return the pointer to the pair of the key region, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero code is
     * appended at the end of each region of the key and the value, each region can be treated
     * as a C-style string.  The return value should be deleted explicitly by the caller with
     * the detele[] operator.
     */
    char* get(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp = NULL,
              bool step = false) {
      _assert_(ksp && vbp && vsp);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : kbuf_(NULL), ksiz_(0), vbuf_(NULL), vsiz_(0), xt_(0) {}
        char* pop(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp) {
          *ksp = ksiz_;
          *vbp = vbuf_;
          *vsp = vsiz_;
          if (xtp) *xtp = xt_;
          return kbuf_;
        }
        void clear() {
          delete[] kbuf_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          size_t rsiz = ksiz + 1 + vsiz + 1;
          kbuf_ = new char[rsiz];
          std::memcpy(kbuf_, kbuf, ksiz);
          kbuf_[ksiz] = '\0';
          ksiz_ = ksiz;
          vbuf_ = kbuf_ + ksiz + 1;
          std::memcpy(vbuf_, vbuf, vsiz);
          vbuf_[vsiz] = '\0';
          vsiz_ = vsiz;
          xt_ = *xtp;
          return NOP;
        }
        char* kbuf_;
        size_t ksiz_;
        char* vbuf_;
        size_t vsiz_;
        int64_t xt_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, false, step)) {
        visitor.clear();
        *ksp = 0;
        *vbp = NULL;
        *vsp = 0;
        if (xtp) *xtp = 0;
        return NULL;
      }
      return visitor.pop(ksp, vbp, vsp, xtp);
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @note Equal to the original Cursor::get method except that parameters are strings
     * to contain the result and the return value is bool for success.
     */
    bool get(std::string* key, std::string* value, int64_t* xtp = NULL, bool step = false) {
      _assert_(key && value);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl(std::string* key, std::string* value) :
            key_(key), value_(value), xt_(0), ok_(false) {}
        bool ok(int64_t* xtp) {
          if (xtp) *xtp = xt_;
          return ok_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          key_->clear();
          key_->append(kbuf, ksiz);
          value_->clear();
          value_->append(vbuf, vsiz);
          xt_ = *xtp;
          ok_ = true;
          return NOP;
        }
        std::string* key_;
        std::string* value_;
        int64_t xt_;
        bool ok_;
      };
      VisitorImpl visitor(key, value);
      if (!accept(&visitor, false, step)) return false;
      return visitor.ok(xtp);
    }
    /**
     * Get a pair of the key and the value of the current record and remove it atomically.
     * @param ksp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param vbp the pointer to the variable into which the pointer to the value region is
     * assigned.
     * @param vsp the pointer to the variable into which the size of the value region is
     * assigned.
     * @param xtp the pointer to the variable into which the absolute expiration time is
     * assigned.  If it is NULL, it is ignored.
     * @return the pointer to the pair of the key region, or NULL on failure.
     * @note If the cursor is invalidated, NULL is returned.  Because an additional zero code is
     * appended at the end of each region of the key and the value, each region can be treated
     * as a C-style string.  The return value should be deleted explicitly by the caller with
     * the detele[] operator.
     */
    char* seize(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp = NULL) {
      _assert_(ksp && vbp && vsp);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : kbuf_(NULL), ksiz_(0), vbuf_(NULL), vsiz_(0), xt_(0) {}
        char* pop(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp) {
          *ksp = ksiz_;
          *vbp = vbuf_;
          *vsp = vsiz_;
          if (xtp) *xtp = xt_;
          return kbuf_;
        }
        void clear() {
          delete[] kbuf_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          size_t rsiz = ksiz + 1 + vsiz + 1;
          kbuf_ = new char[rsiz];
          std::memcpy(kbuf_, kbuf, ksiz);
          kbuf_[ksiz] = '\0';
          ksiz_ = ksiz;
          vbuf_ = kbuf_ + ksiz + 1;
          std::memcpy(vbuf_, vbuf, vsiz);
          vbuf_[vsiz] = '\0';
          vsiz_ = vsiz;
          xt_ = *xtp;
          return REMOVE;
        }
        char* kbuf_;
        size_t ksiz_;
        char* vbuf_;
        size_t vsiz_;
        int64_t xt_;
      };
      VisitorImpl visitor;
      if (!accept(&visitor, true, false)) {
        visitor.clear();
        *ksp = 0;
        *vbp = NULL;
        *vsp = 0;
        if (xtp) *xtp = 0;
        return NULL;
      }
      return visitor.pop(ksp, vbp, vsp, xtp);
    }
    /**
     * Get a pair of the key and the value of the current record and remove it atomically.
     * @note Equal to the original Cursor::seize method except that parameters are strings
     * to contain the result and the return value is bool for success.
     */
    bool seize(std::string* key, std::string* value, int64_t* xtp = NULL) {
      _assert_(key && value);
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl(std::string* key, std::string* value) :
            key_(key), value_(value), xt_(0), ok_(false) {}
        bool ok(int64_t* xtp) {
          if (xtp) *xtp = xt_;
          return ok_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          key_->clear();
          key_->append(kbuf, ksiz);
          value_->clear();
          value_->append(vbuf, vsiz);
          xt_ = *xtp;
          ok_ = true;
          return REMOVE;
        }
        std::string* key_;
        std::string* value_;
        int64_t xt_;
        bool ok_;
      };
      VisitorImpl visitor(key, value);
      if (!accept(&visitor, true, false)) return false;
      return visitor.ok(xtp);
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    TimedDB* db() {
      _assert_(true);
      return db_;
    }
    /**
     * Get the last happened error.
     * @return the last happened error.
     */
    kc::BasicDB::Error error() {
      _assert_(true);
      return db()->error();
    }
   private:
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    TimedDB* db_;
    /** The inner cursor. */
    kc::BasicDB::Cursor* cur_;
    /** The backward flag. */
    bool back_;
  };
  /**
   * Interface to access a record.
   */
  class Visitor {
   public:
    /** Special pointer for no operation. */
    static const char* const NOP;
    /** Special pointer to remove the record. */
    static const char* const REMOVE;
    /**
     * Destructor.
     */
    virtual ~Visitor() {
      _assert_(true);
    }
    /**
     * Visit a record.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @param vbuf the pointer to the value region.
     * @param vsiz the size of the value region.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.  The initial value is the absolute expiration time.
     * @return If it is the pointer to a region, the value is replaced by the content.  If it
     * is Visitor::NOP, nothing is modified.  If it is Visitor::REMOVE, the record is removed.
     */
    virtual const char* visit_full(const char* kbuf, size_t ksiz,
                                   const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ && sp && xtp);
      return NOP;
    }
    /**
     * Visit a empty record space.
     * @param kbuf the pointer to the key region.
     * @param ksiz the size of the key region.
     * @param sp the pointer to the variable into which the size of the region of the return
     * value is assigned.
     * @param xtp the pointer to the variable into which the expiration time from now in seconds
     * is assigned.
     * @return If it is the pointer to a region, the value is replaced by the content.  If it
     * is Visitor::NOP or Visitor::REMOVE, nothing is modified.
     */
    virtual const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
      _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp && xtp);
      return NOP;
    }
    /**
     * Preprocess the main operations.
     */
    virtual void visit_before() {
      _assert_(true);
    }
    /**
     * Postprocess the main operations.
     */
    virtual void visit_after() {
      _assert_(true);
    }
  };
  /**
   * Interface to trigger update operations.
   */
  class UpdateTrigger {
   public:
    /**
     * Destructor.
     */
    virtual ~UpdateTrigger() {
      _assert_(true);
    }
    /**
     * Trigger an update operation.
     * @param mbuf the pointer to the message region.
     * @param msiz the size of the message region.
     */
    virtual void trigger(const char* mbuf, size_t msiz) = 0;
    /**
     * Begin transaction.
     */
    virtual void begin_transaction() = 0;
    /**
     * End transaction.
     * @param commit true to commit the transaction, or false to abort the transaction.
     */
    virtual void end_transaction(bool commit) = 0;
  };
  /**
   * Merge modes.
   */
  enum MergeMode {
    MSET,                                ///< overwrite the existing value
    MADD,                                ///< keep the existing value
    MREPLACE,                            ///< modify the existing record only
    MAPPEND                              ///< append the new value
  };
  /**
   * Default constructor.
   */
  explicit TimedDB() :
      xlock_(), db_(), mtrigger_(this), utrigger_(NULL), omode_(0),
      opts_(0), capcnt_(0), capsiz_(0), xcur_(NULL), xsc_(0) {
    _assert_(true);
    db_.tune_meta_trigger(&mtrigger_);
  }
  /**
   * Destructor.
   */
  virtual ~TimedDB() {
    _assert_(true);
    if (omode_ != 0) close();
  }
  /**
   * Set the internal database object.
   * @param db the internal database object.  Its possession is transferred inside and the
   * object is deleted automatically.
   * @return true on success, or false on failure.
   */
  bool set_internal_db(kc::BasicDB* db) {
    _assert_(db);
    if (omode_ != 0) {
      set_error(kc::BasicDB::Error::INVALID, "already opened");
      return false;
    }
    db_.set_internal_db(db);
    return true;
  }
  /**
   * Get the last happened error.
   * @return the last happened error.
   */
  kc::BasicDB::Error error() const {
    _assert_(true);
    return db_.error();
  }
  /**
   * Set the error information.
   * @param code an error code.
   * @param message a supplement message.
   */
  void set_error(kc::BasicDB::Error::Code code, const char* message) {
    _assert_(message);
    db_.set_error(code, message);
  }
  /**
   * Open a database file.
   * @param path the path of a database file.  The same as with kc::PolyDB.  In addition, the
   * following tuning parameters are supported.  "ktopts" sets options and the value can contain
   * "p" for the persistent option.  "ktcapcnt" sets the capacity by record number.  "ktcapsiz"
   * sets the capacity by database size.
   * @param mode the connection mode.  The same as with kc::PolyDB.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& path = ":",
            uint32_t mode = kc::BasicDB::OWRITER | kc::BasicDB::OCREATE) {
    _assert_(true);
    if (omode_ != 0) {
      set_error(kc::BasicDB::Error::INVALID, "already opened");
      return false;
    }
    kc::ScopedSpinLock lock(&xlock_);
    std::vector<std::string> elems;
    kc::strsplit(path, '#', &elems);
    capcnt_ = -1;
    capsiz_ = -1;
    opts_ = 0;
    std::vector<std::string>::iterator it = elems.begin();
    std::vector<std::string>::iterator itend = elems.end();
    if (it != itend) ++it;
    while (it != itend) {
      std::vector<std::string> fields;
      if (kc::strsplit(*it, '=', &fields) > 1) {
        const char* key = fields[0].c_str();
        const char* value = fields[1].c_str();
        if (!std::strcmp(key, "ktcapcnt") || !std::strcmp(key, "ktcapcount") ||
            !std::strcmp(key, "ktcap_count")) {
          capcnt_ = kc::atoix(value);
        } else if (!std::strcmp(key, "ktcapsiz") || !std::strcmp(key, "ktcapsize") ||
                   !std::strcmp(key, "ktcap_size")) {
          capsiz_ = kc::atoix(value);
        } else if (!std::strcmp(key, "ktopts") || !std::strcmp(key, "ktoptions")) {
          if (std::strchr(value, 'p')) opts_ |= TPERSIST;
        }
      }
      ++it;
    }
    if (!db_.open(path, mode)) return false;
    kc::BasicDB* idb = db_.reveal_inner_db();
    if (idb) {
      const std::type_info& info = typeid(*idb);
      if (info == typeid(kc::HashDB)) {
        kc::HashDB* hdb = (kc::HashDB*)idb;
        char* opq = hdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == MAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && hdb->count() < 1) {
            *(uint8_t*)opq = MAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            hdb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::TreeDB)) {
        kc::TreeDB* tdb = (kc::TreeDB*)idb;
        char* opq = tdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == MAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && tdb->count() < 1) {
            *(uint8_t*)opq = MAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            tdb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::DirDB)) {
        kc::DirDB* ddb = (kc::DirDB*)idb;
        char* opq = ddb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == MAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && ddb->count() < 1) {
            *(uint8_t*)opq = MAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            ddb->synchronize_opaque();
          }
        }
      } else if (info == typeid(kc::ForestDB)) {
        kc::ForestDB* fdb = (kc::ForestDB*)idb;
        char* opq = fdb->opaque();
        if (opq) {
          if (*(uint8_t*)opq == MAGICDATA) {
            opts_ = *(uint8_t*)(opq + 1);
          } else if ((mode & kc::BasicDB::OWRITER) && fdb->count() < 1) {
            *(uint8_t*)opq = MAGICDATA;
            *(uint8_t*)(opq + 1) = opts_;
            fdb->synchronize_opaque();
          }
        }
      }
    }
    omode_ = mode;
    if ((omode_ & kc::BasicDB::OWRITER) && !(opts_ & TPERSIST)) {
      xcur_ = db_.cursor();
      if (db_.count() > 0) xcur_->jump();
    }
    xsc_ = 0;
    return true;
  }
  /**
   * Close the database file.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    if (omode_ == 0) {
      set_error(kc::BasicDB::Error::INVALID, "not opened");
      return false;
    }
    kc::ScopedSpinLock lock(&xlock_);
    bool err = false;
    delete xcur_;
    xcur_ = NULL;
    if (!db_.close()) err = true;
    omode_ = 0;
    return !err;
  }
  /**
   * Accept a visitor to a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   * @note the operation for each record is performed atomically and other threads accessing the
   * same record are blocked.
   */
  bool accept(const char* kbuf, size_t ksiz, Visitor* visitor, bool writable = true) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && visitor);
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, false);
    if (!db_.accept(kbuf, ksiz, &myvisitor, writable)) err = true;
    if (xcur_) {
      int64_t xtsc = writable ? XTSCUNIT : XTSCUNIT / XTREADFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Accept a visitor to multiple records at once.
   * @param keys specifies a string vector of the keys.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @return true on success, or false on failure.
   * @note The operations for specified records are performed atomically and other threads
   * accessing the same records are blocked.  To avoid deadlock, any database operation must not
   * be performed in this function.
   */
  bool accept_bulk(const std::vector<std::string>& keys, Visitor* visitor,
                   bool writable = true) {
    _assert_(visitor);
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, false);
    if (!db_.accept_bulk(keys, &myvisitor, writable)) err = true;
    if (xcur_) {
      int64_t xtsc = writable ? XTSCUNIT : XTSCUNIT / XTREADFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Iterate to accept a visitor for each record.
   * @param visitor a visitor object.
   * @param writable true for writable operation, or false for read-only operation.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   * @note The whole iteration is performed atomically and other threads are blocked.
   */
  bool iterate(Visitor *visitor, bool writable = true,
               kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(visitor);
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, true);
    if (!db_.iterate(&myvisitor, writable, checker)) err = true;
    if (xcur_) {
      int64_t count = db_.count();
      int64_t xtsc = writable ? XTSCUNIT : XTSCUNIT / XTREADFREQ;
      if (count > 0) xtsc *= count / XTITERFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Scan each record in parallel.
   * @param visitor a visitor object.
   * @param thnum the number of worker threads.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   * @note This function is for reading records and not for updating ones.  The return value of
   * the visitor is just ignored.  To avoid deadlock, any explicit database operation must not
   * be performed in this function.
   */
  bool scan_parallel(Visitor *visitor, size_t thnum,
                     kc::BasicDB::ProgressChecker* checker = NULL) {
    bool err = false;
    int64_t ct = std::time(NULL);
    TimedVisitor myvisitor(this, visitor, ct, true);
    if (!db_.scan_parallel(&myvisitor, thnum, checker)) err = true;
    if (xcur_) {
      int64_t count = db_.count();
      int64_t xtsc = XTSCUNIT / XTREADFREQ;
      if (count > 0) xtsc *= count / XTITERFREQ;
      if (!expire_records(xtsc)) err = true;
    }
    return !err;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param proc a postprocessor object.  If it is NULL, no postprocessing is performed.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   * @note The operation of the postprocessor is performed atomically and other threads accessing
   * the same record are blocked.  To avoid deadlock, any explicit database operation must not
   * be performed in this function.
   */
  bool synchronize(bool hard = false, kc::BasicDB::FileProcessor* proc = NULL,
                   kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.synchronize(hard, proc, checker);
  }
  /**
   * Occupy database by locking and do something meanwhile.
   * @param writable true to use writer lock, or false to use reader lock.
   * @param proc a processor object.  If it is NULL, no processing is performed.
   * @return true on success, or false on failure.
   * @note The operation of the processor is performed atomically and other threads accessing
   * the same record are blocked.  To avoid deadlock, any explicit database operation must not
   * be performed in this function.
   */
  bool occupy(bool writable = true, kc::BasicDB::FileProcessor* proc = NULL) {
    _assert_(true);
    return db_.occupy(writable, proc);
  }
  /**
   * Create a copy of the database file.
   * @param dest the path of the destination file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool copy(const std::string& dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.copy(dest, checker);
  }
  /**
   * Begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction(bool hard = false) {
    _assert_(true);
    return db_.begin_transaction(hard);
  }
  /**
   * Try to begin transaction.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @return true on success, or false on failure.
   */
  bool begin_transaction_try(bool hard = false) {
    _assert_(true);
    return db_.begin_transaction_try(hard);
  }
  /**
   * End transaction.
   * @param commit true to commit the transaction, or false to abort the transaction.
   * @return true on success, or false on failure.
   */
  bool end_transaction(bool commit = true) {
    _assert_(true);
    return db_.end_transaction(commit);
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    return db_.clear();
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    return db_.count();
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    return db_.size();
  }
  /**
   * Get the path of the database file.
   * @return the path of the database file, or an empty string on failure.
   */
  std::string path() {
    _assert_(true);
    return db_.path();
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    if (!db_.status(strmap)) return false;
    (*strmap)["ktopts"] = kc::strprintf("%u", opts_);
    (*strmap)["ktcapcnt"] = kc::strprintf("%lld", (long long)capcnt_);
    (*strmap)["ktcapsiz"] = kc::strprintf("%lld", (long long)capsiz_);
    return true;
  }
  /**
   * Set the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the value is overwritten.
   */
  bool set(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
           int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
          vbuf_(vbuf), vsiz_(vsiz), xt_(xt) {}
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::set method except that the parameters are std::string.
   */
  bool set(const std::string& key, const std::string& value, int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return set(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Add a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the record is not modified and false is returned.
   */
  bool add(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
           int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
          vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
     private:
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::DUPREC, "record duplication");
      return false;
    }
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::add method except that the parameters are std::string.
   */
  bool add(const std::string& key, const std::string& value, int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return add(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Replace the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, no new record is created and false is returned.
   * If the corresponding record exists, the value is modified.
   */
  bool replace(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
               int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
          vbuf_(vbuf), vsiz_(vsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return false;
    }
    return true;
  }
  /**
   * Replace the value of a record.
   * @note Equal to the original DB::replace method except that the parameters are std::string.
   */
  bool replace(const std::string& key, const std::string& value, int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return replace(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Append the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, a new record is created.  If the corresponding
   * record exists, the given value is appended at the end of the existing value.
   */
  bool append(const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz,
              int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf && vsiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(const char* vbuf, size_t vsiz, int64_t xt) :
          vbuf_(vbuf), vsiz_(vsiz), xt_(xt), nbuf_(NULL) {}
      ~VisitorImpl() {
        if (nbuf_) delete[] nbuf_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        size_t nsiz = vsiz + vsiz_;
        nbuf_ = new char[nsiz];
        std::memcpy(nbuf_, vbuf, vsiz);
        std::memcpy(nbuf_ + vsiz, vbuf_, vsiz_);
        *sp = nsiz;
        *xtp = xt_;
        return nbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        *xtp = xt_;
        return vbuf_;
      }
      const char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
      char* nbuf_;
    };
    VisitorImpl visitor(vbuf, vsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    return true;
  }
  /**
   * Set the value of a record.
   * @note Equal to the original DB::append method except that the parameters are std::string.
   */
  bool append(const std::string& key, const std::string& value, int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return append(key.c_str(), key.size(), value.c_str(), value.size(), xt);
  }
  /**
   * Add a number to the numeric integer value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param num the additional number.
   * @param orig the origin number if no record corresponds to the key.  If it is INT64MIN and
   * no record corresponds, this function fails.  If it is INT64MAX, the value is set as the
   * additional number regardless of the current value.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return the result value, or kyotocabinet::INT64MIN on failure.
   * @note The value is serialized as an 8-byte binary integer in big-endian order, not a decimal
   * string.  If existing value is not 8-byte, this function fails.
   */
  int64_t increment(const char* kbuf, size_t ksiz, int64_t num, int64_t orig = 0,
                    int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(int64_t num, int64_t orig, int64_t xt) :
          num_(num), orig_(orig), xt_(xt), big_(0) {}
      int64_t num() {
        return num_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (vsiz != sizeof(num_)) {
          num_ = kc::INT64MIN;
          return NOP;
        }
        int64_t onum;
        if (orig_ == kc::INT64MAX) {
          onum = 0;
        } else {
          std::memcpy(&onum, vbuf, vsiz);
          onum = kc::ntoh64(onum);
          if (num_ == 0) {
            num_ = onum;
            return NOP;
          }
        }
        num_ += onum;
        big_ = kc::hton64(num_);
        *sp = sizeof(big_);
        *xtp = xt_;
        return (const char*)&big_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        if (orig_ == kc::INT64MIN) {
          num_ = kc::INT64MIN;
          return NOP;
        }
        if (orig_ != kc::INT64MAX) num_ += orig_;
        big_ = kc::hton64(num_);
        *sp = sizeof(big_);
        *xtp = xt_;
        return (const char*)&big_;
      }
      int64_t num_;
      int64_t orig_;
      int64_t xt_;
      uint64_t big_;
    };
    VisitorImpl visitor(num, orig, xt);
    if (!accept(kbuf, ksiz, &visitor, num != 0 || orig != kc::INT64MIN)) return kc::INT64MIN;
    num = visitor.num();
    if (num == kc::INT64MIN) {
      set_error(kc::BasicDB::Error::LOGIC, "logical inconsistency");
      return num;
    }
    return num;
  }
  /**
   * Add a number to the numeric integer value of a record.
   * @note Equal to the original DB::increment method except that the parameter is std::string.
   */
  int64_t increment(const std::string& key, int64_t num, int64_t orig = 0,
                    int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return increment(key.c_str(), key.size(), num, orig, xt);
  }
  /**
   * Add a number to the numeric double value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param num the additional number.
   * @param orig the origin number if no record corresponds to the key.  If it is negative
   * infinity and no record corresponds, this function fails.  If it is positive infinity, the
   * value is set as the additional number regardless of the current value.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return the result value, or Not-a-number on failure.
   * @note The value is serialized as an 16-byte binary fixed-point number in big-endian order,
   * not a decimal string.  If existing value is not 16-byte, this function fails.
   */
  double increment_double(const char* kbuf, size_t ksiz, double num, double orig = 0,
                          int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(double num, double orig, int64_t xt) :
          DECUNIT(1000000000000000LL), num_(num), orig_(orig), xt_(xt), buf_() {}
      double num() {
        return num_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (vsiz != sizeof(buf_)) {
          num_ = kc::nan();
          return NOP;
        }
        int64_t linteg, lfract;
        if (kc::chkinf(orig_) && orig_ >= 0) {
          linteg = 0;
          lfract = 0;
        } else {
          std::memcpy(&linteg, vbuf, sizeof(linteg));
          linteg = kc::ntoh64(linteg);
          std::memcpy(&lfract, vbuf + sizeof(linteg), sizeof(lfract));
          lfract = kc::ntoh64(lfract);
        }
        if (lfract == kc::INT64MIN && linteg == kc::INT64MIN) {
          num_ = kc::nan();
          return NOP;
        } else if (linteg == kc::INT64MAX) {
          num_ = HUGE_VAL;
          return NOP;
        } else if (linteg == kc::INT64MIN) {
          num_ = -HUGE_VAL;
          return NOP;
        }
        if (num_ == 0.0 && !(kc::chkinf(orig_) && orig_ >= 0)) {
          num_ = linteg + (double)lfract / DECUNIT;
          return NOP;
        }
        long double dinteg;
        long double dfract = std::modfl(num_, &dinteg);
        if (kc::chknan(dinteg)) {
          linteg = kc::INT64MIN;
          lfract = kc::INT64MIN;
          num_ = kc::nan();
        } else if (kc::chkinf(dinteg)) {
          linteg = dinteg > 0 ? kc::INT64MAX : kc::INT64MIN;
          lfract = 0;
          num_ = dinteg;
        } else {
          linteg += (int64_t)dinteg;
          lfract += (int64_t)(dfract * DECUNIT);
          if (lfract >= DECUNIT) {
            linteg += 1;
            lfract -= DECUNIT;
          }
          num_ = linteg + (double)lfract / DECUNIT;
        }
        linteg = kc::hton64(linteg);
        std::memcpy(buf_, &linteg, sizeof(linteg));
        lfract = kc::hton64(lfract);
        std::memcpy(buf_ + sizeof(linteg), &lfract, sizeof(lfract));
        *sp = sizeof(buf_);
        *xtp = xt_;
        return buf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        if (kc::chknan(orig_) || (kc::chkinf(orig_) && orig_ < 0)) {
          num_ = kc::nan();
          return NOP;
        }
        if (!kc::chkinf(orig_)) num_ += orig_;
        long double dinteg;
        long double dfract = std::modfl(num_, &dinteg);
        int64_t linteg, lfract;
        if (kc::chknan(dinteg)) {
          linteg = kc::INT64MIN;
          lfract = kc::INT64MIN;
        } else if (kc::chkinf(dinteg)) {
          linteg = dinteg > 0 ? kc::INT64MAX : kc::INT64MIN;
          lfract = 0;
        } else {
          linteg = (int64_t)dinteg;
          lfract = (int64_t)(dfract * DECUNIT);
        }
        linteg = kc::hton64(linteg);
        std::memcpy(buf_, &linteg, sizeof(linteg));
        lfract = kc::hton64(lfract);
        std::memcpy(buf_ + sizeof(linteg), &lfract, sizeof(lfract));
        *sp = sizeof(buf_);
        *xtp = xt_;
        return buf_;
      }
      const int64_t DECUNIT;
      double num_;
      double orig_;
      int64_t xt_;
      char buf_[sizeof(int64_t)*2];
    };
    VisitorImpl visitor(num, orig, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return kc::nan();
    num = visitor.num();
    if (kc::chknan(num)) {
      set_error(kc::BasicDB::Error::LOGIC, "logical inconsistency");
      return kc::nan();
    }
    return num;
  }
  /**
   * Add a number to the numeric double value of a record.
   * @note Equal to the original DB::increment_double method except that the parameter is
   * std::string.
   */
  double increment_double(const std::string& key, double num, double orig = 0,
                          int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return increment_double(key.c_str(), key.size(), num, orig, xt);
  }
  /**
   * Perform compare-and-swap.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param ovbuf the pointer to the old value region.  NULL means that no record corresponds.
   * @param ovsiz the size of the old value region.
   * @param nvbuf the pointer to the new value region.  NULL means that the record is removed.
   * @param nvsiz the size of new old value region.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @return true on success, or false on failure.
   */
  bool cas(const char* kbuf, size_t ksiz,
           const char* ovbuf, size_t ovsiz, const char* nvbuf, size_t nvsiz,
           int64_t xt = kc::INT64MAX) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(const char* ovbuf, size_t ovsiz, const char* nvbuf, size_t nvsiz,
                           int64_t xt) :
          ovbuf_(ovbuf), ovsiz_(ovsiz), nvbuf_(nvbuf), nvsiz_(nvsiz), xt_(xt), ok_(false) {}
      bool ok() const {
        return ok_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        if (!ovbuf_ || vsiz != ovsiz_ || std::memcmp(vbuf, ovbuf_, vsiz)) return NOP;
        ok_ = true;
        if (!nvbuf_) return REMOVE;
        *sp = nvsiz_;
        *xtp = xt_;
        return nvbuf_;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        if (ovbuf_) return NOP;
        ok_ = true;
        if (!nvbuf_) return NOP;
        *sp = nvsiz_;
        *xtp = xt_;
        return nvbuf_;
      }
      const char* ovbuf_;
      size_t ovsiz_;
      const char* nvbuf_;
      size_t nvsiz_;
      int64_t xt_;
      bool ok_;
    };
    VisitorImpl visitor(ovbuf, ovsiz, nvbuf, nvsiz, xt);
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::LOGIC, "status conflict");
      return false;
    }
    return true;
  }
  /**
   * Perform compare-and-swap.
   * @note Equal to the original DB::cas method except that the parameters are std::string.
   */
  bool cas(const std::string& key,
           const std::string& ovalue, const std::string& nvalue, int64_t xt = kc::INT64MAX) {
    _assert_(true);
    return cas(key.c_str(), key.size(),
               ovalue.c_str(), ovalue.size(), nvalue.c_str(), nvalue.size(), xt);
  }
  /**
   * Remove a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @return true on success, or false on failure.
   * @note If no record corresponds to the key, false is returned.
   */
  bool remove(const char* kbuf, size_t ksiz) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl() : ok_(false) {}
      bool ok() const {
        return ok_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        ok_ = true;
        return REMOVE;
      }
      const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
        return REMOVE;
      }
      bool ok_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, true)) return false;
    if (!visitor.ok()) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return false;
    }
    return true;
  }
  /**
   * Remove a record.
   * @note Equal to the original DB::remove method except that the parameter is std::string.
   */
  bool remove(const std::string& key) {
    _assert_(true);
    return remove(key.c_str(), key.size());
  }
  /**
   * Retrieve the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @param xtp the pointer to the variable into which the absolute expiration time is assigned.
   * If it is NULL, it is ignored.
   * @return the pointer to the value region of the corresponding record, or NULL on failure.
   * @note If no record corresponds to the key, NULL is returned.  Because an additional zero
   * code is appended at the end of the region of the return value, the return value can be
   * treated as a C-style string.  Because the region of the return value is allocated with the
   * the new[] operator, it should be released with the delete[] operator when it is no longer
   * in use.
   */
  char* get(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl() : vbuf_(NULL), vsiz_(0), xt_(0) {}
      char* pop(size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        if (xtp) *xtp = xt_;
        return vbuf_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vbuf_ = new char[vsiz+1];
        std::memcpy(vbuf_, vbuf, vsiz);
        vbuf_[vsiz] = '\0';
        vsiz_ = vsiz;
        xt_ = *xtp;
        return NOP;
      }
      char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, false)) {
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    size_t vsiz;
    char* vbuf = visitor.pop(&vsiz, xtp);
    if (!vbuf) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    *sp = vsiz;
    return vbuf;
  }
  /**
   * Retrieve the value of a record.
   * @note Equal to the original DB::get method except that the first parameters is the key
   * string and the second parameter is a string to contain the result and the return value is
   * bool for success.
   */
  bool get(const std::string& key, std::string* value, int64_t* xtp = NULL) {
    _assert_(value);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(std::string* value) : value_(value), ok_(false), xt_(0) {}
      bool ok(int64_t* xtp) {
        if (xtp) *xtp = xt_;
        return ok_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        value_->clear();
        value_->append(vbuf, vsiz);
        ok_ = true;
        xt_ = *xtp;
        return NOP;
      }
      std::string* value_;
      bool ok_;
      int64_t xt_;
    };
    VisitorImpl visitor(value);
    if (!accept(key.data(), key.size(), &visitor, false)) {
      if (xtp) *xtp = 0;
      return false;
    }
    if (!visitor.ok(xtp)) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return false;
    }
    return true;
  }
  /**
   * Retrieve the value of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the buffer into which the value of the corresponding record is
   * written.
   * @param max the size of the buffer.
   * @param xtp the pointer to the variable into which the expiration time from now in seconds
   * is assigned.  If it is NULL, it is ignored.
   * @return the size of the value, or -1 on failure.
   */
  int32_t get(const char* kbuf, size_t ksiz, char* vbuf, size_t max, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && vbuf);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(char* vbuf, size_t max) :
          vbuf_(vbuf), max_(max), vsiz_(-1), xt_(0) {}
      int32_t vsiz(int64_t* xtp) {
        if (xtp) *xtp = xt_;
        return vsiz_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vsiz_ = vsiz;
        xt_ = *xtp;
        size_t max = vsiz < max_ ? vsiz : max_;
        std::memcpy(vbuf_, vbuf, max);
        return NOP;
      }
      char* vbuf_;
      size_t max_;
      int32_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor(vbuf, max);
    if (!accept(kbuf, ksiz, &visitor, false)) return -1;
    int32_t vsiz = visitor.vsiz(xtp);
    if (vsiz < 0) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return -1;
    }
    return vsiz;
  }
  /**
   * Check the existence of a record.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param xtp the pointer to the variable into which the absolute expiration time is assigned.
   * If it is NULL, it is ignored.
   * @return the size of the value, or -1 on failure.
   */
  int32_t check(const char* kbuf, size_t ksiz, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl() : vsiz_(-1), xt_(0) {}
      int32_t vsiz(int64_t* xtp) {
        if (xtp) *xtp = xt_;
        return vsiz_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vsiz_ = vsiz;
        xt_ = *xtp;
        return NOP;
      }
      char* vbuf_;
      int32_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, false)) {
      if (xtp) *xtp = 0;
      return -1;
    }
    int32_t vsiz = visitor.vsiz(xtp);
    if (vsiz < 0) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      if (xtp) *xtp = 0;
      return -1;
    }
    return vsiz;
  }
  /**
   * Check the existence of a record.
   * @note Equal to the original DB::check method except that the first parameters is the key
   * string.
   */
  int32_t check(const std::string& key, int64_t* xtp = NULL) {
    return check(key.data(), key.size(), xtp);
  }
  /**
   * Retrieve the value of a record and remove it atomically.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @param xtp the pointer to the variable into which the absolute expiration time is assigned.
   * If it is NULL, it is ignored.
   * @return the pointer to the value region of the corresponding record, or NULL on failure.
   * @note If no record corresponds to the key, NULL is returned.  Because an additional zero
   * code is appended at the end of the region of the return value, the return value can be
   * treated as a C-style string.  Because the region of the return value is allocated with the
   * the new[] operator, it should be released with the delete[] operator when it is no longer
   * in use.
   */
  char* seize(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp = NULL) {
    _assert_(kbuf && ksiz <= kc::MEMMAXSIZ && sp);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl() : vbuf_(NULL), vsiz_(0), xt_(0) {}
      char* pop(size_t* sp, int64_t* xtp) {
        *sp = vsiz_;
        if (xtp) *xtp = xt_;
        return vbuf_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        vbuf_ = new char[vsiz+1];
        std::memcpy(vbuf_, vbuf, vsiz);
        vbuf_[vsiz] = '\0';
        vsiz_ = vsiz;
        xt_ = *xtp;
        return REMOVE;
      }
      char* vbuf_;
      size_t vsiz_;
      int64_t xt_;
    };
    VisitorImpl visitor;
    if (!accept(kbuf, ksiz, &visitor, true)) {
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    size_t vsiz;
    char* vbuf = visitor.pop(&vsiz, xtp);
    if (!vbuf) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    *sp = vsiz;
    return vbuf;
  }
  /**
   * Retrieve the value of a record and remove it atomically.
   * @note Equal to the original DB::get method except that the first parameters is the key
   * string and the second parameter is a string to contain the result and the return value is
   * bool for success.
   */
  bool seize(const std::string& key, std::string* value, int64_t* xtp = NULL) {
    _assert_(value);
    class VisitorImpl : public Visitor {
     public:
      explicit VisitorImpl(std::string* value) : value_(value), ok_(false), xt_(0) {}
      bool ok(int64_t* xtp) {
        if (xtp) *xtp = xt_;
        return ok_;
      }
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
        value_->clear();
        value_->append(vbuf, vsiz);
        ok_ = true;
        xt_ = *xtp;
        return REMOVE;
      }
      std::string* value_;
      bool ok_;
      int64_t xt_;
    };
    VisitorImpl visitor(value);
    if (!accept(key.data(), key.size(), &visitor, true)) {
      if (xtp) *xtp = 0;
      return false;
    }
    if (!visitor.ok(xtp)) {
      set_error(kc::BasicDB::Error::NOREC, "no record");
      return false;
    }
    return true;
  }
  /**
   * Store records at once.
   * @param recs the records to store.
   * @param xt the expiration time from now in seconds.  If it is negative, the absolute value
   * is treated as the epoch time.
   * @param atomic true to perform all operations atomically, or false for non-atomic operations.
   * @return the number of stored records, or -1 on failure.
   */
  int64_t set_bulk(const std::map<std::string, std::string>& recs,
                   int64_t xt = kc::INT64MAX, bool atomic = true) {
    _assert_(true);
    if (atomic) {
      std::vector<std::string> keys;
      keys.reserve(recs.size());
      std::map<std::string, std::string>::const_iterator rit = recs.begin();
      std::map<std::string, std::string>::const_iterator ritend = recs.end();
      while (rit != ritend) {
        keys.push_back(rit->first);
        ++rit;
      }
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl(const std::map<std::string, std::string>& recs, int64_t xt) :
            recs_(recs), xt_(xt) {}
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          std::map<std::string, std::string>::const_iterator rit =
              recs_.find(std::string(kbuf, ksiz));
          if (rit == recs_.end()) return NOP;
          *sp = rit->second.size();
          *xtp = xt_;
          return rit->second.data();
        }
        const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
          std::map<std::string, std::string>::const_iterator rit =
              recs_.find(std::string(kbuf, ksiz));
          if (rit == recs_.end()) return NOP;
          *sp = rit->second.size();
          *xtp = xt_;
          return rit->second.data();
        }
        const std::map<std::string, std::string>& recs_;
        int64_t xt_;
      };
      VisitorImpl visitor(recs, xt);
      if (!accept_bulk(keys, &visitor, true)) return -1;
      return keys.size();
    }
    std::map<std::string, std::string>::const_iterator rit = recs.begin();
    std::map<std::string, std::string>::const_iterator ritend = recs.end();
    while (rit != ritend) {
      if (!set(rit->first.data(), rit->first.size(), rit->second.data(), rit->second.size(), xt))
        return -1;
      ++rit;
    }
    return recs.size();
  }
  /**
   * Remove records at once.
   * @param keys the keys of the records to remove.
   * @param atomic true to perform all operations atomically, or false for non-atomic operations.
   * @return the number of removed records, or -1 on failure.
   */
  int64_t remove_bulk(const std::vector<std::string>& keys, bool atomic = true) {
    _assert_(true);
    if (atomic) {
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl() : cnt_(0) {}
        int64_t cnt() const {
          return cnt_;
        }
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          cnt_++;
          return REMOVE;
        }
        const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp, int64_t* xtp) {
          return REMOVE;
        }
        int64_t cnt_;
      };
      VisitorImpl visitor;
      if (!accept_bulk(keys, &visitor, true)) return -1;
      return visitor.cnt();
    }
    int64_t cnt = 0;
    std::vector<std::string>::const_iterator kit = keys.begin();
    std::vector<std::string>::const_iterator kitend = keys.end();
    while (kit != kitend) {
      if (remove(kit->data(), kit->size())) {
        cnt++;
      } else if (error() != kc::BasicDB::Error::NOREC) {
        return -1;
      }
      ++kit;
    }
    return cnt;
  }
  /**
   * Retrieve records at once.
   * @param keys the keys of the records to retrieve.
   * @param recs a string map to contain the retrieved records.
   * @param atomic true to perform all operations atomically, or false for non-atomic operations.
   * @return the number of retrieved records, or -1 on failure.
   */
  int64_t get_bulk(const std::vector<std::string>& keys,
                   std::map<std::string, std::string>* recs, bool atomic = true) {
    _assert_(recs);
    if (atomic) {
      class VisitorImpl : public Visitor {
       public:
        explicit VisitorImpl(std::map<std::string, std::string>* recs) : recs_(recs) {}
       private:
        const char* visit_full(const char* kbuf, size_t ksiz,
                               const char* vbuf, size_t vsiz, size_t* sp, int64_t* xtp) {
          (*recs_)[std::string(kbuf, ksiz)] = std::string(vbuf, vsiz);
          return NOP;
        }
        std::map<std::string, std::string>* recs_;
      };
      VisitorImpl visitor(recs);
      if (!accept_bulk(keys, &visitor, false)) return -1;
      return recs->size();
    }
    std::vector<std::string>::const_iterator kit = keys.begin();
    std::vector<std::string>::const_iterator kitend = keys.end();
    while (kit != kitend) {
      size_t vsiz;
      const char* vbuf = get(kit->data(), kit->size(), &vsiz);
      if (vbuf) {
        (*recs)[*kit] = std::string(vbuf, vsiz);
        delete[] vbuf;
      } else if (error() != kc::BasicDB::Error::NOREC) {
        return -1;
      }
      ++kit;
    }
    return recs->size();
  }
  /**
   * Dump records into a data stream.
   * @param dest the destination stream.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool dump_snapshot(std::ostream* dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(dest);
    return db_.dump_snapshot(dest, checker);
  }
  /**
   * Dump records into a file.
   * @param dest the path of the destination file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool dump_snapshot(const std::string& dest, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.dump_snapshot(dest, checker);
  }
  /**
   * Load records from a data stream.
   * @param src the source stream.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool load_snapshot(std::istream* src, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(src);
    return db_.load_snapshot(src, checker);
  }
  /**
   * Load records from a file.
   * @param src the path of the source file.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool load_snapshot(const std::string& src, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(true);
    return db_.load_snapshot(src, checker);
  }
  /**
   * Dump records atomically into a file.
   * @param dest the path of the destination file.
   * @param zcomp the data compressor object.  If it is NULL, no compression is performed.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool dump_snapshot_atomic(const std::string& dest, kc::Compressor* zcomp = NULL,
                            kc::BasicDB::ProgressChecker* checker = NULL);
  /**
   * Load records atomically from a file.
   * @param src the path of the source file.
   * @param zcomp the data compressor object.  If it is NULL, no decompression is performed.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool load_snapshot_atomic(const std::string& src, kc::Compressor* zcomp = NULL,
                            kc::BasicDB::ProgressChecker* checker = NULL);
  /**
   * Reveal the inner database object.
   * @return the inner database object, or NULL on failure.
   */
  kc::BasicDB* reveal_inner_db() {
    _assert_(true);
    return db_.reveal_inner_db();
  }
  /**
   * Scan the database and eliminate regions of expired records.
   * @param step the number of steps.  If it is not more than 0, the whole region is scanned.
   * @return true on success, or false on failure.
   */
  bool vacuum(int64_t step = 0) {
    _assert_(true);
    bool err = false;
    if (xcur_) {
      if (step > 1) {
        if (step > kc::INT64MAX / XTSCUNIT) step = kc::INT64MAX / XTSCUNIT;
        if (!expire_records(step * XTSCUNIT)) err = true;
      } else {
        xcur_->jump();
        xsc_ = 0;
        if (!expire_records(kc::INT64MAX)) err = true;
        xsc_ = 0;
      }
    }
    if (!defrag(step)) err = true;
    return !err;
  }
  /**
   * Recover the database with an update log message.
   * @param mbuf the pointer to the message region.
   * @param msiz the size of the message region.
   * @return true on success, or false on failure.
   */
  bool recover(const char* mbuf, size_t msiz) {
    _assert_(mbuf && msiz <= kc::MEMMAXSIZ);
    bool err = false;
    if (msiz < 1) {
      set_error(kc::BasicDB::Error::INVALID, "invalid message format");
      return false;
    }
    const char* rp = mbuf;
    uint8_t op = *(uint8_t*)(rp++);
    msiz--;
    switch (op) {
      case USET: {
        if (msiz < 2) {
          set_error(kc::BasicDB::Error::INVALID, "invalid message format");
          return false;
        }
        uint64_t ksiz;
        size_t step = kc::readvarnum(rp, msiz, &ksiz);
        rp += step;
        msiz -= step;
        uint64_t vsiz;
        step = kc::readvarnum(rp, msiz, &vsiz);
        rp += step;
        msiz -= step;
        const char* kbuf = rp;
        const char* vbuf = rp + ksiz;
        if (msiz != ksiz + vsiz) {
          set_error(kc::BasicDB::Error::INVALID, "invalid message format");
          return false;
        }
        if (!db_.set(kbuf, ksiz, vbuf, vsiz)) err = true;
        if (utrigger_) log_update(utrigger_, kbuf, ksiz, vbuf, vsiz);
        break;
      }
      case UREMOVE: {
        if (msiz < 1) {
          set_error(kc::BasicDB::Error::INVALID, "invalid message format");
          return false;
        }
        uint64_t ksiz;
        size_t step = kc::readvarnum(rp, msiz, &ksiz);
        rp += step;
        msiz -= step;
        const char* kbuf = rp;
        if (msiz != ksiz) {
          set_error(kc::BasicDB::Error::INVALID, "invalid message format");
          return false;
        }
        if (!db_.remove(kbuf, ksiz) && db_.error() != kc::BasicDB::Error::NOREC) err = true;
        if (utrigger_) log_update(utrigger_, kbuf, ksiz, TimedVisitor::REMOVE, 0);
        break;
      }
      case UCLEAR: {
        if (msiz != 0) {
          set_error(kc::BasicDB::Error::INVALID, "invalid message format");
          return false;
        }
        if (!db_.clear()) err = true;
        break;
      }
      default: {
        break;
      }
    }
    if (xcur_ && !expire_records(XTSCUNIT)) err = true;
    return !err;
  }
  /**
   * Get keys matching a prefix string.
   * @param prefix the prefix string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_prefix(const std::string& prefix, std::vector<std::string>* strvec,
                       int64_t max = -1, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(strvec);
    return db_.match_prefix(prefix, strvec, max, checker);
  }
  /**
   * Get keys matching a regular expression string.
   * @param regex the regular expression string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_regex(const std::string& regex, std::vector<std::string>* strvec,
                      int64_t max = -1, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(strvec);
    return db_.match_regex(regex, strvec, max, checker);
  }
  /**
   * Get keys similar to a string in terms of the levenshtein distance.
   * @param origin the origin string.
   * @param range the maximum distance of keys to adopt.
   * @param utf flag to treat keys as UTF-8 strings.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_similar(const std::string& origin, size_t range, bool utf,
                        std::vector<std::string>* strvec,
                        int64_t max = -1, kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(strvec);
    return db_.match_similar(origin, range, utf, strvec, max, checker);
  }
  /**
   * Merge records from other databases.
   * @param srcary an array of the source detabase objects.
   * @param srcnum the number of the elements of the source array.
   * @param mode the merge mode.  TimedDB::MSET to overwrite the existing value, TimedDB::MADD to
   * keep the existing value, TimedDB::MREPLACE to modify the existing record only,
   * TimedDB::MAPPEND to append the new value.
   * @param checker a progress checker object.  If it is NULL, no checking is performed.
   * @return true on success, or false on failure.
   */
  bool merge(TimedDB** srcary, size_t srcnum, MergeMode mode = MSET,
             kc::BasicDB::ProgressChecker* checker = NULL) {
    _assert_(srcary && srcnum <= kc::MEMMAXSIZ);
    bool err = false;
    kc::Comparator* comp = kc::LEXICALCOMP;
    std::priority_queue<MergeLine> lines;
    int64_t allcnt = 0;
    for (size_t i = 0; i < srcnum; i++) {
      MergeLine line;
      line.cur = srcary[i]->cursor();
      line.comp = comp;
      line.cur->jump();
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, &line.xt, true);
      if (line.kbuf) {
        lines.push(line);
        int64_t count = srcary[i]->count();
        if (count > 0) allcnt += count;
      } else {
        delete line.cur;
      }
    }
    if (checker && !checker->check("merge", "beginning", 0, allcnt)) {
      set_error(kc::BasicDB::Error::LOGIC, "checker failed");
      err = true;
    }
    int64_t curcnt = 0;
    while (!err && !lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      switch (mode) {
        case MSET: {
          if (!set(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt)) err = true;
          break;
        }
        case MADD: {
          if (!add(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt) &&
              error() != kc::BasicDB::Error::DUPREC) err = true;
          break;
        }
        case MREPLACE: {
          if (!replace(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt) &&
              error() != kc::BasicDB::Error::NOREC) err = true;
          break;
        }
        case MAPPEND: {
          if (!append(line.kbuf, line.ksiz, line.vbuf, line.vsiz, -line.xt)) err = true;
          break;
        }
      }
      delete[] line.kbuf;
      line.kbuf = line.cur->get(&line.ksiz, &line.vbuf, &line.vsiz, &line.xt, true);
      if (line.kbuf) {
        lines.push(line);
      } else {
        delete line.cur;
      }
      curcnt++;
      if (checker && !checker->check("merge", "processing", curcnt, allcnt)) {
        set_error(kc::BasicDB::Error::LOGIC, "checker failed");
        err = true;
        break;
      }
    }
    if (checker && !checker->check("merge", "ending", -1, allcnt)) {
      set_error(kc::BasicDB::Error::LOGIC, "checker failed");
      err = true;
    }
    while (!lines.empty()) {
      MergeLine line = lines.top();
      lines.pop();
      delete[] line.kbuf;
      delete line.cur;
    }
    return !err;
  }
  /**
   * Create a cursor object.
   * @return the return value is the created cursor object.
   * @note Because the object of the return value is allocated by the constructor, it should be
   * released with the delete operator when it is no longer in use.
   */
  Cursor* cursor() {
    _assert_(true);
    return new Cursor(this);
  }
  /**
   * Set the internal logger.
   * @param logger the logger object.  The same as with kc::BasicDB.
   * @param kinds kinds of logged messages by bitwise-or:  The same as with kc::BasicDB.
   * @return true on success, or false on failure.
   */
  bool tune_logger(kc::BasicDB::Logger* logger,
                   uint32_t kinds = kc::BasicDB::Logger::WARN | kc::BasicDB::Logger::ERROR) {
    _assert_(logger);
    return db_.tune_logger(logger, kinds);
  }
  /**
   * Set the internal update trigger.
   * @param trigger the trigger object.
   * @return true on success, or false on failure.
   */
  bool tune_update_trigger(UpdateTrigger* trigger) {
    _assert_(trigger);
    utrigger_ = trigger;
    return true;
  }
  /**
   * Tokenize an update log message.
   * @param mbuf the pointer to the message region.
   * @param msiz the size of the message region.
   * @param tokens a string vector to contain the result.
   * @return true on success, or false on failure.
   */
  static bool tokenize_update_log(const char* mbuf, size_t msiz,
                                  std::vector<std::string>* tokens) {
    _assert_(mbuf && msiz <= kc::MEMMAXSIZ && tokens);
    tokens->clear();
    if (msiz < 1) return false;
    const char* rp = mbuf;
    uint8_t op = *(uint8_t*)(rp++);
    msiz--;
    switch (op) {
      case USET: {
        if (msiz < 2) return false;
        uint64_t ksiz;
        size_t step = kc::readvarnum(rp, msiz, &ksiz);
        rp += step;
        msiz -= step;
        uint64_t vsiz;
        step = kc::readvarnum(rp, msiz, &vsiz);
        rp += step;
        msiz -= step;
        const char* kbuf = rp;
        const char* vbuf = rp + ksiz;
        if (msiz != ksiz + vsiz) return false;
        tokens->push_back("set");
        tokens->push_back(std::string(kbuf, ksiz));
        tokens->push_back(std::string(vbuf, vsiz));
        break;
      }
      case UREMOVE: {
        if (msiz < 1) return false;
        uint64_t ksiz;
        size_t step = kc::readvarnum(rp, msiz, &ksiz);
        rp += step;
        msiz -= step;
        const char* kbuf = rp;
        if (msiz != ksiz) return false;
        tokens->push_back("remove");
        tokens->push_back(std::string(kbuf, ksiz));
        break;
      }
      case UCLEAR: {
        if (msiz != 0) return false;
        tokens->push_back("clear");
        break;
      }
      default: {
        tokens->push_back("unknown");
        tokens->push_back(std::string(mbuf, msiz));
        break;
      }
    }
    return true;
  }
  /**
   * Get status of an atomic snapshot file.
   * @param src the path of the source file.
   * @param tsp the pointer to the variable into which the time stamp of the snapshot data is
   * assigned.  If it is NULL, it is ignored.
   * @param cntp the pointer to the variable into which the number of records in the original
   * database is assigned.  If it is NULL, it is ignored.
   * @param sizp the pointer to the variable into which the size of the original database is
   * assigned.  If it is NULL, it is ignored.
   * @return true on success, or false on failure.
   */
  static bool status_snapshot_atomic(const std::string& src, uint64_t* tsp = NULL,
                                     int64_t* cntp = NULL, int64_t* sizp = NULL);
 private:
  /**
   * Tuning Options.
   */
  enum Option {
    TPERSIST = 1 << 1                    ///< disable expiration
  };
  /**
   * Update Operations.
   */
  enum UpdateOperation {
    UNOP = 0xa0,                         ///< no operation
    USET,                                ///< setting the value
    UREMOVE,                             ///< removing the record
    UOPEN,                               ///< opening
    UCLOSE,                              ///< closing
    UCLEAR,                              ///< clearing
    UITERATE,                            ///< iteration
    USYNCHRONIZE,                        ///< synchronization
    UBEGINTRAN,                          ///< beginning transaction
    UCOMMITTRAN,                         ///< committing transaction
    UABORTTRAN,                          ///< aborting transaction
    UUNKNOWN                             ///< unknown operation
  };
  /**
   * Visitor to handle records with time stamps.
   */
  class TimedVisitor : public kc::BasicDB::Visitor {
   public:
    TimedVisitor(TimedDB* db, TimedDB::Visitor* visitor, int64_t ct, bool isiter) :
        db_(db), visitor_(visitor), ct_(ct), isiter_(isiter), jbuf_(NULL), again_(false) {
      _assert_(db && visitor && ct >= 0);
    }
    ~TimedVisitor() {
      _assert_(true);
      delete[] jbuf_;
    }
    bool again() {
      _assert_(true);
      return again_;
    }
   private:
    const char* visit_full(const char* kbuf, size_t ksiz,
                           const char* vbuf, size_t vsiz, size_t* sp) {
      _assert_(kbuf && vbuf && sp);
      if (db_->opts_ & TimedDB::TPERSIST) {
        size_t rsiz;
        int64_t xt = kc::INT64MAX;
        const char* rbuf = visitor_->visit_full(kbuf, ksiz, vbuf, vsiz, &rsiz, &xt);
        *sp = rsiz;
        if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, rbuf, rsiz);
        return rbuf;
      }
      if (vsiz < (size_t)XTWIDTH) return NOP;
      int64_t xt = kc::readfixnum(vbuf, XTWIDTH);
      if (ct_ > xt) {
        if (isiter_) {
          again_ = true;
          return NOP;
        }
        db_->set_error(kc::BasicDB::Error::NOREC, "no record (expired)");
        size_t rsiz;
        const char* rbuf = visitor_->visit_empty(kbuf, ksiz, &rsiz, &xt);
        if (rbuf == TimedDB::Visitor::NOP) return NOP;
        if (rbuf == TimedDB::Visitor::REMOVE) {
          if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, REMOVE, 0);
          return REMOVE;
        }
        delete[] jbuf_;
        xt = modify_exptime(xt, ct_);
        size_t jsiz;
        jbuf_ = make_record_value(rbuf, rsiz, xt, &jsiz);
        *sp = jsiz;
        if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, jbuf_, jsiz);
        return jbuf_;
      }
      vbuf += XTWIDTH;
      vsiz -= XTWIDTH;
      size_t rsiz;
      const char* rbuf = visitor_->visit_full(kbuf, ksiz, vbuf, vsiz, &rsiz, &xt);
      if (rbuf == TimedDB::Visitor::NOP) return NOP;
      if (rbuf == TimedDB::Visitor::REMOVE) {
        if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, REMOVE, 0);
        return REMOVE;
      }
      delete[] jbuf_;
      xt = modify_exptime(xt, ct_);
      size_t jsiz;
      jbuf_ = make_record_value(rbuf, rsiz, xt, &jsiz);
      *sp = jsiz;
      if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, jbuf_, jsiz);
      return jbuf_;
    }
    const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp) {
      if (db_->opts_ & TimedDB::TPERSIST) {
        size_t rsiz;
        int64_t xt = kc::INT64MAX;
        const char* rbuf = visitor_->visit_empty(kbuf, ksiz, &rsiz, &xt);
        *sp = rsiz;
        if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, rbuf, rsiz);
        return rbuf;
      }
      size_t rsiz;
      int64_t xt = -1;
      const char* rbuf = visitor_->visit_empty(kbuf, ksiz, &rsiz, &xt);
      if (rbuf == TimedDB::Visitor::NOP) return NOP;
      if (rbuf == TimedDB::Visitor::REMOVE) {
        if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, REMOVE, 0);
        return REMOVE;
      }
      delete[] jbuf_;
      xt = modify_exptime(xt, ct_);
      size_t jsiz;
      jbuf_ = make_record_value(rbuf, rsiz, xt, &jsiz);
      *sp = jsiz;
      if (db_->utrigger_) log_update(db_->utrigger_, kbuf, ksiz, jbuf_, jsiz);
      return jbuf_;
    }
    void visit_before() {
      _assert_(true);
      visitor_->visit_before();
    }
    void visit_after() {
      _assert_(true);
      visitor_->visit_after();
    }
    TimedDB* db_;
    TimedDB::Visitor* visitor_;
    int64_t ct_;
    bool isiter_;
    char* jbuf_;
    bool again_;
  };
  /**
   * Trigger of meta database operations.
   */
  class TimedMetaTrigger : public kc::BasicDB::MetaTrigger {
   public:
    TimedMetaTrigger(TimedDB* db) : db_(db) {
      _assert_(db);
    }
   private:
    void trigger(Kind kind, const char* message) {
      _assert_(message);
      if (!db_->utrigger_) return;
      switch (kind) {
        case CLEAR: {
          char mbuf[1];
          *mbuf = UCLEAR;
          db_->utrigger_->trigger(mbuf, 1);
          break;
        }
        case BEGINTRAN: {
          db_->utrigger_->begin_transaction();
          break;
        }
        case COMMITTRAN: {
          db_->utrigger_->end_transaction(true);
          break;
        }
        case ABORTTRAN: {
          db_->utrigger_->end_transaction(false);
          break;
        }
        default: {
          break;
        }
      }
    }
    TimedDB* db_;
  };
  /**
   * Front line of a merging list.
   */
  struct MergeLine {
    TimedDB::Cursor* cur;                ///< cursor
    kc::Comparator* comp;                ///< comparator
    char* kbuf;                          ///< pointer to the key
    size_t ksiz;                         ///< size of the key
    const char* vbuf;                    ///< pointer to the value
    size_t vsiz;                         ///< size of the value
    int64_t xt;                          ///< expiration time
    /** comparing operator */
    bool operator <(const MergeLine& right) const {
      return comp->compare(kbuf, ksiz, right.kbuf, right.ksiz) > 0;
    }
  };
  /**
   * Remove expired records.
   * @param score the score of expiration.
   * @return true on success, or false on failure.
   */
  bool expire_records(int64_t score) {
    _assert_(score >= 0);
    xsc_ += score;
    if (xsc_ < XTSCUNIT * XTUNIT) return true;
    if (!xlock_.lock_try()) return true;
    int64_t step = (int64_t)xsc_ / XTSCUNIT;
    xsc_ -= step * XTSCUNIT;
    int64_t ct = std::time(NULL);
    class VisitorImpl : public kc::BasicDB::Visitor {
     public:
      VisitorImpl(int64_t ct) : ct_(ct) {}
     private:
      const char* visit_full(const char* kbuf, size_t ksiz,
                             const char* vbuf, size_t vsiz, size_t* sp) {
        if (vsiz < (size_t)XTWIDTH) return NOP;
        int64_t xt = kc::readfixnum(vbuf, XTWIDTH);
        if (ct_ <= xt) return NOP;
        return REMOVE;
      }
      int64_t ct_;
    };
    VisitorImpl visitor(ct);
    bool err = false;
    for (int64_t i = 0; i < step; i++) {
      if (!xcur_->accept(&visitor, true, true)) {
        kc::BasicDB::Error::Code code = db_.error().code();
        if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
          xcur_->jump();
        } else {
          err = true;
        }
        xsc_ = 0;
        break;
      }
    }
    if (capcnt_ > 0) {
      int64_t count = db_.count();
      while (count > capcnt_) {
        if (!xcur_->remove()) {
          kc::BasicDB::Error::Code code = db_.error().code();
          if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
            xcur_->jump();
          } else {
            err = true;
          }
          break;
        }
        count--;
      }
      if (!defrag(step)) err = true;
    }
    if (capsiz_ > 0) {
      int64_t size = db_.size();
      if (size > capsiz_) {
        for (int64_t i = 0; i < step; i++) {
          if (!xcur_->remove()) {
            kc::BasicDB::Error::Code code = db_.error().code();
            if (code == kc::BasicDB::Error::INVALID || code == kc::BasicDB::Error::NOREC) {
              xcur_->jump();
            } else {
              err = true;
            }
            break;
          }
        }
        if (!defrag(step)) err = true;
      }
    }
    xlock_.unlock();
    return !err;
  }
  /**
   * Perform defragmentation of the database file.
   * @param step the number of steps.  If it is not more than 0, the whole region is defraged.
   * @return true on success, or false on failure.
   */
  bool defrag(int step) {
    _assert_(true);
    bool err = false;
    kc::BasicDB* idb = db_.reveal_inner_db();
    if (idb) {
      const std::type_info& info = typeid(*idb);
      if (info == typeid(kc::HashDB)) {
        kc::HashDB* hdb = (kc::HashDB*)idb;
        if (!hdb->defrag(step)) err = true;
      } else if (info == typeid(kc::TreeDB)) {
        kc::TreeDB* tdb = (kc::TreeDB*)idb;
        if (!tdb->defrag(step)) err = true;
      }
    }
    return !err;
  }
  /**
   * Make the record value with meta data.
   * @param vbuf the buffer of the original record value.
   * @param vsiz the size of the original record value.
   * @param xt the expiration time.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return the pointer to the result buffer.
   */
  static char* make_record_value(const char* vbuf, size_t vsiz, int64_t xt, size_t* sp) {
    _assert_(vbuf && vsiz <= kc::MEMMAXSIZ);
    size_t jsiz = vsiz + XTWIDTH;
    char* jbuf = new char[jsiz];
    kc::writefixnum(jbuf, xt, XTWIDTH);
    std::memcpy(jbuf + XTWIDTH, vbuf, vsiz);
    *sp = jsiz;
    return jbuf;
  }
  /**
   * Modify an expiration time by the current time.
   * @param xt the expiration time.
   * @param ct the current time.
   * @return the modified expiration time.
   */
  static int64_t modify_exptime(int64_t xt, int64_t ct) {
    _assert_(true);
    if (xt < 0) {
      if (xt < kc::INT64MIN / 2) xt = kc::INT64MIN / 2;
      xt = -xt;
    } else {
      if (xt > kc::INT64MAX / 2) xt = kc::INT64MAX / 2;
      xt += ct;
    }
    if (xt > XTMAX) xt = XTMAX;
    return xt;
  }
  /**
   * Log a record update operation.
   * @param utrigger the update trigger.
   * @param kbuf the pointer to the key region.
   * @param ksiz the size of the key region.
   * @param vbuf the pointer to the value region.
   * @param vsiz the size of the value region.
   */
  static void log_update(UpdateTrigger* utrigger, const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz) {
    _assert_(utrigger && kbuf);
    if (vbuf == TimedVisitor::REMOVE) {
      size_t msiz = 1 + sizeof(uint64_t) + ksiz;
      char stack[LOGBUFSIZ];
      char* mbuf = msiz > sizeof(stack) ? new char[msiz] : stack;
      char* wp = mbuf;
      *(wp++) = UREMOVE;
      wp += kc::writevarnum(wp, ksiz);
      std::memcpy(wp, kbuf, ksiz);
      wp += ksiz;
      utrigger->trigger(mbuf, wp - mbuf);
      if (mbuf != stack) delete[] mbuf;
    } else if (vbuf != TimedVisitor::NOP) {
      size_t msiz = 1 + sizeof(uint64_t) * 2 + ksiz + vsiz;
      char stack[LOGBUFSIZ];
      char* mbuf = msiz > sizeof(stack) ? new char[msiz] : stack;
      char* wp = mbuf;
      *(wp++) = USET;
      wp += kc::writevarnum(wp, ksiz);
      wp += kc::writevarnum(wp, vsiz);
      std::memcpy(wp, kbuf, ksiz);
      wp += ksiz;
      std::memcpy(wp, vbuf, vsiz);
      wp += vsiz;
      utrigger->trigger(mbuf, wp - mbuf);
      if (mbuf != stack) delete[] mbuf;
    }
  }
  /** Dummy constructor to forbid the use. */
  TimedDB(const TimedDB&);
  /** Dummy Operator to forbid the use. */
  TimedDB& operator =(const TimedDB&);
  /** The expiration cursor lock. */
  kc::SpinLock xlock_;
  /** The internal database. */
  kc::PolyDB db_;
  /** The internal meta trigger. */
  TimedMetaTrigger mtrigger_;
  /** The internal update trigger. */
  UpdateTrigger* utrigger_;
  /** The open mode. */
  uint32_t omode_;
  /** The options. */
  uint8_t opts_;
  /** The capacity of record number. */
  int64_t capcnt_;
  /** The capacity of memory usage. */
  int64_t capsiz_;
  /** The cursor for expiration. */
  kc::PolyDB::Cursor* xcur_;
  /** The score of expiration. */
  kc::AtomicInt64 xsc_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
