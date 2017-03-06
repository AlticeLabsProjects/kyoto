/*************************************************************************************************
 * Remote database
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


#ifndef _KTREMOTEDB_H                    // duplication check
#define _KTREMOTEDB_H

#include <ktcommon.h>
#include <ktutil.h>
#include <ktsocket.h>
#include <ktthserv.h>
#include <kthttp.h>
#include <ktrpc.h>
#include <ktulog.h>
#include <ktshlib.h>
#include <kttimeddb.h>
#include <ktdbext.h>

namespace kyototycoon {                  // common namespace


/**
 * Remote database.
 * @note This class is a concrete class to access remote database servers.  This class can be
 * inherited but overwriting methods is forbidden.  Before every database operation, it is
 * necessary to call the RemoteDB::open method in order to connect to a database server.  To
 * avoid resource starvation it is important to close every database file by the RemoteDB::close
 * method when the connection is no longer in use.  Although all methods of this class are
 * thread-safe, its instance does not have mutual exclusion mechanism.  So, multiple threads
 * must not share the same instance and they must use their own respective instances.
 */
class RemoteDB {
 public:
  class Cursor;
  class Error;
  struct BulkRecord;
  /** The maximum size of each record data. */
  static const size_t DATAMAXSIZ = 1ULL << 28;
 private:
  struct OrderedKey;
  /** An alias of list of cursors. */
  typedef std::list<Cursor*> CursorList;
  /** The size for a record buffer. */
  static const int32_t RECBUFSIZ = 2048;
 public:
  /**
   * Cursor to indicate a record.
   */
  class Cursor {
    friend class RemoteDB;
   public:
    /**
     * Constructor.
     * @param db the container database object.
     */
    explicit Cursor(RemoteDB* db) : db_(db), id_(0) {
      _assert_(db);
      uint64_t uid = (((uint64_t)(intptr_t)db_ >> 8) << 16) ^ ((uint64_t)(intptr_t)this >> 8);
      uid ^= ((uint64_t)(kc::time() * 65536)) << 24;
      id_ = ((uid << 16) & (kc::INT64MAX >> 4)) + (++db->curcnt_);
      db_->curs_.push_back(this);
    }
    /**
     * Destructor.
     */
    virtual ~Cursor() {
      _assert_(true);
      if (!db_) return;
      std::map<std::string, std::string> inmap;
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      db_->rpc_.call("cur_delete", &inmap, &outmap);
      db_->curs_.remove(this);
    }
    /**
     * Jump the cursor to the first record for forward scan.
     * @return true on success, or false on failure.
     */
    bool jump() {
      _assert_(true);
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["key"] = std::string(kbuf, ksiz);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["key"] = std::string(kbuf, ksiz);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_jump_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_step", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_step_back", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
      return true;
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      inmap["value"] = std::string(vbuf, vsiz);
      if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_set_value", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_remove", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return false;
      }
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get_key", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return NULL;
      }
      size_t ksiz;
      const char* kbuf = strmapget(outmap, "key", &ksiz);
      if (!kbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      char* rbuf = new char[ksiz+1];
      std::memcpy(rbuf, kbuf, ksiz);
      rbuf[ksiz] = '\0';
      *sp = ksiz;
      return rbuf;
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get_value", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return NULL;
      }
      size_t vsiz;
      const char* vbuf = strmapget(outmap, "value", &vsiz);
      if (!vbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      char* rbuf = new char[vsiz+1];
      std::memcpy(rbuf, vbuf, vsiz);
      rbuf[vsiz] = '\0';
      *sp = vsiz;
      return rbuf;
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
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      if (step) inmap["step"] = "";
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_get", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        *ksp = 0;
        *vbp = NULL;
        *vsp = 0;
        return NULL;
      }
      size_t ksiz = 0;
      const char* kbuf = strmapget(outmap, "key", &ksiz);
      size_t vsiz = 0;
      const char* vbuf = strmapget(outmap, "value", &vsiz);
      if (!kbuf || !vbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        *ksp = 0;
        *vbp = NULL;
        *vsp = 0;
        return NULL;
      }
      const char* rp = strmapget(outmap, "xt");
      int64_t xt = rp ? kc::atoi(rp) : kc::INT64MAX;
      char* rbuf = new char[ksiz+vsiz+2];
      std::memcpy(rbuf, kbuf, ksiz);
      rbuf[ksiz] = '\0';
      std::memcpy(rbuf + ksiz + 1 , vbuf, vsiz);
      rbuf[ksiz+1+vsiz] = '\0';
      *ksp = ksiz;
      *vbp = rbuf + ksiz + 1;
      *vsp = vsiz;
      if (xtp) *xtp = xt;
      return rbuf;
    }
    /**
     * Get a pair of the key and the value of the current record.
     * @note Equal to the original Cursor::get method except that parameters are strings
     * to contain the result and the return value is bool for success.
     */
    bool get(std::string* key, std::string* value, int64_t* xtp = NULL, bool step = false) {
      _assert_(key && value);
      size_t ksiz, vsiz;
      const char* vbuf;
      char* kbuf = get(&ksiz, &vbuf, &vsiz, xtp, step);
      if (!kbuf) return false;
      key->clear();
      key->append(kbuf, ksiz);
      value->clear();
      value->append(vbuf, vsiz);
      delete[] kbuf;
      return true;
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
     * the detele[] operator.  The cursor is moved to the next record implicitly.
     */
    char* seize(size_t* ksp, const char** vbp, size_t* vsp, int64_t* xtp = NULL) {
      _assert_(ksp && vbp && vsp);
      std::map<std::string, std::string> inmap;
      db_->set_sig_param(inmap);
      db_->set_db_param(inmap);
      set_cur_param(inmap);
      std::map<std::string, std::string> outmap;
      RPCClient::ReturnValue rv = db_->rpc_.call("cur_seize", &inmap, &outmap);
      if (rv != RPCClient::RVSUCCESS) {
        db_->set_rpc_error(rv, outmap);
        return NULL;
      }
      size_t ksiz = 0;
      const char* kbuf = strmapget(outmap, "key", &ksiz);
      size_t vsiz = 0;
      const char* vbuf = strmapget(outmap, "value", &vsiz);
      if (!kbuf || !vbuf) {
        db_->set_error(RPCClient::RVELOGIC, "no information");
        return NULL;
      }
      const char* rp = strmapget(outmap, "xt");
      int64_t xt = rp ? kc::atoi(rp) : kc::INT64MAX;
      char* rbuf = new char[ksiz+vsiz+2];
      std::memcpy(rbuf, kbuf, ksiz);
      rbuf[ksiz] = '\0';
      std::memcpy(rbuf + ksiz + 1 , vbuf, vsiz);
      rbuf[ksiz+1+vsiz] = '\0';
      *ksp = ksiz;
      *vbp = rbuf + ksiz + 1;
      *vsp = vsiz;
      if (xtp) *xtp = xt;
      return rbuf;
    }
    /**
     * Get a pair of the key and the value of the current record and remove it atomically.
     * @note Equal to the original Cursor::seize method except that parameters are strings
     * to contain the result and the return value is bool for success.
     */
    bool seize(std::string* key, std::string* value, int64_t* xtp = NULL) {
      _assert_(key && value);
      size_t ksiz, vsiz;
      const char* vbuf;
      char* kbuf = seize(&ksiz, &vbuf, &vsiz, xtp);
      if (!kbuf) return false;
      key->clear();
      key->append(kbuf, ksiz);
      value->clear();
      value->append(vbuf, vsiz);
      delete[] kbuf;
      return true;
    }
    /**
     * Get the database object.
     * @return the database object.
     */
    RemoteDB* db() {
      _assert_(true);
      return db_;
    }
    /**
     * Get the last happened error.
     * @return the last happened error.
     */
    Error error() {
      _assert_(true);
      return db()->error();
    }
   private:
    /**
     * Set the parameter of the target cursor.
     * @param inmap the string map to contain the input parameters.
     */
    void set_cur_param(std::map<std::string, std::string>& inmap) {
      _assert_(true);
      kc::strprintf(&inmap["CUR"], "%lld", (long long)id_);
    }
    /** Dummy constructor to forbid the use. */
    Cursor(const Cursor&);
    /** Dummy Operator to forbid the use. */
    Cursor& operator =(const Cursor&);
    /** The inner database. */
    RemoteDB* db_;
    /** The ID number. */
    int64_t id_;
  };
  /**
   * Error data.
   */
  class Error {
   public:
    /**
     * Error codes.
     */
    enum Code {
      SUCCESS = RPCClient::RVSUCCESS,    ///< success
      NOIMPL = RPCClient::RVENOIMPL,     ///< not implemented
      INVALID = RPCClient::RVEINVALID,   ///< invalid operation
      LOGIC = RPCClient::RVELOGIC,       ///< logical inconsistency
      TIMEOUT = RPCClient::RVETIMEOUT,   ///< timeout
      INTERNAL = RPCClient::RVEINTERNAL, ///< internal error
      NETWORK = RPCClient::RVENETWORK,   ///< network error
      EMISC = RPCClient::RVEMISC         ///< miscellaneous error
    };
    /**
     * Default constructor.
     */
    explicit Error() : code_(SUCCESS), message_("no error") {
      _assert_(true);
    }
    /**
     * Copy constructor.
     * @param src the source object.
     */
    Error(const Error& src) : code_(src.code_), message_(src.message_) {
      _assert_(true);
    }
    /**
     * Constructor.
     * @param code an error code.
     * @param message a supplement message.
     */
    explicit Error(Code code, const std::string& message) : code_(code), message_(message) {
      _assert_(true);
    }
    /**
     * Destructor.
     */
    ~Error() {
      _assert_(true);
    }
    /**
     * Set the error information.
     * @param code an error code.
     * @param message a supplement message.
     */
    void set(Code code, const std::string& message) {
      _assert_(true);
      code_ = code;
      message_ = message;
    }
    /**
     * Get the error code.
     * @return the error code.
     */
    Code code() const {
      _assert_(true);
      return code_;
    }
    /**
     * Get the readable string of the code.
     * @return the readable string of the code.
     */
    const char* name() const {
      _assert_(true);
      return codename(code_);
    }
    /**
     * Get the supplement message.
     * @return the supplement message.
     */
    const char* message() const {
      _assert_(true);
      return message_.c_str();
    }
    /**
     * Get the readable string of an error code.
     * @param code the error code.
     * @return the readable string of the error code.
     */
    static const char* codename(Code code) {
      _assert_(true);
      switch (code) {
        case SUCCESS: return "success";
        case NOIMPL: return "not implemented";
        case INVALID: return "invalid operation";
        case LOGIC: return "logical inconsistency";
        case INTERNAL: return "internal error";
        case NETWORK: return "network error";
        default: break;
      }
      return "miscellaneous error";
    }
    /**
     * Assignment operator from the self type.
     * @param right the right operand.
     * @return the reference to itself.
     */
    Error& operator =(const Error& right) {
      _assert_(true);
      if (&right == this) return *this;
      code_ = right.code_;
      message_ = right.message_;
      return *this;
    }
    /**
     * Cast operator to integer.
     * @return the error code.
     */
    operator int32_t() const {
      return code_;
    }
   private:
    /** The error code. */
    Code code_;
    /** The supplement message. */
    std::string message_;
  };
  /**
   * Record for bulk operation.
   */
  struct BulkRecord {
    uint16_t dbidx;                      ///< index of the target database
    std::string key;                     ///< key
    std::string value;                   ///< value
    int64_t xt;                          ///< expiration time
  };
  /**
   * Magic data in binary protocol.
   */
  enum BinaryMagic {
    BMNOP = 0xb0,                        ///< no operation
    BMREPLICATION = 0xb1,                ///< replication
    BMPLAYSCRIPT = 0xb4,                 ///< call a scripting procedure
    BMSETBULK = 0xb8,                    ///< set in bulk
    BMREMOVEBULK = 0xb9,                 ///< remove in bulk
    BMGETBULK = 0xba,                    ///< get in bulk
    BMERROR = 0xbf                       ///< error
  };
  /**
   * Options in binary protocol.
   */
  enum BinaryOption {
    BONOREPLY = 1 << 0                   ///< no reply
  };
  /**
   * Default constructor.
   */
  explicit RemoteDB() :
      rpc_(), ecode_(RPCClient::RVSUCCESS), emsg_("no error"),
      dbexpr_(""), curs_(), curcnt_(0),
      sigwait_(false), sigwaitname_(""), sigwaittime_(0), sigsend_(false), sigsendbrd_(false) {
    _assert_(true);
  }
  /**
   * Destructor.
   */
  virtual ~RemoteDB() {
    _assert_(true);
    if (!curs_.empty()) {
      CursorList::const_iterator cit = curs_.begin();
      CursorList::const_iterator citend = curs_.end();
      while (cit != citend) {
        Cursor* cur = *cit;
        cur->db_ = NULL;
        ++cit;
      }
    }
  }
  /**
   * Get the last happened error code.
   * @return the last happened error code.
   */
  Error error() const {
    _assert_(true);
    return Error((Error::Code)ecode_, emsg_);
  }
  /**
   * Open the connection.
   * @param host the name or the address of the server.  If it is an empty string, the local host
   * is specified.
   * @param port the port number of the server.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& host = "", int32_t port = DEFPORT, double timeout = -1) {
    _assert_(true);
    if (!rpc_.open(host, port, timeout)) {
      set_error(RPCClient::RVENETWORK, "connection failed");
      return false;
    }
    return true;
  }
  /**
   * Close the connection.
   * @param grace true for graceful shutdown, or false for immediate disconnection.
   * @return true on success, or false on failure.
   */
  bool close(bool grace = true) {
    _assert_(true);
    return rpc_.close();
  }
  /**
   * Get the report of the server information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool report(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    strmap->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("report", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    strmap->insert(outmap.begin(), outmap.end());
    return true;
  }
  /**
   * Call a procedure of the scripting extension.
   * @param name the name of the procedure to call.
   * @param params a string map containing the input parameters.
   * @param result a string map to contain the output data.
   * @return true on success, or false on failure.
   */
  bool play_script(const std::string& name, const std::map<std::string, std::string>& params,
                   std::map<std::string, std::string>* result) {
    _assert_(result);
    result->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    inmap["name"] = name;
    std::map<std::string, std::string>::const_iterator it = params.begin();
    std::map<std::string, std::string>::const_iterator itend = params.end();
    while (it != itend) {
      std::string key = "_";
      key.append(it->first);
      inmap[key] = it->second;
      ++it;
    }
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("play_script", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    it = outmap.begin();
    itend = outmap.end();
    while (it != itend) {
      const char* kbuf = it->first.data();
      size_t ksiz = it->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        (*result)[key] = it->second;
      }
      ++it;
    }
    return true;
  }
  /**
   * Set the replication configuration.
   * @param host the name or the address of the master server.  If it is an empty string,
   * replication is disabled.
   * @param port the port number of the server.
   * @param ts the maximum time stamp of already read logs.  If it is kyotocabinet::UINT64MAX,
   * the current setting is not modified.  If it is kyotocabinet::UINT64MAX - 1, the current
   * time is specified.
   * @param iv the interval of each replication operation in milliseconds.  If it is negative,
   * the current interval is not modified.
   * @return true on success, or false on failure.
   */
  bool tune_replication(const std::string& host = "", int32_t port = DEFPORT,
                        uint64_t ts = kc::UINT64MAX, double iv = -1) {
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    if (!host.empty()) inmap["host"] = host;
    if (port != DEFPORT) kc::strprintf(&inmap["port"], "%d", port);
    if (ts == kc::UINT64MAX - 1) {
      inmap["ts"] = "now";
    } else if (ts != kc::UINT64MAX) {
      kc::strprintf(&inmap["ts"], "%llu", (unsigned long long)ts);
    }
    if (iv >= 0) kc::strprintf(&inmap["iv"], "%.6f", iv);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("tune_replication", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Get status of each update log files.
   * @param fstvec a vector to store status structures of each update log files.
   * @return true on success, or false on failure.
   */
  bool ulog_list(std::vector<UpdateLogger::FileStatus>* fstvec) {
    _assert_(fstvec);
    fstvec->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("ulog_list", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    std::map<std::string, std::string>::iterator it = outmap.begin();
    std::map<std::string, std::string>::iterator itend = outmap.end();
    while (it != itend) {
      size_t idx = it->second.find(':');
      if (idx != std::string::npos) {
        const std::string& tsstr = it->second.substr(idx + 1);
        int64_t fsiz = kc::atoi(it->second.c_str());
        int64_t fts = kc::atoi(tsstr.c_str());
        if (!it->first.empty() && fsiz >= 0 && fts >= 0) {
          UpdateLogger::FileStatus fs = { it->first, (uint64_t)fsiz, (uint64_t)fts };
          fstvec->push_back(fs);
        }
      }
      ++it;
    }
    return true;
  }
  /**
   * Remove old update log files.
   * @param ts the maximum time stamp of disposable logs.
   * @return true on success, or false on failure.
   */
  bool ulog_remove(uint64_t ts = kc::UINT64MAX) {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    kc::strprintf(&inmap["ts"], "%llu", (unsigned long long)ts);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("ulog_remove", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Get the miscellaneous status information.
   * @param strmap a string map to contain the result.
   * @return true on success, or false on failure.
   */
  bool status(std::map<std::string, std::string>* strmap) {
    _assert_(strmap);
    strmap->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    strmap->insert(outmap.begin(), outmap.end());
    return true;
  }
  /**
   * Remove all records.
   * @return true on success, or false on failure.
   */
  bool clear() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("clear", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Synchronize updated contents with the file and the device.
   * @param hard true for physical synchronization with the device, or false for logical
   * synchronization with the file system.
   * @param command the command name to process the database file.  If it is an empty string, no
   * postprocessing is performed.
   * @return true on success, or false on failure.
   */
  bool synchronize(bool hard, const std::string& command = "") {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    if (hard) inmap["hard"] = "";
    if (!command.empty()) inmap["command"] = command;
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("synchronize", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Get the number of records.
   * @return the number of records, or -1 on failure.
   */
  int64_t count() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "count");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Get the size of the database file.
   * @return the size of the database file in bytes, or -1 on failure.
   */
  int64_t size() {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("status", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "size");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("set", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("add", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("replace", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    inmap["value"] = std::string(vbuf, vsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("append", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    kc::strprintf(&inmap["num"], "%lld", (long long)num);
    kc::strprintf(&inmap["orig"], "%lld", (long long)orig);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("increment", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return kc::INT64MIN;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return kc::INT64MIN;
    }
    return kc::atoi(rp);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    kc::strprintf(&inmap["num"], "%f", num);
    kc::strprintf(&inmap["orig"], "%f", orig);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("increment_double", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return kc::nan();
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return kc::nan();
    }
    return kc::atof(rp);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    if (ovbuf) inmap["oval"] = std::string(ovbuf, ovsiz);
    if (nvbuf) inmap["nval"] = std::string(nvbuf, nvsiz);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("cas", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("remove", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("get", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    size_t vsiz;
    const char* vbuf = strmapget(outmap, "value", &vsiz);
    if (!vbuf) {
      set_error(RPCClient::RVELOGIC, "no information");
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    const char* rp = strmapget(outmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : kc::INT64MAX;
    char* rbuf = new char[vsiz+1];
    std::memcpy(rbuf, vbuf, vsiz);
    rbuf[vsiz] = '\0';
    *sp = vsiz;
    if (xtp) *xtp = xt;
    return rbuf;
  }
  /**
   * Retrieve the value of a record.
   * @note Equal to the original DB::get method except that the first parameters is the key
   * string and the second parameter is a string to contain the result and the return value is
   * bool for success.
   */
  bool get(const std::string& key, std::string* value, int64_t* xtp = NULL) {
    _assert_(value);
    size_t vsiz;
    char* vbuf = get(key.c_str(), key.size(), &vsiz, xtp);
    if (!vbuf) return false;
    value->clear();
    value->append(vbuf, vsiz);
    delete[] vbuf;
    return true;
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("check", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      if (xtp) *xtp = 0;
      return -1;
    }
    const char* rp = strmapget(outmap, "vsiz");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      if (xtp) *xtp = 0;
      return -1;
    }
    int32_t vsiz = kc::atoi(rp);
    rp = strmapget(outmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : kc::INT64MAX;
    if (xtp) *xtp = xt;
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["key"] = std::string(kbuf, ksiz);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("seize", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    size_t vsiz;
    const char* vbuf = strmapget(outmap, "value", &vsiz);
    if (!vbuf) {
      set_error(RPCClient::RVELOGIC, "no information");
      *sp = 0;
      if (xtp) *xtp = 0;
      return NULL;
    }
    const char* rp = strmapget(outmap, "xt");
    int64_t xt = rp ? kc::atoi(rp) : kc::INT64MAX;
    char* rbuf = new char[vsiz+1];
    std::memcpy(rbuf, vbuf, vsiz);
    rbuf[vsiz] = '\0';
    *sp = vsiz;
    if (xtp) *xtp = xt;
    return rbuf;
  }
  /**
   * Retrieve the value of a record and remove it atomically.
   * @note Equal to the original DB::seize method except that the first parameters is the key
   * string and the second parameter is a string to contain the result and the return value is
   * bool for success.
   */
  bool seize(const std::string& key, std::string* value, int64_t* xtp = NULL) {
    _assert_(value);
    size_t vsiz;
    char* vbuf = seize(key.c_str(), key.size(), &vsiz, xtp);
    if (!vbuf) return false;
    value->clear();
    value->append(vbuf, vsiz);
    delete[] vbuf;
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
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    if (xt < TimedDB::XTMAX) kc::strprintf(&inmap["xt"], "%lld", (long long)xt);
    if (atomic) inmap["atomic"] = "";
    std::map<std::string, std::string>::const_iterator it = recs.begin();
    std::map<std::string, std::string>::const_iterator itend = recs.end();
    while (it != itend) {
      std::string key = "_";
      key.append(it->first);
      inmap[key] = it->second;
      ++it;
    }
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("set_bulk", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Store records at once.
   * @param keys the keys of the records to remove.
   * @param atomic true to perform all operations atomically, or false for non-atomic operations.
   * @return the number of removed records, or -1 on failure.
   */
  int64_t remove_bulk(const std::vector<std::string>& keys, bool atomic = true) {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    if (atomic) inmap["atomic"] = "";
    std::vector<std::string>::const_iterator it = keys.begin();
    std::vector<std::string>::const_iterator itend = keys.end();
    while (it != itend) {
      std::string key = "_";
      key.append(*it);
      inmap[key] = "";
      ++it;
    }
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("remove_bulk", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
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
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    if (atomic) inmap["atomic"] = "";
    std::vector<std::string>::const_iterator it = keys.begin();
    std::vector<std::string>::const_iterator itend = keys.end();
    while (it != itend) {
      std::string key = "_";
      key.append(*it);
      inmap[key] = "";
      ++it;
    }
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("get_bulk", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    std::map<std::string, std::string>::const_iterator oit = outmap.begin();
    std::map<std::string, std::string>::const_iterator oitend = outmap.end();
    while (oit != oitend) {
      const char* kbuf = oit->first.data();
      size_t ksiz = oit->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        (*recs)[key] = oit->second;
      }
      ++oit;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Scan the database and eliminate regions of expired records.
   * @param step the number of steps.  If it is not more than 0, the whole region is scanned.
   * @return true on success, or false on failure.
   */
  bool vacuum(int64_t step = 0) {
    _assert_(true);
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    if (step > 0) kc::strprintf(&inmap["step"], "%lld", (long long)step);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("vacuum", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return false;
    }
    return true;
  }
  /**
   * Get keys matching a prefix string.
   * @param prefix the prefix string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_prefix(const std::string& prefix, std::vector<std::string>* strvec,
                       int64_t max = -1) {
    _assert_(strvec);
    strvec->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["prefix"] = prefix;
    if (max >= 0) kc::strprintf(&inmap["max"], "%lld", (long long)max);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("match_prefix", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    std::map<std::string, std::string>::const_iterator oit = outmap.begin();
    std::map<std::string, std::string>::const_iterator oitend = outmap.end();
    std::vector<OrderedKey> okeys;
    while (oit != oitend) {
      const char* kbuf = oit->first.data();
      size_t ksiz = oit->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        OrderedKey okey;
        okey.order = kc::atoi(oit->second.c_str());
        okey.key = key;
        okeys.push_back(okey);
      }
      ++oit;
    }
    std::sort(okeys.begin(), okeys.end());
    std::vector<OrderedKey>::const_iterator okit = okeys.begin();
    std::vector<OrderedKey>::const_iterator okitend = okeys.end();
    while (okit != okitend) {
      strvec->push_back(okit->key);
      ++okit;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Get keys matching a regular expression string.
   * @param regex the regular expression string.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_regex(const std::string& regex, std::vector<std::string>* strvec,
                      int64_t max = -1) {
    _assert_(strvec);
    strvec->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["regex"] = regex;
    if (max >= 0) kc::strprintf(&inmap["max"], "%lld", (long long)max);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("match_regex", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    std::map<std::string, std::string>::const_iterator oit = outmap.begin();
    std::map<std::string, std::string>::const_iterator oitend = outmap.end();
    std::vector<OrderedKey> okeys;
    while (oit != oitend) {
      const char* kbuf = oit->first.data();
      size_t ksiz = oit->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        OrderedKey okey;
        okey.order = kc::atoi(oit->second.c_str());
        okey.key = key;
        okeys.push_back(okey);
      }
      ++oit;
    }
    std::sort(okeys.begin(), okeys.end());
    std::vector<OrderedKey>::const_iterator okit = okeys.begin();
    std::vector<OrderedKey>::const_iterator okitend = okeys.end();
    while (okit != okitend) {
      strvec->push_back(okit->key);
      ++okit;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Get keys similar to a string in terms of the levenshtein distance.
   * @param origin the origin string.
   * @param range the maximum distance of keys to adopt.
   * @param utf flag to treat keys as UTF-8 strings.
   * @param strvec a string vector to contain the result.
   * @param max the maximum number to retrieve.  If it is negative, no limit is specified.
   * @return the number of retrieved keys or -1 on failure.
   */
  int64_t match_similar(const std::string& origin, size_t range, bool utf,
                        std::vector<std::string>* strvec, int64_t max = -1) {
    _assert_(strvec);
    strvec->clear();
    std::map<std::string, std::string> inmap;
    set_sig_param(inmap);
    set_db_param(inmap);
    inmap["origin"] = origin;
    kc::strprintf(&inmap["range"], "%lld", (long long)range);
    kc::strprintf(&inmap["utf"], "%d", (int)utf);
    if (max >= 0) kc::strprintf(&inmap["max"], "%lld", (long long)max);
    std::map<std::string, std::string> outmap;
    RPCClient::ReturnValue rv = rpc_.call("match_similar", &inmap, &outmap);
    if (rv != RPCClient::RVSUCCESS) {
      set_rpc_error(rv, outmap);
      return -1;
    }
    std::map<std::string, std::string>::const_iterator oit = outmap.begin();
    std::map<std::string, std::string>::const_iterator oitend = outmap.end();
    std::vector<OrderedKey> okeys;
    while (oit != oitend) {
      const char* kbuf = oit->first.data();
      size_t ksiz = oit->first.size();
      if (ksiz > 0 && *kbuf == '_') {
        std::string key(kbuf + 1, ksiz - 1);
        OrderedKey okey;
        okey.order = kc::atoi(oit->second.c_str());
        okey.key = key;
        okeys.push_back(okey);
      }
      ++oit;
    }
    std::sort(okeys.begin(), okeys.end());
    std::vector<OrderedKey>::const_iterator okit = okeys.begin();
    std::vector<OrderedKey>::const_iterator okitend = okeys.end();
    while (okit != okitend) {
      strvec->push_back(okit->key);
      ++okit;
    }
    const char* rp = strmapget(outmap, "num");
    if (!rp) {
      set_error(RPCClient::RVELOGIC, "no information");
      return -1;
    }
    return kc::atoi(rp);
  }
  /**
   * Set the target database.
   * @param expr the expression of the target database.
   */
  void set_target(const std::string& expr) {
    _assert_(true);
    dbexpr_ = expr;
  }
  /**
   * Set the signal waiting condition of the next procedure.
   * @param name the name of the condition variable.
   * @param timeout the timeout in seconds.
   * @note This setting is used only in the next procedure call and then cleared.
   */
  void set_signal_waiting(const std::string& name, double timeout = 0) {
    _assert_(true);
    sigwait_ = true;
    sigwaitname_ = name;
    sigwaittime_ = timeout;
  }
  /**
   * Set the signal sending condition of the next procedure.
   * @param name the name of the condition variable.
   * @param broadcast true to send the signal to every corresponding thread, or false to send it
   * to just one thread.
   * @note This setting is used only in the next procedure call and then cleared.
   */
  void set_signal_sending(const std::string& name, bool broadcast = false) {
    _assert_(true);
    sigsend_ = true;
    sigsendname_ = name;
    sigsendbrd_ = broadcast;
  }
  /**
   * Call a procedure of the scripting extension in the binary protocol.
   * @param name the name of the procedure to call.
   * @param params a string map containing the input parameters.
   * @param result a string map to contain the output data.  If it is NULL, it is ignored.
   * @param opts the optional features by bitwise-or: RemoteDB::BONOREPLY to ignore reply from
   * the server.
   * @return true on success, or false on failure.
   */
  bool play_script_binary(const std::string& name,
                          const std::map<std::string, std::string>& params,
                          std::map<std::string, std::string>* result = NULL, uint32_t opts = 0) {
    _assert_(true);
    if (result) result->clear();
    size_t rsiz = 1 + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + name.size();
    std::map<std::string, std::string>::const_iterator it = params.begin();
    std::map<std::string, std::string>::const_iterator itend = params.end();
    while (it != itend) {
      rsiz += sizeof(uint32_t) + sizeof(uint32_t);
      rsiz += it->first.size() + it->second.size();
      ++it;
    }
    char* rbuf = new char[rsiz];
    char* wp = rbuf;
    *(wp++) = BMPLAYSCRIPT;
    uint32_t flags = 0;
    if (opts & BONOREPLY) flags |= BONOREPLY;
    kc::writefixnum(wp, flags, sizeof(flags));
    wp += sizeof(flags);
    kc::writefixnum(wp, name.size(), sizeof(uint32_t));
    wp += sizeof(uint32_t);
    kc::writefixnum(wp, params.size(), sizeof(uint32_t));
    wp += sizeof(uint32_t);
    std::memcpy(wp, name.data(), name.size());
    wp += name.size();
    it = params.begin();
    while (it != itend) {
      kc::writefixnum(wp, it->first.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      kc::writefixnum(wp, it->second.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      std::memcpy(wp, it->first.data(), it->first.size());
      wp += it->first.size();
      std::memcpy(wp, it->second.data(), it->second.size());
      wp += it->second.size();
      ++it;
    }
    Socket* sock = rpc_.reveal_core()->reveal_core();
    char stack[RECBUFSIZ];
    bool err = false;
    if (sock->send(rbuf, rsiz)) {
      if (!(opts & BONOREPLY)) {
        char hbuf[sizeof(uint32_t)];
        int32_t c = sock->receive_byte();
        if (c == BMPLAYSCRIPT) {
          if (sock->receive(hbuf, sizeof(hbuf))) {
            uint32_t rnum = kc::readfixnum(hbuf, sizeof(uint32_t));
            for (int64_t i = 0; !err && i < rnum; i++) {
              char ubuf[sizeof(uint32_t)+sizeof(uint32_t)];
              if (sock->receive(ubuf, sizeof(ubuf))) {
                const char* rp = ubuf;
                size_t ksiz = kc::readfixnum(rp, sizeof(uint32_t));
                rp += sizeof(uint32_t);
                size_t vsiz = kc::readfixnum(rp, sizeof(uint32_t));
                rp += sizeof(uint32_t);
                if (ksiz <= DATAMAXSIZ && vsiz <= DATAMAXSIZ) {
                  size_t jsiz = ksiz + vsiz;
                  char* jbuf = jsiz > sizeof(stack) ? new char[jsiz] : stack;
                  if (sock->receive(jbuf, jsiz)) {
                    std::string key(jbuf, ksiz);
                    std::string value(jbuf + ksiz, vsiz);
                    if (result) (*result)[key] = value;
                  } else {
                    ecode_ = RPCClient::RVENETWORK;
                    emsg_ = "receive failed";
                    err = true;
                  }
                  if (jbuf != stack) delete[] jbuf;
                } else {
                  ecode_ = RPCClient::RVEINTERNAL;
                  emsg_ = "internal error";
                  err = true;
                }
              } else {
                ecode_ = RPCClient::RVENETWORK;
                emsg_ = "receive failed";
                err = true;
              }
            }
          } else {
            ecode_ = RPCClient::RVENETWORK;
            emsg_ = "receive failed";
            err = true;
          }
        } else if (c == BMERROR) {
          ecode_ = RPCClient::RVEINTERNAL;
          emsg_ = "internal error";
          err = true;
        } else {
          ecode_ = RPCClient::RVENETWORK;
          emsg_ = "receive failed";
          err = true;
        }
      }
    } else {
      ecode_ = RPCClient::RVENETWORK;
      emsg_ = "send failed";
      err = true;
    }
    delete[] rbuf;
    return !err;
  }
  /**
   * Store records at once in the binary protocol.
   * @param recs the records to store.
   * @param opts the optional features by bitwise-or: RemoteDB::BONOREPLY to ignore reply from
   * the server.
   * @return the number of stored records, or -1 on failure.
   */
  int64_t set_bulk_binary(const std::vector<BulkRecord>& recs, uint32_t opts = 0) {
    _assert_(true);
    size_t rsiz = 1 + sizeof(uint32_t) + sizeof(uint32_t);
    std::vector<BulkRecord>::const_iterator it = recs.begin();
    std::vector<BulkRecord>::const_iterator itend = recs.end();
    while (it != itend) {
      rsiz += sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(int64_t);
      rsiz += it->key.size() + it->value.size();
      ++it;
    }
    char* rbuf = new char[rsiz];
    char* wp = rbuf;
    *(wp++) = BMSETBULK;
    uint32_t flags = 0;
    if (opts & BONOREPLY) flags |= BONOREPLY;
    kc::writefixnum(wp, flags, sizeof(flags));
    wp += sizeof(flags);
    kc::writefixnum(wp, recs.size(), sizeof(uint32_t));
    wp += sizeof(uint32_t);
    it = recs.begin();
    while (it != itend) {
      kc::writefixnum(wp, it->dbidx, sizeof(uint16_t));
      wp += sizeof(uint16_t);
      kc::writefixnum(wp, it->key.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      kc::writefixnum(wp, it->value.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      kc::writefixnum(wp, it->xt, sizeof(int64_t));
      wp += sizeof(int64_t);
      std::memcpy(wp, it->key.data(), it->key.size());
      wp += it->key.size();
      std::memcpy(wp, it->value.data(), it->value.size());
      wp += it->value.size();
      ++it;
    }
    Socket* sock = rpc_.reveal_core()->reveal_core();
    int64_t rv;
    if (sock->send(rbuf, rsiz)) {
      if (opts & BONOREPLY) {
        rv = 0;
      } else {
        char hbuf[sizeof(uint32_t)];
        int32_t c = sock->receive_byte();
        if (c == BMSETBULK) {
          if (sock->receive(hbuf, sizeof(hbuf))) {
            rv = kc::readfixnum(hbuf, sizeof(uint32_t));
          } else {
            ecode_ = RPCClient::RVENETWORK;
            emsg_ = "receive failed";
            rv = -1;
          }
        } else if (c == BMERROR) {
          ecode_ = RPCClient::RVEINTERNAL;
          emsg_ = "internal error";
          rv = -1;
        } else {
          ecode_ = RPCClient::RVENETWORK;
          emsg_ = "receive failed";
          rv = -1;
        }
      }
    } else {
      ecode_ = RPCClient::RVENETWORK;
      emsg_ = "send failed";
      rv = -1;
    }
    delete[] rbuf;
    return rv;
  }
  /**
   * Store records at once in the binary protocol.
   * @param recs the records to remove.
   * @param opts the optional features by bitwise-or: RemoteDB::BONOREPLY to ignore reply from
   * the server.
   * @return the number of removed records, or -1 on failure.
   */
  int64_t remove_bulk_binary(const std::vector<BulkRecord>& recs, uint32_t opts = 0) {
    _assert_(true);
    size_t rsiz = 1 + sizeof(uint32_t) + sizeof(uint32_t);
    std::vector<BulkRecord>::const_iterator it = recs.begin();
    std::vector<BulkRecord>::const_iterator itend = recs.end();
    while (it != itend) {
      rsiz += sizeof(uint16_t) + sizeof(uint32_t);
      rsiz += it->key.size();
      ++it;
    }
    char* rbuf = new char[rsiz];
    char* wp = rbuf;
    *(wp++) = BMREMOVEBULK;
    uint32_t flags = 0;
    if (opts & BONOREPLY) flags |= BONOREPLY;
    kc::writefixnum(wp, flags, sizeof(flags));
    wp += sizeof(flags);
    kc::writefixnum(wp, recs.size(), sizeof(uint32_t));
    wp += sizeof(uint32_t);
    it = recs.begin();
    while (it != itend) {
      kc::writefixnum(wp, it->dbidx, sizeof(uint16_t));
      wp += sizeof(uint16_t);
      kc::writefixnum(wp, it->key.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      std::memcpy(wp, it->key.data(), it->key.size());
      wp += it->key.size();
      ++it;
    }
    Socket* sock = rpc_.reveal_core()->reveal_core();
    int64_t rv;
    if (sock->send(rbuf, rsiz)) {
      if (opts & BONOREPLY) {
        rv = 0;
      } else {
        char hbuf[sizeof(uint32_t)];
        int32_t c = sock->receive_byte();
        if (c == BMREMOVEBULK) {
          if (sock->receive(hbuf, sizeof(hbuf))) {
            rv = kc::readfixnum(hbuf, sizeof(uint32_t));
          } else {
            ecode_ = RPCClient::RVENETWORK;
            emsg_ = "receive failed";
            rv = -1;
          }
        } else if (c == BMERROR) {
          ecode_ = RPCClient::RVEINTERNAL;
          emsg_ = "internal error";
          rv = -1;
        } else {
          ecode_ = RPCClient::RVENETWORK;
          emsg_ = "receive failed";
          rv = -1;
        }
      }
    } else {
      ecode_ = RPCClient::RVENETWORK;
      emsg_ = "send failed";
      rv = -1;
    }
    delete[] rbuf;
    return rv;
  }
  /**
   * Retrieve records at once in the binary protocol.
   * @param recs the records to retrieve.  The value member and the xt member of each retrieved
   * record will be set appropriately.  The xt member of each missing record will be -1.
   * @return the number of retrieved records, or -1 on failure.
   */
  int64_t get_bulk_binary(std::vector<BulkRecord>* recs) {
    _assert_(recs);
    size_t rsiz = 1 + sizeof(uint32_t) + sizeof(uint32_t);
    std::vector<BulkRecord>::iterator it = recs->begin();
    std::vector<BulkRecord>::iterator itend = recs->end();
    while (it != itend) {
      rsiz += sizeof(uint16_t) + sizeof(uint32_t);
      rsiz += it->key.size();
      ++it;
    }
    char* rbuf = new char[rsiz];
    char* wp = rbuf;
    *(wp++) = BMGETBULK;
    kc::writefixnum(wp, 0, sizeof(uint32_t));
    wp += sizeof(uint32_t);
    kc::writefixnum(wp, recs->size(), sizeof(uint32_t));
    wp += sizeof(uint32_t);
    std::map<std::string, BulkRecord*> map;
    it = recs->begin();
    while (it != itend) {
      kc::writefixnum(wp, it->dbidx, sizeof(uint16_t));
      wp += sizeof(uint16_t);
      kc::writefixnum(wp, it->key.size(), sizeof(uint32_t));
      wp += sizeof(uint32_t);
      std::memcpy(wp, it->key.data(), it->key.size());
      wp += it->key.size();
      it->xt = -1;
      std::string mkey((char*)&it->dbidx, sizeof(uint16_t));
      mkey.append(it->key);
      map[mkey] = &*it;
      ++it;
    }
    Socket* sock = rpc_.reveal_core()->reveal_core();
    char stack[RECBUFSIZ];
    int64_t rv = -1;
    bool err = false;
    if (sock->send(rbuf, rsiz)) {
      char hbuf[sizeof(uint32_t)];
      int32_t c = sock->receive_byte();
      if (c == BMGETBULK) {
        if (sock->receive(hbuf, sizeof(hbuf))) {
          rv = kc::readfixnum(hbuf, sizeof(uint32_t));
          for (int64_t i = 0; !err && i < rv; i++) {
            char ubuf[sizeof(uint16_t)+sizeof(uint32_t)+sizeof(uint32_t)+sizeof(int64_t)];
            if (sock->receive(ubuf, sizeof(ubuf))) {
              const char* rp = ubuf;
              uint16_t dbidx = kc::readfixnum(rp, sizeof(uint16_t));
              rp += sizeof(uint16_t);
              size_t ksiz = kc::readfixnum(rp, sizeof(uint32_t));
              rp += sizeof(uint32_t);
              size_t vsiz = kc::readfixnum(rp, sizeof(uint32_t));
              rp += sizeof(uint32_t);
              int64_t xt = kc::readfixnum(rp, sizeof(uint64_t));
              if (ksiz <= DATAMAXSIZ && vsiz <= DATAMAXSIZ) {
                size_t jsiz = ksiz + vsiz;
                char* jbuf = jsiz > sizeof(stack) ? new char[jsiz] : stack;
                if (sock->receive(jbuf, jsiz)) {
                  std::string key(jbuf, ksiz);
                  std::string value(jbuf + ksiz, vsiz);
                  std::string mkey((char*)&dbidx, sizeof(uint16_t));
                  mkey.append(jbuf, ksiz);
                  std::map<std::string, BulkRecord*>::iterator it = map.find(mkey);
                  if (it != map.end()) {
                    BulkRecord* rec = it->second;
                    rec->value = value;
                    rec->xt = xt;
                  }
                } else {
                  ecode_ = RPCClient::RVENETWORK;
                  emsg_ = "receive failed";
                  err = true;
                }
                if (jbuf != stack) delete[] jbuf;
              } else {
                ecode_ = RPCClient::RVEINTERNAL;
                emsg_ = "internal error";
                err = true;
              }
            } else {
              ecode_ = RPCClient::RVENETWORK;
              emsg_ = "receive failed";
              err = true;
            }
          }
        } else {
          ecode_ = RPCClient::RVENETWORK;
          emsg_ = "receive failed";
          err = true;
        }
      } else if (c == BMERROR) {
        ecode_ = RPCClient::RVEINTERNAL;
        emsg_ = "internal error";
        err = true;
      } else {
        ecode_ = RPCClient::RVENETWORK;
        emsg_ = "receive failed";
        err = true;
      }
    } else {
      ecode_ = RPCClient::RVENETWORK;
      emsg_ = "send failed";
      err = true;
    }
    delete[] rbuf;
    return err ? -1 : rv;
  }
  /**
   * Get the expression of the socket.
   * @return the expression of the socket or an empty string on failure.
   */
  const std::string expression() {
    _assert_(true);
    return rpc_.expression();
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
 private:
  /**
   * Key with the order.
   */
  struct OrderedKey {
    int64_t order;
    std::string key;
    bool operator <(const OrderedKey& right) const {
      return order < right.order;
    }
  };
  /**
   * Set the parameter of signaling.
   * @param inmap the string map to contain the input parameters.
   */
  void set_sig_param(std::map<std::string, std::string>& inmap) {
    if (sigwait_) {
      inmap["WAIT"] = sigwaitname_;
      sigwait_ = false;
      sigwaitname_.clear();
      if (sigwaittime_ > 0) kc::strprintf(&inmap["WAITTIME"], "%.6f", sigwaittime_);
    }
    if (sigsend_) {
      inmap["SIGNAL"] = sigsendname_;
      if (sigsendbrd_) inmap["SIGNALBROAD"] = "";
      sigsend_ = false;
      sigsendname_.clear();
    }
  }
  /**
   * Set the parameter of the target database.
   * @param inmap the string map to contain the input parameters.
   */
  void set_db_param(std::map<std::string, std::string>& inmap) {
    _assert_(true);
    if (dbexpr_.empty()) return;
    inmap["DB"] = dbexpr_;
  }
  /**
   * Set the error status of RPC.
   * @param rv the return value by the RPC client.
   * @param message a supplement message.
   */
  void set_error(RPCClient::ReturnValue rv, const char* message) {
    _assert_(true);
    ecode_ = rv;
    emsg_ = message;
  }
  /**
   * Set the error status of RPC.
   * @param rv the return value by the RPC client.
   * @param outmap the string map to contain the output parameters.
   */
  void set_rpc_error(RPCClient::ReturnValue rv,
                     const std::map<std::string, std::string>& outmap) {
    _assert_(true);
    ecode_ = rv;
    size_t vsiz;
    const char* vbuf = strmapget(outmap, "ERROR", &vsiz);
    if (vbuf) {
      emsg_ = std::string(vbuf, vsiz);
    } else {
      emsg_ = "unexpected error";
    }
  }
  /** Dummy constructor to forbid the use. */
  RemoteDB(const RemoteDB&);
  /** Dummy Operator to forbid the use. */
  RemoteDB& operator =(const RemoteDB&);
  /** The RPC client. */
  RPCClient rpc_;
  /** The last happened error code. */
  RPCClient::ReturnValue ecode_;
  /** The last happened error messagee. */
  std::string emsg_;
  /** The target database expression. */
  std::string dbexpr_;
  /** The cursor objects. */
  CursorList curs_;
  /** The count of cursor generation. */
  int64_t curcnt_;
  /** The flag of signal waiting. */
  bool sigwait_;
  /** The name of the condition variable of signal waiting. */
  std::string sigwaitname_;
  /** The timeout of signal waiting. */
  double sigwaittime_;
  /** The flag of signal sending. */
  bool sigsend_;
  /** The name of the condition variable of signal sending. */
  std::string sigsendname_;
  /** The flag of broadcast of signal sending. */
  bool sigsendbrd_;
};


/**
 * Replication client.
 */
class ReplicationClient {
 public:
  /**
   * Opening options.
   */
  enum Option {
    WHITESID = 1 << 0                    ///< fetch messages of the specified SID only
  };
  /**
   * Default constructor.
   */
  explicit ReplicationClient() : sock_(), alive_(false) {
    _assert_(true);
  }
  /**
   * Open the connection.
   * @param host the name or the address of the server.  If it is an empty string, the local host
   * is specified.
   * @param port the port number of the server.
   * @param timeout the timeout of each operation in seconds.  If it is not more than 0, no
   * timeout is specified.
   * @param ts the maximum time stamp of already read logs.
   * @param sid the server ID number.
   * @param opts the optional features by bitwise-or: ReplicationClient::WHITESID to fetch
   * messages whose server ID number is the specified one only.
   * @return true on success, or false on failure.
   */
  bool open(const std::string& host = "", int32_t port = DEFPORT, double timeout = -1,
            uint64_t ts = 0, uint16_t sid = 0, uint32_t opts = 0) {
    _assert_(true);
    const std::string& thost = host.empty() ? "localhost" : host;
    const std::string& addr = Socket::get_host_address(thost);
    if (addr.empty() || port < 1) return false;
    std::string expr;
    kc::strprintf(&expr, "%s:%d", addr.c_str(), port);
    if (timeout > 0) sock_.set_timeout(timeout);
    if (!sock_.open(expr)) return false;
    uint32_t flags = 0;
    if (opts & WHITESID) flags |= WHITESID;
    char tbuf[1+sizeof(flags)+sizeof(ts)+sizeof(sid)];
    char* wp = tbuf;
    *(wp++) = RemoteDB::BMREPLICATION;
    kc::writefixnum(wp, flags, sizeof(flags));
    wp += sizeof(flags);
    kc::writefixnum(wp, ts, sizeof(ts));
    wp += sizeof(ts);
    kc::writefixnum(wp, sid, sizeof(sid));
    wp += sizeof(sid);
    if (!sock_.send(tbuf, sizeof(tbuf)) || sock_.receive_byte() != RemoteDB::BMREPLICATION) {
      sock_.close();
      return false;
    }
    alive_ = true;
    return true;
  }
  /**
   * Close the connection.
   * @return true on success, or false on failure.
   */
  bool close() {
    _assert_(true);
    return sock_.close(false);
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
    *sp = 0;
    *tsp = 0;
    int32_t magic = sock_.receive_byte();
    if (magic == RemoteDB::BMREPLICATION) {
      char hbuf[sizeof(uint64_t)+sizeof(uint32_t)];
      if (!sock_.receive(hbuf, sizeof(hbuf))) {
        alive_ = false;
        return NULL;
      }
      const char* rp = hbuf;
      uint64_t ts = kc::readfixnum(rp, sizeof(uint64_t));
      rp += sizeof(uint64_t);
      size_t msiz = kc::readfixnum(rp, sizeof(uint32_t));
      rp += sizeof(uint32_t);
      char* mbuf = new char[msiz];
      if (!sock_.receive(mbuf, msiz)) {
        delete[] mbuf;
        alive_ = false;
        return NULL;
      }
      *sp = msiz;
      *tsp = ts;
      return mbuf;
    } else if (magic == RemoteDB::BMNOP) {
      char hbuf[sizeof(uint64_t)];
      if (!sock_.receive(hbuf, sizeof(hbuf))) {
        alive_ = false;
        return NULL;
      }
      *tsp = kc::readfixnum(hbuf, sizeof(uint64_t));
      char c = RemoteDB::BMREPLICATION;
      sock_.send(&c, 1);
    } else {
      alive_ = false;
    }
    return NULL;
  }
  /**
   * Check whether the connection is alive.
   * @return true if alive, false if not.
   */
  bool alive() {
    _assert_(true);
    return alive_;
  }
 private:
  /** The client socket. */
  Socket sock_;
  /** The alive flag. */
  bool alive_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
