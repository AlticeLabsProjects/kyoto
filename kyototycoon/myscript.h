/*************************************************************************************************
 * The scripting extension
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


#ifndef _MYSCRIPT_H                      // duplication check
#define _MYSCRIPT_H

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
#include <ktremotedb.h>

namespace kc = kyotocabinet;
namespace kt = kyototycoon;


/**
 * Script processor.
 */
class ScriptProcessor {
 public:
  /**
   * Default constructor.
   */
  explicit ScriptProcessor() ;
  /**
   * Destructor.
   */
  ~ScriptProcessor();
  /**
   * Set domain-specific resources.
   * @param thid the ID number of the worker thread.
   * @param serv the server object.
   * @param dbs an array of database objects.
   * @param dbnum the number of elements of the array.
   * @param dbmap the map binding database names to their indices.
   * @return the return value of the procedure.
   */
  bool set_resources(int32_t thid, kt::RPCServer* serv, kt::TimedDB* dbs, int32_t dbnum,
                     const std::map<std::string, int32_t>* dbmap);
  /**
   * Load a script file.
   * @param path the path of the sciprt file.
   * @return true on success, or false on failure.
   */
  bool load(const std::string& path);
  /**
   * Clear the internal state.
   */
  void clear();
  /**
   * Call a procedure.
   * @param name the name of the procecude.
   * @param inmap a string map which contains the input of the procedure.
   * @param outmap a string map to contain the input parameters.
   * @return the return value of the procedure.
   */
  kt::RPCClient::ReturnValue call(const std::string& name,
                                  const std::map<std::string, std::string>& inmap,
                                  std::map<std::string, std::string>& outmap);
 private:
  /** Dummy constructor to forbid the use. */
  ScriptProcessor(const ScriptProcessor&);
  /** Dummy Operator to forbid the use. */
  ScriptProcessor& operator =(const ScriptProcessor&);
  /** Opaque pointer. */
  void* opq_;
};


#endif                                   // duplication check


// END OF FILE
