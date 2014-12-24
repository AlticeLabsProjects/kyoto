/*************************************************************************************************
 * Interface of pluggable server abstraction
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


#ifndef _KTPLUGSERV_H                    // duplication check
#define _KTPLUGSERV_H

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

namespace kyototycoon {                  // common namespace


/**
 * Interface of pluggable server abstraction.
 */
class PluggableServer {
 public:
  /**
   * Destructor.
   */
  virtual ~PluggableServer() {
    _assert_(true);
  }
  /**
   * Configure server settings.
   * @param dbary an array of the database objects.
   * @param dbnum the number of the database objects.
   * @param logger the logger object.
   * @param logkinds kinds of logged messages by bitwise-or: Logger::DEBUG for debugging,
   * Logger::INFO for normal information, Logger::SYSTEM for system information, and
   * Logger::ERROR for fatal error.
   * @param expr an expression given in the command line.
   */
  virtual void configure(TimedDB* dbary, size_t dbnum,
                         ThreadedServer::Logger* logger, uint32_t logkinds,
                         const char* expr) = 0;
  /**
   * Start the service.
   * @return true on success, or false on failure.
   */
  virtual bool start() = 0;
  /**
   * Stop the service.
   * @return true on success, or false on failure.
   */
  virtual bool stop() = 0;
  /**
   * Finish the service.
   * @return true on success, or false on failure.
   */
  virtual bool finish() = 0;
};


/**
 * The name of the initializer function.
 */
const char* const KTSERVINITNAME = "ktservinit";


extern "C" {
/**
 * Initializer of a server implementation.
 * @note Each shared library of a pluggable server module must implement a function whose name is
 * "ktservinit" and return a new instance of a derived class of the PluggableServer class.  The
 * instance will be deleted implicitly by the caller.
 */
typedef PluggableServer* (*KTSERVINIT)();
}


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
