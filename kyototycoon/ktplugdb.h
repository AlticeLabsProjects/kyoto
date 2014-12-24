/*************************************************************************************************
 * Interface of pluggable database abstraction
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


#ifndef _KTPLUGDB_H                      // duplication check
#define _KTPLUGDB_H

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
 * Interface of pluggable database abstraction.
 */
class PluggableDB : public kc::BasicDB {
 public:
  /**
   * Destructor.
   */
  virtual ~PluggableDB() {
    _assert_(true);
  }
};


/**
 * The name of the initializer function.
 */
const char* const KTDBINITNAME = "ktdbinit";


extern "C" {
/**
 * Initializer of a database implementation.
 * @note Each shared library of a pluggable database module must implement a function whose name
 * is "ktdbinit" and return a new instance of a derived class of the PluggableDB class.  The
 * instance will be deleted implicitly by the caller.
 */
typedef PluggableDB* (*KTDBINIT)();
}


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
