/*************************************************************************************************
 * Shared library
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


#ifndef _KTSHLIB_H                       // duplication check
#define _KTSHLIB_H

#include <ktcommon.h>
#include <ktutil.h>

namespace kyototycoon {                  // common namespace


/**
 * Shared library.
 */
class SharedLibrary {
 public:
  /**
   * Default constructor.
   */
  SharedLibrary();
  /**
   * Destructor
   */
  ~SharedLibrary();
  /**
   * Open a shared library.
   * @param path the path of the shared library file.
   * @return true on success, or false on failure.
   */
  bool open(const char* path);
  /**
   * Close the shared library.
   * @return true on success, or false on failure.
   */
  bool close();
  /**
   * Get the pointer to a symbol.
   * @param name the name of the symbol.
   * @return the pointer to the symbol, or NULL on failure.
   */
  void* symbol(const char* name);
 private:
  /** Opaque pointer. */
  void* opq_;
};


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
