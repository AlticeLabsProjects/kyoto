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


#include "ktshlib.h"
#include "myconf.h"

namespace kyototycoon {                  // common namespace


/**
 * SharedLibrary internal.
 */
struct SharedLibraryCore {
  void* lib;                             ///< library handle
};


/**
 * Default constructor.
 */
SharedLibrary::SharedLibrary() {
  _assert_(true);
  SharedLibraryCore* core = new SharedLibraryCore;
  core->lib = NULL;
  opq_ = core;
}


/**
 * Destructor
 */
SharedLibrary::~SharedLibrary() {
  _assert_(true);
  SharedLibraryCore* core = (SharedLibraryCore*)opq_;
  if (core->lib) close();
  delete core;
}


/**
 * Open a shared library.
 */
bool SharedLibrary::open(const char* path) {
  _assert_(path);
  SharedLibraryCore* core = (SharedLibraryCore*)opq_;
  if (core->lib) return false;
  void* lib = ::dlopen(path, RTLD_LAZY);
  if (!lib) return false;
  core->lib = lib;
  return true;
}


/**
 * Close the shared library.
 */
bool SharedLibrary::close() {
  _assert_(true);
  SharedLibraryCore* core = (SharedLibraryCore*)opq_;
  if (!core->lib) return false;
  bool err = false;
  if (::dlclose(core->lib) != 0) err = true;
  core->lib = NULL;
  return !err;
}


/**
 * Get the pointer to a symbol.
 */
void* SharedLibrary::symbol(const char* name) {
  _assert_(name);
  SharedLibraryCore* core = (SharedLibraryCore*)opq_;
  if (!core->lib) return NULL;
  return ::dlsym(core->lib, name);
}


}                                        // common namespace

// END OF FILE
