---------------------------------------------------------------------------------------------------
-- Scripting extension of Kyoto Tycoon
--                                                                Copyright (C) 2009-2012 FAL Labs
-- This file is part of Kyoto Tycoon.
-- This program is free software: you can redistribute it and/or modify it under the terms of
-- the GNU General Public License as published by the Free Software Foundation, either version
-- 3 of the License, or any later version.
-- This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
-- without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
-- See the GNU General Public License for more details.
-- You should have received a copy of the GNU General Public License along with this program.
-- If not, see <http://www.gnu.org/licenses/>.
---------------------------------------------------------------------------------------------------


---
-- The scripting extension of Kyoto Tycoon.
--
module("kyototycoon", package.seeall)


---
-- Running environment.
-- @class table
-- @name __kyototycoon__
-- @field RVSUCCESS status code: success
-- @field RVENOIMPL status code: not implemented
-- @field RVEINVALID status code: invalid operation
-- @field RVELOGIC status code: logical inconsistency
-- @field RVEINTERNAL status code: internal error
-- @field VERSION the version information
-- @field thid the ID number of the worker thread
-- @field db the primary database object
-- @field dbs an array of database objects
--
__kyototycoon__ = {
   RVSUCCESS = 0,
   RVENOIMPL = 1,
   RVEINVALID = 2,
   RVELOGIC = 3,
   RVEINTERNAL = 4,
   VERSION = "x.y.z",
   thid = 0,
   db = {},
   dbs = {},
}


--- Log a message
-- @param kind the kind of the event.  "debug" for debugging, "info" for normal information, "system" for system information, and "error" for fatal error.
-- @param message the supplement message.
-- @return always nil.
function log(kind, message) end


--- Convert a string to an integer.
-- @param str the string.
-- @return the integer.  If the string does not contain numeric expression, 0 is returned.
function atoi(str) end


--- Convert a string with a metric prefix to an integer.
-- @param str the string, which can be trailed by a binary metric prefix.  "K", "M", "G", "T", "P", and "E" are supported.  They are case-insensitive.
-- @return the integer.  If the string does not contain numeric expression, 0 is returned.  If the integer overflows the domain, INT64_MAX or INT64_MIN is returned according to the sign.
function atoix(str) end


--- Convert a string to a real number.
-- @param str the string.
-- @return the real number.  If the string does not contain numeric expression, 0.0 is returned.
function atof(str) end


--- Get the hash value of a string by MurMur hashing.
-- @param str the string.
-- @return the hash value.
function hash_murmur(str) end


--- Get the hash value of a string by FNV hashing.
-- @param str the string.
-- @return the hash value.
function hash_fnv(str) end


--- Calculate the levenshtein distance of two strings.
-- @param a one string.
-- @param b the other string.
-- @param utf flag to treat keys as UTF-8 strings.  If it is omitted, false is specified.
-- @return the levenshtein distance.
function levdist(a, b, utf) end


--- Get the time of day in seconds.
-- @return the time of day in seconds.  The accuracy is in microseconds.
function time() end


--- Suspend execution of the current thread.
-- @param sec the interval of the suspension in seconds.  If it is omitted, the processor is yielded from the current thread.
function sleep(sec) end


--- Serialize an array of numbers into a string.
-- @param format the format string.  It should be composed of conversion characters.  "c" for int8_t, "C" for uint8_t, "s" for int16_t, "S" for uint16_t, "i" for int32_t, "I" for uint32_t, "l" for int64_t, "L" for uint64_t, "f" for float, "d" for double, "n" for uint16_t in network byte order, "N" for uint32_t in network byte order, "M" for uint64_t in network byte order, and "w" for BER encoding.  They can be trailed by a numeric expression standing for the iteration count or by "*" for the rest all iteration.
-- @param ary the array of numbers.  It can be trailed optional arguments, which are treated as additional elements of the array.
-- @return the serialized string.
function pack(format, ary, ...) end


--- Deserialize a binary string into an array of numbers.
-- @param format the format string.  It should be composed of conversion characters as with the pack function.
-- @param str the binary string.
-- @return the deserialized array.
function unpack(format, str) end


--- Split a string into substrings.
-- @param str the string
-- @param delims a string including separator characters.  If it is omitted, the zero code is specified.
-- @return an array of substrings.
function split(str, delims) end


--- Encode or decode a string.
-- @param mode the encoding method; "url" for URL encoding, "~url" for URL decoding, "base" for Base64 encoding, "~base" for Base64 decoding, "hex" for hexadecimal encoding, "~hex" for hexadecimal decoding, "zlib" for ZLIB raw compressing, "~zlib" for ZLIB raw decompressing, "deflate" for ZLIB deflate compressing, "~deflate" for ZLIB deflate decompressing, "gzip" for ZLIB gzip compressing, "~gzip" for ZLIB gzip decompressing.
-- @param str the string.
-- @return the result string.
function codec(mode, str) end


--- Perform bit operation of an integer.
-- @param mode the operator; "and" for bitwise-and operation, "or" for bitwise-or operation, "xor" for bitwise-xor operation, "not" for bitwise-not operation, "left" for left shift operation, "right" for right shift operation.
-- @param num the integer, which is treated as an unsigned 32-bit integer.
-- @param aux the auxiliary operand for some operators.
-- @return the result value.
function bit(mode, num, aux) end


--- Perform substring matching or replacement without evaluating any meta character.
-- @param str the source string.
-- @param pattern the matching pattern.
-- @param alt the alternative string corresponding for the pattern.  If it is omitted, matching check is performed.
-- @return If the alternative string is specified, the converted string is returned.  If the alternative string is not specified, the index of the substring matching the given pattern or 0 is returned.
function strstr(str, pattern, alt) end


--- Perform forward matching without evaluating any meta character.
-- @param str the source string.
-- @param pattern the matching pattern.
-- @return true if they matches, or false if not.
function strfwm(str, pattern) end


--- Perform backward matching without evaluating any meta character.
-- @param str the source string.
-- @param pattern the matching pattern.
-- @return true if they matches, or false if not.
function strbwm(str, pattern) end


--- Perform pattern matching or replacement with regular expressions.
-- @param str the source string.
-- @param pattern the pattern of regular expressions.
-- @param alt the alternative string corresponding for the pattern.  If it is not defined, matching check is performed.
-- @return If the alternative string is specified, the converted string is returned.  If the alternative string is not specified, the boolean value of whether the source string matches the pattern is returned.
function regex(str, pattern, alt) end


--- Serialize an array into a string.
-- @param src the source table.
-- @return the result string.
function arraydump(src) end


--- Deserialize a string into an array.
-- @param src the source string.
-- @return the result map.
function arrayload(src) end


--- Serialize a map into a string.
-- @param src the source table.
-- @return the result string.
function mapdump(src) end


--- Deserialize a string into a map.
-- @param src the source string.
-- @return the result map.
function mapload(src) end


---
-- Error data.
-- @class table
-- @name Error
-- @field SUCCESS error code: success
-- @field NOIMPL error code: not implemented
-- @field INVALID error code: invalid operation
-- @field NOREPOS error code: no repository
-- @field NOPERM error code: no permission
-- @field BROKEN error code: broken file
-- @field DUPREC error code: record duplication
-- @field NOREC error code: no record
-- @field LOGIC error code: logical inconsistency
-- @field SYSTEM error code: system error
-- @field MISC error code: miscellaneous error
--
Error = {
   SUCCESS = 0,
   NOIMPL = 1,
   INVALID = 2,
   NOREPOS = 3,
   NOPERM = 4,
   BROKEN = 5,
   DUPREC = 6,
   NOREC = 7,
   LOGIC = 8,
   SYSTEM = 9,
   MISC = 15,
}
error = {}


--- Create an error object.
-- @param code the error code.
-- @param message the supplement message.
-- @return the error object.
function Error:new(code, message) end


--- Set the error information.
-- @param code the error code.
-- @param message the supplement message.
-- @return always nil.
function error:set(code, message) end


--- Get the error code.
-- @return the error code.
function error:code() end


--- Get the readable string of the code.
-- @return the readable string of the code.
function error:name() end


--- Get the supplement message.
-- @return the supplement message.
function error:message() end


--- Get the string expression.
-- @return the string expression.
function error:__tostring() end


---
-- Interface to access a record.
-- @class table
-- @name Visitor
-- @field NOP magic data: no operation
-- @field REMOVE magic data: remove the record
--
Visitor = {
   NOP = "(magic data)",
   REMOVE = "(magic data)",
}
visitor = {}


--- Create a visitor object.
-- @return the visitor object.
function Visitor:new() end


--- Visit a record.
-- @param key the key.
-- @param value the value.
-- @param xt the absolute expiration time.
-- @return If it is a string, the value is replaced by the content.  If it is Visitor.NOP, nothing is modified.  If it is Visitor.REMOVE, the record is removed.  The expiration time can be also returned as the second value.
function visitor:visit_full(key, value, xt) end


--- Visit a empty record space.
-- @param key the key.
-- @return If it is a string, the value is replaced by the content.  If it is Visitor.NOP or Visitor.REMOVE, nothing is modified.  The expiration time can be also returned as the second value.
function visitor:visit_empty(key) end


--- Preprocess the main operations.
function visitor:visit_before() end


--- Postprocess the main operations.
function visitor:visit_after() end


---
-- Interface to process the database file.
-- @class table
-- @name FileProcessor
--
FileProcessor = {}
fileprocessor = {}


--- Create a file processor object.
-- @return the file processor object.
function FileProcessor:new() end


--- Process the database file.
-- @param path the path of the database file.
-- @param count the number of records.
-- @param size the size of the available region.
-- @return true on success, or false on failure.
function fileprocessor:process(path, count, size) end


---
-- Interface of cursor to indicate a record.
-- @class table
-- @name Cursor
Cursor = {}
cursor = {}


--- Get the string expression.
-- @return the string expression.
function cursor:__tostring() end


--- Get a pair of the key and the value of the current record.
-- @param db ignored.  This is just for compatibility with the "next" function for the generic "for" loop.
-- @return a pair of the key and the value of the current record, or nil on failure.
-- @usage If the cursor is invalidated, nil is returned.
function cursor:__call(db) end


--- Disable the cursor.
-- @return always nil.
-- @usage This method should be called explicitly when the cursor is no longer in use.
function cursor:disable() end


--- Accept a visitor to the current record.
-- @param visitor a visitor object which implements the Visitor interface, or a function object which receives the key and the value.
-- @param writable true for writable operation, or false for read-only operation.  If it is omitted, true is specified.
-- @param step true to move the cursor to the next record, or false for no move.  If it is omitted, false is specified.
-- @return true on success, or false on failure.
-- @usage The operation for each record is performed atomically and other threads accessing the same record are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function cursor:accept(visitor, writable, step) end


--- Set the value of the current record.
-- @param value the value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @param step true to move the cursor to the next record, or false for no move.  If it is omitted, false is specified.
-- @return true on success, or false on failure.
function cursor:set_value(value, xt, step) end


--- Remove the current record.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, false is returned.  The cursor is moved to the next record implicitly.
function cursor:remove() end


--- Get the key of the current record.
-- @param step true to move the cursor to the next record, or false for no move.  If it is omitted, false is specified.
-- @return the key of the current record, or nil on failure.
-- @usage If the cursor is invalidated, nil is returned.
function cursor:get_key(step) end


--- Get the value of the current record.
-- @param step true to move the cursor to the next record, or false for no move.  If it is omitted, false is specified.
-- @return the value of the current record, or nil on failure.
-- @usage If the cursor is invalidated, nil is returned.
function cursor:get_value(step) end


--- Get a pair of the key and the value of the current record.
-- @param step true to move the cursor to the next record, or false for no move.  If it is omitted, false is specified.
-- @return a trio of the key and the value and the expiration time of the current record, or nil on failure.
-- @usage If the cursor is invalidated, nil is returned.
function cursor:get(step) end


--- Get a pair of the key and the value of the current record and remove it atomically.
-- @return a trio of the key and the value and the expiration time of the current record, or nil on failure.
-- @usage If the cursor is invalidated, nil is returned.  The cursor is moved to the next record implicitly.
function cursor:seize() end


--- Jump the cursor to a record for forward scan.
-- @param key the key of the destination record.  if it is omitted, the destination is the first record.
-- @return true on success, or false on failure.
function cursor:jump(key) end


--- Jump the cursor to a record for backward scan.
-- @param key the key of the destination record.  if it is omitted, the destination is the last record.
-- @return true on success, or false on failure.
-- @usage This method is dedicated to tree databases.  Some database types, especially hash databases, will provide a dummy implementation.
function cursor:jump_back(key) end


--- Step the cursor to the next record.
-- @return true on success, or false on failure.
function cursor:step() end


--- Step the cursor to the previous record.
-- @return true on success, or false on failure.
-- @usage This method is dedicated to tree databases.  Some database types, especially hash databases, will provide a dummy implementation.
function cursor:step_back() end


--- Get the database object.
-- @return the database object.
function cursor:db() end


--- Get the last happened error.
-- @return the last happened error.
function cursor:error() end


---
-- Interface of database abstraction.
-- @class table
-- @name DB
-- @field OREADER open mode: open as a reader
-- @field OWRITER open mode: open as a writer
-- @field OCREATE open mode: writer creating
-- @field OTRUNCATE open mode: writer truncating
-- @field OAUTOTRAN open mode: auto transaction
-- @field OAUTOSYNC open mode: auto synchronization
-- @field ONOLOCK open mode: open without locking
-- @field OTRYLOCK open mode: lock without blocking
-- @field ONOREPAIR open mode: open without auto repair
-- @field MSET merge mode: overwrite the existing value
-- @field MADD merge mode: keep the existing value
-- @field MREPLACE merge mode: modify the existing record only
-- @field MAPPEND merge mode: append the new value
-- @field XNOLOCK mapreduce option: avoid locking against update operations
-- @field XNOCOMP mapreduce option: avoid compression of temporary databases
--
DB = {
   OREADER = 1,
   OWRITER = 2,
   OCREATE = 4,
   OTRUNCATE = 8,
   OAUTOTRAN = 16,
   OAUTOSYNC = 32,
   ONOLOCK = 64,
   OTRYLOCK = 128,
   ONOREPAIR = 256,
   MSET = 0,
   MADD = 1,
   MREPLACE = 2,
   MAPPEND = 3,
   XNOLOCK = 1,
   XNOCOMP = 256,
}
db = {}


--- Create a database object.
-- @param ptr a light user data of the pointer to a database object to use internally.  If it is omitted, the internal database object is created and destroyed implicitly.
-- @return the database object.
function DB:new(ptr) end


--- Create a database object.
-- @return a light user data of the pointer to the created database object.  It should be released with the delete_ptr method when it is no longer in use.
function DB:new_ptr() end


--- Delete a database object.
-- @param ptr a light user data of the pointer to the database object.
-- @return always nil.
function DB:delete_ptr() end


--- Process a database by a functor.
-- @param proc the functor to process the database, whose object is passd as the parameter.
-- @param path the same to the one of the open method.
-- @param mode the same to the one of the open method.
-- @return nil on success, or an error object on failure.
function DB:process(proc, path, mode) end


--- Get the string expression.
-- @return the string expression.
function db:__tostring() end


--- Retrieve the value of a record.
-- @param key the key.
-- @return the value of the corresponding record, or nil on failure.
function db:__index(key) end


--- Set the value of a record.
-- @param key the key.
-- @param value the value.  If it is nil, the record is removed.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, a new record is created.  If the corresponding record exists, the value is overwritten.
function db:__newindex(key, value) end


--- Open a database file.
-- @param path the path of a database file.  If it is "-", the database will be a prototype hash database.  If it is "+", the database will be a prototype tree database.  If it is ":", the database will be a stash database.  If it is "*", the database will be a cache hash database.  If it is "%", the database will be a cache tree database.  If its suffix is ".kch", the database will be a file hash database.  If its suffix is ".kct", the database will be a file tree database.  If its suffix is ".kcd", the database will be a directory hash database.  If its suffix is ".kcf", the database will be a directory tree database.  If its suffix is ".kcx", the database will be a plain text database.  Otherwise, this function fails.  Tuning parameters can trail the name, separated by "#".  Each parameter is composed of the name and the value, separated by "=".  If the "type" parameter is specified, the database type is determined by the value in "-", "+", ":", "*", "%", "kch", "kct", "kcd", kcf", and "kcx".  All database types support the logging parameters of "log", "logkinds", and "logpx".  The prototype hash database and the prototype tree database do not support any other tuning parameter.  The stash database supports "bnum".  The cache hash database supports "opts", "bnum", "zcomp", "capcnt", "capsiz", and "zkey".  The cache tree database supports all parameters of the cache hash database except for capacity limitation, and supports "psiz", "rcomp", "pccap" in addition.  The file hash database supports "apow", "fpow", "opts", "bnum", "msiz", "dfunit", "zcomp", and "zkey".  The file tree database supports all parameters of the file hash database and "psiz", "rcomp", "pccap" in addition.  The directory hash database supports "opts", "zcomp", and "zkey".  The directory tree database supports all parameters of the directory hash database and "psiz", "rcomp", "pccap" in addition.  The plain text database does not support any other tuning parameter.
-- @param mode the connection mode.  DB.OWRITER as a writer, DB.OREADER as a reader.  The following may be added to the writer mode by bitwise-or: DB.OCREATE, which means it creates a new database if the file does not exist, DB.OTRUNCATE, which means it creates a new database regardless if the file exists, DB.OAUTOTRAN, which means each updating operation is performed in implicit transaction, DB.OAUTOSYNC, which means each updating operation is followed by implicit synchronization with the file system.  The following may be added to both of the reader mode and the writer mode by bitwise-or: DB.ONOLOCK, which means it opens the database file without file locking, DB.OTRYLOCK, which means locking is performed without blocking, DB.ONOREPAIR, which means the database file is not repaired implicitly even if file destruction is detected.  If it is omitted, DB.OWRITER + DB.OCREATE is specified.
-- @return true on success, or false on failure.
-- @usage The tuning parameter "log" is for the original "tune_logger" and the value specifies the path of the log file, or "-" for the standard output, or "+" for the standard error.  "logkinds" specifies kinds of logged messages and the value can be "debug", "info", "warn", or "error".  "logpx" specifies the prefix of each log message.  "opts" is for "tune_options" and the value can contain "s" for the small option, "l" for the linear option, and "c" for the compress option.  "bnum" corresponds to "tune_bucket".  "zcomp" is for "tune_compressor" and the value can be "zlib" for the ZLIB raw compressor, "def" for the ZLIB deflate compressor, "gz" for the ZLIB gzip compressor, "lzo" for the LZO compressor, "lzma" for the LZMA compressor, or "arc" for the Arcfour cipher.  "zkey" specifies the cipher key of the compressor.  "capcnt" is for "cap_count".  "capsiz" is for "cap_size".  "psiz" is for "tune_page".  "rcomp" is for "tune_comparator" and the value can be "lex" for the lexical comparator or "dec" for the decimal comparator.  "pccap" is for "tune_page_cache".  "apow" is for "tune_alignment".  "fpow" is for "tune_fbp".  "msiz" is for "tune_map".  "dfunit" is for "tune_defrag".  Every opened database must be closed by the DB::close method when it is no longer in use.  It is not allowed for two or more database objects in the same process to keep their connections to the same database file at the same time.
function db:open(path, mode) end


--- Close the database file.
-- @return true on success, or false on failure.
function db:close() end


--- Accept a visitor to a record.
-- @param key the key.
-- @param visitor a visitor object which implements the Visitor interface, or a function object which receives the key and the value.
-- @param writable true for writable operation, or false for read-only operation.  If it is omitted, true is specified.
-- @return true on success, or false on failure.
-- @usage The operation for each record is performed atomically and other threads accessing the same record are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function db:accept(key, visitor, writable) end


--- Accept a visitor to multiple records at once.
-- @param keys specifies an array of the keys.
-- @param visitor a visitor object which implements the Visitor interface, or a function object which receives the key and the value.
-- @param writable true for writable operation, or false for read-only operation.  If it is omitted, true is specified.
-- @return true on success, or false on failure.
-- @usage The operations for specified records are performed atomically and other threads accessing the same records are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function db:accept_bulk(keys, visitor, writable) end


--- Iterate to accept a visitor for each record.
-- @param visitor a visitor object which implements the Visitor interface, or a function object which receives the key and the value.
-- @param writable true for writable operation, or false for read-only operation.  If it is omitted, true is specified.
-- @return true on success, or false on failure.
-- @usage The whole iteration is performed atomically and other threads are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function db:iterate(visitor, writable) end


--- Set the value of a record.
-- @param key the key.
-- @param value the value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, a new record is created.  If the corresponding record exists, the value is overwritten.
function db:set(key, value, xt) end


--- Add a record.
-- @param key the key.
-- @param value the value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, a new record is created.  If the corresponding record exists, the record is not modified and false is returned.
function db:add(key, value, xt) end


--- Replace the value of a record.
-- @param key the key.
-- @param value the value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, no new record is created and false is returned.  If the corresponding record exists, the value is modified.
function db:replace(key, value, xt) end


--- Append the value of a record.
-- @param key the key.
-- @param value the value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, a new record is created.  If the corresponding record exists, the given value is appended at the end of the existing value.
function db:append(key, value, xt) end


--- Add a number to the numeric integer value of a record.
-- @param key the key.
-- @param num the additional number.  if it is omitted, 0 is specified.
-- @param orig the origin number if no record corresponds to the key.  If it is omitted, 0 is specified.  If it is negative infinity and no record corresponds, this method fails.  If it is positive infinity, the value is set as the additional number regardless of the current value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return the result value, or nil on failure.
-- @usage The value is serialized as an 8-byte binary integer in big-endian order, not a decimal string.  If existing value is not 8-byte, this method fails.
function db:increment(key, num, orig, xt) end


--- Add a number to the numeric double value of a record.
-- @param key the key.
-- @param num the additional number.  if it is omitted, 0 is specified.
-- @param orig the origin number if no record corresponds to the key.  If it is omitted, 0 is specified.  If it is negative infinity and no record corresponds, this method fails.  If it is positive infinity, the value is set as the additional number regardless of the current value.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return the result value, or nil on failure.
-- @usage The value is serialized as an 16-byte binary fixed-point number in big-endian order, not a decimal string.  If existing value is not 16-byte, this method fails.
function db:increment_double(key, num, orig, xt) end


--- Perform compare-and-swap.
-- @param key the key.
-- @param oval the old value.  nil means that no record corresponds.
-- @param nval the new value.  nil means that the record is removed.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @return true on success, or false on failure.
function db:cas(key, oval, nval, xt) end


--- Remove a record.
-- @param key the key.
-- @return true on success, or false on failure.
-- @usage If no record corresponds to the key, false is returned.
function db:remove(key) end


--- Retrieve the value of a record.
-- @param key the key.
-- @return the value of the corresponding record, or nil on failure.  The absolute expiration time is also returned as the second value.
function db:get(key) end


--- Check the existence of a record.
-- @param key the key.
-- @return the size of the value, or -1 on failure.
function db:check(key) end


--- Retrieve the value of a record and remove it atomically.
-- @param key the key.
-- @return the value of the corresponding record, or nil on failure.  The absolute expiration time is also returned as the second value.
function db:seize(key) end


--- Store records at once.
-- @param recs a table of the records to store.
-- @param xt the expiration time from now in seconds.  If it is negative, the absolute value is treated as the epoch time.  If it is omitted, no expiration time is specified.
-- @param atomic true to perform all operations atomically, or false for non-atomic operations.  If it is omitted, true is specified.
-- @return the number of stored records, or -1 on failure.
function db:set_bulk(recs, atomic) end


--- Remove records at once.
-- @param keys an array of the keys of the records to remove.
-- @param atomic true to perform all operations atomically, or false for non-atomic operations.  If it is omitted, true is specified.
-- @return the number of removed records, or -1 on failure.
function db:remove_bulk(keys, atomic) end


--- Retrieve records at once.
-- @param keys an array of the keys of the records to retrieve.
-- @param atomic true to perform all operations atomically, or false for non-atomic operations.  If it is omitted, true is specified.
-- @return a table of retrieved records, or nil on failure.
function db:get_bulk(keys, atomic) end


--- Remove all records.
-- @return true on success, or false on failure.
function db:clear() end


--- Synchronize updated contents with the file and the device.
-- @param hard true for physical synchronization with the device, or false for logical synchronization with the file system.  If it is omitted, false is specified.
-- @param proc a postprocessor object which implements the FileProcessor interface, or a function object which receives the same parameters.  If it is omitted or nil, no postprocessing is performed.
-- @return true on success, or false on failure.
-- @usage The operation of the processor is performed atomically and other threads accessing the same record are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function db:synchronize(hard, proc) end


--- Occupy database by locking and do something meanwhile.
-- @param writable true to use writer lock, or false to use reader lock.  If it is omitted, false is specified.
-- @param proc a processor object which implements the FileProcessor interface, or a function object which receives the same parameters.  If it is omitted or nil, no processing is performed.
-- @return true on success, or false on failure.
-- @usage The operation of the processor is performed atomically and other threads accessing the same record are blocked.  To avoid deadlock, any explicit database operation must not be performed in this method.
function db:occupy(writable, proc) end


--- Create a copy of the database file.
-- @param dest the path of the destination file.
-- @return true on success, or false on failure.
function db:copy(dest) end


--- Begin transaction.
-- @param hard true for physical synchronization with the device, or false for logical synchronization with the file system.  If it is omitted, false is specified.
-- @return true on success, or false on failure.
function db:begin_transaction(hard) end


--- End transaction.
-- @param commit true to commit the transaction, or false to abort the transaction.  If it is omitted, true is specified.
-- @return true on success, or false on failure.
function db:end_transaction(commit) end


--- Perform entire transaction by a functor.
-- @param proc the functor of operations during transaction.  If the function returns true, the transaction is committed.  If the function returns false or an exception is thrown, the transaction is aborted.
-- @param hard true for physical synchronization with the device, or false for logical synchronization with the file system.  If it is omitted, false is specified.
-- @return true on success, or false on failure.
function db:transaction(proc, hard) end


--- Dump records into a snapshot file.
-- @param dest the name of the destination file.
-- @return true on success, or false on failure.
function db:dump_snapshot(dest) end


--- Load records from a snapshot file.
-- @param src the name of the source file.
-- @return true on success, or false on failure.
function db:load_snapshot(src) end


--- Get the number of records.
-- @return the number of records, or -1 on failure.
function db:count() end


--- Get the size of the database file.
-- @return the size of the database file in bytes, or -1 on failure.
function db:size() end


--- Get the path of the database file.
-- @return the path of the database file, or nil on failure.
function db:path() end


--- Get the miscellaneous status information.
-- @return a table of the status information, or nil on failure.
function db:status() end


--- Get keys matching a prefix string.
-- @param prefix the prefix string.
-- @param max the maximum number to retrieve.  If it is omitted or negative, no limit is specified.
-- @return an array of matching keys, or nil on failure.
function db:match_prefix(prefix, max) end


--- Get keys matching a regular expression string.
-- @param regex the regular expression string.
-- @param max the maximum number to retrieve.  If it is omitted or negative, no limit is specified.
-- @return an array of matching keys, or nil on failure.
function db:match_regex(regex, max) end


--- Get keys similar to a string in terms of the levenshtein distance.
-- @param origin the origin string.
-- @param range the maximum distance of keys to adopt.  If it is omitted, 1 is specified.
-- @param utf flag to treat keys as UTF-8 strings.  If it is omitted, false is specified.
-- @param max the maximum number to retrieve.  If it is omitted or negative, no limit is specified.
-- @return an array of matching keys, or nil on failure.
function db:match_similar(origin, range, utf, max) end


--- Merge records from other databases.
-- @param srcary an array of the source detabase objects.
-- @param mode the merge mode.  DB.MSET to overwrite the existing value, DB.MADD to keep the existing value, DB.MAPPEND to append the new value.  If it is omitted, DB.MSET is specified.
-- @return true on success, or false on failure.
function db:merge(srcary, mode) end


--- Execute a MapReduce process.
-- @param map a function to map a record data.  It is called for each record in the database and receives the key, the value, and a function to emit the mapped records.  The emitter function receives a key and a value.  The mapper function should return true normally or false on failure.
-- @param reduce a function to reduce a record data.  It is called for each record generated by emitted records by the map function and receives the key and a function to iterate to get each value.  The reducer function should return true normally or false on failure.
-- @param tmppath the path of a directory for the temporary data storage.  If it is omitted, temporary data are handled on memory.
-- @param opts the optional features by bitwise-or: DB.XNOLOCK to avoid locking against update operations by other threads, DB.XNOCOMP to avoid compression of temporary databases.
-- @param dbnum the number of temporary databases.  If it is omitted, the default setting is specified.
-- @param clim the limit size of the internal cache.  If it is omitted, the default setting is specified.
-- @param cbnum the bucket number of the internal cache.  If it is omitted, the default setting is specified.
-- @param log a function to log each progression message.  If it is omitted, log messages are ignored.
-- @param proc a function called before the mapper, between the mapper and the reducer, and after the reducer.  In the first two calls, the function receives a function to emit the mapped records.  If it is omitted, such triggers are ignored.
-- @return true on success, or false on failure.
function db:mapreduce(map, reduce, tmppath, opts, dbnum, clim, cbnum, log, proc) end


--- Create a cursor object.
-- @return the return value is the created cursor object.  Each cursor should be disabled with the Cursor#disable method when it is no longer in use.
function db:cursor() end


--- Process a cursor by a functor.
-- @param proc the functor of operations for the cursor.  The cursor is disabled implicitly after the block.
-- @return always nil.
function db:cursor_process(proc) end


--- Create three objects for the generic "for" loop.
-- @return multiple values composed of the "next" functor, the database object itself, and nil.
function db:pairs() end



-- END OF FILE
