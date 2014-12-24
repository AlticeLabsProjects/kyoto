/*************************************************************************************************
 * Utility functions
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


#ifndef _KTUTIL_H                        // duplication check
#define _KTUTIL_H

#include <ktcommon.h>

namespace kyototycoon {                  // common namespace


/** The package version. */
extern const char* const VERSION;


/** The library version. */
extern const int32_t LIBVER;


/** The library revision. */
extern const int32_t LIBREV;


/** The extra feature list. */
extern const char* const FEATURES;


/** The default port number. */
const int32_t DEFPORT = 1978;


/**
 * Set the signal handler for termination signals.
 * @param handler the function pointer of the signal handler.
 * @return true on success, or false on failure.
 */
bool setkillsignalhandler(void (*handler)(int));


/**
 * Set the signal mask of the current to ignore all.
 * @return true on success, or false on failure.
 */
bool maskthreadsignal();


/**
 * Switch the process into the background.
 * @return true on success, or false on failure.
 */
bool daemonize();


/**
 * Execute a shell command.
 * @param args an array of the command name and its arguments.
 * @return the exit code of the command or `INT32_MIN' on failure.
 * @note The command name and the arguments are quoted and meta characters are escaped.
 */
int32_t executecommand(const std::vector<std::string>& args);


/**
 * Get the C-style string value of a record in a string map.
 * @param map the target string map.
 * @param key the key.
 * @param sp the pointer to the variable into which the size of the region of the return value
 * is assigned.  If it is NULL, it is ignored.
 * @return the C-style string value of the corresponding record, or NULL if there is no
 * corresponding record.
 */
const char* strmapget(const std::map<std::string, std::string>& map, const char* key,
                      size_t* sp = NULL);


/**
 * Print all records in a string vector.
 * @param vec the target string vector.
 * @param strm the output stream.
 */
void printstrvec(const std::vector<std::string>& vec, std::ostream& strm = std::cout);


/**
 * Print all records in a string map.
 * @param map the target string map.
 * @param strm the output stream.
 */
void printstrmap(const std::map<std::string, std::string>& map, std::ostream& strm = std::cout);


/**
 * Break up a URL into elements.
 * @param url the URL string.
 * @param elems the map object to contain the result elements.  The key "self" indicates the URL
 * itself.  "scheme" indicates the scheme.  "host" indicates the host of the server.  "port"
 * indicates the port number of the server.  "authority" indicates the authority information.
 * "path" indicates the path of the resource.  "file" indicates the file name without the
 * directory section.  "query" indicates the query string.  "fragment" indicates the fragment
 * string.
 * @note Supported schema are HTTP, HTTPS, FTP, and FILE.  Both of absolute URL and relative URL
 * are supported.
 */
void urlbreak(const char* url, std::map<std::string, std::string>* elems);


/**
 * Escape meta characters in a string with the entity references of XML.
 * @param str the string.
 * @return the escaped string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* xmlescape(const char* str);


/**
 * Unescape meta characters in a string with the entity references of XML.
 * @param str the string.
 * @return the unescaped string.
 * @note Because the region of the return value is allocated with the the new[] operator, it
 * should be released with the delete[] operator when it is no longer in use.
 */
char* xmlunescape(const char* str);


/**
 * Parse a www-form-urlencoded string and store each records into a map.
 * @param str the source string.
 * @param map the destination string map.
 */
void wwwformtomap(const std::string& str, std::map<std::string, std::string>* map);


/**
 * Serialize a string map into a www-form-urlencoded string.
 * @param map the source string map.
 * @param str the destination string.
 */
void maptowwwform(const std::map<std::string, std::string>& map, std::string* str);


/**
 * Parse a TSV string and store each records into a map.
 * @param str the source string.
 * @param map the destination string map.
 */
void tsvtomap(const std::string& str, std::map<std::string, std::string>* map);


/**
 * Serialize a string map into a TSV string.
 * @param map the source string map.
 * @param str the destination string.
 */
void maptotsv(const std::map<std::string, std::string>& map, std::string* str);


/**
 * Encode each record of a string map.
 * @param map the string map.
 * @param mode the encoding mode.  'B' for Base64 encoding, 'Q' for Quoted-printable encoding,
 * 'U' for URL encoding.
 */
void tsvmapencode(std::map<std::string, std::string>* map, int32_t mode);


/**
 * Decode each record of a string map.
 * @param map the string map.
 * @param mode the encoding mode.  'B' for Base64 encoding, 'Q' for Quoted-printable encoding,
 * 'U' for URL encoding.
 */
void tsvmapdecode(std::map<std::string, std::string>* map, int32_t mode);


/**
 * Check the best suited encoding of a string map.
 * @param map the string map.
 * @return the the best suited encoding.  0 for the raw format, 'B' for Base64 encoding, 'Q' for
 * Quoted-printable encoding,
 */
int32_t checkmapenc(const std::map<std::string, std::string>& map);


/**
 * Capitalize letters of a string.
 * @param str the string to convert.
 * @return the string itself.
 */
char* strcapitalize(char* str);


/**
 * Check a string is composed of alphabets or numbers only.
 * @return true if it is composed of alphabets or numbers only, or false if not.
 */
bool strisalnum(const char* str);


/**
 * Tokenize a string separating by space characters.
 * @param str the source string.
 * @param tokens a string vector to contain the result tokens.
 */
void strtokenize(const char* str, std::vector<std::string>* tokens);


/**
 * Get the Gregorian calendar of a time.
 * @param t the source time in seconds from the epoch.  If it is kyotocabinet::INT64MAX, the
 * current time is specified.
 * @param jl the jet lag of a location in seconds.  If it is kyotocabinet::INT32MAX, the local
 * jet lag is specified.
 * @param yearp the pointer to a variable to which the year is assigned.  If it is NULL, it is
 * not used.
 * @param monp the pointer to a variable to which the month is assigned.  If it is NULL, it is
 * not used.  1 means January and 12 means December.
 * @param dayp the pointer to a variable to which the day of the month is assigned.  If it is
 * NULL, it is not used.
 * @param hourp the pointer to a variable to which the hours is assigned.  If it is NULL, it is
 * not used.
 * @param minp the pointer to a variable to which the minutes is assigned.  If it is NULL, it is
 * not used.
 * @param secp the pointer to a variable to which the seconds is assigned.  If it is NULL, it is
 * not used.
 */
void getcalendar(int64_t t, int32_t jl,
                 int32_t* yearp = NULL, int32_t* monp = NULL, int32_t* dayp = NULL,
                 int32_t* hourp = NULL, int32_t* minp = NULL, int32_t* secp = NULL);


/**
 * Format a date as a string in W3CDTF.
 * @param t the source time in seconds from the epoch.  If it is kyotocabinet::INT64MAX, the
 * current time is specified.
 * @param jl the jet lag of a location in seconds.  If it is kyotocabinet::INT32MAX, the local
 * jet lag is specified.
 * @param buf the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 */
void datestrwww(int64_t t, int32_t jl, char* buf);


/**
 * Format a date as a string in W3CDTF with the fraction part.
 * @param t the source time in seconds from the epoch.  If it is Not-a-Number, the current time
 * is specified.
 * @param jl the jet lag of a location in seconds.  If it is kyotocabinet::INT32MAX, the local
 * jet lag is specified.
 * @param acr the accuracy of time by the number of columns of the fraction part.
 * @param buf the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 */
void datestrwww(double t, int32_t jl, int32_t acr, char* buf);


/**
 * Format a date as a string in RFC 1123 format.
 * @param t the source time in seconds from the epoch.  If it is kyotocabinet::INT64MAX, the
 * current time is specified.
 * @param jl the jet lag of a location in seconds.  If it is kyotocabinet::INT32MAX, the local
 * jet lag is specified.
 * @param buf the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 */
void datestrhttp(int64_t t, int32_t jl, char* buf);


/**
 * Get the time value of a date string.
 * @param str the date string in decimal, hexadecimal, W3CDTF, or RFC 822 (1123).  Decimal can
 * be trailed by "s" for in seconds, "m" for in minutes, "h" for in hours, and "d" for in days.
 * @return the time value of the date or INT64_MIN if the format is invalid.
 */
int64_t strmktime(const char* str);


/**
 * Get the jet lag of the local time.
 * @return the jet lag of the local time in seconds.
 */
int32_t jetlag();


/**
 * Get the day of week of a date.
 * @param year the year of a date.
 * @param mon the month of the date.
 * @param day the day of the date.
 * @return the day of week of the date.  0 means Sunday and 6 means Saturday.
 */
int32_t dayofweek(int32_t year, int32_t mon, int32_t day);


/**
 * Get the local time of a time.
 * @param time the time.
 * @param result the resulb buffer.
 * @return true on success, or false on failure.
 */
bool getlocaltime(time_t time, struct std::tm* result);


/**
 * Get the GMT local time of a time.
 * @param time the time.
 * @param result the resulb buffer.
 * @return true on success, or false on failure.
 */
bool getgmtime(time_t time, struct std::tm* result);


/**
 * Make the GMT from a time structure.
 * @param tm the pointer to the time structure.
 * @return the GMT.
 */
time_t mkgmtime(struct std::tm *tm);


/**
 * Get the C-style string value of a record in a string map.
 */
inline const char* strmapget(const std::map<std::string, std::string>& map, const char* key,
                             size_t* sp) {
  _assert_(key);
  std::map<std::string, std::string>::const_iterator it = map.find(key);
  if (it == map.end()) {
    if (sp) *sp = 0;
    return NULL;
  }
  if (sp) *sp = it->second.size();
  return it->second.c_str();
}


/**
 * Print all records in a string vector.
 */
inline void printstrvec(const std::vector<std::string>& vec, std::ostream& strm) {
  _assert_(true);
  std::vector<std::string>::const_iterator it = vec.begin();
  std::vector<std::string>::const_iterator itend = vec.end();
  while (it != itend) {
    strm << *it << std::endl;
    ++it;
  }
}


/**
 * Print all records in a string map.
 */
inline void printstrmap(const std::map<std::string, std::string>& map, std::ostream& strm) {
  _assert_(true);
  std::map<std::string, std::string>::const_iterator it = map.begin();
  std::map<std::string, std::string>::const_iterator itend = map.end();
  while (it != itend) {
    strm << it->first << '\t' << it->second << std::endl;
    ++it;
  }
}


/**
 * Break up a URL into elements.
 */
inline void urlbreak(const char* url, std::map<std::string, std::string>* elems) {
  _assert_(url);
  char* trim = kc::strdup(url);
  kc::strtrim(trim);
  char* rp = trim;
  char* norm = new char[std::strlen(trim)*3+1];
  char* wp = norm;
  while (*rp != '\0') {
    if (*rp > 0x20 && *rp < 0x7f) {
      *(wp++) = *rp;
    } else {
      *(wp++) = '%';
      int32_t num = *(unsigned char*)rp >> 4;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
      num = *rp & 0x0f;
      if (num < 10) {
        *(wp++) = '0' + num;
      } else {
        *(wp++) = 'a' + num - 10;
      }
    }
    rp++;
  }
  *wp = '\0';
  rp = norm;
  (*elems)["self"] = rp;
  bool serv = false;
  if (kc::strifwm(rp, "http://")) {
    (*elems)["scheme"] = "http";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "https://")) {
    (*elems)["scheme"] = "https";
    rp += 8;
    serv = true;
  } else if (kc::strifwm(rp, "ftp://")) {
    (*elems)["scheme"] = "ftp";
    rp += 6;
    serv = true;
  } else if (kc::strifwm(rp, "sftp://")) {
    (*elems)["scheme"] = "sftp";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ftps://")) {
    (*elems)["scheme"] = "ftps";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "tftp://")) {
    (*elems)["scheme"] = "tftp";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ldap://")) {
    (*elems)["scheme"] = "ldap";
    rp += 7;
    serv = true;
  } else if (kc::strifwm(rp, "ldaps://")) {
    (*elems)["scheme"] = "ldaps";
    rp += 8;
    serv = true;
  } else if (kc::strifwm(rp, "file://")) {
    (*elems)["scheme"] = "file";
    rp += 7;
    serv = true;
  }
  char* ep;
  if ((ep = std::strchr(rp, '#')) != NULL) {
    (*elems)["fragment"] = ep + 1;
    *ep = '\0';
  }
  if ((ep = std::strchr(rp, '?')) != NULL) {
    (*elems)["query"] = ep + 1;
    *ep = '\0';
  }
  if (serv) {
    if ((ep = std::strchr(rp, '/')) != NULL) {
      (*elems)["path"] = ep;
      *ep = '\0';
    } else {
      (*elems)["path"] = "/";
    }
    if ((ep = std::strchr(rp, '@')) != NULL) {
      *ep = '\0';
      if (rp[0] != '\0') (*elems)["authority"] = rp;
      rp = ep + 1;
    }
    if ((ep = std::strchr(rp, ':')) != NULL) {
      if (ep[1] != '\0') (*elems)["port"] = ep + 1;
      *ep = '\0';
    }
    if (rp[0] != '\0') (*elems)["host"] = rp;
  } else {
    (*elems)["path"] = rp;
  }
  delete[] norm;
  delete[] trim;
  const char* file = strmapget(*elems, "path");
  if (file) {
    const char* pv = std::strrchr(file, '/');
    if (pv) file = pv + 1;
    if (*file != '\0' && std::strcmp(file, ".") && std::strcmp(file, ".."))
      (*elems)["file"] = file;
  }
}


/**
 * Escape meta characters in a string with the entity references of XML.
 */
inline char* xmlescape(const char* str) {
  _assert_(str);
  const char* rp = str;
  size_t bsiz = 0;
  while (*rp != '\0') {
    switch (*rp) {
      case '&': bsiz += 5; break;
      case '<': bsiz += 4; break;
      case '>': bsiz += 4; break;
      case '"': bsiz += 6; break;
      default:  bsiz++; break;
    }
    rp++;
  }
  char* buf = new char[bsiz+1];
  char* wp = buf;
  while (*str != '\0') {
    switch (*str) {
      case '&': {
        std::memcpy(wp, "&amp;", 5);
        wp += 5;
        break;
      }
      case '<': {
        std::memcpy(wp, "&lt;", 4);
        wp += 4;
        break;
      }
      case '>': {
        std::memcpy(wp, "&gt;", 4);
        wp += 4;
        break;
      }
      case '"': {
        std::memcpy(wp, "&quot;", 6);
        wp += 6;
        break;
      }
      default: {
        *(wp++) = *str;
        break;
      }
    }
    str++;
  }
  *wp = '\0';
  return buf;
}


/**
 * Unescape meta characters in a string with the entity references of XML.
 */
inline char* xmlunescape(const char* str) {
  _assert_(str);
  char* buf = new char[std::strlen(str)+1];
  char* wp = buf;
  while (*str != '\0') {
    if (*str == '&') {
      if (kc::strfwm(str, "&amp;")) {
        *(wp++) = '&';
        str += 5;
      } else if (kc::strfwm(str, "&lt;")) {
        *(wp++) = '<';
        str += 4;
      } else if (kc::strfwm(str, "&gt;")) {
        *(wp++) = '>';
        str += 4;
      } else if (kc::strfwm(str, "&quot;")) {
        *(wp++) = '"';
        str += 6;
      } else {
        *(wp++) = *(str++);
      }
    } else {
      *(wp++) = *(str++);
    }
  }
  *wp = '\0';
  return buf;
}


/**
 * Parse a www-form-urlencoded string and store each records into a map.
 */
inline void wwwformtomap(const std::string& str, std::map<std::string, std::string>* map) {
  _assert_(true);
  const char* rp = str.data();
  const char* pv = rp;
  const char* ep = rp + str.size();
  while (rp < ep) {
    if (*rp == '&' || *rp == ';') {
      while (pv < rp && *pv > '\0' && *pv <= ' ') {
        pv++;
      }
      if (rp > pv) {
        size_t len = rp - pv;
        char* rbuf = new char[len+1];
        std::memcpy(rbuf, pv, len);
        rbuf[len] = '\0';
        char* sep = std::strchr(rbuf, '=');
        const char* vp = "";
        if (sep) {
          *(sep++) = '\0';
          vp = sep;
        }
        size_t ksiz;
        char* kbuf = kc::urldecode(rbuf, &ksiz);
        size_t vsiz;
        char* vbuf = kc::urldecode(vp, &vsiz);
        std::string key(kbuf, ksiz);
        std::string value(vbuf, vsiz);
        (*map)[key] = value;
        delete[] vbuf;
        delete[] kbuf;
        delete[] rbuf;
      }
      pv = rp + 1;
    }
    rp++;
  }
  while (pv < rp && *pv > '\0' && *pv <= ' ') {
    pv++;
  }
  if (rp > pv) {
    size_t len = rp - pv;
    char* rbuf = new char[len+1];
    std::memcpy(rbuf, pv, len);
    rbuf[len] = '\0';
    const char* vp = "";
    char* sep = std::strchr(rbuf, '=');
    if (sep) {
      *(sep++) = '\0';
      vp = sep;
    }
    size_t ksiz;
    char* kbuf = kc::urldecode(rbuf, &ksiz);
    size_t vsiz;
    char* vbuf = kc::urldecode(vp, &vsiz);
    std::string key(kbuf, ksiz);
    std::string value(vbuf, vsiz);
    (*map)[key] = value;
    delete[] vbuf;
    delete[] kbuf;
    delete[] rbuf;
  }
}


/**
 * Serialize a string map into a www-form-urlencoded string.
 */
inline void maptowwwform(const std::map<std::string, std::string>& map, std::string* str) {
  _assert_(true);
  std::map<std::string, std::string>::const_iterator it = map.begin();
  std::map<std::string, std::string>::const_iterator itend = map.end();
  str->reserve(kc::UINT8MAX);
  it = map.begin();
  while (it != itend) {
    if (str->size() > 0) str->append("&");
    char* zstr = kc::urlencode(it->first.data(), it->first.size());
    str->append(zstr);
    delete[] zstr;
    str->append("=");
    zstr = kc::urlencode(it->second.data(), it->second.size());
    str->append(zstr);
    delete[] zstr;
    ++it;
  }
}


/**
 * Parse a TSV string and store each records into a map.
 */
inline void tsvtomap(const std::string& str, std::map<std::string, std::string>* map) {
  _assert_(true);
  std::string::const_iterator it = str.begin();
  std::string::const_iterator pv = it;
  std::string field;
  while (it != str.end()) {
    if (*it == '\n') {
      if (it > pv) {
        std::string::const_iterator ev = it;
        if (ev[-1] == '\r') ev--;
        std::string::const_iterator rv = pv;
        while (rv < ev) {
          if (*rv == '\t') {
            std::string name(pv, rv);
            rv++;
            std::string value(rv, ev);
            (*map)[name] = value;
            break;
          }
          rv++;
        }
      }
      pv = it + 1;
    }
    ++it;
  }
  if (it > pv) {
    std::string::const_iterator ev = it;
    if (ev[-1] == '\r') ev--;
    std::string::const_iterator rv = pv;
    while (rv < ev) {
      if (*rv == '\t') {
        std::string name(pv, rv);
        rv++;
        std::string value(rv, ev);
        (*map)[name] = value;
        break;
      }
      rv++;
    }
  }
}


/**
 * Serialize a string map into a TSV string.
 */
inline void maptotsv(const std::map<std::string, std::string>& map, std::string* str) {
  _assert_(true);
  std::map<std::string, std::string>::const_iterator it = map.begin();
  std::map<std::string, std::string>::const_iterator itend = map.end();
  size_t size = 0;
  while (it != itend) {
    size += it->first.size() + it->second.size() + 2;
    ++it;
  }
  str->reserve(size);
  it = map.begin();
  while (it != itend) {
    str->append(it->first);
    str->append("\t");
    str->append(it->second);
    str->append("\n");
    ++it;
  }
}


/**
 * Encode each record of a string map.
 */
inline void tsvmapencode(std::map<std::string, std::string>* map, int32_t mode) {
  _assert_(map);
  std::map<std::string, std::string> nmap;
  std::map<std::string, std::string>::iterator it = map->begin();
  std::map<std::string, std::string>::iterator itend = map->end();
  while (it != itend) {
    char* kstr, *vstr;
    switch (mode) {
      case 'B': case 'b': {
        kstr = kc::baseencode(it->first.data(), it->first.size());
        vstr = kc::baseencode(it->second.data(), it->second.size());
        break;
      }
      case 'Q': case 'q': {
        kstr = kc::quoteencode(it->first.data(), it->first.size());
        vstr = kc::quoteencode(it->second.data(), it->second.size());
        break;
      }
      case 'U': case 'u': {
        kstr = kc::urlencode(it->first.data(), it->first.size());
        vstr = kc::urlencode(it->second.data(), it->second.size());
        break;
      }
      default: {
        kstr = NULL;
        vstr = NULL;
        break;
      }
    }
    if (kstr && vstr) {
      std::string key(kstr);
      std::string value(vstr);
      nmap[key] = value;
    }
    delete[] vstr;
    delete[] kstr;
    ++it;
  }
  map->swap(nmap);
}


/**
 * Decode each record of a string map.
 */
inline void tsvmapdecode(std::map<std::string, std::string>* map, int32_t mode) {
  _assert_(map);
  std::map<std::string, std::string> nmap;
  std::map<std::string, std::string>::iterator it = map->begin();
  std::map<std::string, std::string>::iterator itend = map->end();
  while (it != itend) {
    char* kbuf, *vbuf;
    size_t ksiz, vsiz;
    switch (mode) {
      case 'B': case 'b': {
        kbuf = kc::basedecode(it->first.c_str(), &ksiz);
        vbuf = kc::basedecode(it->second.c_str(), &vsiz);
        break;
      }
      case 'Q': case 'q': {
        kbuf = kc::quotedecode(it->first.c_str(), &ksiz);
        vbuf = kc::quotedecode(it->second.c_str(), &vsiz);
        break;
      }
      case 'U': case 'u': {
        kbuf = kc::urldecode(it->first.c_str(), &ksiz);
        vbuf = kc::urldecode(it->second.c_str(), &vsiz);
        break;
      }
      default: {
        kbuf = NULL;
        vbuf = NULL;
        break;
      }
    }
    if (kbuf && vbuf) {
      std::string key(kbuf, ksiz);
      std::string value(vbuf, vsiz);
      nmap[key] = value;
    }
    delete[] vbuf;
    delete[] kbuf;
    ++it;
  }
  map->swap(nmap);
}


/**
 * Check the best suited encoding of a string map.
 */
inline int32_t checkmapenc(const std::map<std::string, std::string>& map) {
  _assert_(true);
  bool bin = false;
  size_t blen = 0;
  size_t ulen = 0;
  std::map<std::string, std::string>::const_iterator it = map.begin();
  std::map<std::string, std::string>::const_iterator itend = map.end();
  while (it != itend) {
    const char* buf = it->first.data();
    size_t size = it->first.size();
    if (size > kc::UINT8MAX) {
      size = kc::UINT8MAX;
      bin = true;
    }
    blen += size * 6 / 4 + 3;
    for (size_t i = 0; i < size; i++) {
      int32_t c = ((unsigned char*)buf)[i];
      if (c < ' ' || c == 0x7f) bin = true;
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') || (c != '\0' && std::strchr("_-.!~*'()", c))) {
        ulen++;
      } else {
        ulen += 3;
      }
    }
    buf = it->second.data();
    size = it->second.size();
    if (size > kc::UINT8MAX) {
      size = kc::UINT8MAX;
      bin = true;
    }
    blen += size * 6 / 4 + 3;
    for (size_t i = 0; i < size; i++) {
      int32_t c = ((unsigned char*)buf)[i];
      if (c < ' ' || c == 0x7f) bin = true;
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') || (c != '\0' && std::strchr("_-.!~*'()", c))) {
        ulen++;
      } else {
        ulen += 3;
      }
    }
    ++it;
  }
  if (!bin) return 0;
  return blen < ulen ? 'B' : 'U';
}


/**
 * Capitalize letters of a string.
 */
inline char* strcapitalize(char* str) {
  _assert_(str);
  char* wp = str;
  bool head = true;
  while (*wp != '\0') {
    if (head && *wp >= 'a' && *wp <= 'z') *wp -= 'a' - 'A';
    head = *wp == '-' || *wp == ' ';
    wp++;
  }
  return str;
}


/**
 * Check a string is composed of alphabets or numbers only.
 */
inline bool strisalnum(const char* str) {
  _assert_(str);
  while (*str != '\0') {
    if (!(*str >= 'a' && *str <= 'z') && !(*str >= 'A' && *str <= 'Z') &&
        !(*str >= '0' && *str <= '9')) return false;
    str++;
  }
  return true;
}


/**
 * Tokenize a string separating by space characters.
 */
inline void strtokenize(const char* str, std::vector<std::string>* tokens) {
  _assert_(str && tokens);
  tokens->clear();
  while (*str == ' ' || *str == '\t') {
    str++;
  }
  const char* pv = str;
  while (*str != '\0') {
    if (*str > '\0' && *str <= ' ') {
      if (str > pv) {
        std::string elem(pv, str - pv);
        tokens->push_back(elem);
      }
      while (*str > '\0' && *str <= ' ') {
        str++;
      }
      pv = str;
    } else {
      str++;
    }
  }
  if (str > pv) {
    std::string elem(pv, str - pv);
    tokens->push_back(elem);
  }
}


}                                        // common namespace

#endif                                   // duplication check

// END OF FILE
