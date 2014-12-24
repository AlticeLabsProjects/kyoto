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


#include "ktutil.h"
#include "myconf.h"

namespace kyototycoon {                  // common namespace


/** The package version. */
const char* const VERSION = _KT_VERSION;


/** The library version. */
const int32_t LIBVER = _KT_LIBVER;


/** The library revision. */
const int32_t LIBREV = _KT_LIBREV;


/** The extra feature list. */
const char* const FEATURES = ""
#if defined(_KT_EVENT_EPOLL)
    "(epoll)"
#elif defined(_KT_EVENT_KQUEUE)
    "(kqueue)"
#else
    "(select)"
#endif
#if _KT_LUA
    "(lua)"
#endif
    ;


/**
 * Set the signal handler for termination signals.
 */
bool setkillsignalhandler(void (*handler)(int)) {
  _assert_(handler);
  int32_t signals[] = {
    SIGHUP, SIGINT, SIGUSR1, SIGUSR2, SIGTERM
  };
  bool err = false;
  for (size_t i = 0; i < sizeof(signals) / sizeof(*signals); i++) {
    struct ::sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_flags = 0;
    sa.sa_handler = handler;
    sigfillset(&sa.sa_mask);
    if (::sigaction(signals[i], &sa, NULL) != 0) err = true;
  }
  return !err;
}


/**
 * Set the signal mask of the current to ignore all.
 */
bool maskthreadsignal() {
  _assert_(true);
  bool err = false;
  ::sigset_t sigmask;
  sigfillset(&sigmask);
  if (::pthread_sigmask(SIG_BLOCK, &sigmask, NULL) != 0) err = true;
  return !err;
}


/**
 * Switch the process into the background.
 */
bool daemonize() {
  _assert_(true);
  std::fflush(stdout);
  std::fflush(stderr);
  switch (::fork()) {
    case -1: return false;
    case 0: break;
    default: ::_exit(0);
  }
  if (::setsid() == -1) return false;
  switch (::fork()) {
    case -1: return false;
    case 0: break;
    default: ::_exit(0);
  }
  ::umask(0);
  if (::chdir("/") == -1) return false;
  ::close(0);
  ::close(1);
  ::close(2);
  int32_t fd = ::open("/dev/null", O_RDWR, 0);
  if (fd != -1) {
    ::dup2(fd, 0);
    ::dup2(fd, 1);
    ::dup2(fd, 2);
    if (fd > 2) ::close(fd);
  }
  return true;
}


/**
 * Execute a shell command.
 */
int32_t executecommand(const std::vector<std::string>& args) {
  _assert_(true);
  if (args.empty()) return -1;
  std::string phrase;
  for (size_t i = 0; i < args.size(); i++) {
    const char* rp = args[i].c_str();
    char* token = new char[args[i].size() * 2 + 1];
    char* wp = token;
    while (*rp != '\0') {
      switch (*rp) {
        case '"': case '\\': case '$': case '`': case '!': {
          *(wp++) = '\\';
          *(wp++) = *rp;
          break;
        }
        default: {
          *(wp++) = *rp;
          break;
        }
      }
      rp++;
    }
    *wp = '\0';
    if (!phrase.empty()) phrase.append(" ");
    kc::strprintf(&phrase, "\"%s\"", token);
    delete[] token;
  }
  int32_t rv = std::system(phrase.c_str());
  if (WIFEXITED(rv)) {
    rv = WEXITSTATUS(rv);
  } else {
    rv = kc::INT32MAX;
  }
  return rv;
}


/**
 * Get the Gregorian calendar of a time.
 */
void getcalendar(int64_t t, int32_t jl,
                 int32_t* yearp, int32_t* monp, int32_t* dayp,
                 int32_t* hourp, int32_t* minp, int32_t* secp) {
  _assert_(true);
  if (t == kc::INT64MAX) t = std::time(NULL);
  if (jl == kc::INT32MAX) jl = jetlag();
  time_t tt = (time_t)t + jl;
  struct std::tm ts;
  if (!getgmtime(tt, &ts)) {
    if (yearp) *yearp = 0;
    if (monp) *monp = 0;
    if (dayp) *dayp = 0;
    if (hourp) *hourp = 0;
    if (minp) *minp = 0;
    if (secp) *secp = 0;
  }
  if (yearp) *yearp = ts.tm_year + 1900;
  if (monp) *monp = ts.tm_mon + 1;
  if (dayp) *dayp = ts.tm_mday;
  if (hourp) *hourp = ts.tm_hour;
  if (minp) *minp = ts.tm_min;
  if (secp) *secp = ts.tm_sec;
}


/**
 * Format a date as a string in W3CDTF.
 */
void datestrwww(int64_t t, int32_t jl, char* buf) {
  _assert_(buf);
  if (t == kc::INT64MAX) t = std::time(NULL);
  if (jl == kc::INT32MAX) jl = jetlag();
  time_t tt = (time_t)t + jl;
  struct std::tm ts;
  if (!getgmtime(tt, &ts)) std::memset(&ts, 0, sizeof(ts));
  ts.tm_year += 1900;
  ts.tm_mon += 1;
  jl /= 60;
  char tzone[16];
  if (jl == 0) {
    std::sprintf(tzone, "Z");
  } else if (jl < 0) {
    jl *= -1;
    std::sprintf(tzone, "-%02d:%02d", jl / 60, jl % 60);
  } else {
    std::sprintf(tzone, "+%02d:%02d", jl / 60, jl % 60);
  }
  std::sprintf(buf, "%04d-%02d-%02dT%02d:%02d:%02d%s",
               ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec, tzone);
}


/**
 * Format a date as a string in W3CDTF with the fraction part.
 */
void datestrwww(double t, int32_t jl, int32_t acr, char* buf) {
  _assert_(acr >= 0 && buf);
  if (kc::chknan(t)) t = kc::time();
  double tinteg, tfract;
  tfract = std::fabs(std::modf(t, &tinteg));
  if (jl == kc::INT32MAX) jl = jetlag();
  if (acr > 12) acr = 12;
  time_t tt = (time_t)tinteg + jl;
  struct std::tm ts;
  if (!getgmtime(tt, &ts)) std::memset(&ts, 0, sizeof(ts));
  ts.tm_year += 1900;
  ts.tm_mon += 1;
  jl /= 60;
  char tzone[16];
  if (jl == 0) {
    std::sprintf(tzone, "Z");
  } else if (jl < 0) {
    jl *= -1;
    std::sprintf(tzone, "-%02d:%02d", jl / 60, jl % 60);
  } else {
    std::sprintf(tzone, "+%02d:%02d", jl / 60, jl % 60);
  }
  if (acr < 1) {
    std::sprintf(buf, "%04d-%02d-%02dT%02d:%02d:%02d%s",
                 ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec, tzone);
  } else {
    char dec[16];
    std::sprintf(dec, "%.12f", tfract);
    char* wp = dec;
    if (*wp == '0') wp++;
    wp[acr+1] = '\0';
    std::sprintf(buf, "%04d-%02d-%02dT%02d:%02d:%02d%s%s",
                 ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec, wp, tzone);
  }
}


/**
 * Format a date as a string in RFC 1123 format.
 */
void datestrhttp(int64_t t, int32_t jl, char* buf) {
  _assert_(buf);
  if (t == kc::INT64MAX) t = std::time(NULL);
  if (jl == kc::INT32MAX) jl = jetlag();
  time_t tt = (time_t)t + jl;
  struct std::tm ts;
  if (!getgmtime(tt, &ts)) std::memset(&ts, 0, sizeof(ts));
  ts.tm_year += 1900;
  ts.tm_mon += 1;
  jl /= 60;
  char* wp = buf;
  switch (dayofweek(ts.tm_year, ts.tm_mon, ts.tm_mday)) {
    case 0: wp += std::sprintf(wp, "Sun, "); break;
    case 1: wp += std::sprintf(wp, "Mon, "); break;
    case 2: wp += std::sprintf(wp, "Tue, "); break;
    case 3: wp += std::sprintf(wp, "Wed, "); break;
    case 4: wp += std::sprintf(wp, "Thu, "); break;
    case 5: wp += std::sprintf(wp, "Fri, "); break;
    case 6: wp += std::sprintf(wp, "Sat, "); break;
  }
  wp += std::sprintf(wp, "%02d ", ts.tm_mday);
  switch (ts.tm_mon) {
    case 1: wp += std::sprintf(wp, "Jan "); break;
    case 2: wp += std::sprintf(wp, "Feb "); break;
    case 3: wp += std::sprintf(wp, "Mar "); break;
    case 4: wp += std::sprintf(wp, "Apr "); break;
    case 5: wp += std::sprintf(wp, "May "); break;
    case 6: wp += std::sprintf(wp, "Jun "); break;
    case 7: wp += std::sprintf(wp, "Jul "); break;
    case 8: wp += std::sprintf(wp, "Aug "); break;
    case 9: wp += std::sprintf(wp, "Sep "); break;
    case 10: wp += std::sprintf(wp, "Oct "); break;
    case 11: wp += std::sprintf(wp, "Nov "); break;
    case 12: wp += std::sprintf(wp, "Dec "); break;
  }
  wp += std::sprintf(wp, "%04d %02d:%02d:%02d ", ts.tm_year, ts.tm_hour, ts.tm_min, ts.tm_sec);
  if (jl == 0) {
    std::sprintf(wp, "GMT");
  } else if (jl < 0) {
    jl *= -1;
    std::sprintf(wp, "-%02d%02d", jl / 60, jl % 60);
  } else {
    std::sprintf(wp, "+%02d%02d", jl / 60, jl % 60);
  }
}


/**
 * Get the time value of a date string.
 */
int64_t strmktime(const char* str) {
  _assert_(str);
  while (*str > '\0' && *str <= ' ') {
    str++;
  }
  if (*str == '\0') return kc::INT64MIN;
  if (str[0] == '0' && (str[1] == 'x' || str[1] == 'X')) return kc::atoih(str + 2);
  struct std::tm ts;
  std::memset(&ts, 0, sizeof(ts));
  ts.tm_year = 70;
  ts.tm_mon = 0;
  ts.tm_mday = 1;
  ts.tm_hour = 0;
  ts.tm_min = 0;
  ts.tm_sec = 0;
  ts.tm_isdst = 0;
  int32_t len = std::strlen(str);
  time_t t = (time_t)kc::atoi(str);
  const char* pv = str;
  while (*pv >= '0' && *pv <= '9') {
    pv++;
  }
  while (*pv > '\0' && *pv <= ' ') {
    pv++;
  }
  if (*pv == '\0') return t;
  if ((pv[0] == 's' || pv[0] == 'S') && pv[1] >= '\0' && pv[1] <= ' ') return t;
  if ((pv[0] == 'm' || pv[0] == 'M') && pv[1] >= '\0' && pv[1] <= ' ') return t * 60;
  if ((pv[0] == 'h' || pv[0] == 'H') && pv[1] >= '\0' && pv[1] <= ' ') return t * 60 * 60;
  if ((pv[0] == 'd' || pv[0] == 'D') && pv[1] >= '\0' && pv[1] <= ' ') return t * 60 * 60 * 24;
  if (len > 4 && str[4] == '-') {
    ts.tm_year = kc::atoi(str) - 1900;
    if ((pv = std::strchr(str, '-')) != NULL && pv - str == 4) {
      const char* rp = pv + 1;
      ts.tm_mon = kc::atoi(rp) - 1;
      if ((pv = std::strchr(rp, '-')) != NULL && pv - str == 7) {
        rp = pv + 1;
        ts.tm_mday = kc::atoi(rp);
        if ((pv = std::strchr(rp, 'T')) != NULL && pv - str == 10) {
          rp = pv + 1;
          ts.tm_hour = kc::atoi(rp);
          if ((pv = std::strchr(rp, ':')) != NULL && pv - str == 13) {
            rp = pv + 1;
            ts.tm_min = kc::atoi(rp);
          }
          if ((pv = std::strchr(rp, ':')) != NULL && pv - str == 16) {
            rp = pv + 1;
            ts.tm_sec = kc::atoi(rp);
          }
          if ((pv = std::strchr(rp, '.')) != NULL && pv - str >= 19) rp = pv + 1;
          pv = rp;
          while (*pv >= '0' && *pv <= '9') {
            pv++;
          }
          if ((*pv == '+' || *pv == '-') && std::strlen(pv) >= 6 && pv[3] == ':')
            ts.tm_sec -= (kc::atoi(pv + 1) * 3600 +
                          kc::atoi(pv + 4) * 60) * (pv[0] == '+' ? 1 : -1);
        }
      }
    }
    return mkgmtime(&ts);
  }
  if (len > 4 && str[4] == '/') {
    ts.tm_year = kc::atoi(str) - 1900;
    if ((pv = std::strchr(str, '/')) != NULL && pv - str == 4) {
      const char* rp = pv + 1;
      ts.tm_mon = kc::atoi(rp) - 1;
      if ((pv = std::strchr(rp, '/')) != NULL && pv - str == 7) {
        rp = pv + 1;
        ts.tm_mday = kc::atoi(rp);
        if ((pv = std::strchr(rp, ' ')) != NULL && pv - str == 10) {
          rp = pv + 1;
          ts.tm_hour = kc::atoi(rp);
          if ((pv = std::strchr(rp, ':')) != NULL && pv - str == 13) {
            rp = pv + 1;
            ts.tm_min = kc::atoi(rp);
          }
          if ((pv = std::strchr(rp, ':')) != NULL && pv - str == 16) {
            rp = pv + 1;
            ts.tm_sec = kc::atoi(rp);
          }
          if ((pv = std::strchr(rp, '.')) != NULL && pv - str >= 19) rp = pv + 1;
          pv = rp;
          while (*pv >= '0' && *pv <= '9') {
            pv++;
          }
          if ((*pv == '+' || *pv == '-') && std::strlen(pv) >= 6 && pv[3] == ':')
            ts.tm_sec -= (kc::atoi(pv + 1) * 3600 +
                          kc::atoi(pv + 4) * 60) * (pv[0] == '+' ? 1 : -1);
        }
      }
    }
    return mkgmtime(&ts);
  }
  const char* crp = str;
  if (len >= 4 && str[3] == ',') crp = str + 4;
  while (*crp == ' ') {
    crp++;
  }
  ts.tm_mday = kc::atoi(crp);
  while ((*crp >= '0' && *crp <= '9') || *crp == ' ') {
    crp++;
  }
  if (kc::strifwm(crp, "Jan")) {
    ts.tm_mon = 0;
  } else if (kc::strifwm(crp, "Feb")) {
    ts.tm_mon = 1;
  } else if (kc::strifwm(crp, "Mar")) {
    ts.tm_mon = 2;
  } else if (kc::strifwm(crp, "Apr")) {
    ts.tm_mon = 3;
  } else if (kc::strifwm(crp, "May")) {
    ts.tm_mon = 4;
  } else if (kc::strifwm(crp, "Jun")) {
    ts.tm_mon = 5;
  } else if (kc::strifwm(crp, "Jul")) {
    ts.tm_mon = 6;
  } else if (kc::strifwm(crp, "Aug")) {
    ts.tm_mon = 7;
  } else if (kc::strifwm(crp, "Sep")) {
    ts.tm_mon = 8;
  } else if (kc::strifwm(crp, "Oct")) {
    ts.tm_mon = 9;
  } else if (kc::strifwm(crp, "Nov")) {
    ts.tm_mon = 10;
  } else if (kc::strifwm(crp, "Dec")) {
    ts.tm_mon = 11;
  } else {
    ts.tm_mon = -1;
  }
  if (ts.tm_mon >= 0) crp += 3;
  while (*crp == ' ') {
    crp++;
  }
  ts.tm_year = kc::atoi(crp);
  if (ts.tm_year >= 1969) ts.tm_year -= 1900;
  while (*crp >= '0' && *crp <= '9') {
    crp++;
  }
  while (*crp == ' ') {
    crp++;
  }
  if (ts.tm_mday > 0 && ts.tm_mon >= 0 && ts.tm_year >= 0) {
    int32_t clen = std::strlen(crp);
    if (clen >= 8 && crp[2] == ':' && crp[5] == ':') {
      ts.tm_hour = kc::atoi(crp + 0);
      ts.tm_min = kc::atoi(crp + 3);
      ts.tm_sec = kc::atoi(crp + 6);
      if (clen >= 14 && crp[8] == ' ' && (crp[9] == '+' || crp[9] == '-')) {
        ts.tm_sec -= ((crp[10] - '0') * 36000 + (crp[11] - '0') * 3600 +
                      (crp[12] - '0') * 600 + (crp[13] - '0') * 60) * (crp[9] == '+' ? 1 : -1);
      } else if (clen > 9) {
        if (!std::strcmp(crp + 9, "JST")) {
          ts.tm_sec -= 9 * 3600;
        } else if (!std::strcmp(crp + 9, "CCT")) {
          ts.tm_sec -= 8 * 3600;
        } else if (!std::strcmp(crp + 9, "KST")) {
          ts.tm_sec -= 9 * 3600;
        } else if (!std::strcmp(crp + 9, "EDT")) {
          ts.tm_sec -= -4 * 3600;
        } else if (!std::strcmp(crp + 9, "EST")) {
          ts.tm_sec -= -5 * 3600;
        } else if (!std::strcmp(crp + 9, "CDT")) {
          ts.tm_sec -= -5 * 3600;
        } else if (!std::strcmp(crp + 9, "CST")) {
          ts.tm_sec -= -6 * 3600;
        } else if (!std::strcmp(crp + 9, "MDT")) {
          ts.tm_sec -= -6 * 3600;
        } else if (!std::strcmp(crp + 9, "MST")) {
          ts.tm_sec -= -7 * 3600;
        } else if (!std::strcmp(crp + 9, "PDT")) {
          ts.tm_sec -= -7 * 3600;
        } else if (!std::strcmp(crp + 9, "PST")) {
          ts.tm_sec -= -8 * 3600;
        } else if (!std::strcmp(crp + 9, "HDT")) {
          ts.tm_sec -= -9 * 3600;
        } else if (!std::strcmp(crp + 9, "HST")) {
          ts.tm_sec -= -10 * 3600;
        }
      }
    }
    return mkgmtime(&ts);
  }
  return kc::INT64MIN;
}


/**
 * Get the jet lag of the local time.
 */
int32_t jetlag() {
#if defined(_SYS_LINUX_)
  _assert_(true);
  ::tzset();
  return -timezone;
#else
  _assert_(true);
  time_t t = 86400;
  struct std::tm gts;
  if (!getgmtime(t, &gts)) return 0;
  struct std::tm lts;
  t = 86400;
  if (!getlocaltime(t, &lts)) return 0;
  return std::mktime(&lts) - std::mktime(&gts);
#endif
}


/**
 * Get the day of week of a date.
 */
int32_t dayofweek(int32_t year, int32_t mon, int32_t day) {
  _assert_(true);
  if (mon < 3) {
    year--;
    mon += 12;
  }
  return (day + ((8 + (13 * mon)) / 5) + (year + (year / 4) - (year / 100) + (year / 400))) % 7;
}


/**
 * Get the local time of a time.
 */
bool getlocaltime(time_t time, struct std::tm* result) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(result);
  return ::localtime_s(result, &time) == 0;
#else
  _assert_(result);
  return ::localtime_r(&time, result) != NULL;
#endif
}


/**
 * Get the GMT local time of a time.
 */
bool getgmtime(time_t time, struct std::tm* result) {
#if defined(_SYS_MSVC_) || defined(_SYS_MINGW_)
  _assert_(result);
  return ::gmtime_s(result, &time) == 0;
#else
  _assert_(result);
  return ::gmtime_r(&time, result)!= NULL;
#endif
}


/**
 * Make the GMT from a time structure.
 */
time_t mkgmtime(struct std::tm *tm) {
#if defined(_SYS_LINUX_)
  _assert_(tm);
  return ::timegm(tm);
#else
  _assert_(tm);
  return std::mktime(tm) + jetlag();
#endif
}


}                                        // common namespace

// END OF FILE
