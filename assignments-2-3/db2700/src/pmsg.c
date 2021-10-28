/*********************************************************
 * Pmsg for assignments in the Databases course INF-2700 *
 * UIT - The Arctic University of Norway                 *
 * Author: Weihai Yu                                     *
 *********************************************************/

#include "pmsg.h"
#include <stdio.h>
#include <stdarg.h>

pmsg_level msglevel = INFO;

void put_msg(pmsg_level level, char const* format, ...) {
  va_list args;
  FILE *stream;

  if (level > msglevel) return;
  if (level == FORCE)
    stream = stdout;
  else
    stream = level <= WARN ? stderr: stdout;

  va_start(args, format);
  switch (level) {
  case FATAL: fprintf(stream, "FATAL: "); break;
  case ERROR: fprintf(stream, "ERROR: "); break;
  case WARN:  fprintf(stream, "WARN:  "); break;
  case INFO:  fprintf(stream, "INFO:  "); break;
  case DEBUG: fprintf(stream, "DEBUG: "); break;
  case FORCE: break;
  }

  vfprintf(stream, format, args);
  va_end(args);
}


void append_msg(pmsg_level level, char const* format, ...) {
  va_list args;
  FILE *stream;

  if (level>msglevel) return;
  if (level == FORCE)
    stream = stdout;
  else
    stream = level <= WARN ? stderr: stdout;
  va_start(args, format);
  vfprintf(stream, format, args);
  va_end(args);
}
