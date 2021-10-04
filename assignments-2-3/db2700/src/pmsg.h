/** @file pmsg.h
 * @brief Print a message to stderr when @em level is higher than the global @em msglevel.
 * @author Weihai Yu
 */

#ifndef PMSG_H
#define PMSG_H

typedef enum {
  FORCE, /**< a forced msg */
  FATAL, /**< a fatal error */
  ERROR, /**< a handleable error condition */
  WARN,  /**< a warning */
  INFO,  /**< generic (useful) information about system operation */
  DEBUG  /**< low-level information for developers */
} pmsg_level;

/** global msg level */
extern pmsg_level msglevel; /* the higher, the more messages... */

/** @brief Start a new message line. */
void put_msg(pmsg_level level, char const* format, ...);
/* print a message, if it is considered significant enough.
   Adapted from [K&R2], p. 174 */

/**  @brief Appends additional info to the current message line */
void append_msg(pmsg_level level, char const * format, ...);

#endif /* PMSG_H */
