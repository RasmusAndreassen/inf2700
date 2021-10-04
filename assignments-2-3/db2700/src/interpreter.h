/** @file interpreter.h
 * @brief Interpreter of database commands
 * @author Weihai Yu
 *
 * Reading and running SQL-like commands.
 *
 * For simplicity, we restrict the syntax of the commands.
 * Please read the source code to see the restrictions.
 */

#ifndef _INTERPRETER_H_
#define _INTERPRETER_H_

/** Read and run commands. */
extern void interpret (int argc, char* argv[]);

#endif
