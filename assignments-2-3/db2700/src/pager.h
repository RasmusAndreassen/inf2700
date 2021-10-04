/** @file pager.h
 * @brief A pager for accessing file blocks through buffer pages.
 * @author Weihai Yu
 *
 * A pager maintains a number of buffer pages in the main memory.
 *
 * Start a pager with @ref pager_init "pager_init()"
 * and terminate it with @ref pager_terminate "pager_terminate()".
 *
 * To process data stored in a table file, first use @ref get_page "get_page()"
 * to get the page of a given block.
 * If there is no page for the block yet, @ref get_page "get_page()"
 * tries to associate a page with the block. If it is the first time to
 * get a page for a table that does not exist yet, @ref get_page "get_page()"
 * will try to create a file for the table.
 * The file is located at @ref sys_dir "sys_dir".
 *
 * It is also possible to use @ref get_page_for_append "get_page_for_append()"
 * or @ref get_next_page "get_next_page()" to get an appropriate page.
 *
 * After getting the page, use @ref read_page "read_page()"
 * to read the content of a block into the page,
 * or @ref write_page "write_page()"
 * to write the content of a page into the block.
 *
 * When a page is @em pinned to a block, it cannot be replaced by
 * another block unless all pages are pinned.
 * @ref unpin "unpin()" a page to allow the page to be associated
 * with another block.
 *
 * A page has a <em>current position</em> that can be obtained with
 * @ref page_current_pos "page_current_pos()".
 * To access a data value of type @em x at the current position,
 * where @em x could be int or str,
 * use @ref page_get_int "page_get_x()" and @ref page_put_int "page_put_x()".
 * To access a value at a particular position,
 * use @ref page_get_int_at "page_get_x_at()" and @ref page_put_int_at "page_put_x_at()".
 *
 * @ref put_block_info "put_..._info()" are useful for printing out various info
 * during debugging.
 *
 * Reset the pager profiler with @ref pager_profiler_reset "pager_profiler_reset()"
 * before @ref put_pager_profiler_info "profiling" the pager.
 *
 * See source code in @ref schema.c for examples of how to use the pager.
 */

#ifndef _PAGER_H_
#define _PAGER_H_

#include <stdio.h>
#include <stdlib.h>
#include "pmsg.h"

/** block size in number of bytes */
#define BLOCK_SIZE 512L

/** buffer size in number of pages */
#define NUM_PAGES 10

/** number of bytes as page header */
#define PAGE_HEADER_SIZE 20

/** max number of open files */
#define MAX_OPEN_FILES 10

/** an integer consists of 4 bytes */
#define INT_SIZE 4

typedef struct block_struct * block_p;
typedef struct page_struct * page_p;

/** Database buffer */
extern page_p pages[];

extern void put_file_info(pmsg_level level, char const* fname);
extern void put_page_info(pmsg_level level, page_p p);
extern void put_block_info(pmsg_level level, block_p b);
extern void put_pager_info(pmsg_level level, char const* msg);
extern void put_pager_profiler_info(pmsg_level level);
extern void put_pqueues_info(pmsg_level level);

/** Set the directory of the system */
extern int set_system_dir(char const* dir);

/** Get the directory of the system */
extern char* system_dir();

/** Initiates a pager.
Memory of buffer pages are allocated.
Must be called first.
*/
extern int pager_init(void);
/** Terminates a pager.
Memory of buffer pages are released.
If there are dirty pages, they are writtern back to the file blocks.
All open files are closed.
Must be called before the program exits.
*/
extern void pager_terminate(void);

/** Reset th pager profiler */
extern void pager_profiler_reset(void);

/** Get a page for a file block.
A block is identified by the file and the block number @em blknr
(starting at 0. blknr = -1 for the last block at the end of the file).
get_page() does the following
- open_tbl_file() (and open/create the file on demand);
- returns the block if it is already managed in the file handle;
- otherwise,
  - make it managed in the @ref file_handle_struct "file handle";
  - pin the block to a buffer page (and read the block into the page).
  - Returns NULL upon failure of getting the page or pinning (reading) the page.
  - The current position of the page is set to right after the header
*/
extern page_p get_page(char const* fname, int blknr);
/** Get the last block and move the current position to the end */
extern page_p get_page_for_append(char const* fname);
/** Get the next page */
extern page_p get_next_page(page_p p);
/** Set current position to the beginning */
void page_set_pos_begin(page_p p);
/** Number of blocks in the file */
extern int file_num_blocks(char const* fname);
/** Close the file */
extern int close_file(char const* fname);

/** Pin the block to a buffer page and read the block into the page. */
extern page_p pin(block_p b);
/** Unpin the page. If the page is dirty, write the content back to disk. */
extern void unpin(page_p p);
/** Read the content of the page from disk.
If the content of the page is already uptodate, return immediately.
*/
extern int read_page(page_p p);
/** Write the content of the (dirty) page to disk. */
extern int write_page(page_p p);
/** Return page's block number. */
extern int page_block_nr(page_p p);
/** Return page's current position. */
extern int page_current_pos(page_p p);
/** Set page's current position. */
extern int page_set_current_pos(page_p p, int pos);

/** Check if @em offset is valid for getting a value */
extern int page_valid_pos_for_get(page_p p, int offset);
/** Check if @em offset is valid for putting a value with lenth @em len */
extern int page_valid_pos_for_put(page_p p, int offset, int len);

/** returns true (non-zero) if current position is at the @em end-of-page */
extern int eop(page_p p);
/** returns true (non-zero) if current position is at the @em end-of-file */
extern int peof(page_p p);
/** Retrieve the int value at the current position. */
extern int page_get_int(page_p p);
/** Put the int value @em val at the current position.
Returns 0 if there is not enough space at current position.
The current position is moved to the next value.
*/
extern int page_put_int(page_p p, int val);
/** Retrieve the int value at @em offset.
The current position is moved to the next value.
*/
extern int page_get_int_at(page_p p, int offset);
/** Put the int value @em val at @em offset.
Returns 0 if fails (@em offset out of range)
*/
extern int page_put_int_at(page_p p, int offset, int val);

/** Retrieve the string value at the current position.
The return value -1 indicates a failure.
It is the resposibility of the program using the pager to manage
the returned  string memory
(i.e., to free the memory when the string is no longer used).
*/
extern int page_get_str(page_p p, char* str, int len);
/** Put the string value @em str of length @em len at the current position.
@em len includes the ending '\\n' of the string.
Returns 0 if there is not enough space at current position.
The current position is moved to the next value.
*/
extern int page_put_str(page_p p, char const* str, int len);
/** Retrieve the string value at @em offset.
The return value -1 indicates a failure.
It is the resposibility of the program using the pager to manage
the returned string memory.
The current position is moved to the next value.
*/
extern int page_get_str_at(page_p p, int offset, char* str, int len);
/** Put the string value @em str of length @em len at @em offset.
@em len includes the ending '\\n' of the string.
Returns 0 if fails (@em offset out of range).
*/
extern int page_put_str_at(page_p p, int offset, char const* str, int len);

#endif
