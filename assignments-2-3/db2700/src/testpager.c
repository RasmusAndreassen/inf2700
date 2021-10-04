#include "testpager.h"
#include "pmsg.h"
#include <string.h>

#define NUM_BLOCKS_IN_FILE 20 /* can be greater than NUM_PAGES */
#define NUM_RECORDS_IN_BLOCK 3
static int ints_in[NUM_RECORDS_IN_BLOCK] = {10, 20, 30};
static char* strs_in[NUM_RECORDS_IN_BLOCK] = {"a char string", "and yet", "another one"};
static size_t str_len = 14;


void test_page_write(char const* fname) {
  put_msg(INFO, "test_page_write() ...\n");
  pager_init();
  /* put_pager_info(DEBUG, "After pager_init"); */

  page_p pg;
  for (size_t bnr = 0; bnr < NUM_BLOCKS_IN_FILE; bnr++) {
    pg = get_page(fname, bnr);
    /* alternatively, append. But beware that the file grows
       with every run of the test
       pg = get_page_for_append(fname);
    */
    if (!pg) {
      put_msg(FATAL, "get_page %d fails\n", bnr);
      put_pager_info(FATAL, "After get_page");
      exit(EXIT_FAILURE);
    }

    /* put_pager_info(DEBUG, "After get_page"); */

    for (size_t i=0; i<NUM_RECORDS_IN_BLOCK; i++) {
      page_put_int(pg, ints_in[i] + bnr);
      page_put_str(pg, strs_in[i], str_len);
      /* put_pager_info(DEBUG, "After writing a record"); */
    }

    /* put_pager_info(DEBUG, "Before write_page"); */
    write_page(pg);
    unpin(pg);
    /* put_pager_info(DEBUG, "After unpin"); */
  }

  put_pager_profiler_info(INFO);
  pager_terminate();
  /* put_pager_info(DEBUG, "After pager_terminate"); */
  put_msg(INFO, "test_page_write() done.\n");
}

void test_page_read(char const* fname) {
  put_msg(INFO, "test_page_read() ...\n");
  pager_init();
  /* put_pager_info(DEBUG, "After pager_init"); */

  page_p pg;
  int int_out;
  char str_out[14];

  for (size_t bnr = 0; bnr < NUM_BLOCKS_IN_FILE; bnr++) {
    pg = get_page(fname, bnr);
    if (!pg) {
      put_msg(FATAL, "get_page %d fails\n", bnr);
      put_pager_info(FATAL, "After get_page");
      exit(EXIT_FAILURE);
    }

    /* put_pager_info(DEBUG, "After get_page"); */

    int i = 0;
    while (!eop(pg)) {
      int_out = page_get_int(pg);
      if (int_out != ints_in[i] + bnr) {
        put_msg(FATAL,
                "test_page_read fails: (read: %d, should be %d)\n",
                int_out, ints_in[i] + bnr);
        put_pager_info(FATAL, "After page_get_int");
        exit(EXIT_FAILURE);
      }
      page_get_str(pg, str_out, str_len);
      if (strcmp(str_out, strs_in[i]) != 0) {
        put_msg(FATAL,
                "test_page_read fails: (read: \"%s\", should be \"%s\")\n",
                str_out, strs_in[i] + bnr);
        put_pager_info(FATAL, "After page_get_str");
        exit(EXIT_FAILURE);
      }
      i++;
    }
    unpin(pg);
  }

  put_pager_profiler_info(INFO);
  pager_terminate();
  /* put_pager_info(DEBUG, "After pager_terminate"); */
  put_msg(INFO, "test_page_read() succeeds.\n");
}

void test_page_write_with_offset(char const* fname) {
  put_msg(INFO, "test_page_write_with_offset() ...\n");
  pager_init();
  /* put_pager_info(DEBUG, "After pager_init"); */

  page_p pg;
  for (size_t bnr = 0; bnr < NUM_BLOCKS_IN_FILE; bnr++) {
    pg = get_page(fname, bnr);
    if (!pg) {
      put_msg(FATAL, "get_page %d fails\n", bnr);
      put_pager_info(FATAL, "After get_page");
      exit(EXIT_FAILURE);
    }

    /* put_pager_info(DEBUG, "After get_page"); */

    for (size_t i=0; i<NUM_RECORDS_IN_BLOCK; i++) {
      int offset = PAGE_HEADER_SIZE + i*(INT_SIZE + str_len);
      page_put_int_at(pg, offset, ints_in[i] + bnr);
      offset += INT_SIZE;
      page_put_str_at(pg, offset, strs_in[i], str_len);
      /* put_pager_info(DEBUG, "After writing a record"); */
    }

    /* put_pager_info(DEBUG, "Before write_page"); */
    write_page(pg);
    unpin(pg);
    /* put_pager_info(DEBUG, "After unpin"); */
  }

  put_pager_profiler_info(INFO);
  pager_terminate();
  /* put_pager_info(DEBUG, "After pager_terminate"); */
  put_msg(INFO, "test_page_write_with_offset() done.\n");
}

void test_page_read_with_offset(char const* fname) {
  put_msg(INFO, "test_page_read_with_offset() ...\n");
  pager_init();
  /* put_pager_info(DEBUG, "After pager_init"); */

  page_p pg;
  int int_out;
  char str_out[14];

  for (size_t bnr = 0; bnr < NUM_BLOCKS_IN_FILE; bnr++) {
    pg = get_page(fname, bnr);
    if (!pg) {
      put_msg(FATAL, "get_page %d fails\n", bnr);
      put_pager_info(FATAL, "After get_page");
      exit(EXIT_FAILURE);
    }

    /* put_pager_info(DEBUG, "After get_page"); */

    int i = 0;
    while (!eop(pg)) {
      int offset = PAGE_HEADER_SIZE + i*(INT_SIZE + str_len);
      int_out = page_get_int_at(pg, offset);
      if (int_out != ints_in[i] + bnr) {
        put_msg(FATAL,
                "test_page_read fails: (read: %d, should be %d)\n",
                int_out, ints_in[i] + bnr);
        put_pager_info(FATAL, "After page_get_int_at");
        exit(EXIT_FAILURE);
      }
      offset += INT_SIZE;
      page_get_str_at(pg, offset, str_out, str_len);
      if (strcmp(str_out, strs_in[i]) != 0) {
        put_msg(FATAL,
                "test_page_read fails: (read: \"%s\", should be \"%s\")\n",
                str_out, strs_in[i] + bnr);
        put_pager_info(FATAL, "After page_get_str_at");
        exit(EXIT_FAILURE);
      }
      i++;
    }
    unpin(pg);
  }

  put_pager_profiler_info(INFO);
  pager_terminate();
  /* put_pager_info(DEBUG, "After pager_terminate"); */
  put_msg(INFO, "test_page_read_with_offset() succeeds.\n");
}
