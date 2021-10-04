#include <string.h>
#include "testschema.h"
#include "test_data_gen.h"
#include "pmsg.h"

#define NUM_RECORDS 1000

/* The records generated in test_tbl_write().
   Will be used in test_tbl_read() to check for correctness. */
record in_recs[NUM_RECORDS];


void test_tbl_write(char const* tbl_name) {
  put_msg(INFO, "test_tbl_write (\"%s\") ...\n", tbl_name);

  open_db();
  /* put_pager_info(DEBUG, "After open_db"); */

  char id_attr[11] = "Id", str_attr[11] = "Str";
  char *attrs[] = {strcat(id_attr, tbl_name), strcat(str_attr,tbl_name), "Int"};
  int attr_types[] = {INT_TYPE, STR_TYPE, INT_TYPE};
  schema_p sch = create_test_schema(tbl_name, 3, attrs, attr_types);

  test_data_gen(sch, in_recs, NUM_RECORDS);

  for (size_t rec_n = 0; rec_n < NUM_RECORDS; rec_n++) {
    /* put_msg(DEBUG, "rec_n %d  ", rec_n); */
    append_record(in_recs[rec_n], sch);
    /* put_pager_info(DEBUG, "After writing a record"); */
  }

  /* put_pager_info(DEBUG, "Before page_terminate"); */
  put_db_info(DEBUG);
  close_db();
  /* put_pager_info(DEBUG, "After close_db"); */

  put_pager_profiler_info(INFO);
  put_msg(INFO,  "test_tbl_write() done.\n\n");
}

void test_tbl_read(char const* tbl_name) {
  put_msg(INFO,  "test_tbl_read (\"%s\") ...\n", tbl_name);

  open_db();
  /* put_pager_info(DEBUG, "After open_db"); */

  schema_p sch = get_schema(tbl_name);
  tbl_p tbl = get_table(tbl_name);
  record out_rec = new_record(sch);
  set_tbl_position(tbl, TBL_BEG);
  int rec_n = 0;

  while (!eot(tbl)) {
    get_record(out_rec, sch);
    if (!equal_record(out_rec, in_recs[rec_n], sch)) {
      put_msg(FATAL, "test_tbl_read:\n");
      put_record_info(FATAL, out_rec, sch);
      put_msg(FATAL, "should be:\n");
      put_record_info(FATAL, in_recs[rec_n], sch);
      exit(EXIT_FAILURE);
    }
    release_record(in_recs[rec_n++], sch);
    /* put_pager_info(DEBUG, "After reading a record"); */
  }

  if (rec_n != NUM_RECORDS)
    put_msg(ERROR, "only %d of %d records read", rec_n, NUM_RECORDS);

  release_record(out_rec, sch);

  /* put_pager_info(DEBUG, "Before page_terminate"); */
  put_pager_profiler_info(INFO);
  close_db();
  /* put_pager_info(DEBUG, "After close_db"); */

  put_msg(INFO,  "test_tbl_read() succeeds.\n");

}

void test_tbl_natural_join(char const* my_tbl, char const* yr_tbl) {
  put_msg(INFO, "test_tbl_natural_join (\"%s\", \"%s\") ...\n", my_tbl, yr_tbl);

  test_tbl_write(yr_tbl);

  open_db();

  tbl_p tbl_m = get_table(my_tbl);
  tbl_p tbl_y = get_table(yr_tbl);

  table_natural_join(tbl_m, tbl_y);

  put_db_info(DEBUG);
  close_db();
  /* put_pager_info(DEBUG, "After close_db"); */

  put_pager_profiler_info(INFO);
  put_msg(INFO,  "test_tbl_natural_join() done.\n\n");
}
