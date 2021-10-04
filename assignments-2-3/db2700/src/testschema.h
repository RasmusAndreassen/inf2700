#ifndef _TESTSCHEMA_H_
#define _TESTSCHEMA_H_

#include "schema.h"

extern void test_tbl_write(char const* tbl_name);
extern void test_tbl_read(char const* tbl_name);
extern void test_tbl_natural_join(char const* my_tbl, char const* yr_tbl);

#endif
