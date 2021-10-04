#include "test_data_gen.h"
#include <stdlib.h>
#include <stdio.h>

#define TEST_STR_LEN 30

schema_p create_test_schema(char const* name, int n_attrs,
                            char* attrs[], int attr_types[])
{
  /* Force a fresh start. Clean what is left from a previous run. */
  remove_schema(get_schema(name));

  schema_p sch = new_schema(name);
  for (size_t i = 0; i < n_attrs; i++)
    switch (attr_types[i])
      {
      case INT_TYPE:
        add_field(sch, new_int_field(attrs[i]));
        break;
      case STR_TYPE:
        add_field(sch, new_str_field(attrs[i], TEST_STR_LEN));
        break;
      default:
        put_msg(ERROR, "unknown attr type for %s\n", attrs[i]);
      }
  put_schema_info(DEBUG, sch);
  return sch;
}

void prepare_test_data_gen()
{
  srand(2);
}

void test_data_gen(schema_p s, record* r, int n)
{
  for (size_t i = 0; i < n; i++)
    {
      r[i] = new_record(s);
      fill_gen_record(s, r[i], i);
    }
}

/** values (id, schema_Val_id, rand()) */
void fill_gen_record(schema_p s, record r, int id)
{
  char test_str[TEST_STR_LEN];
  sprintf(test_str, "%s_Val_%d", schema_name(s), id);
  fill_record(r, s, id, test_str, rand()%100);
}
