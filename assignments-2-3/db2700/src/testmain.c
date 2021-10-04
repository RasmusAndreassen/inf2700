#include "test_data_gen.h"
#include "testschema.h"
#include "pmsg.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void handle_test_options(int argc, char* argv[]) {
  int c;
  char new_sys_dir[512];

  new_sys_dir[0] = '\0';
  msglevel = INFO;

  while ((c = getopt(argc, argv, "hm:d:")) != -1)
    switch (c) {
    case 'h':
      printf("Usage: runtest [switches]\n");
      printf("\t-h           help, print this message\n");
      printf("\t-m [fewid]   msg level [fatal,error,warn,info,debug]\n");
      printf("\t-d db_dir  default to ./tests/testdb\n");
      exit(0);
    case 'm':
      switch (optarg[0]) {
      case 'f': msglevel = FATAL; break;
      case 'e': msglevel = ERROR; break;
      case 'w': msglevel = WARN; break;
      case 'i': msglevel = INFO; break;
      case 'd': msglevel = DEBUG; break;
      }
      break;
    case 'd':
      strcpy(new_sys_dir, optarg);
      break;
    case '?':
      if (optopt == 'm' || optopt == 'd' || optopt == 'c')
        printf("Option -%c requires an argument.\n", optopt);
      else if (isprint(optopt))
        printf("Unknown option `-%c'.\n", optopt);
      else
        printf("Unknown option character `\\x%x'.\n", optopt);
      abort();
    default:
      abort();
    }

  if (new_sys_dir[0] == '\0')
    strcpy(new_sys_dir, "./tests/testdb");

  if (!set_system_dir(new_sys_dir)) {
    put_msg(ERROR, "cannot set system dir at %s\n", new_sys_dir);
    exit(EXIT_FAILURE);
  }
}

int main(int argc, char* argv[]) {
  handle_test_options(argc, argv);
  prepare_test_data_gen();
  /*
  test_page_write("testpage");
  test_page_read("testpage");

  test_page_write_with_offset("testpage_w_offset");
  test_page_read_with_offset("testpage_w_offset");
  */

  char my_tbl[] = "Me";
  test_tbl_write(my_tbl);
  test_tbl_read(my_tbl);

  test_tbl_natural_join(my_tbl, "You");

  return (0);
}
