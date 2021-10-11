/************************************************************
 * Interpreter for db2700 in the Databases course INF-2700  *
 * UIT - The Arctic University of Norway                    *
 * Author: Weihai Yu                                        *
 ************************************************************/

#include "interpreter.h"
#include "schema.h"
#include "pmsg.h"
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#define MAX_LINE_WIDTH 512
#define MAX_TOKEN_LEN 32
#define MAX_ATTRS 10

static const char* const t_database = "database";
static const char* const t_show = "show";
static const char* const t_pager = "pager";
static const char* const t_print = "print";
static const char* const t_create = "create";
static const char* const t_drop = "drop";
static const char* const t_table = "table";
static const char* const t_insert = "insert";
static const char* const t_into = "into";
static const char* const t_values = "values";
static const char* const t_select = "select";
static const char* const t_quit = "quit";
static const char* const t_help = "help";
static const char* const t_int = "int";

static FILE *in_s; /* input stream, default to stdin */

int b_search = 0;
int no_interface = 0;

static int init_with_options (int argc, char* argv[]) {
  char cmd_file[MAX_LINE_WIDTH] = "";
  char db_dir[MAX_LINE_WIDTH] = "";
  int c;

  msglevel = INFO;

  while ((c = getopt(argc, argv, "hnm:b:d:c:")) != -1)
    switch (c) {
    case 'h':
      printf("Usage: runtest [switches]\n");
      printf("\t-h           help, print this message\n");
      printf("\t-m [fewid]   msg level [fatal,error,warn,info,debug]\n");
      printf("\t-d db_dir    default to ./tests/testfront\n");
      printf("\t-c cmd file  eg. ./tests/testcmd.dbcmd, default to stdin\n");
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
      strcpy(db_dir, optarg);
    case 'c':
      strcpy(cmd_file, optarg);
      break;
    case 'b':
      switch (optarg[0])
      {
      case 't':
      case 'y': b_search = 1;break;
      case 'f':
      case 'n': b_search = 0;break;
      default:
        printf("Option -b requires arguments yes/no or true/false\n");
        abort();
      }
      break;
    case 'n':
      no_interface = 1;
      break;
    case '?':
      if (optopt == 'm' || optopt == 'd' || optopt == 'c' || optopt == 'b')
        printf("Option -%c requires an argument.\n", optopt);
      else if (isprint(optopt))
        printf("Unknown option `-%c'.\n", optopt);
      else
        printf("Unknown option character `\\x%x'.\n", optopt);
      abort();
    default:
      abort();
    }

  if (cmd_file[0] == '\0')
    in_s = stdin;
  else {
    struct stat in_stat;
    stat(cmd_file, &in_stat);
    if (S_ISREG(in_stat.st_mode)) {
      in_s = fopen( cmd_file, "r");
      if (in_s == NULL) {
        printf("Cannot open file: %s\n", cmd_file);
        in_s = stdin;
      } else
        put_msg(DEBUG, "file \"%s\" is open for read.\n", cmd_file);
    } else {
      printf("\n\"%s\" is not a regular file, fall back to stdin ...\n\n",
             cmd_file);
      in_s = stdin;
    }
  }

  if (in_s == stdin && !no_interface) {
    printf("Welcome to db2700 session\n");
    printf("  - Enter \"help\" for instructions\n");
    printf("  - Enter \"quit\" to leave the session\n");
  }

  if (db_dir[0] == '\0')
    strcpy(db_dir, "./tests/testfront");

  if (!set_system_dir(db_dir)) {
    put_msg(ERROR, "cannot set database at %s\n", db_dir);
    return 0;
  }

  return open_db();
}

static int next_token_max_len(char* token, size_t len) {
  char format[8];
  snprintf(format, sizeof format, "%%%zus", len);

  if (fscanf(in_s, format, token) != 1)
    return 0;
  else
    return 1;
}

static int next_token(char* token) {
  return next_token_max_len(token, MAX_TOKEN_LEN);
}

static char next_char() {
  char chars[2] = "";
  if (next_token_max_len(chars, 1))
    return chars[0];
  return 0;
}

/* Used to read till the end of a statement or a parenthesis.
   For simplicity, we require that a statement
    - does not go beyond a single line
    - ends with a semicolon
    - does not contain a semicolon */
static int read_till(char* stmnt_str, char c) {
  char format[16];
  snprintf(format, sizeof format, "%%%u[^%c\n];", MAX_LINE_WIDTH, c);
  if (fscanf(in_s, format, stmnt_str))
    return 1;
  else
    return 0;
}

static void skip_line() {
  char rest_of_line[MAX_LINE_WIDTH];
  fgets(rest_of_line, MAX_LINE_WIDTH, in_s);
}

void release_strs(char** strs, size_t n) {
  for (int i = 0; i < n; i++)
    if (strs[i]) {
      free(strs[i]);
      strs[i] = 0;
    }
}

/* split str into substrings, separated by char c,
   which is not white space.
   A substring allows exactly the given number of white spaces
   (only 0 or 1 for our usage). */
static int str_split(char* str, char c, char* substrs[],
		     int max_count, int n_white_space) {
  if ((n_white_space != 0) && (n_white_space != 1)) {
      put_msg(ERROR,
              "str_split: only 0 or 1 white space in substrings allowed.\n");
      return 0;
    }

  size_t count = 0;
  char *p;

  for (p = str; *p != '\0'; p++) {
    if (*p == c) {
      *p = '\0';
      if (++count > max_count) {
        put_msg(DEBUG, "str_split: too many substrings.\n");
        return 0;
      }
    }
  }
  count++;

  char str_tmp1[MAX_TOKEN_LEN] = "",
    str_tmp2[MAX_TOKEN_LEN] = "",
    str_tmp3[MAX_TOKEN_LEN] = "";
  p = str;

  for (int i = 0; i < count; i++, p += strlen(p) + 1) {
    int n_tmps = sscanf(p,"%s%s%s", str_tmp1, str_tmp2, str_tmp3);

    if (n_tmps != n_white_space + 1) {
      put_msg(DEBUG,
              "str_split: empty string or too many white spaces.\n");
      release_strs(substrs, i);
      return 0;
    }
    if (n_white_space == 0) {
      substrs[i] = malloc(strlen(str_tmp1) + 1);
      strcpy(substrs[i], str_tmp1);
    } else {
      substrs[i] = malloc(strlen(str_tmp1) + strlen(str_tmp2) + 2);
      strcpy(substrs[i], str_tmp1);
      strcat(substrs[i], " ");
      strcat(substrs[i], str_tmp2);
    }
  }

  return count;
}

static void error_near(char const* near_str) {
  if (near_str) {
    put_msg(ERROR, "There is an error near\n");
    put_msg(ERROR, "  ... >>>%s<<<", near_str);
  } else {
    put_msg(ERROR, "There is an error near\n  ... >>> ");
  }

  char rest_of_line[MAX_LINE_WIDTH];

  if (fgets(rest_of_line, MAX_LINE_WIDTH, in_s)) {
    append_msg(ERROR, rest_of_line);
  }
}

static void show_help_info() {
  printf("You can run the following commands:\n");
  printf(" - help\n");
  printf(" - quit\n");
  printf(" - # some comments in the rest of a line\n");
  printf(" - print text\n");
  printf(" - show database\n");
  printf(" - create table table_name ( field_name field_type, ... )\n");
  printf(" - drop table table_name (CAUTION: data will be deleted!!!)\n");
  printf(" - insert into table_name values ( value_1, value_2, ... )\n");
  printf(" - select attr1, attr2 from table_name where attr = int_val;\n\n");
}

static void quit() {
  close_db();
  if (in_s != stdin) fclose(in_s);
}

static void show() {
  char token[MAX_TOKEN_LEN];
  if (!next_token(token)) {
    put_msg(ERROR, "Show what?\n");
    return;
  }
  if (strcmp(token, t_database) == 0) {
    put_db_info(FORCE);
  }
  else
  if (strcmp(token, t_pager) == 0) {
    put_pager_profiler_info(FORCE);
  } else {
    put_msg(ERROR, "Cannot show \"%s\".\n", token);
    return;
  }
}

static void print_str() {
  char rest_of_line[MAX_LINE_WIDTH];

  if (fgets(rest_of_line, MAX_LINE_WIDTH, in_s))
    printf("%s", rest_of_line + 1);
}

static void create_tbl() {
  char tbl_name[MAX_TOKEN_LEN], token[MAX_TOKEN_LEN];

  if (!next_token(token)) {
    put_msg(ERROR, "Must create something.\n");
    return;
  }
  if (strcmp(token, t_table) != 0) {
    put_msg(ERROR, "Cannot create \"%s\".\n", token);
    return;
  }
  if (!next_token(tbl_name)) {
    put_msg(ERROR, "create table: missing table name.\n");
    return;
  }
  if (next_char() != '(') {
    error_near(0);
    return;
  }

  put_msg(DEBUG, "create table name: \"%s\".\n", tbl_name);

  char attrs_str[MAX_LINE_WIDTH];

  if (!read_till(attrs_str, ')')) {
    error_near(0);
    return;
  }
  if (next_char() != ')' && next_char() != ';') {
    error_near(0);
    skip_line();
    return;
  }
  skip_line();

  schema_p sch = get_schema(tbl_name);

  if (sch) {
    put_msg(ERROR, "Table \"%s\" already exists.\n", tbl_name);
    return;
  }

  char* attrs[MAX_ATTRS];
  int num_attrs = str_split(attrs_str, ',', attrs, MAX_ATTRS, 1);
  if (num_attrs == 0) {
    put_msg(ERROR, "create table %s: incorrect attributes\n", tbl_name);
    return;
  }

  sch = new_schema(tbl_name);

  char attr_name[MAX_TOKEN_LEN], attr_type[MAX_TOKEN_LEN];
  int str_attr_len;

  for (int i = 0; i < num_attrs; i++) {
    if (sscanf(attrs[i], "%s%s", attr_name, attr_type) != 2) {
      put_msg(ERROR, "create table %s: incorrect attribute \"%s\"\n",
	      tbl_name, attrs[i]);
      goto abort_create;
    }

    if (strcmp(attr_type, t_int) == 0) {
      add_field(sch, new_int_field(attr_name));
    } else if (sscanf(attr_type, "str[%d]", &str_attr_len) == 1) {
      add_field(sch, new_str_field(attr_name, str_attr_len));
    } else {
      put_msg(ERROR, "create table %s: unknown type \"%s\" for attribute \"%s\"\n",
	      tbl_name, attr_type, attr_name);
      goto abort_create;
    }
  }

  release_strs(attrs, num_attrs);
  return;

 abort_create:
  release_strs(attrs, num_attrs);
  remove_schema(sch);
}

static void drop_tbl() {
  char tbl_name[MAX_TOKEN_LEN], token[MAX_TOKEN_LEN];

  if (!next_token(token) || strcmp(token, t_table) != 0) {
    put_msg(ERROR, "drop what?\n");
    skip_line();
    return;
  }
  if (!next_token(tbl_name) || tbl_name[0] == '#') {
    put_msg(ERROR, "drop table: nothing to drop.\n");
    skip_line();
    return;
  }

  char *p = strchr(tbl_name, ';');
  if (p) {
    *p = 0;
  } else {
    if (next_char() != ';') {
      put_msg(ERROR, "drop table: syntax error (missing ';').\n");
      skip_line();
      return;
    }
  }

  skip_line();

  put_msg(DEBUG, "drop table name: \"%s\".\n", tbl_name);
  remove_table(get_table(tbl_name));
}

static record new_filled_record(schema_p sch, char* const* vals) {
  record r = new_record(sch);
  int int_val = 0;
  field_desc_p fld_d;
  size_t i = 0;
  for (fld_d = schema_first_fld_desc(sch);
       fld_d != NULL;
       fld_d = field_desc_next(fld_d), i++) {
    if (is_int_field(fld_d)) {
      char* p;
      int_val = strtol(vals[i], &p, 10);
      if (p == vals[i] || *p != '\0') {
        put_msg(ERROR, "\"%s\" is not an integer value.\n", vals[i]);
        release_record(r, sch);
        return 0;
      }
      assign_int_field(r[i], int_val);
    } else {
      assign_str_field(r[i], vals[i]);
    }
  }
  put_record_info(DEBUG, r, sch);
  return r;
}

static void insert_row() {
  char tbl_name[MAX_TOKEN_LEN], token[MAX_TOKEN_LEN];

  if (!next_token(token) || strcmp(token, t_into)) {
    put_msg(ERROR, "\"insert\" must be followed with \"into\".\n");
    return;
  }
  if (!next_token(tbl_name)) {
    put_msg(ERROR, "insert into: missing table name.\n");
    return;
  }
  if (!next_token(token) || strcmp(token, t_values)) {
    put_msg(ERROR, "\"insert into %s\" must be followed with \"values\".\n",
	    tbl_name);
    return;
  }
  if (next_char() != '(') {
    error_near(0);
    return;
  }

  put_msg(DEBUG, "insert into: \"%s\".\n", tbl_name);
  schema_p sch = get_schema(tbl_name);
  if (!sch) {
    put_msg(ERROR, "Schema \"%s\" does not exist.\n", tbl_name);
    skip_line();
    return;
  }

  char vals_str[MAX_LINE_WIDTH];

  if (!read_till(vals_str, ')')) {
    error_near(0);
    return;
  }
  if (next_char() != ')' && next_char() != ';') {
    error_near(0);
    skip_line();
    return;
  }
  skip_line();

  char* vals[MAX_ATTRS];
  int num_vals = str_split(vals_str, ',', vals, schema_num_flds(sch), 0);
  if (num_vals == 0) {
    put_msg(ERROR, "insert into %s: wrong number of values\n", tbl_name);
    return;
  }

  /* put_schema_info(DEBUG, sch); */

  record rec = new_filled_record(sch, vals);
  release_strs(vals, num_vals);

  if (rec) {
    append_record(rec, sch);
    release_record(rec, sch);
  }
}

/** @brief Select descriptor */
/** A selector descriptor contains the elements of a "select" statement.
*/
typedef struct select_desc {
  tbl_p from_tbl, right_tbl;
  char where_attr[MAX_TOKEN_LEN], where_op[3];
  int where_val;
  int num_attrs;
  char* attrs[MAX_ATTRS];
} select_desc;

static select_desc* new_select_desc() {
  select_desc* slct = malloc(sizeof (select_desc));
  for ( size_t i = 0; i < 10; i++ ) slct->attrs[i] = 0;
  slct->where_attr[0] = '\0';
  slct->where_op[0] = '\0';
  slct->num_attrs = 0;
  slct->from_tbl = 0;
  slct->right_tbl = 0;
  return slct;
}

static void release_select_desc(select_desc* slct) {
  if (!slct) return;
  release_strs(slct->attrs, slct->num_attrs);
  free(slct); slct = 0;
}

static select_desc* parse_select() {
  select_desc *slct = new_select_desc();
  char in_str[MAX_LINE_WIDTH] = "";
  char from_str[MAX_TOKEN_LEN] = "", join_with[MAX_TOKEN_LEN] = "";
  char *where_str, *join_str, *p;

  if (!read_till(in_str, ';')) {
    error_near("select ");
    return 0;
  }

  p = strstr(in_str, " from ");
  if (!p) {
    put_msg(ERROR, "select %s: from which table to select?\n", in_str);
    release_select_desc(slct);
    return 0;
  }
  *p = '\0';
  p += 6;
  if (sscanf(p, "%s", from_str) != 1) {
    put_msg(ERROR, "select from what?\n");
    release_select_desc(slct);
    return 0;
  }
  slct->from_tbl = get_table(from_str);
  if (!slct->from_tbl) {
    put_msg(ERROR, "select: table \"%s\" does not exist.\n", from_str);
    release_select_desc(slct);
    return 0;
  }

  slct->num_attrs = str_split(in_str, ',', slct->attrs, MAX_ATTRS, 0);
  if (slct->num_attrs == 0) {
    put_msg(ERROR, "select from %s: select what?\n", p);
    release_select_desc(slct);
    return 0;
  }

  p += strlen(from_str);
  join_str = strstr(p, " natural join ");
  if (join_str) {
    join_str += 14;
    put_msg(DEBUG, "from: \"%s\", natural join: \"%s\"\n",
            from_str, join_str);
    if (sscanf(join_str, "%s", join_with) != 1) {
      put_msg(ERROR, "natural join with \"%s\" is not supported.\n",
              join_str);
      release_select_desc(slct);
      return 0;
    }
    if (strcmp(from_str, join_str) == 0) {
      put_msg(ERROR, "natural join on same table is not supported.\n");
      release_select_desc(slct);
      return 0;
    }
    slct->right_tbl = get_table(join_with);
    if (!slct->right_tbl) {
      put_msg(ERROR, "natural join: table \"%s\" does not exist.\n",
              join_with);
      release_select_desc(slct);
      return 0;
    }
  }

  where_str = strstr(p, " where ");
  if (where_str) where_str += 7;

  put_msg(DEBUG, "from: \"%s\", where: \"%s\"\n", from_str, where_str);

  if (where_str) {
    if (sscanf(where_str, "%s %s %d",
               slct->where_attr, slct->where_op, &slct->where_val)
        != 3) {
      put_msg(ERROR, "query \"%s\" is not supported.\n", where_str);
      release_select_desc(slct);
      return 0;
    }
  }
  return slct;
}

static void select_rows() {
  select_desc *slct = parse_select();
  tbl_p where_tbl = 0, res_tbl = 0, join_tbl = 0;

  if (!slct) return;

  if (slct->right_tbl) {
    join_tbl = table_natural_join(slct->from_tbl, slct->right_tbl);
    if (!join_tbl) {
      release_select_desc(slct);
      return;
    }
  }

  if (slct->where_attr[0] != '\0' && slct->where_op[0] != '\0') {
    where_tbl = table_search(join_tbl ? join_tbl : slct->from_tbl,
                             slct->where_attr,
                             slct->where_op,
                             slct->where_val,
                             b_search);
    if (!where_tbl) {
      release_select_desc(slct);
      return;
    }
  }

  if (slct->attrs[0][0] == '*')
    table_display(where_tbl ? where_tbl
                  : (join_tbl ? join_tbl : slct->from_tbl));
  else {
    res_tbl = table_project(where_tbl ? where_tbl : slct->from_tbl,
                            slct->num_attrs, slct->attrs);
    table_display(res_tbl);
  }

  remove_table(join_tbl);
  remove_table(where_tbl);
  remove_table(res_tbl);
  release_select_desc(slct);
}


void interpret(int argc, char* argv[]) {
  if (!init_with_options(argc, argv))
    exit(EXIT_FAILURE);

  char token[MAX_TOKEN_LEN];

  while (!feof(in_s)) {
    if (in_s == stdin && !no_interface)
      printf("db2700> ");
    if (!next_token(token)) {
      put_msg(ERROR, "Getting input failed\n");
      exit(EXIT_FAILURE);
    }
    put_msg(DEBUG, "current token is \"%s\".\n", token);
    if (strcmp(token, t_quit) == 0) { quit(); break;}
    if (token[0] == '#')
      { skip_line(); continue; }
    if (strcmp(token, t_help) == 0)
      { show_help_info(); continue; }
    if (strcmp(token, t_show) == 0)
      { show(); continue; }
    if (strcmp(token, t_print) == 0)
      { print_str(); continue; }
    if (strcmp(token, t_create) == 0)
      { create_tbl(); continue; }
    if (strcmp(token, t_drop) == 0)
      { drop_tbl(); continue; }
    if (strcmp(token, t_insert) == 0)
      { insert_row(); continue; }
    if (strcmp(token, t_select) == 0)
      { select_rows(); continue; }
    error_near(token);
  }
}
