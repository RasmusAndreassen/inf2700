#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sqlite3.h>

#define MIN(x,y) (x<y)?x:y

sqlite3 *db = 0;
char* spec_buff = 0;
char const* spec = 0;
char* query_buff = 0;

int prepare_test(char const* const test_dir, char const* const test_file_name);
void run_test(void);
void terminate_test(void);

int main (int argc, char* argv[]) {
  if (argc != 3) {
    printf("Please provide your test dir and your test file in the dir.\n");
    printf("For example, run './testsql test test_queries_messages'\n");
    printf("Note: dir and file names cannot contain white spaces.\n");
    return -1;
  }

  if (!prepare_test(argv[1], argv[2]))
    return -1;

  run_test();

  terminate_test();

  return 0;
}

size_t file_size(FILE* file) {
  long current_pos = ftell(file);
  fseek(file, 0L, SEEK_END);
  long size = ftell(file);
  fseek(file, current_pos, SEEK_SET);
  return (size_t) size;
}

char* read_file(char const* const file_name) {
  FILE *file = fopen(file_name, "r");

  if (!file) return 0;

  size_t n_bytes = file_size(file);

  char* buff = malloc(n_bytes + 1);
  if (buff) {
    fread(buff, 1, n_bytes, file);
    buff[n_bytes] = '\0';
  }

  fclose(file);

  return buff;
}

char const* next_line(char const* const str) {
  char* ch_p = strchr(str, '\n');
  return ch_p ? ++ch_p : 0;
}

char const* skip_chars(char const* const str,
                       const char ch) {
  if (!str) return 0;

  char const* ch_p = str;
  while (*ch_p == ch) ++ch_p;
  return ch_p;
}

int parse_spec_token(char const* const token_name,
		     char* const token,
		     size_t token_len_max) {
  if (!spec) return 0;

  char format[64];
  snprintf(format, sizeof format,
	   "%s: %%%zus\\n", token_name, token_len_max);

  if (sscanf(spec, format, token) != 1)
    return 0;
  else
    return 1;
}

int prepare_test(char const* const test_dir,
                 char const* const test_file_name) {
  if (chdir(test_dir) == -1) {
    printf("Cannot reach test dir \"%s\"\n", test_dir);
    return 0;
  }

  spec_buff = read_file(test_file_name);
  if (!spec_buff) {
    printf("Cannot open test file \"%s\"\n", test_file_name);
    return 0;
  }
  spec = spec_buff;

  char testdb_file_name[32];
  if (!parse_spec_token("testdb",
			testdb_file_name,
			sizeof testdb_file_name)) {
    printf("Please provide your test database in \"test_file_name\"\n");
    return 0;
  }
  spec = next_line(spec);

// TODO: might not be finished

  int res_code =
  sqlite3_open(testdb_file_name, &db);
  if (res_code != SQLITE_OK) {
    printf("%s\n", sqlite3_errmsg(db));
    terminate_test();
    return 0;
  }

  char query_file_name[32];
  if (!parse_spec_token("query_file: %s",
			query_file_name,
			sizeof query_file_name)) {
    printf("Please provide your query file in \"test_file_name\"\n");
    terminate_test();
    return 0;
  }
  spec = next_line(spec);

  query_buff = read_file(query_file_name);
  if (!query_buff) {
    printf("Cannot open query file \"%s\"\n", query_file_name);
    terminate_test();
    return 0;
  }

  return 1;
}

int get_next_query(char* const query_name,
		               const size_t query_name_len,
                   char* const expectation,
		               const size_t expectation_len) {
  if (!spec) return 0;

  spec = strstr(spec, "query:");

  if (!(spec && parse_spec_token("query",
				 query_name,
				 query_name_len)))
    return 0;

  spec = next_line(spec);
  char const* expect_begin = skip_chars(spec, '-') + 1;
  char const*  expect_end = strstr(expect_begin, "----");
  spec = next_line(expect_end);

  if (expect_begin == expect_end) {
    *expectation = '\0';
    return 1;
  }

  size_t expect_len = expect_end - expect_begin;
  if (expect_len >= expectation_len) {
    printf("The expectation of query '%s' is too long.\n",
	    query_name);
    return 0;
  }

  strncpy(expectation, expect_begin, expect_len);
  expectation[expect_len] = '\0';

  return 1;
}

char const* get_query_str(char const* const query_name) {
  char const* query_p = strstr(query_buff, query_name);

  return query_p ? next_line(query_p) : 0;
}

int query_exec(char const* const query_name,
               char* const query_res,
	             const size_t res_len,
               char* const query_err,
	             const size_t err_len)
{
  char const* query_p = get_query_str(query_name);
  if (!query_p) {
    snprintf(query_err, err_len,
	     "test \"%s\" not found.",
	     query_name);
    return 0;}

  size_t query_len = strstr(query_p, ";\n") - query_p + 1;


 
  sqlite3_stmt *statement;
  int out_code = sqlite3_prepare(db, query_p, query_len, &statement, NULL); // generate a statement
  if (out_code != SQLITE_OK) {
    snprintf(query_err, err_len, "test \"%s\" error: %s", query_name, sqlite3_errmsg(db)); //check for errors
    return 0;
  }

  char *stylus = query_res,         // stylus to write output to query_res without changing the value of query_res itself
       *end    = query_res+res_len; // future-proof check for end of result buffer

  while ((sqlite3_step(statement) == SQLITE_ROW) && (stylus < end)) { // write until no more output or no more buffer space

    for (int i = 0; (i < sqlite3_column_count(statement)) && (stylus < end); i++) {
      stylus +=
      sprintf(stylus, "|%s", sqlite3_column_text(statement, i));  // this prints every column in every row
    }

    stylus +=
    sprintf(stylus, "|\n"); // end of the row

    
  }
  

  sqlite3_finalize(statement);  // tidyup

  return 1;
}

void test_query(char const* const query_name,
                char const* const expectation) {
  char query_res[1024] = "";
  char query_err[1024] = "";

  if (!query_exec(query_name,
		  query_res, sizeof query_res,
		  query_err, sizeof query_err)) {
    printf("%s\n", query_err);
    return;
  }

  if (strcmp(query_res, expectation) == 0) {
    printf("test \"%s\" succeeded\n", query_name);
  } else {
    printf("test \"%s\" failed\n", query_name);
    printf("exepected:\n%s\n", expectation);
    printf("got:\n%s\n", query_res);
  }
}

void run_test() {
  char query_name[128], expectation[1024];
  while (get_next_query(query_name, sizeof query_name,
			expectation, sizeof expectation)) {
    test_query(query_name, expectation);
  }
}

void terminate_test() {

  while (sqlite3_close(db) != SQLITE_OK);

  if (spec_buff) {
    free(spec_buff);
    spec_buff = 0;
  }
  if (query_buff) {
    free(query_buff);
    query_buff = 0;
  }
}
