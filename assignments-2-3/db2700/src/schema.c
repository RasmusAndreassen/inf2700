/***********************************************************
 * Schema for assignments in the Databases course INF-2700 *
 * UIT - The Arctic University of Norway                   *
 * Author: Weihai Yu                                       *
 ************************************************************/

#include "schema.h"
#include "pmsg.h"
#include <string.h>

static void display_record(record,schema_p);

/** @brief Field descriptor */
typedef struct field_desc_struct {
  char *name;        /**< field name */
  field_type type;   /**< field type */
  int len;           /**< field length (number of bytes) */
  int offset;        /**< offset from the beginning of the record */
  field_desc_p next; /**< next field_desc of the table, NULL if no more */
} field_desc_struct;

/** @brief Table/record schema */
/** A schema is a linked list of @ref field_desc_struct "field descriptors".
    All records of a table are of the same length.
*/
typedef struct schema_struct {
  char *name;           /**< schema (table) name */
  field_desc_p first;   /**< first field_desc */
  field_desc_p last;    /**< last field_desc */
  int num_fields;       /**< number of fields in the table */
  int len;              /**< record length */
  tbl_p tbl;            /**< table descriptor */
} schema_struct;

/** @brief Table descriptor */
/** A table descriptor allows us to find the schema and
    run-time infomation about the table.
 */
typedef struct tbl_desc_struct {
  schema_p sch;      /**< schema of this table. */
  int num_records;   /**< number of records this table has. */
  page_p current_pg; /**< current page being accessed. */
  tbl_p next;        /**< next tbl_desc in the database. */
} tbl_desc_struct;


/** @brief Database tables*/
tbl_p db_tables; /**< a linked list of table descriptors */

void put_field_info(pmsg_level level, field_desc_p f) {
  if (!f) {
    put_msg(level,  "  empty field\n");
    return;
  }
  put_msg(level, "  \"%s\", ", f->name);
  if (is_int_field(f))
    append_msg(level,  "int ");
  else
    append_msg(level,  "str ");
  append_msg(level, "field, len: %d, offset: %d, ", f->len, f->offset);
  if (f->next)
    append_msg(level,  ", next field: %s\n", f->next->name);
  else
    append_msg(level,  "\n");
}

void put_schema_info(pmsg_level level, schema_p s) {
  if (!s) {
    put_msg(level,  "--empty schema\n");
    return;
  }
  field_desc_p f;
  put_msg(level, "--schema %s: %d field(s), totally %d bytes\n",
          s->name, s->num_fields, s->len);
  for (f = s->first; f; f = f->next)
    put_field_info(level, f);
  put_msg(level, "--\n");
}

void put_tbl_info(pmsg_level level, tbl_p t) {
  if (!t) {
    put_msg(level,  "--empty tbl desc\n");
    return;
  }
  put_schema_info(level, t->sch);
  put_file_info(level, t->sch->name);
  put_msg(level, " %d blocks, %d records\n",
          file_num_blocks(t->sch->name), t->num_records);
  put_msg(level, "----\n");
}

void put_record_info(pmsg_level level, record r, schema_p s) {
  field_desc_p f;
  size_t i = 0;
  put_msg(level, "Record: ");
  for (f = s->first; f; f = f->next, i++) {
    if (is_int_field(f))
      append_msg(level,  "%d", *(int *)r[i]);
    else
      append_msg(level,  "%s", (char *)r[i]);

    if (f->next)
      append_msg(level,  " | ");
  }
  append_msg(level,  "\n");
}

void put_db_info(pmsg_level level) {
  char *db_dir = system_dir();
  if (!db_dir) return;
  put_msg(level, "======Database at %s:\n", db_dir);
  for (tbl_p tbl = db_tables; tbl; tbl = tbl->next)
    put_tbl_info(level, tbl);
  put_msg(level, "======\n");
}

field_desc_p new_int_field(char const* name) {
  field_desc_p res = malloc(sizeof (field_desc_struct));
  res->name = strdup(name);
  res->type = INT_TYPE;
  res->len = INT_SIZE;
  res->offset = 0;
  res->next = 0;
  return res;
}

field_desc_p new_str_field(char const* name, int len) {
  field_desc_p res = malloc(sizeof (field_desc_struct));
  res->name = strdup(name);
  res->type = STR_TYPE;
  res->len = len;
  res->offset = 0;
  res->next = 0;
  return res;
}

static void release_field_desc(field_desc_p f) {
  if (f) {
    free(f->name);
    free(f);
    f = 0;
  }
}

int is_int_field(field_desc_p f) {
  return f ? (f->type == INT_TYPE) : 0;
}

field_desc_p field_desc_next(field_desc_p f) {
  if (f)
    return f->next;
  else {
    put_msg(ERROR, "field_desc_next: NULL field_desc_next.\n");
    return 0;
  }
}

static schema_p make_schema(char const* name) {
  schema_p res = malloc(sizeof (schema_struct));
  res->name = strdup(name);
  res->first = 0;
  res->last = 0;
  res->num_fields = 0;
  res->len = 0;
  return res;
}

/** Release the memory allocated for the schema and its field descriptors.*/
static void release_schema(schema_p sch) {
  if (!sch) return;

  field_desc_p f, nextf;
  f = sch->first;
  while (f) {
    nextf = f->next;
    release_field_desc(f);
    f = nextf;
  }
  free(sch->name);
  free(sch);
}

char const* const schema_name(schema_p sch) {
  if (sch)
    return sch->name;
  else {
    put_msg(ERROR, "schema_name: NULL schema.\n");
    return 0;
  }
}

field_desc_p schema_first_fld_desc(schema_p sch) {
  if (sch)
    return sch->first;
  else {
    put_msg(ERROR, "schema_first_fld_desc: NULL schema.\n");
    return 0;
  }
}

field_desc_p schema_last_fld_desc(schema_p sch) {
  if (sch)
    return sch->last;
  else {
    put_msg(ERROR, "schema_last_fld_desc: NULL schema.\n");
    return 0;
  }
}

int schema_num_flds(schema_p sch) {
  if (sch)
    return sch->num_fields;
  else {
    put_msg(ERROR, "schema_num_flds: NULL schema.\n");
    return -1;
  }
}

int schema_len(schema_p sch) {
  if (sch)
    return sch->len;
  else {
    put_msg(ERROR, "schema_len: NULL schema.\n");
    return -1;
  }
}

const char tables_desc_file[] = "db.db"; /***< File holding table descriptors */

/** @b concat_names
 * 
 * returns pointer to an allocated string consisting of the two names separated by
 * @ref sep. Is caller responsibility to free pointer
 * 
 * @param sep
 */
static char* concat_names(char const* name1, char const* sep, char const* name2) {
  char *res = malloc((sizeof name1) + (sizeof sep) + (sizeof name2) + 1);
  strcpy(res, name1);
  strcat(res, sep);
  strcat(res, name2);
  return res;
}

static void save_tbl_desc(FILE *fp, tbl_p tbl) {
  schema_p sch = tbl->sch;
  fprintf(fp, "%s %d\n", sch->name, sch->num_fields);
  field_desc_p fld = schema_first_fld_desc(sch);
  while (fld) {
    fprintf(fp, "%s %d %d %d\n",
            fld->name, fld->type, fld->len, fld->offset);
    fld = fld->next;
  }
  fprintf(fp, "%d\n", tbl->num_records);
}

/** @b save_tbl_descs
 * 
 * write all table descriptors in memory back to drive
 */
static void save_tbl_descs() {
  /* backup the descriptors first in case we need some manual investigation */
  char *tbl_desc_backup = concat_names("__backup", "_", tables_desc_file);
  rename(tables_desc_file, tbl_desc_backup);
  free(tbl_desc_backup);

  FILE *dbfile = fopen(tables_desc_file, "w");
  tbl_p tbl = db_tables, next_tbl = 0;
  while (tbl) {
    save_tbl_desc(dbfile, tbl);
    release_schema(tbl->sch);
    next_tbl = tbl->next;
    free(tbl);
    tbl = next_tbl;
  }
  fclose(dbfile);
}

/** @b read_tbl_descs
 * 
 * reads table descriptors into memory
 */
static void read_tbl_descs() {
  FILE *fp = fopen(tables_desc_file, "r");
  if (!fp) return;
  char name[30] = "";
  schema_p sch;
  field_desc_p fld;
  int num_flds = 0, fld_type, fld_len;
  while (!feof(fp)) {
    if (fscanf(fp, "%s %d\n", name, &num_flds) < 2) {
      fclose(fp);
      return;
    }
    sch = new_schema(name);
    for (size_t i = 0; i < num_flds; i++) {
      fscanf(fp, "%s %d %d", name, &(fld_type), &(fld_len));
      switch (fld_type) {
      case INT_TYPE:
        fld = new_int_field(name);
        break;
      case STR_TYPE:
        fld = new_str_field(name, fld_len);
        break;
      }
      fscanf(fp, "%d\n", &(fld->offset));
      add_field(sch, fld);
    }
    fscanf(fp, "%d\n", &(sch->tbl->num_records));
  }
  db_tables = sch->tbl;
  fclose(fp);
}

/** @b open_db
 * 
 * initialises pager, and reads the table descriptors into memory
 */
int open_db(void) {
  pager_terminate(); /* first clean up for a fresh start */
  pager_init();
  read_tbl_descs();
  return 1;
}

/** @b close_db
 * 
 * writes the table descriptors to drive, and terminates pager
 */
void close_db(void) {
  save_tbl_descs();
  db_tables = 0;
  pager_terminate();
}

/** @b new_schema
 * 
 * Returns a new schema with a pre-allocated, pre-inserted
 * table descriptor
 */
schema_p new_schema(char const* name) {
  tbl_p tbl = malloc(sizeof (tbl_desc_struct));
  tbl->sch = make_schema(name);
  tbl->sch->tbl = tbl;
  tbl->num_records = 0;
  tbl->current_pg = 0;
  tbl->next = db_tables;
  db_tables = tbl;
  return tbl->sch;
}

/** @b get_table
 * 
 * Finds and returns the first table which schema name matches
 * the given name, otherwise returns @b NULL
 * 
 * @param name
 */
tbl_p get_table(char const* name) {
  for (tbl_p tbl = db_tables; tbl; tbl = tbl->next)
    if (strcmp(name, tbl->sch->name) == 0)
      return tbl;
  return NULL;
}

/** @b get_schema
 * 
 * Finds and returns the first schema which name matches
 * the given name, otherwise returns @b NULL
 * 
 * @param name
 */
schema_p get_schema(char const* name) {
  tbl_p tbl = get_table(name);
  if (tbl) return tbl->sch;
  else return 0;
}

void remove_table(tbl_p t) {
  if (!t) return;

  for (tbl_p tbl = db_tables, prev = 0;
       tbl;
       prev = tbl, tbl = tbl->next)
    if (tbl == t) {
      if (t == db_tables)
        db_tables = t->next;
      else
        prev->next = t->next;

      close_file(t->sch->name);
      char *tbl_backup = concat_names("_", "_", t->sch->name);
      rename(t->sch->name, tbl_backup);
      free(tbl_backup);
      release_schema(t->sch);
      free(t);
      return;
    }
}

void remove_schema(schema_p s) {
  if (s) remove_table(s->tbl);
}

/** @b dup_field
 * 
 * creates a new field that is a copy of f,
 * but doesn't add it to any table
 * 
 * @param f source
 */
static field_desc_p dup_field(field_desc_p f) {
  field_desc_p res = malloc(sizeof (field_desc_struct));
  res->name = strdup(f->name);
  res->type = f->type;
  res->len = f->len;
  res->offset = 0;
  res->next = 0;
  return res;
}

/** @b copy_schema
 * 
 * returns a brand new schema with the same fields as the old schema
 * 
 * @param s           source to copy
 * @param dest_name   name of new schema
 */
static schema_p copy_schema(schema_p s, char const* dest_name) {
  if (!s) return 0;
  schema_p dest = new_schema(dest_name);
  for (field_desc_p f = s->first; f; f = f->next)
    add_field(dest, dup_field(f));
  return dest;
}

/** @b copy_schema
 * 
 * returns the the field of 
 * 
 * @param s      source to search
 * @param name   name of new schema
 */
static field_desc_p get_field(schema_p s, char const* name) {
  for (field_desc_p f = s->first; f; f = f->next)
    if (strcmp(f->name, name) == 0) return f;
  return 0;
}

/** @b tmp_schema_name
 * 
 * returns a temporary name based on the operation name and the table name
 * 
 * @param op_name  operation name
 * @param name     table name
 */
static char* tmp_schema_name(char const* op_name, char const* name) {
  char *res = malloc((sizeof op_name) + (sizeof name) + 10);
  int i = 0;
  do
    sprintf(res, "%s__%s_%d", op_name, name, i++);
  while (get_schema(res));

  return res;
}

/** @b make_sub_schema
 * @param s          source schema
 * @param num_fields number of fields to duplicate
 * @param fields     name of fields to duplicate
 */
static schema_p make_sub_schema(schema_p s, int num_fields, char *fields[]) {
  if (!s) return 0;

  char *sub_sch_name = tmp_schema_name("project", s->name);
  schema_p res = new_schema(sub_sch_name);
  free(sub_sch_name);
  
  field_desc_p f = 0;
  for (size_t i= 0; i < num_fields; i++) {
    f = get_field(s, fields[i]);
    if (f)
      add_field(res, dup_field(f));
    else {
      put_msg(ERROR, "\"%s\" has no \"%s\" field\n",
              s->name, fields[i]);
      remove_schema(res);
      return 0;
    }
  }
  return res;
}

/** @b add_field
 * 
 * adds a new field to the target schema
 * 
 * @param s  destination schema
 * @param f  field to add to schema
 */
int add_field(schema_p s, field_desc_p f) {
  if (!s) return 0;
  if (s->len + f->len > BLOCK_SIZE - PAGE_HEADER_SIZE) {
    put_msg(ERROR,
            "schema already has %d bytes, adding %d will exceed limited %d bytes.\n",
            s->len, f->len, BLOCK_SIZE - PAGE_HEADER_SIZE);
    return 0;
  }
  if (s->num_fields == 0) {
    s->first = f;
    f->offset = 0;
  }
  else {
    s->last->next = f;
    f->offset = s->len;
  }
  s->last = f;
  s->num_fields++;
  s->len += f->len;
  return s->num_fields;
}

/** @b new_record
 * 
 * returns a new record
 * 
 * @param s 
 */
record new_record(schema_p s) {
  if (!s) {
    put_msg(ERROR,  "new_record: NULL schema!\n");
    exit(EXIT_FAILURE);
  }
  record res = malloc((sizeof (void *)) * s->num_fields);

  /* allocate memory for the fields */
  field_desc_p f;
  size_t i = 0;
  for (f = s->first; f; f = f->next, i++) {
    res[i] =  malloc(f->len);
  }
  return res;
}

/** @b release_record
 * 
 * frees the memory of the record, based on the details in the schema.
 * make sure they match!
 * 
 * @param r  record to free
 * @param s  containing schema
 */
void release_record(record r, schema_p s) {
  if (!r) {
    put_msg(ERROR,  "release_record: NULL record!\n");
    return;
  }
  if (!s) {
    put_msg(ERROR, "release_record: NULL schema!\n");
  }
  for (size_t i = 0; i < s->num_fields; i++)
    free(r[i]);
  free(r);
  r = 0;
}

/** @b assign_int_field
 * 
 * assigns the integer value of int_val to the field field_p.
 * field should be field(attribute) of record
 * 
 * @param field_p
 * @param int_val
 */
void assign_int_field(void const* field_p, int int_val) {
  *(int *)field_p = int_val;
}

/** @b assign_int_field
 * 
 * copies the string in str_val to the field field_p.
 * field should be field(attribute) of record
 * 
 * @param field_p
 * @param str_val
 */
void assign_str_field(void* field_p, char const* str_val) {
  strcpy(field_p, str_val);
}

/** @b fill_record
 * 
 * fills the record with the provided values, according to the schema format.
 * make sure that record and schema match!
 * 
 * @param r   record to assign values to
 * @param s   schema for format reference
 */
int fill_record(record r, schema_p s, ...) {
  if (!(r && s)) {
    put_msg(ERROR,  "fill_record: NULL record or schema!\n");
    return 0;
  }
  va_list vals;
  va_start(vals, s);
  field_desc_p f;
  size_t i = 0;
  for (f = s->first; f; f = f->next, i++) {
    if (is_int_field(f))
      assign_int_field(r[i], va_arg(vals, int));
    else
      assign_str_field(r[i], va_arg(vals, char*));
  }
  return 1;
}

/** @b fill_sub_record
 * 
 * linearly fills all fields of destination record with values from source record,
 * based on their respective shema
 * 
 * @param dest_r
 * @param dest_s
 * @param src_r
 * @param src_s
 */
static void fill_sub_record(record dest_r, schema_p dest_s,
                            record src_r, schema_p src_s) {
  field_desc_p src_f, dest_f;
  size_t i = 0, j = 0;
  for (dest_f = dest_s->first; dest_f; dest_f = dest_f->next, i++) {
    for (j = 0, src_f = src_s->first;
         strcmp(src_f->name, dest_f->name) != 0;
         j++, src_f = src_f->next)
      ;
    if (is_int_field(dest_f))
      assign_int_field(dest_r[i], *(int *)src_r[j]);
    else
      assign_str_field(dest_r[i], (char *)src_r[j]);
  }
}

/** @b equal_record
 * 
 * compares the given records by each entry based on schema
 * 
 * @param r1 record to be compared
 * @param r2 record to be compared
 * @param s  record schema
 */
int equal_record(record r1, record r2, schema_p s) {
  if (!(r1 && r2 && s)) {
    put_msg(ERROR,  "equal_record: NULL record or schema!\n");
    return 0;
  }

  field_desc_p fd;
  size_t i = 0;;
  for (fd = s->first; fd; fd = fd->next, i++) {
    if (is_int_field(fd)) {
      if (*(int *)r1[i] != *(int *)r2[i])
        return 0;
    }
    else {
      if (strcmp((char *)r1[i], (char *)r2[i]) != 0)
        return 0;
    }
  }
  return 1;
}
/** @b set_tbl_position
 * 
 * sets the current r/w-position to either beginning or end of the table
 * 
 * @param t table to set position of
 * @param pos TBL_BEG for beginning of table, TBL_END for end
 */
void set_tbl_position(tbl_p t, tbl_position pos) {
  switch (pos) {
  case TBL_BEG:
    {
      t->current_pg = get_page(t->sch->name, 0);
      page_set_pos_begin(t-> current_pg);
    }
    break;
  case TBL_END:
    t->current_pg = get_page_for_append(t->sch->name);
  }
}

/** @b eot
 * returns true if the position is at the end of the table
 */
int eot(tbl_p t) {
  return (peof(t->current_pg));
}

/** check if the the current position is valid */
static int page_valid_pos_for_get_with_schema(page_p p, schema_p s) {
  return (page_valid_pos_for_get(p, page_current_pos(p))
          && (page_current_pos(p) - PAGE_HEADER_SIZE) % s->len == 0);
}

/** check if the the current position is valid */
static int page_valid_pos_for_put_with_schema(page_p p, schema_p s) {
  return (page_valid_pos_for_put(p, page_current_pos(p), s->len)
          && (page_current_pos(p) - PAGE_HEADER_SIZE) % s->len == 0);
}
/** @b get_page_for_next_record
 * 
 * finds and returns the page containing the next record
 * 
 * @param s
 */
static page_p get_page_for_next_record(schema_p s) {
  page_p pg = s->tbl->current_pg;
  if (peof(pg)) return 0;
  if (eop(pg)) {
    unpin(pg);
    pg = get_next_page(pg);
    if (!pg) {
      put_msg(FATAL, "get_page_for_next_record failed at block %d\n",
              page_block_nr(pg) + 1);
      exit(EXIT_FAILURE);
    }
    page_set_pos_begin(pg);
    s->tbl->current_pg = pg;
  }
  return pg;
}
/** @b get_page_record
 * 
 * fills in r with the record at the current position of the page
 * 
 * @param r to be written to
 * @param s for reference
 * @param p page to read from
 */
static int get_page_record(page_p p, record r, schema_p s) {
  if (!p) return 0;
  if (!page_valid_pos_for_get_with_schema(p, s)) {
    put_msg(FATAL, "try to get record at invalid position.\n");
    exit(EXIT_FAILURE);
  }
  field_desc_p fld_desc;
  size_t i = 0;
  for (fld_desc = s->first; fld_desc;
       fld_desc = fld_desc->next, i++)
    if (is_int_field(fld_desc))
      assign_int_field(r[i], page_get_int(p));
    else
      page_get_str(p, r[i], fld_desc->len);
  return 1;
}

/** @b get_record
 * 
 * fully abstracts away pager, read record from current position of schema
 * 
 * @param r record space to write to
 * @param s schema to read from
 */
int get_record(record r, schema_p s) {
  page_p pg = get_page_for_next_record(s);
  return pg ? get_page_record(pg, r, s) : 0;
}

static int int_eq(int x, int y) {
  return x == y;
}

static int int_l(int x, int y) {
  return y < x;
}

static int int_g(int x, int y) {
  return y > x;
}

static int int_le(int x, int y) {
  return y <= x;
}

static int int_ge(int x, int y) {
  return y >= x;
}

static int int_neq(int x, int y) {
  return x != y;
}

static int find_record_int_val(record r, schema_p s, int offset,
                               int (*op) (int, int), int val) {
  page_p pg = get_page_for_next_record(s);
  if (!pg) return 0;
  int pos, rec_val;
  for (; pg; pg = get_page_for_next_record(s)) {
    pos = page_current_pos(pg);
    rec_val = page_get_int_at (pg, pos + offset);
    if ((*op) (val, rec_val)) {
      page_set_current_pos(pg, pos);
      get_page_record(pg, r, s);
      return 1;
    }
    else
      page_set_current_pos(pg, pos + s->len);
  }
  return 0;
}

typedef struct reference_of_page_extremities{
  page_p pg;
  int high;
  int low;
} pageref_t;



static int lfind_record_int_val(record r, schema_p s, int offset, int val)
{
  page_p pg = get_page_for_next_record(s);
  if (!pg) return 0;
  int pos, rec_val;
  for (; pg; pg = get_page_for_next_record(s)) {
    pos = page_current_pos(pg);
    rec_val = page_get_int_at (pg, pos + offset);
    if (rec_val == val) {
      page_set_current_pos(pg, pos);
      get_page_record(pg, r, s);
      return 1;
    }
    else
      return 0;
  }
  return 0;
}

// this is a shorthand for taking the average index of two records,
// with their positions as arguments
#define I(pos)      (pos-20)/s->len
#define P(ind)      ind*s->len+20
#define AVG(x1,x2)  (x1+x2)/2
#define pAVG(p1,p2) P(AVG(I(p1),I(p2)))

static int bfind_page_first_int_val(schema_p s, pageref_t ref, int offset, int val)
{
  int rec_val, pos=0;

  rec_val = page_get_int_at(ref.pg, ref.high+offset);

  if (rec_val == val) {
    rec_val = page_get_int_at(ref.pg, ref.high-s->len+offset);
    if (rec_val != val)
      return ref.high;
  }

  while (ref.high != ref.low) {
    pos = pAVG(ref.low,ref.high);
    if (pos == ref.low)
      break;
    rec_val = page_get_int_at(ref.pg, pos+offset);
    
    if (rec_val == val) {
      if (pos > page_seek(ref.pg, P_BEG, 0)) {
        rec_val = page_get_int_at(ref.pg, pos - s->len + offset);
        if (rec_val != val)
          return pos;
        else {
          ref.high = pos;
          continue;
        }
      } else 
        return pos;
    } else if (rec_val < val) {
      ref.low = pos;
      continue;
    } else if (rec_val > val) {
      ref.high = pos;
      continue;
    }
  }

  return -1;
}
// TODO
static int bfind_first_int_val(schema_p s, int offset, int val)
{

  int pos, cpos, rec_val, high, low, mid;
  page_p pg, cpg;
  pageref_t ref;

  // first check first page
  ref.pg =
  pg = get_page(s->name, 0);
  if (!pg)
    goto not;

  ref.low =
  pos = page_seek(pg, P_BEG, 0);
  rec_val = page_get_int_at(pg, pos + offset);

  if (val < rec_val) { // when the reference value is lower than the lowest value
    goto not;
  } else if (rec_val == val) { // will be beginning of linear result
    goto found;
  }

  ref.high =
  pos = page_seek(pg, P_END, -s->len);
  low = page_block_nr(pg);
  rec_val = page_get_int_at(pg, pos + offset);

  if (val <= rec_val) { // reference value is within first page
    pos = bfind_page_first_int_val(s, ref, offset, val);
    if (pos < 0)
      goto not;
    goto found;
  }

  // second check last page

  unpin(pg); // release page, is no longer necessary
  ref.pg =
  pg = get_page(s->name, -1); // get last page
  high = page_block_nr(pg);
  if (high == low)
    goto not;

  ref.high =
  pos = page_seek(pg, P_END, -s->len);
  rec_val = page_get_int_at(pg, pos + offset);

  if (val > rec_val) { // reference larger than largest value
    goto not;
  } else if (val == rec_val) { // needs extra check, because this would otherwise be the end of a linear search
    
    if (pos > page_seek(pg, P_BEG, 0)) {
      if(val != page_get_int_at(pg, pos - s->len + offset)) {
        goto found;
      }
    } else {
      cpg = get_page(s->name, page_block_nr(pg));
      cpos = page_seek(cpg, P_END, -s->len);
      rec_val = page_get_int_at(cpg, cpos+offset);

      if (rec_val != val) {
        goto found;
      } else {
        unpin(pg);
        high--;
        ref.pg =
        pg = cpg;
        ref.high = cpos;
        ref.low =
        pos = page_seek(cpg, P_BEG, 0);
        rec_val = page_get_int_at(cpg, cpos+offset);
        if (rec_val != val) {
          pos = bfind_page_first_int_val(s, ref, offset, val);
          goto found;
        } else {
          goto loop; 
          /** this is the case where the highest value of last page is
           * @em also lowest value of last page, so no need to check again
           */
        }
      }
    }
    
  }

  ref.low = 
  pos = page_seek(pg, P_BEG, 0);
  rec_val = page_get_int_at(pg, pos + offset);
  
  if (val > rec_val) {
    pos = bfind_page_first_int_val(s, ref, offset, val);
    if (pos < 0)
      goto not;
    s->tbl->current_pg = pg;
    page_set_current_pos(pg, pos);
    return 1;
  } else if (val == rec_val) {
    cpg = get_page(s->name, high-1);
    cpos = page_seek(cpg, P_END, -s->len);
    
    if (val != page_get_int_at(cpg, cpos+offset))
      goto found;
    else {
      unpin(pg);
      pg = cpg;
      ref.pg = pg;
      ref.high = cpos;
      ref.low =
      pos = page_seek(cpg, P_BEG, 0);

      if (val != page_get_int_at(cpg, pos+offset)) {
        pos = bfind_page_first_int_val(s, ref, offset, val);
      
        goto found;
      }
        // if searches to the previous page were to continue after this,
        // the algorithm would likely end up performing a linear search.
    }
  }

loop:

  while (high != low){
    unpin(pg);
    mid = (high+low)/2;
    ref.pg =
    pg = get_page(s->name, mid);

    ref.low =
    pos = page_seek(pg, P_BEG, 0);
    rec_val = page_get_int_at(pg, pos + offset);

    if (val < rec_val) {
      high = page_block_nr(pg);
      continue;
    }
    if (val == rec_val) { // match at bottom of page
      cpg = get_page(s->name, mid-1);
      cpos = page_seek(cpg, P_END, -s->len);
      if (val != page_get_int_at(cpg, cpos+offset))
        goto found;
      else {
        high = mid;
        continue;
      }
    }

    ref.high =
    pos = page_seek(pg, P_END, -s->len);
    rec_val = page_get_int_at(pg, pos + offset);

    if (val > rec_val) {
      if (low == page_block_nr(pg))
        goto not;
      low = page_block_nr(pg);
      continue;
    }

    pos = bfind_page_first_int_val(s, ref, offset, val);
    if (pos < 0)
      goto not;
    goto found;

  }

not:

  return 0;

found:

  s->tbl->current_pg = pg;
  page_set_current_pos(pg, pos);
  return 1;

}

/** @b put_page_record
 * 
 * write the provided record (of format in schema)
 * to current position of page
 * 
 * @param p  destination page
 * @param r  record to be written
 * @param s  format schema
 */
static int put_page_record(page_p p, record r, schema_p s) {
  if (!page_valid_pos_for_put_with_schema(p, s))
    return 0;

  field_desc_p fld_desc;
  size_t i = 0;
  for (fld_desc = s->first;
       fld_desc;
       fld_desc = fld_desc->next, i++)
    if (is_int_field(fld_desc))
      page_put_int(p, *(int *)r[i]);
    else
      page_put_str(p, (char *)r[i], fld_desc->len);
  return 1;
}

/** @b put_page_record
 * 
 * writes the provided record to the current position of the schema
 * 
 * @param r  record to be written
 * @param s  target schema
 */
int put_record(record r, schema_p s) {
  page_p p = s->tbl->current_pg;

  if (!page_valid_pos_for_put_with_schema(p, s))
    return 0;

  field_desc_p fld_desc;
  size_t i = 0;
  for (fld_desc = s->first; fld_desc;
       fld_desc = fld_desc->next, i++)
    if (is_int_field(fld_desc))
      page_put_int(p, *(int *)r[i]);
    else
      page_put_str(p, (char *)r[i], fld_desc->len);
  return 1;
}

/** @b append_record
 * 
 * writes the provided record to the end of the schema
 * 
 * @param r  record to be written
 * @param s  target schema
 */
void append_record(record r, schema_p s) {
  tbl_p tbl = s->tbl;
  page_p pg = get_page_for_append(s->name);
  if (!pg) {
    put_msg(FATAL, "Failed to get page for appending to \"%s\".\n",
            s->name);
    exit(EXIT_FAILURE);
  }
  if (!put_page_record(pg, r, s)) {
    /* not enough space in the current page */
    unpin(pg);
    pg = get_next_page(pg);
    if (!pg) {
      put_msg(FATAL, "Failed to get page for \"%s\" block %d.\n",
              s->name, page_block_nr(pg) + 1);
      exit(EXIT_FAILURE);
    }
    if (!put_page_record(pg, r, s)) {
      put_msg(FATAL, "Failed to put record to page for \"%s\" block %d.\n",
              s->name, page_block_nr(pg) + 1);
      exit(EXIT_FAILURE);
    }
  }
  tbl->current_pg = pg;
  tbl->num_records++;
}

static void display_tbl_header(tbl_p t) {
  if (!t) {
    put_msg(INFO,  "Trying to display non-existant table.\n");
    return;
  }
  schema_p s = t->sch;
  for (field_desc_p f = s->first; f; f = f->next)
    put_msg(FORCE, "%20s", f->name);
  put_msg(FORCE, "\n");
  for (field_desc_p f = s->first; f; f = f->next) {
    for (size_t i = 0; i < 20 - strlen(f->name); i++)
      put_msg(FORCE, " ");
    for (size_t i = 0; i < strlen(f->name); i++)
      put_msg(FORCE, "-");
  }
  put_msg(FORCE, "\n");
}

static void display_record(record r, schema_p s) {
  field_desc_p f = s->first;
  for (size_t i = 0; f; f = f->next, i++) {
    if (is_int_field(f))
      put_msg(FORCE, "%20d", *(int *)r[i]);
    else
      put_msg(FORCE, "%20s", (char *)r[i]);
  }
  put_msg(FORCE, "\n");
}

void table_display(tbl_p t) {
  if (!t) return;
  display_tbl_header(t);

  schema_p s = t->sch;
  record rec = new_record(s);
  set_tbl_position(t, TBL_BEG);
  while (get_record(rec, s)) {
    display_record(rec, s);
  }
  put_msg(FORCE, "\n");

  release_record(rec, s);
}

/* We restrict ourselves to equality search on an int attribute */
tbl_p table_search(tbl_p t, char const* attr, char const* op, int val, int b_search) {
  if (!t) return 0;

  int (*cmp_op)() = 0;

  if (strcmp(op, "=") == 0)
    cmp_op = int_eq;

  else if (strcmp(op, "!=") == 0)
    cmp_op = int_neq;

  else if (strcmp(op, "<") == 0)
    cmp_op = int_l;

  else if (strcmp(op, "<=") == 0)
    cmp_op = int_le;

  else if (strcmp(op, ">") == 0)
    cmp_op = int_g;

  else if (strcmp(op, ">=") == 0)
    cmp_op = int_ge;


  if (!cmp_op) {
    put_msg(ERROR, "unknown comparison operator \"%s\".\n", op);
    return 0;
  }


  schema_p s = t->sch;
  field_desc_p f;
  size_t i = 0;
  for (f = s->first; f; f = f->next, i++)
    if (strcmp(f->name, attr) == 0) {
      if (f->type != INT_TYPE) {
        put_msg(ERROR, "\"%s\" is not an integer field.\n", attr);
        return 0;
      }
      break;
    }
  if (!f) return 0;

  char *tmp_name = tmp_schema_name("select", s->name);
  schema_p res_sch = copy_schema(s, tmp_name);
  free(tmp_name);

  record rec = new_record(s);

  //TODO: replace this too
  set_tbl_position(t, TBL_BEG);
  if (b_search && cmp_op == int_eq){
    if (bfind_first_int_val(s, f->offset, val))
    while (lfind_record_int_val(rec, s, f->offset, val)) {
      append_record(rec, res_sch);
    }
  } else {
    while (find_record_int_val(rec, s, f->offset, cmp_op, val)) {
      append_record(rec, res_sch);
    }
  }

  put_db_info(DEBUG);
  release_record(rec, s);

  return res_sch->tbl;
}

tbl_p table_project(tbl_p t, int num_fields, char* fields[]) {
  schema_p s = t->sch;
  schema_p dest = make_sub_schema(s, num_fields, fields);
  if (!dest) return 0;

  record rec = new_record(s), rec_dest = new_record(dest);

  set_tbl_position(t, TBL_BEG);
  while (get_record(rec, s)) {
    fill_sub_record(rec_dest, dest, rec, s);
    put_record_info(DEBUG, rec_dest, dest);
    append_record(rec_dest, dest);
  }

  release_record(rec, s);
  release_record(rec_dest, dest);

  return dest->tbl;
}

tbl_p table_natural_join(tbl_p left, tbl_p right) {
  if (!(left && right)) {
    put_msg(ERROR, "no table found!\n");
    return 0;
  }

  tbl_p res = NULL;
  
  // char *joined_name = concat_names(left->sch->name, "__join__", right->sch->name);
  // schema_p sch = copy_schema(left->sch, joined_name);
  // free(joined_name);

  // for (field_desc_p 
  //       fl = left->sch->first;
  //       fl && fr;
  //     {

  //       fr = right->sch->first;
    
    
  // }
  
  // res = sch->tbl;

  return res;
}
