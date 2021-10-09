/** @file schema.h
 * @brief Table schema.
 * @author Weihai Yu
 *
 * A table @em schema defines a number of data fields.
 * A table @em record holds the data values whose types are
 * defined in the schema.
 *
 * Use @ref new_schema "new_schema()" to create an empty new schema,
 * @ref new_int_field "new_int_field()" and
 * @ref new_str_field "new_str_field()" to create a new field,
 * and @ref add_field "add_field()" to add a field to a schema.
 *
 * Use @ref get_schema "get_schema()" to get a schema, and @ref
 * schema_first_fld_desc "schema_first_fld_desc()", @ref
 * schema_last_fld_desc "schema_last_fld_desc()" and @ref
 * field_desc_next "field_desc_next()" to nevigate through the fields
 * of the schema.
 *
 * Since the data types of record fields are not known at compile
 * time, generic pointer @ref record "(void *)" is used in many places and
 * must be cast to the right C types.  The types of a record's fields are
 * maintained in @ref field_desc_struct "field descriptors" of the
 * corresponding schema. Therefore operations on records and record fields are
 * typically associated with a schema, and navigating the fields of a
 * record involves navigating in parallel the field descriptors of the schema.
 * See the source code of @ref put_record_info and @ref fill_record ect. as
 * examples of how to access field values of a record.
 *
 * The memory of a record is allocated with @ref new_record
 * "new_record()" and released with @ref release_record
 * "release_record()".  Assign field valuse to a record with @ref
 * assign_int_field "assign_int_field()", @ref assign_str_field
 * "assign_str_field()" or @ref fill_record "fill_record()".  Compare
 * if two records have equal values with @ref equal_record
 * "equal_record()".  Access the record at the current position of a
 * page with @ref get_record "get_record()" and @ref put_record
 * "put_record()".
 */

#ifndef _SCHEMA_H_
#define _SCHEMA_H_

#include "pager.h"
#include <stdarg.h>

#define MAX_STR_LEN 100

typedef enum {INT_TYPE, STR_TYPE} field_type;
typedef enum {TBL_BEG, TBL_END} tbl_position;

typedef struct field_desc_struct * field_desc_p;
typedef struct schema_struct * schema_p;
typedef struct tbl_desc_struct * tbl_p;

/** @brief Data record

    A record consists of an array of pointers to field values.
    Because in general, the types of the fields of a record are
    unknown at compile time, the memory of these values has to be
    allocated at run time with @ref new_record.  When accessing these
    values, the generic (void *) pointers must be casted to the
    correct C types (int *) and (char *).  See the source code of @ref
    put_record_info and @ref fill_record as examples of how to access
    field values of a record.  */
typedef void** record;

/* for debugging */
extern void put_field_info(pmsg_level level, field_desc_p f);
extern void put_record_info(pmsg_level level, record const r, schema_p s);
extern void put_schema_info(pmsg_level level, schema_p s);
extern void put_tbl_info(pmsg_level level, tbl_p t);
extern void put_db_info(pmsg_level level);

/* table API */

/** Open a database that was created in a previous session.
    The database and tables are stored at @ref sys_dir.
    Return 0 upon failure. */
extern int open_db(void);
/** Close a database */
extern void close_db(void);

/** Make a new schema and add it to the current database */
extern schema_p new_schema(char const* name);
/** Return an existing schema, NULL if the named schema does not exist. */
extern schema_p get_schema(char const* name);
/** Remove a schema from the current database */
extern void remove_schema(schema_p s);
/** Return name of schema. */
extern char const* schema_name(schema_p sch);
/** Return the first field_desc of schema. */
extern field_desc_p schema_first_fld_desc(schema_p sch);
/** Return the last field_desc of schema. */
extern field_desc_p schema_last_fld_desc(schema_p sch);
/** Return number of fields in schema. */
extern int schema_num_flds(schema_p sch);
/** Return length of schema in number of bytes. */
extern int schema_len(schema_p sch);

/** Make an int field with name @em name. */
extern field_desc_p new_int_field(char const* name);
/** Make an string field with name @em name and length @em len. */
extern field_desc_p new_str_field(char const* name, int len);
/** Check if this is an int field.
    Since there are only int and str fields, "not int" means str.
*/
extern int is_int_field(field_desc_p f);
/** Returns the next field_desc */
extern field_desc_p field_desc_next(field_desc_p f);

/** Add a field to the schema */
extern int add_field(schema_p s, field_desc_p f);
/** Creates a new record of schema @em s.
    It is the responsibility of the using program to free the memory
    of the resulting record using release_record().
*/
extern record new_record(schema_p s);
/** Release the memory allocated for the record and its fields.*/
extern void release_record(record r, schema_p s);
/** Assign an int record field. Note the data type of @em field_pp */
extern void assign_int_field(void const* field_p, int int_val);
/** Assign a str record field. Note the data type of @em field_pp */
extern void assign_str_field(void* field_p, char const* str_val);
/** Fill a record with values. Example: fill_record(r, s, 1, "A string", 136)*/
extern int fill_record(record const r, schema_p s, ...);
/** Compare if two records have equal field values */
extern int equal_record(record const r1, record const r2, schema_p s);

/** Set the current position to the beginning or end of the table.
*/
extern void set_tbl_position(tbl_p t, tbl_position pos);

/** Whether the current position is at @em end of table.
*/
extern int eot(tbl_p t);

/** Retrieve the record value at the current position.
    The current position moves to the next record.
    Returns 1 when @em r is updated, 0 when there is no more record,
    and -1 when something goes wrong.
*/
extern int get_record(record const r, schema_p s);

/** Put the record value at the current position.
    The current position moves to the next record.
    Returns -1 if there is not enough space at current position.
*/
extern int put_record(record const r, schema_p s);
/** Append the record to the table file.
    The current position moves to the new end of the file.
*/
extern void append_record(record const r, schema_p s);

/** Return an existing table desc, NULL if the table does not exist. */
extern tbl_p get_table(char const* name);
/** Remove a table from the current database */
extern void remove_table(tbl_p t);
/** Print all rows of a table. */
extern void table_display(tbl_p s);
/** Make a new table as the result of a search. */
extern tbl_p table_search(tbl_p t, char const* attr,
                          char const* op, int val, int b_search);
/** Make a new table as a result of project. */
extern tbl_p table_project(tbl_p t, int num_fields, char* fields[]);
/** Join two tables and return the joined table. */
extern tbl_p table_natural_join(tbl_p left, tbl_p right);
#endif
