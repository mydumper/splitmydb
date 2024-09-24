/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

        Authors:
                    David Ducos (david dot ducos at gmail dot com)

Se hace en 3 etapas
1- se levanta las referencias
2- se cargan los ids
3- se descarga la data y se insertan los nuevos ids


*/




#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64
#define LAST_INSERT_ID "SELECT LAST_INSERT_ID();"
#include <mysql.h>

#if defined MARIADB_CLIENT_VERSION_STR && !defined MYSQL_SERVER_VERSION
#define MYSQL_SERVER_VERSION MARIADB_CLIENT_VERSION_STR
#endif

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#ifdef ZWRAP_USE_ZSTD
#include "../zstd/zstd_zlibwrapper.h"
#else
#include <zlib.h>
#endif
#include <pcre.h>
#include <signal.h>
#include <glib/gstdio.h>
#include <glib/gerror.h>
#include <gio/gio.h>
#include "config.h"
#include "connection.h"
#include <glib-unix.h>
#include <math.h>
#include "logging.h"
#include "set_verbose.h"
#include "locale.h"
#include <sys/statvfs.h>
#include "write_insert.h"
#include "tables_skiplist.h"
#include "regex.h"
#include "common.h"

/* Some earlier versions of MySQL do not yet define MYSQL_TYPE_JSON */
#ifndef MYSQL_TYPE_JSON
#define MYSQL_TYPE_JSON 245
#endif
GMutex *global_id_hash_mutex=NULL;
GMutex *init_mutex = NULL;
/* Program options */
guint complete_insert = 0;
guint statement_size = 1000000;
guint chunk_filesize = 0;
int build_empty_files = 0;
gboolean load_data = FALSE;
gboolean success_on_1146 = FALSE;
extern guint errors;
extern gchar * where_option;
extern guint number_threads;
gboolean insert_ignore = FALSE;
gchar *set_names_str=NULL;
int skip_tz = 0;
gchar *fields_enclosed_by=NULL;
gchar *fields_escaped_by=NULL;
gchar *fields_terminated_by=NULL;
gchar *lines_starting_by=NULL;
gchar *lines_terminated_by=NULL;
gchar *statement_terminated_by=NULL;
const gchar *dump_directory="";
extern gchar *tables_skiplist_file;
extern gchar *db;
extern gchar *dest_db;
GHashTable * table_hash=NULL;
GList *multicolumn_list=NULL;
//void write_table_job_list_into_db(MYSQL *conn, GList *tj, MYSQL *dest_conn);

GAsyncQueue *fill_thread_data_queue=NULL, *fill_thread_data_queue_return=NULL;
void *fill_hash_thread(void *dataArg);

void init(){
  fields_terminated_by=g_strdup(",");
  lines_starting_by=g_strdup("(");
  lines_terminated_by=g_strdup(")\n");
  statement_terminated_by=g_strdup(";\n");
  table_hash=g_hash_table_new ( g_str_hash, g_str_equal );
  fill_thread_data_queue=g_async_queue_new();
  fill_thread_data_queue_return=g_async_queue_new();
  global_id_hash_mutex=g_mutex_new();
}

void init_fill_id_threads(){
  guint n;
  for (n = 0; n < number_threads; n++) {
    g_thread_new("id_filler",(GThreadFunc)fill_hash_thread, NULL);
  }
}
gchar * build_key(const gchar * database, const gchar * table){
 return g_strdup_printf("`%s`.`%s`",database,table);
}

struct column_hash * column_hash_lookup(GHashTable *column_hash, gchar *k){
  return g_hash_table_lookup(column_hash, k);
}
gboolean is_primary_key(struct db_table *dbt, gchar *column){
  return g_strcmp0(dbt->primary_key, column)==0;
}

gboolean identity_function(GHashTable *tmp_column,gchar ** r, gchar **t){
//  return *r;
  g_debug("identity_function: %s",*r);
  if (tmp_column == NULL || g_hash_table_size(tmp_column) == 0)
    return FALSE;
  *t=g_strdup(g_hash_table_lookup(tmp_column,*r));
  g_debug("Id: %s -> %s", *r, *t);
  if (*t==NULL){
    g_message("ID was not found: %s", *r);
    return FALSE;
  }
  return TRUE;
}

struct db_table * get_dbt(const gchar *database, const gchar *table, gboolean e){

  gchar * temp_key = build_key(database,table);
  struct db_table *dbt = g_hash_table_lookup(table_hash, temp_key);
  if (dbt == NULL){
    g_debug("column_hash_lookup: Table %s has not been read from the structure", temp_key);
    if (e && FALSE) exit(1);
  }
  g_free(temp_key);
  return dbt;
}


void append_child_to_parent(struct db_table * child, struct db_table * parent, gchar *k){
  GHashTable *pht=g_hash_table_lookup(parent->child_table,k);
  g_message("`%s`.`%s` is child of `%s`.`%s` using %s",child->database, child->table, parent->database, parent->table, k);
  if (pht){
    g_hash_table_insert(pht, g_strdup(child->table), child);
  }else{
    pht=g_hash_table_new ( g_str_hash, g_str_equal );
    g_hash_table_insert(pht, g_strdup(child->table), child);
    g_hash_table_insert(parent->child_table, g_strdup(k), pht);
  }
}

gchar *get_primary_key_string(MYSQL *conn, char *database, char *table, gboolean *multicolumn) {

  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("`");

  gchar *query =
          g_strdup_printf("SELECT k.COLUMN_NAME, ORDINAL_POSITION "
                          "FROM information_schema.table_constraints t "
                          "LEFT JOIN information_schema.key_column_usage k "
                          "USING(constraint_name,table_schema,table_name) "
                          "WHERE t.constraint_type IN ('PRIMARY KEY', 'UNIQUE') "
                          "AND t.table_schema='%s' "
                          "AND t.table_name='%s' "
                          "ORDER BY t.constraint_type, ORDINAL_POSITION; ",
                          database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else if (atoi(row[1]) > 1) {
      *multicolumn=TRUE;
      g_string_append(field_list, "`,`");
    } else {
      break;
    }

    gchar *tb = g_strdup(row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);
  g_string_append(field_list, "`");
  // Return NULL if we never found a PRIMARY or UNIQUE key
  if (first) {
    g_string_free(field_list, TRUE);
    return NULL;
  } else {
    return g_string_free(field_list, FALSE);
  }
}

void print_hash2string(GHashTable *hash, GString *str){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, hash );
  g_string_append(str,"(");
  gchar *id;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &id )){
    g_string_append_printf(str,"%s -> %s | ",lkey,id);
  }
  g_string_append(str,")");
}

void hash2string(GHashTable *hash, GString *str){
  gboolean in=TRUE;
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, hash );
  g_string_append(str,"(");
  gchar *id;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &id )){
    if (!in){
      g_string_append(str,",");
    }else{
      in=FALSE;
    }
    g_string_append_printf(str,"%s",lkey);
  }
  g_string_append(str,")");
//  g_message("hash2string: %s",str->str);
}

gboolean hash2string_without_null(GHashTable *hash, GString *str){
  gboolean in=TRUE, has_other_than_null=FALSE;
  GHashTableIter iter;
  gchar * lkey;
  g_mutex_lock(global_id_hash_mutex);
  g_hash_table_iter_init ( &iter, hash );
  g_string_append(str,"(");
  gchar *id;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &id )){
    if (g_strcmp0(lkey,"NULL") !=0){
      has_other_than_null=TRUE;
      if (!in){
        g_string_append(str,",");
      }else{
        in=FALSE;
      }
      g_string_append_printf(str,"%s",lkey);
    }
  }
  g_mutex_unlock(global_id_hash_mutex);
  g_string_append(str,")");
//  g_message("hash2string: %s",str->str);
  return has_other_than_null;
}



void update_ids(){

}
struct column_hash * new_column_hash(struct db_table *dbt, GHashTable * h){
  struct column_hash *ch=g_new(struct column_hash, 1);
  ch->hash=h ; // g_hash_table_new ( g_str_hash, g_str_equal );
  ch->updated=FALSE;
  ch->dbt=dbt;
  return ch;
}

struct column_hash * new_empty_column_hash(struct db_table *dbt){
  return new_column_hash(dbt, g_hash_table_new ( g_str_hash, g_str_equal ));
}

void column_hash_insert(struct column_hash *ch, gchar *k, gchar * v){
  g_hash_table_insert(ch->hash,k,v);
  ch->updated=TRUE;
}

GString *get_insertable_fields_withouPK(MYSQL *conn, char *database, char *table);

struct db_table *new_db_table(char *database, char *table, gint order){
  g_message("New db_table: %s %s", database, table);
  MYSQL * conn=NULL;
  conn = mysql_init(NULL);
  smd_connect(conn,SOURCE,db);
  struct db_table *dbt = g_new(struct db_table, 1);
  dbt->database = g_strdup(database);
  dbt->table = g_strdup(table);
  dbt->table_filename = g_strdup(dbt->table);
  dbt->rows_lock= g_mutex_new();
  dbt->order = order;
//  dbt->escaped_table = scape_string(conn,dbt->table);
//  dbt->anonymized_function=get_anonymized_function_for(conn, dbt->database->name, dbt->table);
//  dbt->has_generated_fields = detect_generated_fields(conn, dbt->database->escaped, dbt->escaped_table);
  dbt->has_generated_fields=FALSE;
  dbt->multicolumn=FALSE;
  dbt->select_fields_with_all_fields=get_insertable_fields(conn, dbt->database, dbt->table);
  if (dbt->has_generated_fields) {
    dbt->select_fields = dbt->select_fields_with_all_fields;
  } else {
    dbt->select_fields = g_string_new("*");
  }
  dbt->select_fields_with_all_fields_withouPK= get_insertable_fields_withouPK(conn, dbt->database, dbt->table);

  dbt->primary_key=get_primary_key_string(conn, dbt->database, dbt->table,&(dbt->multicolumn));
  if (dbt->multicolumn)
    multicolumn_list=g_list_append(multicolumn_list, dbt);

  //  dbt->ids=g_hash_table_new ( g_str_hash, g_str_equal );

  dbt->parent_table=g_hash_table_new ( g_str_hash, g_str_equal );
  g_hash_table_insert(dbt->parent_table,g_strdup(dbt->primary_key),new_empty_column_hash(dbt));

  dbt->child_table=g_hash_table_new ( g_str_hash, g_str_equal );
  dbt->updated=FALSE;
  dbt->rows=0;
/*  if (!datalength)
    dbt->datalength = 0;
  else
    dbt->datalength = g_ascii_strtoull(datalength, NULL, 10);
    */
  mysql_close(conn);
  return dbt;
}
/*
struct db_table *new_db_table(char *database, char *table, gint order, const gchar *column,  struct column_hash* column_id_hash){
  struct db_table *dbt =new_db_table(database, table, order);
  if (g_strcmp0(column,dbt->primary_key)!=0){
    g_hash_table_insert(dbt->parent_table,g_strdup(column),new_column_hash(column_id_hash->dbt,column_id_hash->hash));
  }
}
*/

struct table_job * new_table_job(struct db_table *dbt, char *partition, char *where, guint nchunk){
  struct table_job *tj = g_new0(struct table_job, 1);
// begin Refactoring: We should review this, as dbt->database should not be free, so it might be no need to g_strdup.
  // from the ref table?? TODO
  tj->database=dbt->database;
  tj->table=g_strdup(dbt->table);
// end
  tj->partition=partition;
  tj->where=where;
  tj->nchunk=nchunk;
  tj->filename = build_data_filename(dest_db, dbt->table_filename, tj->nchunk, 0);
g_message("New table job with filename: %s | %s %s", tj->filename,dbt->database, dbt->table_filename);
  tj->dbt=dbt;
  return tj;
}

gchar * build_filename(char *database, char *table, guint part, guint sub_part, const gchar *extension){
  GString *filename = g_string_sized_new(20);
  sub_part == 0 ?
    g_string_append_printf(filename, "%s.%s.%05d.%s", database, table, part, extension):
    g_string_append_printf(filename, "%s.%s.%05d.%05d.%s", database, table, part, sub_part, extension);
  gchar *r = g_build_filename(dump_directory, filename->str, NULL);
  g_string_free(filename,TRUE);
  return r;
}

gchar * build_data_filename(char *database, char *table, guint part, guint sub_part){
  return build_filename(database,table,part,sub_part,"sql");
}

GString *get_insertable_fields_template(MYSQL *conn, char *database, char *table, const char *query_template) {
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;

  GString *field_list = g_string_new("");

  gchar *query =
      g_strdup_printf(query_template,database, table);
  mysql_query(conn, query);
  g_free(query);

  res = mysql_store_result(conn);
  gboolean first = TRUE;
  while ((row = mysql_fetch_row(res))) {
    if (first) {
      first = FALSE;
    } else {
      g_string_append(field_list, ",");
    }

    gchar *tb = g_strdup_printf("`%s`", row[0]);
    g_string_append(field_list, tb);
    g_free(tb);
  }
  mysql_free_result(res);

  return field_list;
}
GString *get_insertable_fields(MYSQL *conn, char *database, char *table) {
  return get_insertable_fields_template(conn,database,table, "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%'");
}
GString *get_insertable_fields_withouPK(MYSQL *conn, char *database, char *table) {
  return get_insertable_fields_template(conn,database,table, "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and extra not like '%%VIRTUAL GENERATED%%' and extra not like '%%STORED GENERATED%%' and extra not like '%%auto_increment%%'");
}

char * escape_string(MYSQL *conn, char *str){
  char * r=g_new(char, strlen(str) * 2 + 1);
  mysql_real_escape_string(conn, r, str, strlen(str));
  return r;
}

void append_columns (GString *statement, MYSQL_FIELD *fields, guint num_fields){
  guint i = 0;
  for (i = 0; i < num_fields; ++i) {
    if (i > 0) {
      g_string_append_c(statement, ',');
    }
    g_string_append_printf(statement, "`%s`", fields[i].name);
  }
}

void append_insert (gboolean condition, GString *statement, char *table, MYSQL_FIELD *fields, guint num_fields){
  if (condition) {
    if (insert_ignore) {
      g_string_printf(statement, "INSERT IGNORE INTO `%s` (", table);
    } else {
      g_string_printf(statement, "INSERT INTO `%s` (", table);
    }
    append_columns(statement,fields,num_fields);
    g_string_append(statement, ") VALUES");
  } else {
    if (insert_ignore) {
      g_string_printf(statement, "INSERT IGNORE INTO `%s` VALUES", table);
    } else {
      g_string_printf(statement, "INSERT INTO `%s` VALUES", table);
    }
  }
}

gboolean write_data(FILE *file, GString *data) {
  size_t written = 0;
  ssize_t r = 0;
  while (written < data->len) {
    r=write(fileno(file), data->str + written, data->len);
    if (r < 0) {
      g_critical("Couldn't write data to a file: %s", strerror(errno));
      errors++;
      return FALSE;
    }
    written += r;
  }

  return TRUE;
}

void get_select(struct db_table *dbt){
(void) dbt;
  //  g_message("select %s from %s.%s where %s ", row[0], dbt->database, dbt->table,column ids);

}


void column_insert(GString *data, MYSQL_ROW row, guint num_fields, struct column_hash * id_hash){
  guint i=0;
  gchar *data_to_insert=NULL;
  g_string_set_size(data,0);
  if (num_fields>1)
    g_string_append(data,"(");
  for (i=0; i<num_fields; i++){
    if (i>0)
      g_string_append(data,",");
    if (row[i] == NULL )
      g_string_append(data,"NULL");
    else
      g_string_append(data,row[i]);
  }
  if (num_fields>1)
    g_string_append(data,")");
  data_to_insert=NULL;
  if (!g_strcmp0(data->str,"NULL")){
      data_to_insert=g_strdup(data->str);
  }
  g_mutex_lock(global_id_hash_mutex);
  g_hash_table_insert(id_hash->hash,g_strdup(data->str),data_to_insert);
  g_mutex_unlock(global_id_hash_mutex);
}


void *fill_hash_thread(void *dataArg){
  (void)dataArg;

  MYSQL_RES *res = NULL;
  MYSQL_ROW row = NULL;
  MYSQL *conn = mysql_init(NULL);
  smd_connect(conn,SOURCE,db);
  GString *data=g_string_sized_new(10);
  GString *this_g_ids=g_string_new("");
  struct fill_thread_data *ftd=NULL;
  gboolean b;
  guint num_fields;
  while(1){
    ftd=g_async_queue_pop(fill_thread_data_queue);
    if (!ftd->dbt)
	    break;
    g_string_set_size(this_g_ids,0);
    b=hash2string_without_null(ftd->id_hash->hash, this_g_ids);
    g_string_set_size(data,0);
    if (g_strstr_len(this_g_ids->str, -1, "NULL") || g_strstr_len(ftd->ids,-1, "NULL"))
      g_error("Tiene null");

    if (ftd->len>1){
      if (g_hash_table_size(ftd->id_hash->hash)> 0 && b){
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE (%s) IN %s AND (%s) %sIN %s", ftd->column_select, ftd->dbt->database, ftd->dbt->table, ftd->column_where, ftd->ids, ftd->column_second, ftd->not_in?"NOT ":"",this_g_ids->str);
      }else{
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE (%s) IN %s", ftd->column_select, ftd->dbt->database, ftd->dbt->table, ftd->column_where, ftd->ids);
      }
    }else{
      if (g_hash_table_size(ftd->id_hash->hash)> 0 && b){
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE %s IN %s AND %s %sIN %s", ftd->column_select, ftd->dbt->database, ftd->dbt->table, ftd->column_where, ftd->ids, ftd->column_second, ftd->not_in?"NOT ":"", this_g_ids->str);
      }else{
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE %s IN %s", ftd->column_select, ftd->dbt->database, ftd->dbt->table, ftd->column_where, ftd->ids);
      }
    }
    g_message("Query in fill_hash_from_ids %d", ftd->step);
//    g_message("Query in fill_hash_from_ids %d: %s", step, data->str);
    if (mysql_query(conn,data->str)){
      g_critical("Error executing query: %s", data->str);
      exit(1);
    }
    res = mysql_store_result(conn);
    num_fields = mysql_num_fields(res);
    g_string_set_size(data,0);
    while ((row = mysql_fetch_row(res))){
      ftd->id_hash->dbt->updated=TRUE;
      ftd->id_hash->updated=TRUE;
      column_insert(data, row, num_fields, ftd->pk_hash);
//      g_message("Inserting %s on column %s on table %s", data->str, column_select, dbt->table);
    }
    mysql_free_result(res);
    g_free(ftd->ids);
    g_free(ftd);
    g_async_queue_push(fill_thread_data_queue_return, GINT_TO_POINTER(1));
  }
  g_string_free(data,TRUE);
  mysql_close(conn);

  return NULL;
}

struct fill_thread_data *new_fill_thread_data(struct db_table *dbt, gchar *column_select, gchar *column_where, gchar *column_second, gchar *ids, struct column_hash * id_hash, struct column_hash * pk_hash, guint len, guint step, gboolean not_in){
  struct fill_thread_data *fdt = g_new0(struct fill_thread_data, 1);
  fdt->dbt=dbt;
  fdt->column_select=column_select;
  fdt->column_where=column_where;
  fdt->column_second=column_second;
  fdt->ids=g_strdup(ids);
  fdt->id_hash=id_hash;
  fdt->pk_hash=pk_hash;
  fdt->len=len;
  fdt->step=step;
  fdt->not_in=not_in;
  return fdt;
}

void fill_hash_from_ids(struct db_table *dbt, gchar *column_select, gchar *column_where, gchar *ids, struct column_hash * id_hash ){
	// parto los ids y actualizo el this_g_ids
	// next_ids=g_strrstr_len(ids,2500,",");
//  MYSQL_RES *res = NULL;
//  MYSQL_ROW row = NULL;
  id_hash->updated=FALSE;
  gchar *next_ids=NULL;
  gchar *current_ids=ids;
//  GString *data=g_string_sized_new(10);
  if (strlen(ids) <= 2){
      g_message("Query in fill_hash_from_ids not enough ids for %s.%s: %s",dbt->database, dbt->table, ids);
      return;
  }

  gchar **pk_fields=g_strsplit(dbt->primary_key, ",",0);
  guint len=g_strv_length(pk_fields);
  g_strfreev(pk_fields);
//  guint j=0;
  guint step=0;
//  guint num_fields;
  gchar nn;
  while (current_ids){

    if (strlen(current_ids) > MAX_LENGTH){
      next_ids=g_strrstr_len(current_ids,MAX_LENGTH,",");
      nn=next_ids[1];
      next_ids[0]=')';
      next_ids[1]='\0';
    }else{
      next_ids=NULL;
    }
 //   g_message("Current ids: %s\nNext ids: %s", current_ids, next_ids);
/*
    g_string_set_size(this_g_ids,0);
    b=hash2string_without_null(id_hash->hash, this_g_ids);
    g_string_set_size(data,0);
    if (g_strstr_len(this_g_ids->str, -1, "NULL") || g_strstr_len(current_ids,-1, "NULL"))
      g_error("Tiene null");

    if (len>1){
      if (g_hash_table_size(id_hash->hash)> 0 && b){
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE (%s) IN %s AND (%s) NOT IN %s", column_select, dbt->database, dbt->table, column_where, current_ids, column_select, this_g_ids->str);
      }else{
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE (%s) IN %s", column_select, dbt->database, dbt->table, column_where, current_ids);
      }
    }else{
      if (g_hash_table_size(id_hash->hash)> 0 && b){
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE %s IN %s AND %s NOT IN %s", column_select, dbt->database, dbt->table, column_where, current_ids, column_select, this_g_ids->str);
      }else{
        g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE %s IN %s", column_select, dbt->database, dbt->table, column_where, current_ids);
      }
    }
    g_message("Query in fill_hash_from_ids %d", step);
//    g_message("Query in fill_hash_from_ids %d: %s", step, data->str);
    if (mysql_query(conn,data->str)){
      g_critical("Error executing query: %s", data->str);
      exit(1);
    }
    res = mysql_store_result(conn);
    num_fields = mysql_num_fields(res);
    g_string_set_size(data,0);
    while ((row = mysql_fetch_row(res))){
      id_hash->dbt->updated=TRUE;
      id_hash->updated=TRUE;
      column_insert(data, row, num_fields, id_hash);
      j++;
//      g_message("Inserting %s on column %s on table %s", data->str, column_select, dbt->table);
    }
    mysql_free_result(res);
    */
    g_async_queue_push(fill_thread_data_queue, new_fill_thread_data(dbt, column_select, column_where, column_select, current_ids, id_hash, id_hash, len, step, TRUE));

    if(step>0){
      current_ids[0]=',';
    }
    current_ids=next_ids;
    if (next_ids){
      next_ids[0]='(';
      next_ids[1]=nn;
    }
    step++;
  }
  while(step>0){
    g_async_queue_pop(fill_thread_data_queue_return);
    step--;
  }
  g_message("Query in fill_hash_from_ids: finish: %s %s %s %s", column_select, dbt->database, dbt->table, column_where);
//  mysql_close(conn);
}

void fill_primary_key_with_column_values(struct db_table *dbt, gchar *column_where, gchar *ids){
    g_message("fill_primary_key_with_column_values %s.%s with column where %s",dbt->database, dbt->table,column_where);
    fill_hash_from_ids(dbt, dbt->primary_key, column_where, ids, column_hash_lookup(dbt->parent_table, dbt->primary_key));
}



void process_multicolumn_tables(){
  GList *mcl=multicolumn_list;
  struct db_table*dbt=NULL;

//  MYSQL_RES *res = NULL;
//  MYSQL_ROW row = NULL;
//  MYSQL * conn=NULL;
  GString *this_g_ids=g_string_new("");
  GString *data=g_string_sized_new(10);
  g_message("Multi column step");
//  guint num_fields;
  GHashTableIter iter;
//  gchar * column;
  gchar **pk_fields;
  guint step=0;
  struct column_hash* pk_hash, *second_column, *first_column;
  gchar *current_ids,*next_ids;
  guint len;
  while (mcl){
    dbt=mcl->data;
    step=0;
    pk_fields=g_strsplit(dbt->primary_key, ",",0);
    len=g_strv_length(pk_fields);
    g_string_printf(data,"SELECT DISTINCT %s FROM `%s`.`%s` WHERE ", dbt->primary_key, dbt->database, dbt->table);

    g_hash_table_iter_init ( &iter, dbt->parent_table );
    //gboolean first=TRUE;
    pk_hash=column_hash_lookup(dbt->parent_table,dbt->primary_key);
    if (len!=2){
      g_error("Multi column table with != 2 amount of columns %s on %s %s ",dbt->primary_key, dbt->database, dbt->table);
    }
    gchar nn;
    first_column=column_hash_lookup(dbt->parent_table, pk_fields[0]);
    second_column=column_hash_lookup(dbt->parent_table, pk_fields[1]);
    if(!first_column || !second_column || !g_hash_table_size(first_column->hash) || !g_hash_table_size(second_column->hash)){
      mcl=mcl->next;
      continue;
    }
    g_string_set_size(this_g_ids,0);
    hash2string_without_null(first_column->hash, this_g_ids);
    current_ids=this_g_ids->str;
    while (current_ids){

      if (strlen(current_ids) > MAX_LENGTH){
        next_ids=g_strrstr_len(current_ids,MAX_LENGTH,",");
        nn=next_ids[1];
        next_ids[0]=')';
        next_ids[1]='\0';
      }else{
        next_ids=NULL;
      }

      g_async_queue_push(fill_thread_data_queue, new_fill_thread_data(dbt, dbt->primary_key, pk_fields[0], pk_fields[1], current_ids, second_column, pk_hash, len, step, FALSE));

      if(step>0){
        current_ids[0]=',';
      }
      current_ids=next_ids;
      if (next_ids){
        next_ids[0]='(';
        next_ids[1]=nn;
      }
      step++;
    }
    while(step>0){
      g_async_queue_pop(fill_thread_data_queue_return);
      step--;
    }


/*
    while (g_hash_table_iter_next ( &iter, (gpointer *) &column, (gpointer *) &id_hash )){
      if (column && !is_primary_key(dbt, column)){
        if(!g_hash_table_size(id_hash->hash)){
		goto next_dbt;
	}
        if(first)
          first=FALSE;
        else
          g_string_append(data," AND ");
        g_string_set_size(this_g_ids, 0);
        hash2string_without_null(id_hash->hash, this_g_ids);
        g_string_append_printf(data," %s IN %s", column, this_g_ids->str);
      }
    }
    g_message("Multi column query: %s", data->str);
    if (mysql_query(conn,data->str)){
      g_critical("Error executing query: %s", data->str);
      exit(1);
    }

    res = mysql_store_result(conn);
    num_fields = mysql_num_fields(res);

    g_string_set_size(data, 0);
    while ((row = mysql_fetch_row(res))){
      column_insert(data, row, num_fields, pk_hash);
    }
next_dbt:
*/
    mcl=mcl->next;
  }

}


void merge_ght(gpointer k, gpointer value, gpointer user_data){
  g_hash_table_insert((GHashTable *)user_data,k, value);
}

void print_columns(struct db_table *dbt){
  GString *str = g_string_sized_new(100);
  struct column_hash* id_hash=column_hash_lookup(dbt->parent_table,dbt->primary_key);
  g_string_set_size(str, 0);
  print_hash2string(id_hash->hash,str);
  g_message("PK: %s %s %p : %s",dbt->database,dbt->table, id_hash->hash, str->str);
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, dbt->parent_table );
  id_hash=NULL;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &id_hash )){
    g_string_set_size(str, 0);
    print_hash2string(id_hash->hash,str);
    g_message("FK: %s %s %s %p : %s",dbt->database,dbt->table,lkey, id_hash->hash, str->str);
  }
  g_string_free(str, TRUE);
}

void print_info(){
  GHashTableIter iter;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, table_hash );
  struct db_table *dbt=NULL;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt )){
    g_message("%s `%s`.`%s`",lkey,dbt->database,dbt->table);
    print_columns(dbt);
  }

}


struct db_table * get_dbt_or_add_if_missing(gchar *database, gchar *table, gint order){
  struct db_table * dbt= get_dbt(database,table, FALSE);
  if (dbt == NULL){
    g_message("Referenced table %s %s not found. Adding it", database,table);
    dbt = new_db_table(database,table, order);
    gchar *tk = build_key(database,table);
    g_hash_table_insert(table_hash,g_strdup(tk),dbt);
    g_free(tk);
  }
  return dbt;

}

struct column_hash * get_parent_column_hash(struct db_table * dbt, gchar *col){
  struct column_hash * ght=column_hash_lookup(dbt->parent_table, col);
  if (ght == NULL){
    gchar *tmp_col=g_strdup_printf("`%s`",col);
    ght=column_hash_lookup(dbt->parent_table, tmp_col);
    if (ght == NULL){
      g_message("column_hash_lookup: Column %s in table %s.%s was not found from structure", tmp_col, dbt->database, dbt->table);
      if(FALSE)exit(1);
    }
    g_free(tmp_col);
  }
  return ght;
}

void insert_into_parent(struct db_table *dbt, gchar *col,struct column_hash * ch){
  gchar *tmp_col=g_strdup_printf("`%s`",col);
  g_hash_table_insert(dbt->parent_table, g_strdup(tmp_col), ch);
  g_free(tmp_col);
}

struct column_hash *get_column_or_add_if_missing(struct db_table *dbt, gchar *col){
  struct column_hash * id_hash=get_parent_column_hash(dbt,col);
  if (!id_hash){
    g_message("Column %s not found in table %s %s. Adding it", col, dbt->database, dbt->table);
    insert_into_parent(dbt, col, new_empty_column_hash(dbt));
    id_hash=get_parent_column_hash(dbt,col);
    g_assert(id_hash != NULL);
  }
  return id_hash;
}
// child points to parent
// parent_table has the pk column
//
void get_structure(gchar * database, gchar * table, GHashTable *nav, gint order){
//  (void) dest_conn;
  (void) order;
  MYSQL_RES *res = NULL;
  MYSQL_ROW row;
  gchar * query = NULL;
  GString *new_where_2 = g_string_sized_new(100);
  g_string_set_size(new_where_2,0);
  gchar * temp_key = build_key(database,table);
  g_hash_table_insert(nav,temp_key,GINT_TO_POINTER(1));
  gchar *col=NULL;

  struct db_table * dbt=get_dbt(database, table, FALSE), *tmp_dbt=NULL;

  if (dbt == NULL){
    dbt = new_db_table( database, table, order);
    g_hash_table_insert(table_hash,g_strdup(temp_key),dbt);
  }

  struct column_hash *id_hash=NULL;
  MYSQL * conn= mysql_init(NULL);
  smd_connect(conn,SOURCE,db);
//DAVID  if (g_strcmp0(dbt->primary_key, column)!=0){
    query=g_strdup_printf("select COLUMN_NAME,REFERENCED_TABLE_SCHEMA,REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'", dbt->database,dbt->table);
    g_message("Getting structure from `%s`.`%s` with query %s", dbt->database, dbt->table, query);
    mysql_query(conn,query);
    res = mysql_store_result(conn);
    while ((row = mysql_fetch_row(res))) {
      g_message("3- Referenced table: %s | %s | %s | %s ", row[0],row[1],row[2],row[3]);
      g_message("%s %s %s is child and %s %s %s is parent", database, table, row[0],row[1],row[2],row[3]);

      if (tables_skiplist_file && check_skiplist(row[1],row[2])){
	      continue;
      }

      tmp_dbt=get_dbt_or_add_if_missing(row[1],row[2],order-1);

/*      tmp_dbt= get_dbt(row[1],row[2], FALSE);
      if (tmp_dbt == NULL){

        g_message("Referenced table %s %s not found. Adding it", row[1], row[2]);
        tmp_dbt = new_db_table( row[1], row[2], order-1);
        temp_key_2 = build_key(row[1],row[2]);
	g_hash_table_insert(table_hash,g_strdup(temp_key_2),dbt);
*/

      struct column_hash *ref_id_hash=get_column_or_add_if_missing(tmp_dbt,row[3]);
/*      id_hash=get_parent_column_hash(tmp_dbt,row[3]);
      if (!id_hash){
        g_message("Column %s not found in table %s %s. Adding it", row[3], tmp_dbt->database, tmp_dbt->table);
        g_hash_table_insert(tmp_dbt->parent_table,g_strdup(row[3]),new_empty_column_hash(tmp_dbt));
        id_hash=get_parent_column_hash(tmp_dbt,row[3]);
        g_assert(id_hash != NULL);
      }
*/

      id_hash=get_parent_column_hash(dbt,row[0]);
      if (id_hash == NULL){
	insert_into_parent(dbt, row[0], new_column_hash(tmp_dbt,ref_id_hash->hash));
	get_structure(row[1], row[2],/* col, id_hash, dest_conn,*/ nav, order - 1);

      }

      append_child_to_parent(dbt,tmp_dbt,row[0]);

      /*
        g_message("4- Linking to %s %s %s: %p", row[1], row[2], row[3], id_hash);
	g_message("Linking table %s.%s <- %s.%s",temp_key_2, row[3], temp_key, row[0]);
        g_free(col);
        col=g_strdup_printf("`%s`",row[3]);
        get_structure(row[1], row[2], col, id_hash, dest_conn, nav, order - 1);
        tmp_dbt= get_dbt(row[1],row[2], TRUE);
	id_hash->dbt=tmp_dbt;
        if (tmp_dbt == NULL){
                g_error("Table %s %s not found", row[1], row[2]);
        }
	append_child_to_parent(dbt,tmp_dbt,row[0]);
	g_message("2- Back to: %s.%s", database, table);
      }else{
        col=g_strdup_printf("`%s`",row[3]);
        id_hash=get_parent_column_hash(tmp_dbt,col);
        if (id_hash == NULL){
          g_error("Table %s %s without pk??", row[1], row[2]);
          id_hash=new_empty_column_hash(tmp_dbt); //g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );
          g_message("3- Inserting new %s %s %s: %p", row[1], row[2], row[3], id_hash);
          g_hash_table_insert(tmp_dbt->parent_table,g_strdup(col),id_hash);
        }
        g_free(col);
        col=g_strdup_printf("`%s`",row[0]);
        g_message("2- Inserting link %s %s %s: %p", database, table, col, id_hash);
//        id_hash=column_hash_lookup(dbt->parent_table,col);
	g_hash_table_insert(dbt->parent_table,g_strdup(col), new_column_hash(tmp_dbt,id_hash->hash));
	append_child_to_parent(dbt,tmp_dbt,row[0]);
	}
*/
	g_free(col);
    }
    mysql_free_result(res);
//DAVID }

  query=g_strdup_printf("select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,REFERENCED_COLUMN_NAME from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_NAME = '%s'", database, table);
  g_message("Query Referencing: %s", query);
  mysql_query(conn,query);
  g_free(query);
  res = mysql_store_result(conn);
  if (res == NULL)
    return;
  while ((row = mysql_fetch_row(res))) {

      if (tables_skiplist_file && check_skiplist(row[0],row[1])){
              continue;
      }

    g_message("3- Referencing table: %s | %s | %s | %s ", row[0],row[1],row[2],row[3]);
    g_message("%s %s %s is child and %s %s %s is parent", row[0],row[1],row[2],database, table,row[3]);

    tmp_dbt=get_dbt_or_add_if_missing(row[0],row[1],order-1);

    struct column_hash * ref_id_hash=get_column_or_add_if_missing(dbt, row[3]);

/*    id_hash=get_parent_column_hash(dbt,row[3]);
    if (!id_hash){
      g_message("Column %s not found in table %s %s. Adding it", row[3], dbt->database, dbt->table);
      g_hash_table_insert(dbt->parent_table,g_strdup(row[3]),new_empty_column_hash(dbt));
      id_hash=get_parent_column_hash(dbt,row[3]);
      g_assert(id_hash != NULL);
    }
*/

      id_hash=get_parent_column_hash(tmp_dbt,row[2]);
      if (id_hash == NULL){
	insert_into_parent(tmp_dbt, row[2], new_column_hash(dbt,ref_id_hash->hash));
	get_structure(row[0], row[1], /*col,id_hash, dest_conn,*/ nav, order + 1);
      }

      append_child_to_parent(tmp_dbt,dbt,row[2]);

/*
    temp_key_2 = build_key(row[0],row[1]);
    col=g_strdup_printf("`%s`",row[3]);
    id_hash=get_parent_column_hash(dbt,col);
    if (id_hash == NULL){
      id_hash=new_empty_column_hash(NULL); //g_hash_table_new_full ( g_str_hash, g_str_equal, g_free, g_free );
      g_message("2- Inserting new %s %s %s: %p", database, table, col, id_hash);
      g_hash_table_insert(dbt->parent_table,g_strdup(col), new_column_hash(tmp_dbt,id_hash->hash));
    }

    g_message("Linking table %s.%s <- %s.%s",temp_key, col, temp_key_2, row[2]);
    col=g_strdup_printf("`%s`",row[2]);
    get_structure(row[0], row[1], col,id_hash, dest_conn, nav, order + 1);
    tmp_dbt= get_dbt(row[0],row[1], FALSE);
    id_hash->dbt=tmp_dbt;
    append_child_to_parent(tmp_dbt,dbt,row[2]);
    g_message("1- Back to: %s.%s", database, table);
    */
  }
  mysql_free_result(res);
  mysql_close(conn);
}


void fill_referenced_table_with_ids(gchar * database, gchar * table, MYSQL *dest_conn);

/*
 * Fill the PK columns with the column in id_hash
 * database
 * table
 * column
 * id_hash
 *
 */

void fill_with_ids(struct db_table *dbt, gchar *column, struct column_hash * column_id_hash , struct db_table *prev_dbt){ //, MYSQL *dest_conn){
/*  if (dbt->multicolumn){
	  g_message("fill_with_ids: multi column found %s %s skipping", dbt->database, dbt->table);
	  return;
  }*/
  g_message("fill_with_ids %s %s with pk %s on Col %s with %p", dbt->database, dbt->table, dbt->primary_key, column, column_id_hash->hash);
  if (tables_skiplist_file && check_skiplist(dbt->database, dbt->table)){
    g_message("Table skipped: %s.%s", dbt->database, dbt->table );
    return;
  }
  struct column_hash * column_pk_hash = column_hash_lookup(dbt->parent_table, dbt->primary_key);

  GString *g_ids = g_string_sized_new(100);
  //hash2string(column_id_hash->hash, g_ids);
  hash2string_without_null(column_id_hash->hash, g_ids);

  if ( !is_primary_key(dbt,column) ){
    // Actualizo la PK con una column con FK
    fill_primary_key_with_column_values(dbt, column, g_ids->str);
  }

  if (!column_pk_hash->dbt->updated){
    g_message("There is no need to fill: %s.%s as %s didn't add new PKs", dbt->database, dbt->table, column);
    return;
    g_message("But going anyways!!!");
  }
  // La PK fue actualizada, por lo tanto debemos cargar las FK de esta tabla
  column_pk_hash->dbt->updated=FALSE;

  g_message("2- Getting data from `%s`.`%s` from column %s on the ids: %s comming from %s %s", dbt->database, dbt->table, column, g_ids->str, prev_dbt->database, prev_dbt->table);
  gchar **pk_fields=g_strsplit(dbt->primary_key, ",",0);
  guint len=g_strv_length(pk_fields);
  guint i;
  struct column_hash *id_hash=NULL;
  if (len>1){
	  // When multicolumn
    for (i=0; i<len; i++){
      id_hash=column_hash_lookup(dbt->parent_table, pk_fields[i]);
      if (id_hash)
        fill_hash_from_ids(dbt, pk_fields[i], column, g_ids->str, id_hash);
    }
  }
  column_pk_hash->updated=FALSE;






//  if (is_pk){
    struct db_table *tmp_dbt=NULL;
    GHashTableIter iter;
    gchar * lkey;

    g_hash_table_iter_init ( &iter, dbt->parent_table );
    struct column_hash * ch=NULL;
    while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &ch )){
      if (!is_primary_key(dbt,lkey)){
        g_message("fill_hash_from_ids %s.%s with select %s and column where %s",dbt->database, dbt->table,lkey, column);
        fill_hash_from_ids(dbt, lkey, column, g_ids->str, ch);
      }
    }

/*

    g_message("fill_with_ids %s.%s starting parent tables",dbt->database, dbt->table);
    g_hash_table_iter_init ( &iter, dbt->parent_table );
    while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &id_hash )){
      if (id_hash->updated  && id_hash->dbt != prev_dbt){
        g_message("Sending from parent fill_with_ids from %s %s to %s %s with column %s and hash: %p | prevdbt: %s %s", dbt->database, dbt->table , id_hash->dbt->database, id_hash->dbt->table, lkey, id_hash->hash, prev_dbt->database, prev_dbt->table);
        fill_with_ids(id_hash->dbt,id_hash->dbt->primary_key,id_hash, dbt);
        id_hash->updated=FALSE;
      }
    }

*/
    g_hash_table_iter_init ( &iter, dbt->child_table );
    GHashTable *tmp_ht;
    g_message("fill_with_ids %s.%s starting child tables",dbt->database, dbt->table);
    while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &tmp_ht )){
      GHashTableIter iter2;
      gchar * lkey2;
      g_hash_table_iter_init ( &iter2,tmp_ht);
      while (g_hash_table_iter_next ( &iter2, (gpointer *) &lkey2, (gpointer *) &tmp_dbt )){
          g_message("Sending from child fill_with_ids from %s %s to %s %s == %s with column %s and hash: %p", dbt->database, dbt->table , tmp_dbt->database, tmp_dbt->table, lkey2, lkey, column_pk_hash->hash );
          fill_with_ids(tmp_dbt, lkey, column_pk_hash, dbt);
      }
    }


//  }


  g_string_free(g_ids, TRUE);
//(void)fill_referencing;
//  fill_referencing_table_with_ids(database, table, dest_conn);
//  fill_referenced_table_with_ids(database, table, dest_conn);


}



void write_sql_column_into_string( MYSQL *conn, gchar **column, MYSQL_FIELD field, gulong length,GString *escaped, GString *statement_row, struct column_hash *tmp_column){
    /* Don't escape safe formats, saves some time */
    gboolean (*fun_ptr_i)(GHashTable*,gchar **, gchar**) = &identity_function;
  //    struct function_pointer *fun_ptr_i=&identity_function;
    gboolean hex_blob=FALSE;
    gchar *new_value_ptr=NULL;
    if (!*column) {
      g_string_append(statement_row, "NULL");
    } else if (field.flags & NUM_FLAG) {
      if (tmp_column && fun_ptr_i(tmp_column->hash, column, &new_value_ptr)){
        g_string_append(statement_row, new_value_ptr);
        g_free(new_value_ptr);
      }else{
        g_string_append(statement_row, *column);
      }
    } else if ( length == 0){
      g_string_append_c(statement_row,*fields_enclosed_by);
      g_string_append_c(statement_row,*fields_enclosed_by);
    } else if ( field.type == MYSQL_TYPE_BLOB && hex_blob ) {
      g_string_set_size(escaped, length * 2 + 1);
      g_string_append(statement_row,"0x");
      mysql_hex_string(escaped->str,*column,length);
      g_string_append(statement_row,escaped->str);
    } else {
      /* We reuse buffers for string escaping, growing is expensive just at
 *        * the beginning */
      g_string_set_size(escaped, length * 2 + 1);
      mysql_real_escape_string(conn, escaped->str, *column, length);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, "CONVERT(");
      g_string_append_c(statement_row, *fields_enclosed_by);
      g_string_append(statement_row, escaped->str);
      g_string_append_c(statement_row, *fields_enclosed_by);
      if (field.type == MYSQL_TYPE_JSON)
        g_string_append(statement_row, " USING UTF8MB4)");
    }
}
/*
    gboolean (*fun_ptr2)(GHashTable*,gchar **, gchar**) = &identity_function;
    for (i = 0; i < select_count ; i++) {
        tmp_column=column_hash_lookup(dbt->parent_table,key_fields[i]);
//       if (tmp_column != NULL){
//          g_message("Column FOUND: `%s`.`%s`.`%s`: ", dbt->database, dbt->table, key_fields[i]);
//        }else{
//          g_message("Column %s: not found in `%s`.`%s`",key_fields[i],dbt->database, dbt->table);
//        }
//         Don't escape safe formats, saves some time 
        if (!row[i]) {
          g_string_append(statement_row, "NULL");
        } else if (fields[i].flags & NUM_FLAG) {
          if (tmp_column && fun_ptr2(tmp_column->hash, &(row[i]), &t)){
            g_string_append(statement_row, t);
            g_free(t);
          }else
            g_string_append(statement_row, row[i]);
//          g_string_append_printf(statement_row_replaced,"%s |", row[i]);
        } else {
          // We reuse buffers for string escaping, growing is expensive just at
          // the beginning 
          g_string_set_size(escaped, lengths[i] * 2 + 1);
          if (tmp_column && fun_ptr2(tmp_column->hash, &(row[i]), &t)){
            mysql_real_escape_string(conn, escaped->str, t, strlen(t));
            g_free(t);
          }else
            mysql_real_escape_string(conn, escaped->str, row[i], lengths[i]);
//          g_string_append_printf(statement_row_replaced,"%s |", row[i]);
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, "CONVERT(");
          g_string_append_c(statement_row, '\'');
          g_string_append(statement_row, escaped->str);
          g_string_append_c(statement_row, '\'');
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, " USING UTF8MB4)");
        }
        if (i < select_count - 1) {
          g_string_append(statement_row, fields_terminated_by);
        } else {
          g_string_append_printf(statement_row,"%s", lines_terminated_by);
        }
    }

*/

void write_row_into_string(MYSQL *conn, struct db_table * dbt, gchar **key_fields, MYSQL_ROW row, MYSQL_FIELD *fields, gulong *lengths, guint num_fields, GString *escaped, GString *statement_row, guint select_count){
  guint i = 0;
  g_string_append(statement_row, lines_starting_by);

  for (i = 0; i < num_fields-1; i++) {
    write_sql_column_into_string( conn, &(row[i]), fields[i], lengths[i], escaped, statement_row, column_hash_lookup(dbt->parent_table,key_fields[i]));
    g_string_append(statement_row, fields_terminated_by);
  }
  write_sql_column_into_string( conn, &(row[i]), fields[i], lengths[i], escaped, statement_row, column_hash_lookup(dbt->parent_table,key_fields[i]));
  if (i < select_count - 1) {
    g_string_append(statement_row, fields_terminated_by);
  } else {
    g_string_append_printf(statement_row,"%s", lines_terminated_by);
  }
}

/* Do actual data chunk reading/writing magic */
guint64 write_table_data_into_db(MYSQL *conn, FILE *file, struct table_job * tj, MYSQL *dest_conn){
  // Split by row is before this step
  // It could write multiple INSERT statments in a data file if statement_size is reached
  guint i;
  guint num_fields = 0;
  guint64 num_rows = 0;
  MYSQL_RES *result = NULL;
  MYSQL_ROW row;
  MYSQL_RES *dest_result = NULL;
  MYSQL_ROW dest_row;
  char *query = NULL;
  gchar *fcfile = NULL;
  struct db_table * dbt = tj->dbt;
  /* Buffer for escaping field values */
  GString *escaped = g_string_sized_new(3000);
  fcfile = g_strdup(tj->filename);
  /* Ghm, not sure if this should be statement_size - but default isn't too big
   * for now */
  GString *statement = g_string_sized_new(statement_size);
  GString *delete_statement = g_string_sized_new(statement_size);
  GString *statement_row = g_string_sized_new(0); //, *statement_row_replaced = g_string_sized_new(0);
  g_string_set_size(statement_row, 0);
  GString *select_fields;
//  (void)dest_conn;
//  struct column_hash *tmp_column;
  struct column_hash * pk_hash=column_hash_lookup(dbt->parent_table,dbt->primary_key);
  GList *values=g_hash_table_get_values(pk_hash->hash);
  gboolean we_need_the_new_pk = values->data == NULL;
  if (!we_need_the_new_pk){
    select_fields = dbt->select_fields_with_all_fields;
  }else
    select_fields = dbt->select_fields_with_all_fields_withouPK;


  /* Poor man's database code */
  query = g_strdup_printf(
      "SELECT %s %s ,%s FROM `%s`.`%s` %s %s %s %s %s %s",
      "/*!40001 SQL_NO_CACHE */" ,
      select_fields->str, dbt->primary_key, tj->database, tj->table, (tj->where || where_option ) ? "WHERE" : "",
      tj->where ? tj->where : "",  (tj->where && where_option ) ? "AND" : "", where_option ? where_option : "", dbt->primary_key ? "ORDER BY" : "",
      dbt->primary_key ? dbt->primary_key : "");
  if (mysql_query(conn, query) || !(result = mysql_use_result(conn))) {
    // ERROR 1146
    g_message("Failed Query: %s",query);
    if (success_on_1146 && mysql_errno(conn) == 1146) {
      g_warning("Error dumping table (%s.%s) data: %s ", tj->database, tj->table,
                mysql_error(conn));
    } else {
      g_critical("Error dumping table (%s.%s) data: %s ", tj->database, tj->table,
                 mysql_error(conn));
      errors++;
    }
    goto cleanup;
  }
  g_message("Extraction Query: %s",query);
  gchar **key_fields=g_strsplit(select_fields->str, ",",0);
  guint select_count=g_strv_length(key_fields);
//  g_strfreev(key_fields);
//  key_fields=g_strsplit(dbt->select_fields_with_all_fields->str, ",",0);

  num_fields = mysql_num_fields(result);
  MYSQL_FIELD *fields = mysql_fetch_fields(result);


  for (i = 0; i < select_count ; i++) {
    g_debug("|%s|",key_fields[i]);
  }

  g_string_set_size(statement, 0);
  /* Poor man's data dump code */
//  gchar *t=NULL;
  while ((row = mysql_fetch_row(result))) {
    gulong *lengths = mysql_fetch_lengths(result);
    num_rows++;
    g_string_printf(statement, "INSERT IGNORE INTO `%s`.`%s` ( %s ) VALUES",dest_db, tj->table, select_fields->str);
    g_string_set_size(statement_row, 0);
//    g_string_set_size(statement_row_replaced, 0);
//    g_string_append(statement_row_replaced, "-- ");
    g_string_append(statement_row, lines_starting_by);

    write_row_into_string(conn, dbt, key_fields, row, fields, lengths, num_fields, escaped, statement_row, select_count);
/*
    gboolean (*fun_ptr2)(GHashTable*,gchar **, gchar**) = &identity_function;
    for (i = 0; i < select_count ; i++) {
        tmp_column=column_hash_lookup(dbt->parent_table,key_fields[i]);
	
//        if (tmp_column != NULL){
//          g_message("Column FOUND: `%s`.`%s`.`%s`: ", dbt->database, dbt->table, key_fields[i]);
//        }else{
//          g_message("Column %s: not found in `%s`.`%s`",key_fields[i],dbt->database, dbt->table);
//        }
	
        // Don't escape safe formats, saves some time 
        if (!row[i]) {
          g_string_append(statement_row, "NULL");
        } else if (fields[i].flags & NUM_FLAG) {
          if (tmp_column && fun_ptr2(tmp_column->hash, &(row[i]), &t)){
            g_string_append(statement_row, t);
            g_free(t);
          }else
            g_string_append(statement_row, row[i]);
//          g_string_append_printf(statement_row_replaced,"%s |", row[i]);
        } else {
          // We reuse buffers for string escaping, growing is expensive just at
          // the beginning 
          g_string_set_size(escaped, lengths[i] * 2 + 1);
          if (tmp_column && fun_ptr2(tmp_column->hash, &(row[i]), &t)){
            mysql_real_escape_string(conn, escaped->str, t, strlen(t));
            g_free(t);
          }else
            mysql_real_escape_string(conn, escaped->str, row[i], lengths[i]);
//          g_string_append_printf(statement_row_replaced,"%s |", row[i]);
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, "CONVERT(");
          g_string_append_c(statement_row, '\'');
          g_string_append(statement_row, escaped->str);
          g_string_append_c(statement_row, '\'');
          if (fields[i].type == MYSQL_TYPE_JSON)
            g_string_append(statement_row, " USING UTF8MB4)");
        }
        if (i < select_count - 1) {
          g_string_append(statement_row, fields_terminated_by);
        } else {
          g_string_append_printf(statement_row,"%s", lines_terminated_by);
        }
    }
    */
    g_string_append(statement, statement_row->str);
    g_string_append(statement, " ; \n");
//    g_string_append(statement,statement_row_replaced->str);
    g_message("Query: %s", statement->str);
    if (mysql_query(dest_conn,statement->str)){
      g_warning("Query execution failed: %s\n%s", statement->str, mysql_error(dest_conn));
      exit(EXIT_FAILURE);
    }else{
      if (we_need_the_new_pk && !dbt->multicolumn){
//	g_message("we_need_the_new_pk");
        if (mysql_query(dest_conn,LAST_INSERT_ID)){
          g_warning("Last inseted id failed");
	  exit(EXIT_FAILURE);
        }else{
          dest_result = mysql_use_result(dest_conn);
          dest_row = mysql_fetch_row(dest_result);
          guint j=0;
          GString *from=g_string_new(""), *to=g_string_new("");
/*
          if (num_fields - select_count > 1){
            g_string_append(from,"(");
            g_string_append(to,"(");
            g_string_append(from,row[select_count+j]);
            tmp_column=g_hash_table_lookup(dbt->parent_table,key_fields[select_count+j]);

            if (fun_ptr2(tmp_column, &(row[select_count+j]), &t)){
              g_string_append(to, t);
              g_free(t);
            }else
              g_string_append(to, row[select_count+j]);
//            g_string_append(to,fun_ptr2(tmp_column, &(row[select_count+j])));
            for (j=1; j< num_fields - select_count; j++){
              g_string_append(from,",");
              g_string_append(to,",");
              g_string_append(from,row[select_count+j]);
              tmp_column=g_hash_table_lookup(dbt->parent_table,key_fields[select_count+j]);

              if (fun_ptr2(tmp_column, &(row[select_count+j]), &t)){
                g_string_append(to, t);
                g_free(t);
              }else
                g_string_append(to, row[select_count+j]);

              //g_string_append(to,fun_ptr2(tmp_column, &(row[select_count+j])));
            }
            g_string_append(from,")");
            g_string_append(to,")");
          }else{
*/
            g_string_append(from,row[select_count]);
            g_string_append(to,dest_row[j]);
	    g_string_append_printf(delete_statement,"DELETE FROM `%s`.`%s` WHERE %s = %s;\n",dest_db,dbt->table,dbt->primary_key,dest_row[j]);
//          }
          g_hash_table_replace(pk_hash->hash,strdup(from->str),strdup(to->str));
//          if (g_strcmp0(row[select_count],last_inserted)==0){
//            g_message("LAST INSERT EQUAL BEFORE? in %s.%s: %d %d %s -> %s | %s", dbt->database, dbt->table, select_count, num_fields, from->str, to->str, (gchar *) g_hash_table_lookup(pk_hash,row[select_count]));
//            exit(1);
//          }
//          gchar* last_inserted=g_strdup(row[select_count]);
          g_message("New key inserted in %s.%s %p: %d %d %s -> %s | %s", dbt->database, dbt->table, pk_hash, select_count, num_fields, from->str, to->str, (gchar *) g_hash_table_lookup(pk_hash->hash,row[select_count]));
          mysql_free_result(dest_result);
        }
      }
    }
    g_string_set_size(statement, 0);
  }
  if (mysql_errno(conn)) {
    g_critical("Could not read data from %s.%s: %s", tj->database, tj->table,
               mysql_error(conn));
    errors++;
  }

  if (statement_row->len > 0) {
    /* this last row has not been written out */
    if (statement->len > 0) {
      /* strange, should not happen */
      g_string_append(statement, statement_row->str);
    } else {
      append_insert (complete_insert, statement, tj->table, fields, num_fields);
      g_string_append(statement, statement_row->str);
    }
  }

  if (delete_statement->len > 0) {
//    g_string_append(delete_statement, statement_terminated_by);
    if (!write_data(file, delete_statement)) {
      g_critical(
          "Could not write out closing newline for %s.%s, now this is sad!",
          tj->database, tj->table);
      goto cleanup;
    }
  }
cleanup:
//  g_strfreev(key_fields);
  g_free(query);

  g_string_free(escaped, TRUE);
  g_string_free(statement, TRUE);
  g_string_free(statement_row, TRUE);
  g_string_free(delete_statement, TRUE);
  if (result) {
    mysql_free_result(result);
  }

  if (file) {
    fclose(file);
  }

  g_string_free(select_fields, TRUE);
  g_free(fcfile);
  g_list_free(values);
  return num_rows;
}

void duplicate (gchar *k, struct db_table *dbt, GHashTable *b){
  g_hash_table_insert(b, g_strdup(k), dbt);
}

void *write_worker(struct data_writer_worker * data_writer_worker) {
  MYSQL *new_conn = mysql_init(NULL);
  smd_connect(new_conn,SOURCE,db);
  MYSQL * dest_conn = mysql_init(NULL);
  smd_connect(dest_conn,DESTINATION,NULL);
  GString *g_ids = g_string_sized_new(100);
  g_string_set_size(g_ids,0);
  if (data_writer_worker->dbt->multicolumn)
    g_string_append_printf(g_ids," (%s) in ",data_writer_worker->dbt->primary_key);
  else
    g_string_append_printf(g_ids," %s in ",data_writer_worker->dbt->primary_key);
  hash2string(data_writer_worker->pk_hash,g_ids);
  struct table_job * tj = new_table_job(data_writer_worker->dbt, NULL, g_ids->str, 1 );
  FILE *file=g_fopen(tj->filename,"w");
  write_table_data_into_db(new_conn, file, tj, dest_conn);
  mysql_close(new_conn);
  mysql_close(dest_conn);
  return NULL;
}

void copy_data(MYSQL *conn, gchar *database, MYSQL *dest_conn){
  (void)conn;
  (void)database;
  (void)dest_conn;
  MYSQL * new_conn=NULL;
  new_conn = mysql_init(NULL);
  smd_connect(new_conn,SOURCE,db);


  GHashTableIter iter,iter2;
  gchar * lkey;
  g_hash_table_iter_init ( &iter, table_hash );
  struct db_table *dbt=NULL, *lowest_dbt=NULL;
  gint lowest=1000;
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt )){
 //   g_message("%s `%s`.`%s`",lkey,dbt->database,dbt->table);
    if (dbt->order < lowest){
      lowest_dbt=dbt;
      lowest=dbt->order;
    }
 //   print_columns(dbt);
  }

  g_message("LOWEST !!! %d `%s`.`%s`",lowest,lowest_dbt->database,lowest_dbt->table);
  g_hash_table_iter_init ( &iter, table_hash );
  while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt )){
    if (dbt->order == lowest){
      g_message("OTHER LOWEST %s `%s`.`%s`",lkey,dbt->database,dbt->table);
    }
  }

  GHashTable *tmp_hash_table=NULL, *next_tmp_hash_table=NULL;

//  g_hash_table_foreach(table_hash, (GHFunc) &duplicate, tmp_hash_table);
  next_tmp_hash_table = table_hash;
  gchar *a;
  guint i=0;
  struct column_hash *b=NULL;
  gboolean we_can_do_it=TRUE;
//  GList* to_remove=NULL;
  GString *log=g_string_sized_new(10);
  GList *list_of_threads=NULL;
  GList *ll;
  GThread *t=NULL;
  struct data_writer_worker *data_writer_worker=NULL;
  while (g_hash_table_size(next_tmp_hash_table)>0){
    if (tmp_hash_table) g_hash_table_destroy(tmp_hash_table);
    tmp_hash_table = next_tmp_hash_table;
    next_tmp_hash_table=g_hash_table_new ( g_str_hash, g_str_equal );
    g_message("Starting round");
    if (g_hash_table_size(tmp_hash_table)>0){
      g_string_set_size(log,0);
      we_can_do_it=TRUE;
      g_hash_table_iter_init ( &iter, tmp_hash_table );
//      to_remove=NULL;
      while (g_hash_table_iter_next ( &iter, (gpointer *) &lkey, (gpointer *) &dbt )){
        g_message("%s `%s`.`%s`",lkey,dbt->database,dbt->table);
        g_hash_table_iter_init ( &iter2, dbt->parent_table);
        we_can_do_it=TRUE;
        while (g_hash_table_iter_next ( &iter2, (gpointer *) &a, (gpointer *) &b ) && we_can_do_it){
          if (g_strcmp0(dbt->primary_key,a)!=0){
            gchar *data=g_hash_table_lookup(b->hash,"NULL");
            if (data!=NULL){
              g_message("El id es NULL(1)!!! `%s`.`%s`.%s",dbt->database,dbt->table,a);
              g_hash_table_insert(b->hash,g_strdup("NULL"), g_strdup("NULL"));
            }
            GList *l=g_hash_table_get_values(b->hash);
            if (l!=NULL && l->data == NULL){
              g_message("Hay Valores pero son nulos!!! `%s`.`%s`.%s %p",dbt->database,dbt->table,a, b);
      	      we_can_do_it=FALSE;
//              break;
            }else{
              g_message("`%s`.`%s`.%s : tiene valores seteados",dbt->database,dbt->table,a);
	    }
	    g_list_free(l);
          }else{
            gchar *data=g_hash_table_lookup(b->hash,"NULL");
            if (data!=NULL){
              g_message("El id es NULL!!!(2) `%s`.`%s`.%s",dbt->database,dbt->table,a);
              g_hash_table_insert(b->hash,g_strdup("NULL"), g_strdup("NULL"));
            }
          }
        }
        if (we_can_do_it){
          g_message("WE CAN DO IT!!! `%s`.`%s`",dbt->database,dbt->table);
//          to_remove=g_list_append(to_remove, g_strdup(lkey));

          GHashTable *pk_hash=((struct column_hash *)g_hash_table_lookup(dbt->parent_table,dbt->primary_key))->hash;
          if (g_hash_table_size(pk_hash) > 0){
            data_writer_worker=g_new(struct data_writer_worker, 1);
            data_writer_worker->pk_hash=pk_hash;
            data_writer_worker->dbt=dbt;

            t=g_thread_create((GThreadFunc)write_worker, data_writer_worker, TRUE, NULL);
            list_of_threads=g_list_append(list_of_threads,t);
/*            g_string_set_size(g_ids,0);
            if (dbt->multicolumn)
              g_string_append_printf(g_ids," (%s) in ",dbt->primary_key);
            else
              g_string_append_printf(g_ids," %s in ",dbt->primary_key);
            hash2string(pk_hash,g_ids);
            struct table_job * tj = new_table_job(dbt, NULL, g_ids->str, 1 );
            file=g_fopen(tj->filename,"w");
            write_table_data_into_db(new_conn, file, tj, dest_conn);
*/
  	    }


        }else{
          g_message("i: %d | WE can NOT do IT!!! `%s`.`%s`.%s \nLOG: %s",i,dbt->database,dbt->table,a, log->str);
          g_message("Inserting into %s: %d", lkey,g_hash_table_size(next_tmp_hash_table));
          g_hash_table_insert(next_tmp_hash_table,g_strdup(lkey),dbt);
          i++;
          if (i>1000){
            g_message("Exiting failure");
            exit(EXIT_FAILURE);
          }
        }
      }
/*      while (to_remove!=NULL){
        g_message("Removing: %s",(gchar *)(to_remove->data));
        g_hash_table_remove(tmp_hash_table, to_remove->data);
        to_remove=to_remove->next;
      }
*/
    }
    g_message("Inserting into FINAL AMOUNT pre while: %d",g_hash_table_size(next_tmp_hash_table));

    while(list_of_threads!=NULL){
      ll=list_of_threads;
      list_of_threads=g_list_remove_link(list_of_threads,ll);
      g_thread_join(ll->data);
 //     g_list_free(ll);
    }


  }
  g_message("Inserting into FINAL AMOUNT: %d",g_hash_table_size(next_tmp_hash_table));
  g_string_free(log,TRUE);
/*
  gchar *query;
  GString *list_tables_done = g_string_sized_new(100);
  query=g_strdup_printf("select table_name from information_schema.TABLES where table_name not in (select TABLE_NAME c from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' group by TABLE_NAME) and table_schema='%s'", database, database);

  copy_to_dest(query, conn, new_conn, dest_conn, database, &list_tables_done);

  query=g_strdup_printf("select table_name from information_schema.TABLES where table_name not in (select TABLE_NAME c from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_name not in (%s) group by TABLE_NAME) and table_schema='%s' and table_name not in (%s)", database, list_tables_done->str, database,list_tables_done->str);
  copy_to_dest(query, conn, new_conn, dest_conn, database, &list_tables_done);
  query=g_strdup_printf("select table_name from information_schema.TABLES where table_name not in (select TABLE_NAME c from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_name not in (%s) group by TABLE_NAME) and table_schema='%s' and table_name not in (%s)", database, list_tables_done->str, database,list_tables_done->str);
  copy_to_dest(query, conn, new_conn, dest_conn, database, &list_tables_done);
  query=g_strdup_printf("select table_name from information_schema.TABLES where table_name not in (select TABLE_NAME c from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_name not in (%s) group by TABLE_NAME) and table_schema='%s' and table_name not in (%s)", database, list_tables_done->str, database,list_tables_done->str);
  copy_to_dest(query, conn, new_conn, dest_conn, database, &list_tables_done);
  query=g_strdup_printf("select table_name from information_schema.TABLES where table_name not in (select TABLE_NAME c from information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL AND REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_name not in (%s) group by TABLE_NAME) and table_schema='%s' and table_name not in (%s)", database, list_tables_done->str, database,list_tables_done->str);
  copy_to_dest(query, conn, new_conn, dest_conn, database, &list_tables_done);
*/
}
