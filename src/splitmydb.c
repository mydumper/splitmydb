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

        Authors:    David Ducos (david dot ducos at gmail dot com)
*/

#include <mysql.h>
#include <stdio.h>
#include <glib.h>
#include <mysql.h>
#include "connection.h"
#include "write_insert.h"
#include "tables_skiplist.h"
#include "set_verbose.h"
#include "common.h"
//gboolean compress_protocol = FALSE;
gchar *set_names_statement=NULL;
gboolean debug = FALSE;
gchar *db = NULL;
gchar *dest_db=NULL;
gchar * input_directory=NULL;
gchar * table_option = NULL;
gchar * column = NULL;
gchar * ids =NULL;
char *defaults_file = NULL;
gchar * where_option;
gchar *tables_skiplist_file = NULL;
guint errors=0;
guint number_threads=4;
static GOptionEntry entries[] = {
    {"directory", 'd', 0, G_OPTION_ARG_STRING, &input_directory,
     "Directory of the dump", NULL},
    {"omit-from-file", 'O', 0, G_OPTION_ARG_STRING, &tables_skiplist_file,
     "File containing a list of database.table entries to skip, one per line ", NULL},
    {"defaults-file", 0, 0, G_OPTION_ARG_FILENAME, &defaults_file,
     "Use a specific defaults file", NULL},
    {"database", 'B', 0, G_OPTION_ARG_STRING, &db,
     "Database where table and fk are located", NULL},
    {"dest-database", 'V', 0, G_OPTION_ARG_STRING, &dest_db,
     "Database that we paste on the insert statement", NULL},
    {"threads", 'n', 0, G_OPTION_ARG_INT, &number_threads,
     "Amount of threads to use with --exec", NULL},
    {"table", 't', 0, G_OPTION_ARG_STRING, &table_option,
     "Table name where we must initialize", NULL},
    {"column", 'c', 0, G_OPTION_ARG_STRING, &column,
     "Column where the ids are located", NULL},
    {"ids", 'i', 0, G_OPTION_ARG_STRING, &ids,
     "List of id, separated by comma", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}};

int main(int argc, char *argv[]) {

  GError *error = NULL;
  GOptionContext *context;
  context = g_option_context_new("Foreign Key follower");
  GOptionGroup *main_group =
      g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
  g_option_group_add_entries(main_group, entries);
// load_connection_entries(main_group);
  load_connection_entries(context);
  g_option_context_set_main_group(context, main_group);

  if (!g_option_context_parse(context, &argc, &argv, &error)) {
    g_print("option parsing failed: %s, try --help\n", error->message);
    exit(EXIT_FAILURE);
  }
  initialize_connection("splitmydb");
  set_connection_defaults_file_and_group(defaults_file,NULL);
  if (tables_skiplist_file)
    read_tables_skiplist(tables_skiplist_file, &errors);
  init();
  MYSQL * conn=NULL, *dest_conn=NULL;
  if (dest_db==NULL){
    g_error("Destination database was not set use -V");
  }
  conn = mysql_init(NULL);
  dest_conn = mysql_init(NULL);
  g_message("Connecting to SOURCE");
  smd_connect(conn,SOURCE,db);
  g_message("Connecting to DESTINATION");
  smd_connect(dest_conn,DESTINATION,dest_db);
  GHashTable * dd=g_hash_table_new ( g_str_hash, g_str_equal );

  g_message("Getting Structure");
  get_structure(db, table_option, dd, 0);
  g_message("Filling IDS");
  struct db_table * dbt=get_dbt(db, table_option, TRUE);
  struct column_hash *ght=get_parent_column_hash(dbt,column);

  gchar **c=g_strsplit(ids,",",-1);
  while (*c){
    column_hash_insert(ght,g_strdup(*c),NULL);
    c++;
  }
  dbt->updated=TRUE;
  ght->updated=TRUE;
  init_fill_id_threads();
  fill_with_ids(dbt, column, ght, dbt);
  process_multicolumn_tables();

  g_message("Printing INFO1");
  print_info();

  g_message("Coping DATA");
  copy_data(conn,db,dest_conn);
  g_message("Printing INFO2");
  print_info();

  return 0;
}
