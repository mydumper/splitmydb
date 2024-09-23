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

    Authors:        David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <mysql.h>
#include <stdio.h>
#include <pcre.h>
#define ZSTD_EXTENSION ".zst"
#define GZIP_EXTENSION ".gz"

#define SOURCE "source"
#define DESTINATION "destination"


extern GList *ignore_errors_list;
extern gchar zstd_paths[2][15];
extern gchar gzip_paths[2][15];
extern gchar **zstd_cmd;
extern gchar **gzip_cmd;
extern const gchar *start_replica;
extern const gchar *stop_replica;
extern const gchar *start_replica_sql_thread;
extern const gchar *stop_replica_sql_thread;
extern const gchar *reset_replica;
extern const gchar *show_replica_status;
extern const gchar *show_all_replicas_status;
extern const gchar *show_binary_log_status;
extern const gchar *change_replication_source;
extern guint source_control_command;
#ifndef _src_common_h
#define _src_common_h
void initialize_share_common();
void initialize_zstd_cmd();
void initialize_gzip_cmd();

struct object_to_export{
  gboolean no_data;
  gboolean no_schema;
  gboolean no_trigger;
};

struct configuration_per_table{
  GHashTable *all_anonymized_function;
  GHashTable *all_where_per_table;
  GHashTable *all_limit_per_table;
  GHashTable *all_num_threads_per_table;
  GHashTable *all_columns_on_select_per_table;
  GHashTable *all_columns_on_insert_per_table;
  GHashTable *all_object_to_export;
  GHashTable *all_partition_regex_per_table;
  GHashTable *all_rows_per_table;
};

#define STREAM_BUFFER_SIZE 1000000
#define DEFAULTS_FILE "/etc/mydumper.cnf"
struct function_pointer;
typedef gchar * (*fun_ptr)(gchar **,gulong*, struct function_pointer*);

struct function_pointer{
  fun_ptr function;
  GHashTable *memory;
  gchar *value;
  GList *parse;
  GList *delimiters;
};

gchar * remove_new_line(gchar *to);
char * checksum_table_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_table(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_process_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_trigger_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_trigger_structure_from_database(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_view_structure(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_database_defaults(MYSQL *conn, char *database, char *table, int *errn);
char * checksum_table_indexes(MYSQL *conn, char *database, char *table, int *errn);
int write_file(FILE * file, char * buff, int len);
void create_backup_dir(char *new_directory, char *new_fifo_directory);
void create_fifo_dir(char *new_fifo_directory);
guint strcount(gchar *text);
void m_remove0(gchar * directory, const gchar * filename);
gboolean m_remove(gchar * directory, const gchar * filename);
GKeyFile * load_config_file(gchar * config_file);
void load_config_group(GKeyFile *kf, GOptionContext *context, const gchar * group);
void execute_gstring(MYSQL *conn, GString *ss);
gchar *replace_escaped_strings(gchar *c);
void escape_tab_with(gchar *to);
void load_hash_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar * group_variables);
//void load_anonymized_functions_from_key_file(GKeyFile *kf, GHashTable *all_anonymized_function, fun_ptr get_function_pointer_for());
//void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * conf_per_table, fun_ptr get_function_pointer_for());
void load_per_table_info_from_key_file(GKeyFile *kf, struct configuration_per_table * conf_per_table, struct function_pointer * init_function_pointer());
void refresh_set_session_from_hash(GString *ss, GHashTable * set_session_hash);
void refresh_set_global_from_hash(GString *ss, GString *sr, GHashTable * set_global_hash);
gboolean is_table_in_list(gchar *database, gchar *table, gchar **tl);
gboolean is_mysql_special_tables(gchar *database, gchar *table);
GHashTable * initialize_hash_of_session_variables();
void load_common_entries(GOptionGroup *main_group);
void free_hash(GHashTable * set_session_hash);
void initialize_common_options(GOptionContext *context, const gchar *group);
gchar **get_table_list(gchar *tables_list);
void free_hash_table(GHashTable * hash);
void remove_definer(GString * data);
void remove_definer_from_gchar(char * str);
void print_version(const gchar *program);
gboolean stream_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error);
void initialize_set_names();
void free_set_names();
gchar *filter_sequence_schemas(const gchar *create_table);
void set_session_hash_insert(GHashTable * set_session_hash, const gchar *key, gchar *value);

#endif

/* using fewer than 2 threads can cause mydumper to hang */
#define MIN_THREAD_COUNT 2
void check_num_threads();

void m_error(const char *fmt, ...);
void m_critical(const char *fmt, ...);
void m_warning(const char *fmt, ...);
void load_hash_of_all_variables_perproduct_from_key_file(GKeyFile *kf, GHashTable * set_session_hash, const gchar *str);
GRecMutex * g_rec_mutex_new();
gboolean read_data(FILE *file, GString *data, gboolean *eof, guint *line);
gchar *m_date_time_new_now_local();

void print_int(const char*_key, int val);
void print_string(const char*_key, const char *val);
void print_bool(const char*_key, gboolean val);
void print_list(const char*_key, GList *list);

gchar *get_zstd_cmd();
gchar *get_gzip_cmd();
char * double_quoute_protect(char *r);
char * backtick_protect(char *r);
char * newline_protect(char *r);
char * newline_unprotect(char *r);
void set_thread_name(const char *format, ...);
extern void trace(const char *format, ...);
#define message(...) \
  if (debug) \
    trace(__VA_ARGS__); \
  else \
    g_message(__VA_ARGS__);

#define array_elements(A) ((guint) (sizeof(A)/sizeof(A[0])))
#define key_strcmp ((int (*)(const void *, const void *)) &strcmp)

#if !GLIB_CHECK_VERSION(2, 68, 0)
extern guint
g_string_replace (GString     *string,
                  const gchar *find,
                  const gchar *replace,
                  guint        limit);
#endif

#if !GLIB_CHECK_VERSION(2, 36, 0)
extern guint g_get_num_processors (void);
#endif
char *show_warnings_if_possible(MYSQL *conn);
int global_process_create_table_statement (gchar * statement, GString *create_table_statement, GString *alter_table_statement, GString *alter_table_constraint_statement, gchar *real_table, gboolean split_indexes);
void initialize_conf_per_table(struct configuration_per_table *cpt);
void parse_object_to_export(struct object_to_export *object_to_export,gchar *val);
gchar *build_dbt_key(gchar *a, gchar *b);

gboolean common_arguments_callback(const gchar *option_name,const gchar *value, gpointer data, GError **error);
void discard_mysql_output(MYSQL *conn);
gboolean m_query(  MYSQL *conn, const gchar *query, void log_fun(const char *, ...) , const char *fmt, ...);
