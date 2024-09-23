#define MAX_LENGTH 5000

struct db_table;

struct column_hash{
  GHashTable *hash;
  gboolean updated;
  struct db_table * dbt;
};

struct db_table {
  char *database;
  char *table;
  char *table_filename;
  char *escaped_table;
  gint order;
  GString *select_fields;
  GString *select_fields_with_all_fields;
  GString *select_fields_with_all_fields_withouPK;
  gboolean has_generated_fields;
  gboolean multicolumn;
  guint64 datalength;
  guint rows;
  GMutex *rows_lock;
  GList *anonymized_function;
  char *primary_key;
  //  GHashTable *ids;
  GHashTable *parent_table;
  GHashTable *child_table;
  gboolean updated;
};

struct fill_thread_data{
  struct db_table *dbt;
  gchar *column_select;
  gchar *column_where;
  gchar *column_second;
  gchar *ids;
  struct column_hash * id_hash;
  struct column_hash * pk_hash;
  guint len;
  guint step;
  gboolean not_in;
};

struct thread_data {
  guint thread_id;
  MYSQL *thrconn;
  GAsyncQueue *queue;
  GAsyncQueue *ready;
  gboolean less_locking_stage;
};

struct table_job {
  char *database;
  char *table;
  char *partition;
  guint nchunk;
  char *filename;
  char *where;
  struct db_table *dbt;
};

struct data_writer_worker{
  struct db_table *dbt;
  GHashTable *pk_hash;
};

void write_table_job_into_db(MYSQL *conn, struct table_job *tj, MYSQL *dest_conn);
gchar * build_data_filename(char *database, char *table, guint part, guint sub_part);
GString *get_insertable_fields(MYSQL *conn, char *database, char *table);
void init();
//void get_data(MYSQL *conn, gchar * database, gchar * table, const gchar *column, gchar *ids, MYSQL *dest_conn, GHashTable *nav );
void get_data(MYSQL *conn, gchar * database, gchar * table, const gchar *column, struct column_hash * column_id_hash, MYSQL *dest_conn, GHashTable *nav );
void get_referenced(MYSQL *conn, MYSQL *dest_conn);
void copy_data(MYSQL *conn, gchar *database, MYSQL *dest_conn);
void print_info();
void get_structure(gchar * database, gchar * table, /*gchar *column, struct column_hash * column_id_hash, MYSQL *dest_conn,*/ GHashTable *nav, gint order);
void fill_with_ids(struct db_table *dbt, gchar *column, struct column_hash * column_id_hash, struct db_table *prev_dbt);
void fill_referencing_table_with_ids(gchar * database, gchar * table, MYSQL *dest_conn);
struct db_table * get_dbt(const gchar *database, const gchar *table, gboolean e);
struct column_hash * get_parent_column_hash(struct db_table * dbt, gchar *column);
void print_hash2string(GHashTable *hash, GString *str);
struct column_hash * new_column_hash(struct db_table *dbt, GHashTable *h);
struct column_hash * new_empty_column_hash(struct db_table *dbt);
void column_hash_insert(struct column_hash *ch, gchar *k, gchar * v);
struct column_hash * column_hash_lookup(GHashTable *column_hash, gchar *k);
void process_multicolumn_tables();
void init_fill_id_threads();
