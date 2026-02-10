#ifndef otterbrix_otterbrix_H
#define otterbrix_otterbrix_H

#include <cstdint>
#include <cstdlib>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct string_view_t {
    const char* data;
    size_t size;
} string_view_t;

typedef struct config_t {
    int level;
    string_view_t log_path;
    string_view_t wal_path;
    string_view_t disk_path;
    string_view_t main_path;
    bool wal_on;
    bool disk_on;
    bool sync_to_disk;
} config_t;

typedef enum state_t
{
    init,
    created,
    destroyed
} state_t;

typedef void* otterbrix_ptr;
typedef void* cursor_ptr;
typedef void* value_ptr;

typedef struct error_message {
    int32_t code;
    char* message;
} error_message;

otterbrix_ptr otterbrix_create(config_t cfg);
void otterbrix_destroy(otterbrix_ptr);

cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query);

cursor_ptr create_database(otterbrix_ptr ptr, string_view_t database_name);
cursor_ptr create_collection(otterbrix_ptr ptr, string_view_t database_name, string_view_t collection_name);

void release_cursor(cursor_ptr ptr);
int32_t cursor_size(cursor_ptr ptr);
int32_t cursor_column_count(cursor_ptr ptr);
bool cursor_has_next(cursor_ptr ptr);
bool cursor_is_success(cursor_ptr ptr);
bool cursor_is_error(cursor_ptr ptr);
error_message cursor_get_error(cursor_ptr ptr);

char* cursor_column_name(cursor_ptr ptr, int32_t column_index);

value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index);

value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name);

void release_value(value_ptr ptr);
bool value_is_null(value_ptr ptr);
bool value_is_bool(value_ptr ptr);
bool value_is_int(value_ptr ptr);
bool value_is_uint(value_ptr ptr);
bool value_is_double(value_ptr ptr);
bool value_is_string(value_ptr ptr);

bool value_get_bool(value_ptr ptr);
int64_t value_get_int(value_ptr ptr);
uint64_t value_get_uint(value_ptr ptr);
double value_get_double(value_ptr ptr);
char* value_get_string(value_ptr ptr);

#ifdef __cplusplus
}
#endif

#endif //otterbrix_otterbrix_H
