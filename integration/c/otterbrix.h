#ifndef otterbrix_otterbrix_H
#define otterbrix_otterbrix_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
#define OTTERBRIX_NOEXCEPT noexcept
extern "C" {
#else
#define OTTERBRIX_NOEXCEPT
#endif

typedef struct string_view_t {
    const char* data;
    size_t size;
} string_view_t;

// ABI NOTE: `disk_on`, `sync_to_disk` and `wal_on` all removed, not deprecated. Every table is
// disk-backed, every commit fsyncs, and the journal is not optional: switching it off left the
// disk index (durable per commit) ahead of the table (durable per checkpoint), and stamped a zero
// checkpoint floor that made the NEXT run replay a tail it had already absorbed.
// Hand-written bindings (integration/csharp) must drop them too, and must drop the LAST field
// first: removing an earlier bool while a later one survives shifts the survivor onto the wrong
// offset, which no compiler reports. bindgen-generated ones (integration/rust/otterbrix-sys)
// follow this header automatically.
typedef struct config_t {
    int level;
    string_view_t log_path;
    string_view_t wal_path;
    string_view_t disk_path;
    string_view_t main_path;
} config_t;

// Contract of every call below; a violation is asserted, never answered:
// - an otterbrix_ptr, cursor_ptr or value_ptr passed in is never nullptr;
// - a string_view_t with size > 0 has non-null data;
// - out_error and rows are never nullptr, params is nullptr only with param_count 0.
// No call throws (each is noexcept): a refusal is an error_message or an error cursor.
// Running out of memory terminates the process, so a call that answers a cursor never answers nullptr.
// When the OS refuses a thread, otterbrix_create answers thread_start_failed for a manager loop thread and
// terminates the process for an actor-zeta scheduler thread (actor-zeta#188).
// A const char* answered from a cursor or a value is borrowed: it stays valid until release_cursor or release_value
// of that handle and is never passed to otterbrix_free_string.
typedef void* otterbrix_ptr;
typedef void* cursor_ptr;
typedef void* value_ptr;

typedef struct error_message {
    int32_t code;
    char* message;
} error_message;

// A refused start answers nullptr and fills *out_error; the caller frees out_error->message with
// otterbrix_free_string. On success out_error->code is 0 and out_error->message is nullptr.
otterbrix_ptr otterbrix_create(config_t cfg, error_message* out_error) OTTERBRIX_NOEXCEPT;
// Every cursor and value holds the engine it came from. otterbrix_destroy gives up the caller's handle: the engine
// (its threads, its memory, the lock on main_path) goes when the last cursor or value is released too, on whichever
// thread releases it, so they may be released in any order and from any thread. After otterbrix_destroy the handle
// itself must not be passed to any call again.
void otterbrix_destroy(otterbrix_ptr) OTTERBRIX_NOEXCEPT;

// A handle is one session: a BEGIN sent through it stays open for its next statement (execute_sql_params included).
cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query) OTTERBRIX_NOEXCEPT;

typedef enum sql_param_kind_t
{
    SQL_PARAM_NULL = 0,
    SQL_PARAM_BOOL,
    SQL_PARAM_INT64,
    SQL_PARAM_UINT64,
    SQL_PARAM_DOUBLE,
    SQL_PARAM_STRING,
} sql_param_kind_t;

typedef struct sql_param_t {
    int32_t index;
    sql_param_kind_t kind;
    uint8_t bool_value;
    int64_t int64_value;
    uint64_t uint64_value;
    double double_value;
    string_view_t string_value;
} sql_param_t;

// A parameter with index < 1 or an unknown kind answers an error cursor with code invalid_parameter.
cursor_ptr execute_sql_params(otterbrix_ptr ptr, string_view_t query, const sql_param_t* params, size_t param_count)
    OTTERBRIX_NOEXCEPT;

void release_cursor(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
int32_t cursor_size(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
// The rows an INSERT / UPDATE / DELETE without RETURNING wrote go to *rows; false (and *rows untouched) for any other
// statement. A RETURNING result is data
bool cursor_affected_rows(cursor_ptr ptr, uint64_t* rows) OTTERBRIX_NOEXCEPT;
// The pg_class oid of the table a CREATE TABLE created goes to *oid; false (and *oid untouched) for any other reply
bool cursor_created_oid(cursor_ptr ptr, uint32_t* oid) OTTERBRIX_NOEXCEPT;
int32_t cursor_column_count(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
int32_t cursor_column_logical_type(cursor_ptr ptr, int32_t column_index) OTTERBRIX_NOEXCEPT;
bool cursor_has_next(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
bool cursor_is_success(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
bool cursor_is_error(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
// A success cursor answers code 0 and "".
int32_t cursor_error_code(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
const char* cursor_error_message(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;
// The 1-based character of the statement text the error points at (PostgreSQL's error position), or -1 when the
// error points at none or the cursor holds no error.
int32_t cursor_error_position(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;

typedef enum transaction_status_t
{
    TRANSACTION_STATUS_IDLE = 0,
    TRANSACTION_STATUS_IN_TRANSACTION = 1,
    TRANSACTION_STATUS_FAILED = 2,
} transaction_status_t;

// A transaction_status_t value: where the statement's session stands once it ran.
int32_t cursor_transaction_status(cursor_ptr ptr) OTTERBRIX_NOEXCEPT;

const char* cursor_column_name(cursor_ptr ptr, int32_t column_index) OTTERBRIX_NOEXCEPT;

value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index) OTTERBRIX_NOEXCEPT;

value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name) OTTERBRIX_NOEXCEPT;

void release_value(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_null(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_bool(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_int(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_uint(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_double(value_ptr ptr) OTTERBRIX_NOEXCEPT;
bool value_is_string(value_ptr ptr) OTTERBRIX_NOEXCEPT;

// A value_get_* whose value_is_* answered false is asserted; a release build answers 0, false, 0.0 or "".
bool value_get_bool(value_ptr ptr) OTTERBRIX_NOEXCEPT;
int64_t value_get_int(value_ptr ptr) OTTERBRIX_NOEXCEPT;
uint64_t value_get_uint(value_ptr ptr) OTTERBRIX_NOEXCEPT;
double value_get_double(value_ptr ptr) OTTERBRIX_NOEXCEPT;
const char* value_get_string(value_ptr ptr) OTTERBRIX_NOEXCEPT;

void otterbrix_free_string(char* str) OTTERBRIX_NOEXCEPT;

#ifdef __cplusplus
}
#endif

#undef OTTERBRIX_NOEXCEPT

#endif //otterbrix_otterbrix_H
