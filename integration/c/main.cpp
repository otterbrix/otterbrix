#include "otterbrix.h"

#include <components/cursor/cursor.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <cassert>
#include <cstring>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using cursor_t = components::cursor::cursor_t;
using otterbrix::otterbrix_t;

namespace {
    // `engine` is declared first so it outlives the cursor it answered: members are destroyed bottom-up.
    struct cursor_storage_t {
        otterbrix::otterbrix_ptr engine;
        boost::intrusive_ptr<cursor_t> cursor;
    };

    enum class value_kind_t
    {
        null,
        boolean,
        signed_int,
        unsigned_int,
        floating,
        string,
        other
    };

    // `engine` is declared first: it owns the resource `text` lives on, and members are destroyed bottom-up.
    struct value_storage_t {
        otterbrix::otterbrix_ptr engine;
        value_kind_t kind;
        bool boolean;
        int64_t signed_int;
        uint64_t unsigned_int;
        double floating;
        std::pmr::string text;
    };

    struct handle_storage_t {
        otterbrix::otterbrix_ptr engine;
        const otterbrix::session_id_t session;
    };

    handle_storage_t* live_otterbrix(otterbrix_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<handle_storage_t*>(ptr);
    }

    cursor_storage_t* convert_cursor(cursor_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<cursor_storage_t*>(ptr);
    }

    value_storage_t* convert_value(value_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<value_storage_t*>(ptr);
    }

    cursor_ptr store_cursor(otterbrix_t* engine, components::cursor::cursor_t_ptr c) {
        auto storage = std::make_unique<cursor_storage_t>();
        storage->engine = otterbrix::otterbrix_ptr{engine};
        storage->cursor = std::move(c);
        return reinterpret_cast<cursor_ptr>(storage.release());
    }

    cursor_ptr error_cursor(otterbrix_t* engine, core::error_code_t code, std::string_view text) {
        auto* resource = engine->dispatcher()->resource();
        return store_cursor(
            engine,
            components::cursor::make_cursor(resource, core::error_t(code, std::pmr::string{text, resource})));
    }

    // Freed by the caller with otterbrix_free_string.
    char* copy_to_c_string(std::string_view text) noexcept {
        auto* copy = new char[text.size() + 1];
        std::memcpy(copy, text.data(), text.size());
        copy[text.size()] = '\0';
        return copy;
    }

    error_message make_error_message(const core::error_t& error) noexcept {
        return error_message{static_cast<int32_t>(error.type),
                             copy_to_c_string(std::string_view{error.what.data(), error.what.size()})};
    }

    std::string_view to_view(string_view_t sv) {
        assert((sv.data != nullptr || sv.size == 0) && "string_view_t: non-zero size with null data");
        return std::string_view{sv.data, sv.size};
    }
} // namespace

extern "C" otterbrix_ptr otterbrix_create(config_t cfg, error_message* out_error) noexcept {
    assert(out_error != nullptr && "otterbrix_create: out_error is required");
    *out_error = error_message{static_cast<int32_t>(core::error_code_t::none), nullptr};
    auto config = configuration::config::create_config(std::filesystem::path{to_view(cfg.main_path)});
    config.log.level = static_cast<log_t::level>(cfg.level);
    config.log.path = std::pmr::string(to_view(cfg.log_path));
    config.wal.path = std::pmr::string(to_view(cfg.wal_path));
    config.disk.path = std::pmr::string(to_view(cfg.disk_path));

    auto engine = otterbrix::make_otterbrix(config);
    if (engine.has_error()) {
        *out_error = make_error_message(engine.error());
        return nullptr;
    }
    return reinterpret_cast<otterbrix_ptr>(new handle_storage_t{engine.value(), otterbrix::session_id_t{}});
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) noexcept { delete live_otterbrix(ptr); }

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query) noexcept {
    const handle_storage_t* handle = live_otterbrix(ptr);
    otterbrix_t* engine = handle->engine.get();
    auto cursor = engine->dispatcher()->execute_sql(handle->session, std::string{to_view(query)});
    return store_cursor(engine, std::move(cursor));
}

extern "C" cursor_ptr
execute_sql_params(otterbrix_ptr ptr, string_view_t query, const sql_param_t* params, size_t param_count) noexcept {
    assert(params != nullptr || param_count == 0);
    const handle_storage_t* handle = live_otterbrix(ptr);
    otterbrix_t* engine = handle->engine.get();
    auto* resource = engine->dispatcher()->resource();
    std::vector<std::pair<size_t, components::vector::vector_t>> bound;
    bound.reserve(param_count);
    for (size_t i = 0; i < param_count; ++i) {
        const sql_param_t& p = params[i];
        if (p.index < 1) {
            return error_cursor(engine,
                                core::error_code_t::invalid_parameter,
                                "sql_param_t: index must be >= 1 (e.g. $1 -> 1)");
        }
        const size_t id = static_cast<size_t>(p.index);
        switch (p.kind) {
            case SQL_PARAM_NULL:
                bound.emplace_back(id,
                                   components::vector::make_null(
                                       resource,
                                       components::types::complex_logical_type{components::types::logical_type::NA}));
                break;
            case SQL_PARAM_BOOL:
                bound.emplace_back(id, components::vector::make_scalar(resource, p.bool_value != 0));
                break;
            case SQL_PARAM_INT64:
                bound.emplace_back(id, components::vector::make_scalar(resource, p.int64_value));
                break;
            case SQL_PARAM_UINT64:
                bound.emplace_back(id, components::vector::make_scalar(resource, p.uint64_value));
                break;
            case SQL_PARAM_DOUBLE:
                bound.emplace_back(id, components::vector::make_scalar(resource, p.double_value));
                break;
            case SQL_PARAM_STRING:
                bound.emplace_back(id, components::vector::make_scalar(resource, to_view(p.string_value)));
                break;
            default:
                return error_cursor(engine, core::error_code_t::invalid_parameter, "sql_param_t: unknown kind");
        }
    }
    auto cursor = engine->dispatcher()->execute_sql_with_params(handle->session, std::string{to_view(query)}, bound);
    return store_cursor(engine, std::move(cursor));
}

extern "C" void release_cursor(cursor_ptr ptr) noexcept { delete convert_cursor(ptr); }

extern "C" int32_t cursor_size(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    if (storage->cursor->is_write()) {
        return 0;
    }
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" bool cursor_affected_rows(cursor_ptr ptr, uint64_t* rows) noexcept {
    assert(rows != nullptr);
    auto storage = convert_cursor(ptr);
    if (!storage->cursor->is_write()) {
        return false;
    }
    *rows = storage->cursor->affected_rows();
    return true;
}

extern "C" bool cursor_created_oid(cursor_ptr ptr, uint32_t* oid) noexcept {
    assert(oid != nullptr);
    auto storage = convert_cursor(ptr);
    const auto created = storage->cursor->created_oid();
    if (created == components::catalog::INVALID_OID) {
        return false;
    }
    *oid = created;
    return true;
}

extern "C" int32_t cursor_column_count(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    if (storage->cursor->is_write()) {
        return 0;
    }
    return static_cast<int32_t>(storage->cursor->column_count());
}

extern "C" int32_t cursor_column_logical_type(cursor_ptr ptr, int32_t column_index) noexcept {
    auto storage = convert_cursor(ptr);
    if (storage->cursor->is_write()) {
        return -1;
    }
    const auto& types = storage->cursor->type_data();
    if (column_index < 0 || static_cast<size_t>(column_index) >= types.size()) {
        return -1;
    }
    return static_cast<int32_t>(types[static_cast<size_t>(column_index)].type());
}

extern "C" bool cursor_has_next(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return storage->cursor->has_next();
}

extern "C" bool cursor_is_success(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_success();
}

extern "C" bool cursor_is_error(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_error();
}

extern "C" int32_t cursor_error_code(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->get_error().type);
}

extern "C" const char* cursor_error_message(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return storage->cursor->get_error().what.c_str();
}

extern "C" int32_t cursor_error_position(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return storage->cursor->get_error().position;
}

static_assert(static_cast<int32_t>(components::session::transaction_status_t::idle) == TRANSACTION_STATUS_IDLE);
static_assert(static_cast<int32_t>(components::session::transaction_status_t::in_transaction) ==
              TRANSACTION_STATUS_IN_TRANSACTION);
static_assert(static_cast<int32_t>(components::session::transaction_status_t::failed) == TRANSACTION_STATUS_FAILED);

extern "C" int32_t cursor_transaction_status(cursor_ptr ptr) noexcept {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->transaction_status());
}

extern "C" const char* cursor_column_name(cursor_ptr ptr, int32_t column_index) noexcept {
    auto storage = convert_cursor(ptr);
    if (storage->cursor->is_write()) {
        return nullptr;
    }
    const auto& types = storage->cursor->type_data();
    if (static_cast<size_t>(column_index) >= types.size()) {
        return nullptr;
    }
    return types[static_cast<size_t>(column_index)].alias().c_str();
}

extern "C" value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index) noexcept {
    auto storage = convert_cursor(ptr);
    auto& cursor = *storage->cursor;

    if (cursor.is_write() || row_index < 0 || column_index < 0 || static_cast<size_t>(row_index) >= cursor.size() ||
        static_cast<size_t>(column_index) >= cursor.column_count()) {
        return nullptr;
    }

    const auto row = static_cast<uint64_t>(row_index);
    const auto col = static_cast<uint64_t>(column_index);

    const auto location = cursor.locate(row);
    if (location.has_error()) {
        return nullptr;
    }
    const auto null_cell = cursor.is_null(col, row);
    if (null_cell.has_error()) {
        return nullptr;
    }
    auto* resource = storage->engine->dispatcher()->resource();
    std::unique_ptr<value_storage_t> value{
        new value_storage_t{storage->engine, value_kind_t::null, false, 0, 0, 0.0, std::pmr::string{resource}}};
    if (!null_cell.value()) {
        using components::types::physical_type;
        const auto& vec = cursor.chunks()[location.value().chunk].data[col];
        switch (vec.type().to_physical_type()) {
            case physical_type::BOOL: {
                const auto cell = cursor.get<bool>(col, row);
                if (cell.has_error()) {
                    return nullptr;
                }
                value->kind = value_kind_t::boolean;
                value->boolean = cell.value();
                break;
            }
            case physical_type::INT8:
            case physical_type::INT16:
            case physical_type::INT32:
            case physical_type::INT64: {
                const auto cell = cursor.get<int64_t>(col, row);
                if (cell.has_error()) {
                    return nullptr;
                }
                value->kind = value_kind_t::signed_int;
                value->signed_int = cell.value();
                break;
            }
            case physical_type::UINT8:
            case physical_type::UINT16:
            case physical_type::UINT32:
            case physical_type::UINT64: {
                const auto cell = cursor.get<uint64_t>(col, row);
                if (cell.has_error()) {
                    return nullptr;
                }
                value->kind = value_kind_t::unsigned_int;
                value->unsigned_int = cell.value();
                break;
            }
            case physical_type::FLOAT:
            case physical_type::DOUBLE: {
                const auto cell = cursor.get<double>(col, row);
                if (cell.has_error()) {
                    return nullptr;
                }
                value->kind = value_kind_t::floating;
                value->floating = cell.value();
                break;
            }
            case physical_type::STRING: {
                const auto cell = cursor.get<std::string_view>(col, row);
                if (cell.has_error()) {
                    return nullptr;
                }
                value->kind = value_kind_t::string;
                value->text.assign(cell.value());
                break;
            }
            default:
                value->kind = value_kind_t::other;
                break;
        }
    }
    return reinterpret_cast<value_ptr>(value.release());
}

extern "C" value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name) noexcept {
    auto storage = convert_cursor(ptr);
    if (storage->cursor->is_write()) {
        return nullptr;
    }
    const auto& types = storage->cursor->type_data();

    const std::string_view name = to_view(column_name);
    for (size_t col = 0; col < types.size(); ++col) {
        if (types[col].alias() == name) {
            return cursor_get_value(ptr, row_index, static_cast<int32_t>(col));
        }
    }
    return nullptr;
}

extern "C" void release_value(value_ptr ptr) noexcept { delete convert_value(ptr); }

extern "C" bool value_is_null(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::null; }

extern "C" bool value_is_bool(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::boolean; }

extern "C" bool value_is_int(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::signed_int; }

extern "C" bool value_is_uint(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::unsigned_int; }

extern "C" bool value_is_double(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::floating; }

extern "C" bool value_is_string(value_ptr ptr) noexcept { return convert_value(ptr)->kind == value_kind_t::string; }

extern "C" bool value_get_bool(value_ptr ptr) noexcept {
    auto storage = convert_value(ptr);
    assert(storage->kind == value_kind_t::boolean && "value_get_bool: the value is not a bool");
    return storage->boolean;
}

extern "C" int64_t value_get_int(value_ptr ptr) noexcept {
    auto storage = convert_value(ptr);
    assert(storage->kind == value_kind_t::signed_int && "value_get_int: the value is not an int");
    return storage->signed_int;
}

extern "C" uint64_t value_get_uint(value_ptr ptr) noexcept {
    auto storage = convert_value(ptr);
    assert(storage->kind == value_kind_t::unsigned_int && "value_get_uint: the value is not a uint");
    return storage->unsigned_int;
}

extern "C" double value_get_double(value_ptr ptr) noexcept {
    auto storage = convert_value(ptr);
    assert(storage->kind == value_kind_t::floating && "value_get_double: the value is not a double");
    return storage->floating;
}

extern "C" const char* value_get_string(value_ptr ptr) noexcept {
    auto storage = convert_value(ptr);
    assert(storage->kind == value_kind_t::string && "value_get_string: the value is not a string");
    return storage->text.c_str();
}

extern "C" void otterbrix_free_string(char* str) noexcept { delete[] str; }
