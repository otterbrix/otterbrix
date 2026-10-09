#include "otterbrix.h"

#include <components/cursor/cursor.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <new>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using cursor_t = components::cursor::cursor_t;
using logical_value_t = components::types::logical_value_t;
using otterbrix::otterbrix_t;

namespace {
    // `engine` is declared first so it outlives the cursor it answered: members are destroyed bottom-up.
    struct cursor_storage_t {
        otterbrix::otterbrix_ptr engine;
        boost::intrusive_ptr<cursor_t> cursor;
    };

    struct value_storage_t {
        otterbrix::otterbrix_ptr engine;
        logical_value_t value;
    };

    configuration::config create_config() { return configuration::config::default_config(); }

    otterbrix_t* live_otterbrix(otterbrix_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<otterbrix_t*>(ptr);
    }

    cursor_storage_t* convert_cursor(cursor_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<cursor_storage_t*>(ptr);
    }

    value_storage_t* convert_value(value_ptr ptr) {
        assert(ptr != nullptr);
        return reinterpret_cast<value_storage_t*>(ptr);
    }

    // otterbrix.h: running out of memory aborts.
    [[noreturn]] void out_of_memory() {
        std::fputs("otterbrix C API: out of memory\n", stderr);
        std::abort();
    }

    cursor_ptr store_cursor(otterbrix_t* engine, components::cursor::cursor_t_ptr c) {
        auto storage = std::make_unique<cursor_storage_t>();
        storage->engine = otterbrix::otterbrix_ptr{engine};
        storage->cursor = std::move(c);
        return reinterpret_cast<cursor_ptr>(storage.release());
    }

    cursor_ptr error_cursor(otterbrix_t* engine, core::error_code_t code, std::string_view text) noexcept {
        try {
            auto* resource = engine->dispatcher()->resource();
            return store_cursor(engine,
                                components::cursor::make_cursor(resource, core::error_t(code, std::pmr::string{text, resource})));
        } catch (const std::bad_alloc&) {
            out_of_memory();
        }
    }

    // Freed by the caller with otterbrix_free_string.
    char* copy_to_c_string(std::string_view text) noexcept {
        auto* copy = new (std::nothrow) char[text.size() + 1];
        if (copy == nullptr) {
            out_of_memory();
        }
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

extern "C" otterbrix_ptr otterbrix_create(config_t cfg, error_message* out_error) {
    assert(out_error != nullptr && "otterbrix_create: out_error is required");
    *out_error = error_message{static_cast<int32_t>(core::error_code_t::none), nullptr};
    try {
        auto config = create_config();
        config.log.level = static_cast<log_t::level>(cfg.level);
        config.log.path = std::pmr::string(to_view(cfg.log_path));
        config.wal.path = std::pmr::string(to_view(cfg.wal_path));
        config.disk.path = std::pmr::string(to_view(cfg.disk_path));
        config.main_path = std::pmr::string(to_view(cfg.main_path));

        auto engine = otterbrix::make_otterbrix(config);
        if (engine.has_error()) {
            *out_error = make_error_message(engine.error());
            return nullptr;
        }
        otterbrix_t* handle = engine.value().get();
        intrusive_ptr_add_ref(handle);
        return reinterpret_cast<otterbrix_ptr>(handle);
    } catch (const std::bad_alloc&) {
        out_of_memory();
    } catch (...) {
        *out_error = error_message{static_cast<int32_t>(core::error_code_t::other_error),
                                   copy_to_c_string("otterbrix_create: unknown C++ exception")};
        return nullptr;
    }
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) { intrusive_ptr_release(live_otterbrix(ptr)); }

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query) {
    otterbrix_t* engine = live_otterbrix(ptr);
    try {
        auto cursor = engine->dispatcher()->execute_sql(otterbrix::session_id_t(), std::string{to_view(query)});
        return store_cursor(engine, std::move(cursor));
    } catch (const std::bad_alloc&) {
        out_of_memory();
    } catch (const std::exception& ex) {
        return error_cursor(engine, core::error_code_t::other_error, ex.what());
    } catch (...) {
        return error_cursor(engine, core::error_code_t::other_error, "unknown C++ exception");
    }
}

extern "C" cursor_ptr
execute_sql_params(otterbrix_ptr ptr, string_view_t query, const sql_param_t* params, size_t param_count) {
    assert(params != nullptr || param_count == 0);
    otterbrix_t* engine = live_otterbrix(ptr);
    try {
        auto* resource = engine->dispatcher()->resource();
        std::vector<std::pair<size_t, logical_value_t>> bound;
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
                    bound.emplace_back(id, logical_value_t(resource, nullptr));
                    break;
                case SQL_PARAM_BOOL:
                    bound.emplace_back(id, logical_value_t(resource, p.bool_value != 0));
                    break;
                case SQL_PARAM_INT64:
                    bound.emplace_back(id, logical_value_t(resource, p.int64_value));
                    break;
                case SQL_PARAM_UINT64:
                    bound.emplace_back(id, logical_value_t(resource, p.uint64_value));
                    break;
                case SQL_PARAM_DOUBLE:
                    bound.emplace_back(id, logical_value_t(resource, p.double_value));
                    break;
                case SQL_PARAM_STRING:
                    bound.emplace_back(id, logical_value_t(resource, to_view(p.string_value)));
                    break;
                default:
                    return error_cursor(engine, core::error_code_t::invalid_parameter, "sql_param_t: unknown kind");
            }
        }
        auto cursor =
            engine->dispatcher()->execute_sql_with_params(otterbrix::session_id_t(), std::string{to_view(query)}, bound);
        return store_cursor(engine, std::move(cursor));
    } catch (const std::bad_alloc&) {
        out_of_memory();
    } catch (const std::exception& ex) {
        return error_cursor(engine, core::error_code_t::other_error, ex.what());
    } catch (...) {
        return error_cursor(engine, core::error_code_t::other_error, "unknown C++ exception");
    }
}

extern "C" void release_cursor(cursor_ptr ptr) {
    delete convert_cursor(ptr);
}

extern "C" int32_t cursor_size(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" bool cursor_affected_rows(cursor_ptr ptr, uint64_t* rows) {
    assert(rows != nullptr);
    auto storage = convert_cursor(ptr);
    if (!storage->cursor->is_write()) {
        return false;
    }
    *rows = storage->cursor->affected_rows();
    return true;
}

extern "C" int32_t cursor_column_count(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->column_count());
}

extern "C" int32_t cursor_column_logical_type(cursor_ptr ptr, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    const auto& types = storage->cursor->type_data();
    if (column_index < 0 || static_cast<size_t>(column_index) >= types.size()) {
        return -1;
    }
    return static_cast<int32_t>(types[static_cast<size_t>(column_index)].type());
}

extern "C" bool cursor_has_next(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->has_next();
}

extern "C" bool cursor_is_success(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_success();
}

extern "C" bool cursor_is_error(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_error();
}

extern "C" error_message cursor_get_error(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return make_error_message(storage->cursor->get_error());
}

extern "C" char* cursor_column_name(cursor_ptr ptr, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    const auto& types = storage->cursor->type_data();
    if (static_cast<size_t>(column_index) >= types.size()) {
        return nullptr;
    }
    return copy_to_c_string(types[static_cast<size_t>(column_index)].alias());
}

extern "C" value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    auto& cursor = *storage->cursor;

    if (row_index < 0 || column_index < 0 || static_cast<size_t>(row_index) >= cursor.size() ||
        static_cast<size_t>(column_index) >= cursor.column_count()) {
        return nullptr;
    }

    try {
        // value() spans the result batch — it locates the chunk owning the global row.
        std::unique_ptr<value_storage_t> value_storage{new value_storage_t{
            storage->engine,
            cursor.value(static_cast<uint64_t>(column_index), static_cast<uint64_t>(row_index))}};
        return reinterpret_cast<value_ptr>(value_storage.release());
    } catch (const std::bad_alloc&) {
        out_of_memory();
    }
}

extern "C" value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name) {
    auto storage = convert_cursor(ptr);
    const auto& types = storage->cursor->type_data();

    const std::string_view name = to_view(column_name);
    for (size_t col = 0; col < types.size(); ++col) {
        if (types[col].alias() == name) {
            return cursor_get_value(ptr, row_index, static_cast<int32_t>(col));
        }
    }
    return nullptr;
}

extern "C" void release_value(value_ptr ptr) {
    delete convert_value(ptr);
}

extern "C" bool value_is_null(value_ptr ptr) {
    auto storage = convert_value(ptr);
    return storage->value.is_null();
}

extern "C" bool value_is_bool(value_ptr ptr) {
    auto storage = convert_value(ptr);
    return storage->value.type().to_physical_type() == components::types::physical_type::BOOL;
}

extern "C" bool value_is_int(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    return pt == components::types::physical_type::INT8 || pt == components::types::physical_type::INT16 ||
           pt == components::types::physical_type::INT32 || pt == components::types::physical_type::INT64;
}

extern "C" bool value_is_uint(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    return pt == components::types::physical_type::UINT8 || pt == components::types::physical_type::UINT16 ||
           pt == components::types::physical_type::UINT32 || pt == components::types::physical_type::UINT64;
}

extern "C" bool value_is_double(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    return pt == components::types::physical_type::FLOAT || pt == components::types::physical_type::DOUBLE;
}

extern "C" bool value_is_string(value_ptr ptr) {
    auto storage = convert_value(ptr);
    return storage->value.type().to_physical_type() == components::types::physical_type::STRING;
}

extern "C" bool value_get_bool(value_ptr ptr) {
    auto storage = convert_value(ptr);
    return storage->value.value<bool>();
}

extern "C" int64_t value_get_int(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    switch (pt) {
        case components::types::physical_type::INT8:
            return storage->value.value<int8_t>();
        case components::types::physical_type::INT16:
            return storage->value.value<int16_t>();
        case components::types::physical_type::INT32:
            return storage->value.value<int32_t>();
        case components::types::physical_type::INT64:
            return storage->value.value<int64_t>();
        default:
            return 0;
    }
}

extern "C" uint64_t value_get_uint(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    switch (pt) {
        case components::types::physical_type::UINT8:
            return storage->value.value<uint8_t>();
        case components::types::physical_type::UINT16:
            return storage->value.value<uint16_t>();
        case components::types::physical_type::UINT32:
            return storage->value.value<uint32_t>();
        case components::types::physical_type::UINT64:
            return storage->value.value<uint64_t>();
        default:
            return 0;
    }
}

extern "C" double value_get_double(value_ptr ptr) {
    auto storage = convert_value(ptr);
    auto pt = storage->value.type().to_physical_type();
    if (pt == components::types::physical_type::FLOAT) {
        return storage->value.value<float>();
    }
    return storage->value.value<double>();
}

extern "C" char* value_get_string(value_ptr ptr) {
    auto storage = convert_value(ptr);
    return copy_to_c_string(storage->value.value<std::string_view>());
}

extern "C" void otterbrix_free_string(char* str) { delete[] str; }
