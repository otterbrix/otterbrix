#include "otterbrix.h"

#include <components/cursor/cursor.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <integration/cpp/base_spaces.hpp>

#include <atomic>
#include <cassert>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using cursor_t = components::cursor::cursor_t;
using logical_value_t = components::types::logical_value_t;

namespace {
    struct spaces_t;

    struct pod_space_t {
        std::atomic<state_t> state;
        std::atomic<uint64_t> holders{1};
        std::unique_ptr<spaces_t> space;
    };

    struct cursor_storage_t {
        state_t state;
        pod_space_t* engine;
        boost::intrusive_ptr<cursor_t> cursor;
    };

    struct value_storage_t {
        state_t state;
        pod_space_t* engine;
        logical_value_t value{std::pmr::null_memory_resource(),
                              components::types::complex_logical_type{components::types::logical_type::NA}};
    };

    configuration::config create_config() { return configuration::config::default_config(); }

    struct spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        explicit spaces_t(host_ptr host)
            : base_otterbrix_t(std::move(host)) {}
    };

    pod_space_t* live_otterbrix(otterbrix_ptr ptr) {
        assert(ptr != nullptr);
        auto spaces = reinterpret_cast<pod_space_t*>(ptr);
        assert(spaces->state.load() == state_t::created);
        if (spaces->state.load() != state_t::created) {
            return nullptr;
        }
        return spaces;
    }

    pod_space_t* hold_engine(pod_space_t* pod) {
        pod->holders.fetch_add(1, std::memory_order_relaxed);
        return pod;
    }

    void release_engine(pod_space_t* pod) {
        if (pod->holders.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            pod->space.reset();
            delete pod;
        }
    }

    cursor_storage_t* convert_cursor(cursor_ptr ptr) {
        assert(ptr != nullptr);
        auto storage = reinterpret_cast<cursor_storage_t*>(ptr);
        assert(storage->state == state_t::created);
        return storage;
    }

    value_storage_t* convert_value(value_ptr ptr) {
        assert(ptr != nullptr);
        auto storage = reinterpret_cast<value_storage_t*>(ptr);
        assert(storage->state == state_t::created);
        return storage;
    }

    cursor_ptr store_cursor(pod_space_t* pod, components::cursor::cursor_t_ptr c) {
        auto storage = std::make_unique<cursor_storage_t>();
        storage->cursor = std::move(c);
        storage->state = state_t::created;
        storage->engine = hold_engine(pod);
        return reinterpret_cast<cursor_ptr>(storage.release());
    }

    cursor_ptr exception_cursor(pod_space_t* pod, const std::exception& ex) {
        if (pod == nullptr || pod->space == nullptr) {
            return nullptr;
        }
        auto* resource = pod->space->dispatcher()->resource();
        try {
            return store_cursor(pod, components::cursor::make_cursor(
                resource,
                core::error_t(core::error_code_t::other_error, std::pmr::string{ex.what(), resource})));
        } catch (...) {
            return nullptr;
        }
    }

    cursor_ptr unknown_exception_cursor(pod_space_t* pod) {
        if (pod == nullptr || pod->space == nullptr) {
            return nullptr;
        }
        auto* resource = pod->space->dispatcher()->resource();
        try {
            return store_cursor(pod, components::cursor::make_cursor(
                resource,
                core::error_t(core::error_code_t::other_error, std::pmr::string{"unknown C++ exception", resource})));
        } catch (...) {
            return nullptr;
        }
    }

    // Freed by the caller with otterbrix_free_string.
    char* copy_to_c_string(std::string_view text) {
        auto* copy = new char[text.size() + 1];
        std::memcpy(copy, text.data(), text.size());
        copy[text.size()] = '\0';
        return copy;
    }

    error_message make_error_message(const core::error_t& error) {
        return error_message{static_cast<int32_t>(error.type),
                             copy_to_c_string(std::string_view{error.what.data(), error.what.size()})};
    }

    std::string string_view_to_string(string_view_t sv) {
        if (sv.size == 0) {
            return {};
        }
        if (sv.data == nullptr) {
            throw std::invalid_argument("string_view_t: non-zero size with null data");
        }
        return std::string(sv.data, sv.size);
    }
} // namespace

extern "C" otterbrix_ptr otterbrix_create(config_t cfg, error_message* out_error) {
    assert(out_error != nullptr && "otterbrix_create: out_error is required");
    if (out_error == nullptr) {
        return nullptr;
    }
    *out_error = error_message{static_cast<int32_t>(core::error_code_t::none), nullptr};
    try {
        auto config = create_config();
        config.log.level = static_cast<log_t::level>(cfg.level);
        config.log.path = std::pmr::string(cfg.log_path.data, cfg.log_path.size);
        config.wal.path = std::pmr::string(cfg.wal_path.data, cfg.wal_path.size);
        config.disk.path = std::pmr::string(cfg.disk_path.data, cfg.disk_path.size);
        config.main_path = std::pmr::string(cfg.main_path.data, cfg.main_path.size);

        auto host = otterbrix::base_otterbrix_t::open(config);
        if (host.has_error()) {
            *out_error = make_error_message(host.error());
            return nullptr;
        }
        auto pod_space = std::make_unique<pod_space_t>();
        pod_space->space = std::make_unique<spaces_t>(std::move(host.value()));
        pod_space->state = state_t::created;
        return reinterpret_cast<void*>(pod_space.release());
    } catch (...) {
        *out_error = error_message{static_cast<int32_t>(core::error_code_t::other_error),
                                   copy_to_c_string("otterbrix_create: unknown C++ exception")};
        return nullptr;
    }
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) {
    assert(ptr != nullptr);
    auto pod_space = reinterpret_cast<pod_space_t*>(ptr);
    const state_t was = pod_space->state.exchange(state_t::destroyed);
    assert(was == state_t::created);
    if (was == state_t::created) {
        release_engine(pod_space);
    }
}

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query_raw) {
    pod_space_t* pod_space = nullptr;
    try {
        pod_space = live_otterbrix(ptr);
        if (pod_space == nullptr) {
            return nullptr;
        }
        auto session = otterbrix::session_id_t();
        std::string query = string_view_to_string(query_raw);
        auto cursor = pod_space->space->dispatcher()->execute_sql(session, query);
        return store_cursor(pod_space, std::move(cursor));
    } catch (const std::exception& ex) {
        return exception_cursor(pod_space, ex);
    } catch (...) {
        return unknown_exception_cursor(pod_space);
    }
}

extern "C" cursor_ptr
execute_sql_params(otterbrix_ptr ptr, string_view_t query_raw, const sql_param_t* params, size_t param_count) {
    pod_space_t* pod_space = nullptr;
    try {
        pod_space = live_otterbrix(ptr);
        if (pod_space == nullptr) {
            return nullptr;
        }
        auto session = otterbrix::session_id_t();
        std::string query = string_view_to_string(query_raw);
        auto* resource = pod_space->space->dispatcher()->resource();
        std::vector<std::pair<size_t, logical_value_t>> bound;
        bound.reserve(param_count);
        for (size_t i = 0; i < param_count; ++i) {
            const sql_param_t& p = params[i];
            if (p.index < 1) {
                throw std::invalid_argument("sql_param_t: index must be >= 1 (e.g. $1 -> 1)");
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
                case SQL_PARAM_STRING: {
                    std::string s = string_view_to_string(p.string_value);
                    bound.emplace_back(id, logical_value_t(resource, std::move(s)));
                    break;
                }
                default:
                    throw std::invalid_argument("sql_param_t: unknown kind");
            }
        }
        auto cursor = pod_space->space->dispatcher()->execute_sql_with_params(session, query, bound);
        return store_cursor(pod_space, std::move(cursor));
    } catch (const std::exception& ex) {
        return exception_cursor(pod_space, ex);
    } catch (...) {
        return unknown_exception_cursor(pod_space);
    }
}

extern "C" void release_cursor(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    storage->state = state_t::destroyed;
    auto* engine = storage->engine;
    delete storage;
    release_engine(engine);
}

extern "C" int32_t cursor_size(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" bool cursor_affected_rows(cursor_ptr ptr, uint64_t* rows) {
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
    try {
        auto storage = convert_cursor(ptr);
        const auto& types = storage->cursor->type_data();
        if (column_index < 0 || static_cast<size_t>(column_index) >= types.size()) {
            return -1;
        }
        return static_cast<int32_t>(types[static_cast<size_t>(column_index)].type());
    } catch (...) {
        return -1;
    }
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
    try {
        auto storage = convert_cursor(ptr);
        return make_error_message(storage->cursor->get_error());
    } catch (...) {
        return error_message{static_cast<int32_t>(core::error_code_t::other_error), nullptr};
    }
}

extern "C" char* cursor_column_name(cursor_ptr ptr, int32_t column_index) {
    try {
        auto storage = convert_cursor(ptr);
        const auto& types = storage->cursor->type_data();
        if (static_cast<size_t>(column_index) < types.size()) {
            auto name = types[static_cast<size_t>(column_index)].alias();
            char* str_ptr = new char[name.size() + 1];
            std::strcpy(str_ptr, std::string(name).data());
            return str_ptr;
        }
        return nullptr;
    } catch (...) {
        return nullptr;
    }
}

extern "C" value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    auto& cursor = *storage->cursor;

    if (row_index < 0 || column_index < 0 || static_cast<size_t>(row_index) >= cursor.size() ||
        static_cast<size_t>(column_index) >= cursor.column_count()) {
        return nullptr;
    }

    auto value_storage = std::make_unique<value_storage_t>();
    value_storage->state = state_t::created;
    // value() spans the result batch — it locates the chunk owning the global row.
    value_storage->value = cursor.value(static_cast<uint64_t>(column_index), static_cast<uint64_t>(row_index));
    value_storage->engine = hold_engine(storage->engine);
    return reinterpret_cast<void*>(value_storage.release());
}

extern "C" value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name) {
    auto storage = convert_cursor(ptr);
    const auto& types = storage->cursor->type_data();

    std::string name(column_name.data, column_name.size);
    for (size_t col = 0; col < types.size(); ++col) {
        if (types[col].alias() == name) {
            return cursor_get_value(ptr, row_index, static_cast<int32_t>(col));
        }
    }
    return nullptr;
}

extern "C" void release_value(value_ptr ptr) {
    auto storage = convert_value(ptr);
    storage->state = state_t::destroyed;
    auto* engine = storage->engine;
    delete storage;
    release_engine(engine);
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
    try {
        auto storage = convert_value(ptr);
        auto sv = storage->value.value<std::string_view>();
        char* str_ptr = new char[sv.size() + 1];
        std::memcpy(str_ptr, sv.data(), sv.size());
        str_ptr[sv.size()] = '\0';
        return str_ptr;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void otterbrix_free_string(char* str) { delete[] str; }
