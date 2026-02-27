#include "otterbrix.h"

#include <components/types/logical_value.hpp>
#include <integration/cpp/base_spaces.hpp>

using cursor_t = components::cursor::cursor_t;
using logical_value_t = components::types::logical_value_t;

namespace {
    struct spaces_t;

    struct pod_space_t {
        state_t state;
        std::unique_ptr<spaces_t> space;
    };

    struct cursor_storage_t {
        state_t state;
        boost::intrusive_ptr<cursor_t> cursor;
    };

    struct value_storage_t {
        state_t state;
        logical_value_t value{std::pmr::null_memory_resource(),
                              components::types::complex_logical_type{components::types::logical_type::NA}};
    };

    configuration::config create_config() { return configuration::config::default_config(); }

    struct spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        spaces_t(const configuration::config& config)
            : base_otterbrix_t(config) {}
    };

    pod_space_t* convert_otterbrix(otterbrix_ptr ptr) {
        assert(ptr != nullptr);
        auto spaces = reinterpret_cast<pod_space_t*>(ptr);
        assert(spaces->state == state_t::created);
        return spaces;
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
} // namespace

extern "C" otterbrix_ptr otterbrix_create(config_t cfg) {
    auto config = create_config();
    config.log.level = static_cast<log_t::level>(cfg.level);
    config.log.path = std::pmr::string(cfg.log_path.data, cfg.log_path.size);
    config.wal.path = std::pmr::string(cfg.wal_path.data, cfg.wal_path.size);
    config.disk.path = std::pmr::string(cfg.disk_path.data, cfg.disk_path.size);
    config.main_path = std::pmr::string(cfg.main_path.data, cfg.main_path.size);
    config.wal.on = cfg.wal_on;
    config.wal.sync_to_disk = cfg.sync_to_disk;
    config.disk.on = cfg.disk_on;

    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->state = state_t::created;
    return reinterpret_cast<void*>(pod_space.release());
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) {
    auto pod_space = convert_otterbrix(ptr);
    pod_space->space.reset();
    pod_space->state = state_t::destroyed;
    delete pod_space;
}

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query_raw) {
    auto pod_space = convert_otterbrix(ptr);
    assert(query_raw.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto cursor = pod_space->space->dispatcher()->execute_sql(session, query);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" cursor_ptr create_database(otterbrix_ptr ptr, string_view_t database_name) {
    auto pod_space = convert_otterbrix(ptr);
    assert(database_name.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string database(database_name.data, database_name.size);
    auto cursor = pod_space->space->dispatcher()->create_database(session, database);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" cursor_ptr create_collection(otterbrix_ptr ptr, string_view_t database_name, string_view_t collection_name) {
    auto pod_space = convert_otterbrix(ptr);
    assert(database_name.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string database(database_name.data, database_name.size);
    std::string collection(collection_name.data, collection_name.size);
    auto cursor = pod_space->space->dispatcher()->create_collection(session, database, collection);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" void release_cursor(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    storage->state = state_t::destroyed;
    delete storage;
}

extern "C" int32_t cursor_size(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" int32_t cursor_column_count(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->chunk_data().column_count());
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
    auto error = storage->cursor->get_error();
    error_message msg;
    msg.code = static_cast<int32_t>(error.type);
    std::string str = error.what;
    msg.message = new char[str.size() + 1];
    std::strcpy(msg.message, str.data());
    return msg;
}

extern "C" char* cursor_column_name(cursor_ptr ptr, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    const auto& chunk = storage->cursor->chunk_data();
    auto types = chunk.types();
    if (static_cast<size_t>(column_index) < types.size()) {
        auto name = types[static_cast<size_t>(column_index)].alias();
        char* str_ptr = new char[name.size() + 1];
        std::strcpy(str_ptr, std::string(name).data());
        return str_ptr;
    }
    return nullptr;
}

extern "C" value_ptr cursor_get_value(cursor_ptr ptr, int32_t row_index, int32_t column_index) {
    auto storage = convert_cursor(ptr);
    const auto& chunk = storage->cursor->chunk_data();

    if (static_cast<size_t>(row_index) >= chunk.size() || static_cast<size_t>(column_index) >= chunk.column_count()) {
        return nullptr;
    }

    auto value_storage = std::make_unique<value_storage_t>();
    value_storage->state = state_t::created;
    value_storage->value = chunk.value(static_cast<uint64_t>(column_index), static_cast<uint64_t>(row_index));
    return reinterpret_cast<void*>(value_storage.release());
}

extern "C" value_ptr cursor_get_value_by_name(cursor_ptr ptr, int32_t row_index, string_view_t column_name) {
    auto storage = convert_cursor(ptr);
    const auto& chunk = storage->cursor->chunk_data();
    auto types = chunk.types();

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
    delete storage;
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
    auto sv = storage->value.value<std::string_view>();
    char* str_ptr = new char[sv.size() + 1];
    std::memcpy(str_ptr, sv.data(), sv.size());
    str_ptr[sv.size()] = '\0';
    return str_ptr;
}
