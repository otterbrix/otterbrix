#pragma once

#include <components/base/identifier_types.hpp>

#include <initializer_list>
#include <string>
#include <utility>

namespace qualified_name_detail {
    inline std::string dotted(std::initializer_list<const std::string*> slots) {
        std::string written;
        for (const auto* slot : slots) {
            if (slot->empty()) {
                continue;
            }
            if (!written.empty()) {
                written += '.';
            }
            written += *slot;
        }
        return written;
    }
} // namespace qualified_name_detail

// A relation as written: [connection.][database.][schema.]relation. An empty slot is filled from the session's place
// where the name is resolved.
struct qualified_name_t {
    core::connection_name_t connection;
    core::dbname_t database;
    core::schema_t schema;
    core::relname_t collection;
    qualified_name_t() = default;

    explicit qualified_name_t(core::relname_t collection)
        : collection(std::move(collection)) {}

    qualified_name_t(core::schema_t schema, core::relname_t collection)
        : schema(std::move(schema))
        , collection(std::move(collection)) {}

    qualified_name_t(core::dbname_t database, core::schema_t schema, core::relname_t collection)
        : database(std::move(database))
        , schema(std::move(schema))
        , collection(std::move(collection)) {}

    qualified_name_t(core::connection_name_t connection,
                     core::dbname_t database,
                     core::schema_t schema,
                     core::relname_t collection)
        : connection(std::move(connection))
        , database(std::move(database))
        , schema(std::move(schema))
        , collection(std::move(collection)) {}

    std::string to_string() const {
        return qualified_name_detail::dotted({&connection.t, &database.t, &schema.t, &collection.t});
    }

    bool empty() const noexcept {
        return connection.t.empty() && database.t.empty() && schema.t.empty() && collection.t.empty();
    }

    bool operator==(const qualified_name_t&) const = default;
};

// A function as written: [database.][schema.]function, resolved as a relation name is.
struct function_qualified_name_t {
    core::dbname_t database;
    core::schema_t schema;
    core::function_name_t function;
    function_qualified_name_t() = default;

    explicit function_qualified_name_t(core::function_name_t function)
        : function(std::move(function)) {}

    function_qualified_name_t(core::schema_t schema, core::function_name_t function)
        : schema(std::move(schema))
        , function(std::move(function)) {}

    function_qualified_name_t(core::dbname_t database, core::schema_t schema, core::function_name_t function)
        : database(std::move(database))
        , schema(std::move(schema))
        , function(std::move(function)) {}

    std::string to_string() const { return qualified_name_detail::dotted({&database.t, &schema.t, &function.t}); }

    bool operator==(const function_qualified_name_t&) const = default;
};
