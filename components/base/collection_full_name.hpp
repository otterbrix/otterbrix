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

// Qualified SQL table identity. Retained at the SQL parser / error reporting
// / membership-cache boundary; storage routing uses pg_class.oid
// (see docs/oid-migration-strategy.md, Phase 8+9 COMPLETE 2026-05-10).
//
// The 4-part shape (uuid.db.schema.table) mirrors PostgreSQL-style fully-
// qualified identifiers — `unique_identifier` is the optional uuid prefix
// used by the SQL parser when the user writes `<uuid>.<db>.<schema>.<rel>`,
// and is consumed by `table_id` for catalog dependency-set keying.
struct qualified_name_t {
    core::uid_t unique_identifier;
    core::dbname_t database;
    core::schema_t schema;
    core::relname_t collection;
    qualified_name_t() = default;

    explicit qualified_name_t(core::relname_t collection)
        : collection(std::move(collection)) {}

    qualified_name_t(core::dbname_t database, core::relname_t collection)
        : database(std::move(database))
        , collection(std::move(collection)) {}

    qualified_name_t(core::dbname_t database, core::schema_t schema, core::relname_t collection)
        : database(std::move(database))
        , schema(std::move(schema))
        , collection(std::move(collection)) {}

    qualified_name_t(core::uid_t unique_identifier,
                     core::dbname_t database,
                     core::schema_t schema,
                     core::relname_t collection)
        : unique_identifier(std::move(unique_identifier))
        , database(std::move(database))
        , schema(std::move(schema))
        , collection(std::move(collection)) {}

    std::string to_string() const {
        return qualified_name_detail::dotted({&unique_identifier.t, &database.t, &schema.t, &collection.t});
    }

    bool empty() const noexcept {
        return unique_identifier.t.empty() && database.t.empty() && schema.t.empty() && collection.t.empty();
    }

    bool operator==(const qualified_name_t&) const = default;
};

struct function_qualified_name_t {
    core::dbname_t database;
    core::schema_t schema;
    core::function_name_t function;
    function_qualified_name_t() = default;

    explicit function_qualified_name_t(core::function_name_t function)
        : function(std::move(function)) {}

    function_qualified_name_t(core::dbname_t database, core::schema_t schema, core::function_name_t function)
        : database(std::move(database))
        , schema(std::move(schema))
        , function(std::move(function)) {}

    std::string to_string() const { return qualified_name_detail::dotted({&database.t, &schema.t, &function.t}); }

    bool operator==(const function_qualified_name_t&) const = default;
};
