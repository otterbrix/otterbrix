#pragma once

#include <sstream>
#include <string>

using uid_name_t = std::string;
using database_name_t = std::string;
using schema_name_t = std::string;
using collection_name_t = std::string;

// Qualified SQL table identity. Retained at the SQL parser / error reporting
// / membership-cache boundary; storage routing uses pg_class.oid
// (see docs/oid-migration-strategy.md, Phase 8+9 COMPLETE 2026-05-10).
//
// The 4-part shape (uuid.db.schema.table) mirrors PostgreSQL-style fully-
// qualified identifiers — `unique_identifier` is the optional uuid prefix
// used by the SQL parser when the user writes `<uuid>.<db>.<schema>.<rel>`,
// and is consumed by `table_id` for catalog dependency-set keying.
struct qualified_name_t {
    uid_name_t unique_identifier;
    database_name_t database;
    schema_name_t schema;
    collection_name_t collection;
    qualified_name_t() = default;

    qualified_name_t(const database_name_t& database, const collection_name_t& collection)
        : database(database)
        , collection(collection) {}

    qualified_name_t(const database_name_t& database, const schema_name_t& schema, const collection_name_t& collection)
        : database(database)
        , schema(schema)
        , collection(collection) {}

    qualified_name_t(const uid_name_t& unique_identifier,
                     const database_name_t& database,
                     const schema_name_t& schema,
                     const collection_name_t& collection)
        : unique_identifier(unique_identifier)
        , database(database)
        , schema(schema)
        , collection(collection) {}

    inline std::string to_string() const {
        std::stringstream s;
        if (empty()) {
            s << "NonCollectionData";
        } else {
            s << database << "." << collection;
        }
        return s.str();
    }

    bool empty() const noexcept {
        return unique_identifier.empty() && database.empty() && schema.empty() && collection.empty();
    }
};

inline bool operator==(const qualified_name_t& c1, const qualified_name_t& c2) {
    return c1.unique_identifier == c2.unique_identifier && c1.database == c2.database && c1.schema == c2.schema &&
           c1.collection == c2.collection;
}

inline bool operator<(const qualified_name_t& c1, const qualified_name_t& c2) {
    return c1.unique_identifier < c2.unique_identifier || c1.database < c2.database ||
           (c1.database == c2.database && c1.schema < c2.schema) ||
           (c1.database == c2.database && c1.schema == c2.schema && c1.collection < c2.collection);
}

struct collection_name_hash {
    inline std::size_t operator()(const qualified_name_t& key) const {
        return std::hash<std::string>()(key.unique_identifier) ^ std::hash<std::string>()(key.database) ^
               std::hash<std::string>()(key.schema) ^ std::hash<std::string>()(key.collection);
    }
};
