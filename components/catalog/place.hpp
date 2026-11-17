#pragma once

#include "catalog_oids.hpp"

#include <components/base/identifier_types.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <string_view>

namespace components::catalog {

    // A schema as the session's current place: [connection.]database.schema; the connection is empty for a local
    // database. Short names are completed with it: `t` and `s.t` take its connection and database, `t` its schema.
    struct place_t {
        core::connection_name_t connection;
        core::dbname_t database;
        core::schema_t schema;

        // The full spelling the session keeps: database.schema, or connection.database.schema.
        std::string to_string() const;
        bool operator==(const place_t&) const = default;
    };

    // A [connection.][database.]schema as written, completed with `place`: no database takes its connection and
    // database, no schema its schema. pg_catalog without a database is main's from any place.
    [[nodiscard]] place_t complete_name(const place_t& place,
                                        std::string_view connection,
                                        std::string_view database,
                                        std::string_view schema);

    // The first level of a name the catalog does not have as written.
    enum class name_level_t : std::uint8_t
    {
        database,
        schema,
        relation
    };

    // A name whose parts hold dots, read as the parts the dots spell: ORMs quote a federated prefix as one identifier
    // ("c.d.s"."t", "c.d.s.t", `c.d.s`.t). The resolves read a name so only where it names nothing as written, and only
    // from the level that is missing: the parts before it are there as written ("m2.shop".public.t of a database
    // "m2.shop" is that database's). True, and the parts, when the first part holding a dot is at `missing` or after
    // it and the parts split into exactly a connection.database.schema (a schema) or a
    // connection.database.schema.relation (a relation).
    [[nodiscard]] bool place_spelled_by_dots(std::string_view connection,
                                             std::string_view database,
                                             std::string_view schema,
                                             name_level_t missing,
                                             place_t* spelled);
    [[nodiscard]] bool relation_spelled_by_dots(std::string_view connection,
                                                std::string_view database,
                                                std::string_view schema,
                                                std::string_view relation,
                                                name_level_t missing,
                                                place_t* place,
                                                core::relname_t* spelled);

    // A search_path value as written ('s' | 'd.s' | 'c.d.s'), its parts lower-cased; one place, no list.
    [[nodiscard]] core::result_wrapper_t<std::pmr::string> canonical_search_path(std::string_view raw,
                                                                                 std::pmr::memory_resource* resource);

    // The place a canonical search_path names; one part ('s', as ALTER SYSTEM keeps it) is a schema of main, two a
    // schema of a local database.
    [[nodiscard]] place_t place_of(std::string_view search_path);

    // A SET's canonical value completed with the current place: one part takes its connection and database; two
    // parts name a local database, as a three-part relation name does.
    [[nodiscard]] place_t complete_place(std::string_view value, const place_t& current);

} // namespace components::catalog
