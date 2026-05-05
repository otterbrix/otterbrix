#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace components::catalog {

    enum class drop_behavior_t : std::uint8_t {
        restrict_ = 0,
        cascade_  = 1,
    };

    enum class ddl_status : std::uint8_t {
        ok               = 0,
        restrict_blocked = 1,
        cycle_detected   = 2,
        not_found        = 3,
    };

} // namespace components::catalog

namespace services::disk {

    // Aliases so existing disk/dispatcher/executor callers compile unchanged.
    using drop_behavior_t = components::catalog::drop_behavior_t;
    using ddl_status      = components::catalog::ddl_status;

    enum class invalidation_kind : std::uint8_t {
        relation_dropped = 1,
        relation_schema_changed = 2,
        index_added = 3,
        index_dropped = 4,
        index_validity_changed = 5,
        function_dropped = 6,
        function_schema_changed = 7,
        type_dropped = 8,
        namespace_dropped = 9,
        computing_schema_changed = 10,
        sequence_added = 11,
        view_added = 12,
        macro_added = 13,
        constraint_added = 14,
        constraint_dropped = 15,
        relation_added = 16,
        type_added = 17,
        function_added = 18,
        sequence_dropped = 19,
        view_dropped = 20,
        macro_dropped = 21,
        database_added = 22,
        database_dropped = 23,
    };

    struct invalidation_event_t {
        std::uint64_t version{0};
        invalidation_kind kind{invalidation_kind::relation_dropped};
        components::catalog::oid_t object_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t parent_oid{components::catalog::INVALID_OID};
    };

    struct ddl_result_t {
        components::cursor::cursor_t_ptr result;
        std::vector<invalidation_event_t> events;
        std::uint64_t new_catalog_version{0};
        components::catalog::oid_t created_oid{components::catalog::INVALID_OID};
        std::unordered_map<std::string, components::catalog::oid_t> all_oids;
        ddl_status status{ddl_status::ok};
        components::catalog::oid_t blocking_oid{components::catalog::INVALID_OID};

        ddl_result_t() = default;
        explicit ddl_result_t(std::pmr::memory_resource* /*resource*/) {}

        bool ok() const noexcept { return status == ddl_status::ok; }
        bool failed() const noexcept { return status != ddl_status::ok; }
    };

} // namespace services::disk
