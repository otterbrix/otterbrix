#pragma once

#include "node.hpp"

#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    // GET_SCHEMA — leaf node carrying a list of (database, collection) ids whose
    // pg_class+pg_attribute schemas should be returned. Lowered into
    // operator_get_schema_t; the operator self-resolves namespaces / tables /
    // columns via async pg_catalog reads (read_rows_by_key on pg_namespace,
    // pg_class, pg_attribute) and emits one complex_logical_type per id —
    // either a `schema` STRUCT, a `latest_types` STRUCT (for relkind='g'
    // computed tables), or logical_type::INVALID.
    //
    // Phase 4 #54 of the pipeline-unification refactor: services/dispatcher/
    // dispatcher.cpp::get_schema uses the same operator infrastructure as
    // every other plan.
    class node_get_schema_t final : public node_t {
    public:
        node_get_schema_t(std::pmr::memory_resource* resource,
                          std::pmr::vector<std::pair<std::string, std::string>> ids);

        const std::pmr::vector<std::pair<std::string, std::string>>& ids() const noexcept { return ids_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::vector<std::pair<std::string, std::string>> ids_;
    };

    using node_get_schema_ptr = boost::intrusive_ptr<node_get_schema_t>;

} // namespace components::logical_plan
