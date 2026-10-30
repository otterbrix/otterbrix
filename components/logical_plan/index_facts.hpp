#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>

#include <memory_resource>
#include <unordered_map>
#include <vector>

namespace components::logical_plan {

    // The indexes of the tables a statement reads, as the index manager reported them before the optimizer runs.
    // Keyed by table_oid, never flattened: flattening would let one table's predicate be judged by another table's
    // index set (bogus index_scan).
    struct table_index_info_t {
        std::pmr::vector<index::keys_base_storage_t> keys;
        std::pmr::vector<index::index_description_t> descriptions;

        explicit table_index_info_t(std::pmr::memory_resource* resource)
            : keys(resource)
            , descriptions(resource) {}
    };

    class index_facts_t {
    public:
        explicit index_facts_t(std::pmr::memory_resource* resource)
            : resource_(resource)
            , tables_(resource) {}

        const table_index_info_t* index_info_for(catalog::oid_t oid) const noexcept {
            auto it = tables_.find(oid);
            return it != tables_.end() ? &it->second : nullptr;
        }

        // Only mutation point; other code reads via index_info_for.
        table_index_info_t& index_info_slot(catalog::oid_t oid) {
            auto it = tables_.find(oid);
            if (it == tables_.end()) {
                it = tables_.emplace(oid, table_index_info_t{resource_}).first;
            }
            return it->second;
        }

        bool has_index_on(catalog::oid_t oid, const expressions::key_t& key) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& keys : info->keys) {
                if (keys.size() == 1 && keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on(catalog::oid_t oid, const expressions::key_t& key, index_type type) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& desc : info->descriptions) {
                if (desc.type != type) {
                    continue;
                }
                if (desc.keys.size() == 1 && desc.keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on_with_other_type(catalog::oid_t oid, const expressions::key_t& key, index_type type) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& desc : info->descriptions) {
                if (desc.type == type) {
                    continue;
                }
                if (desc.keys.size() == 1 && desc.keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        index_type preferred_index_type_for_compare(catalog::oid_t oid,
                                                    const expressions::key_t& key,
                                                    expressions::compare_type compare) const {
            const bool is_range = compare == expressions::compare_type::lt ||
                                  compare == expressions::compare_type::lte ||
                                  compare == expressions::compare_type::gt || compare == expressions::compare_type::gte;
            if (!is_range && has_index_on(oid, key, index_type::hashed)) {
                return index_type::hashed;
            }
            if (is_range && has_index_on(oid, key, index_type::single)) {
                return index_type::single;
            }
            return index_type::no_valid;
        }

    private:
        std::pmr::memory_resource* resource_;
        std::pmr::unordered_map<catalog::oid_t, table_index_info_t> tables_;
    };

} // namespace components::logical_plan
