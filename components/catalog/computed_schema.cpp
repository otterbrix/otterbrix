#include "computed_schema.hpp"

namespace components::catalog {
    computed_schema::computed_schema(std::pmr::memory_resource* resource, uint64_t sparse_threshold)
        : fields_(resource)
        , column_order_(resource)
        , sparse_threshold_(sparse_threshold)
        , non_null_counts_(resource)
        , sparse_flags_(resource)
        , promoted_order_(resource)
        , phys_to_field_type_(resource) {}

    computed_schema::computed_schema(std::pmr::memory_resource* resource,
                                     uint64_t sparse_threshold,
                                     std::unordered_set<std::string> pinned_columns)
        : fields_(resource)
        , column_order_(resource)
        , sparse_threshold_(sparse_threshold)
        , non_null_counts_(resource)
        , sparse_flags_(resource)
        , promoted_order_(resource)
        , phys_to_field_type_(resource)
        , pinned_field_names_(std::move(pinned_columns)) {}

    void computed_schema::append(std::pmr::string field_name, const types::complex_logical_type& type) {
        auto& list = fields_[field_name];
        for (const auto& t : list) {
            if (t == type) {
                return;
            }
        }
        list.emplace_back(type);
        column_order_.emplace_back(field_name, type);

        // Initialize sparse tracking for new (field, type) pairs when threshold > 0
        if (sparse_threshold_ > 0) {
            std::pmr::string phys(storage_column_name(std::string(field_name), type),
                                  fields_.get_allocator().resource());
            if (sparse_flags_.find(phys) == sparse_flags_.end()) {
                // Register reverse mapping for promote_to_regular lookup
                phys_to_field_type_[phys] = {field_name, type};

                bool is_pinned = !pinned_field_names_.empty() &&
                                 pinned_field_names_.count(std::string(field_name)) > 0;
                if (is_pinned) {
                    // Pinned columns go directly to main table — never sparse
                    sparse_flags_[phys] = false;
                    promoted_order_.emplace_back(field_name, type);
                    newly_pinned_.push_back({std::string(field_name), type, std::string(phys)});
                } else {
                    sparse_flags_[phys] = true;
                    non_null_counts_[phys] = 0;
                }
            }
        }
    }

    void computed_schema::append_n(std::pmr::string field_name,
                                   const types::complex_logical_type& type,
                                   size_t /*n*/) {
        append(std::move(field_name), type);
    }

    void computed_schema::drop(std::pmr::string field_name, const types::complex_logical_type& type) {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return;
        }
        auto& list = it->second;
        list.erase(std::remove(list.begin(), list.end(), type), list.end());
        if (list.empty()) {
            fields_.erase(it);
        }
        // Remove sparse tracking
        if (sparse_threshold_ > 0) {
            std::pmr::string phys(storage_column_name(std::string(field_name), type),
                                  fields_.get_allocator().resource());
            sparse_flags_.erase(phys);
            non_null_counts_.erase(phys);
            phys_to_field_type_.erase(phys);
            // Remove from promoted_order_ if present
            promoted_order_.erase(
                std::remove_if(promoted_order_.begin(),
                               promoted_order_.end(),
                               [&](const auto& p) { return p.first == field_name && p.second == type; }),
                promoted_order_.end());
        }
    }

    void computed_schema::drop_n(std::pmr::string field_name,
                                  const types::complex_logical_type& type,
                                  size_t /*n*/) {
        drop(std::move(field_name), type);
    }

    std::vector<types::complex_logical_type>
    computed_schema::find_field_versions(const std::pmr::string& field_name) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return {};
        }
        return {it->second.begin(), it->second.end()};
    }

    types::complex_logical_type computed_schema::latest_types_struct() const {
        std::vector<types::complex_logical_type> retval;

        if (sparse_threshold_ > 0) {
            // Prepend _id as the first logical column
            auto id_type = types::complex_logical_type(types::logical_type::BIGINT);
            id_type.set_alias("_id");
            retval.push_back(std::move(id_type));

            // Use promoted_order_ so column indices match the physical table order.
            // promoted_order_ records columns in the same order as storage_add_column calls,
            // so index K in latest_types_struct() == physical column K in copy_types().
            retval.reserve(retval.size() + promoted_order_.size());
            for (const auto& [name, type] : promoted_order_) {
                if (!has_type(name, type)) {
                    continue;
                }
                auto t = type;
                t.set_alias(name.c_str());
                retval.push_back(std::move(t));
            }
        } else {
            retval.reserve(column_order_.size());
            for (const auto& [name, type] : column_order_) {
                if (!has_type(name, type)) {
                    continue;
                }
                auto t = type;
                t.set_alias(name.c_str());
                retval.push_back(std::move(t));
            }
        }
        return types::complex_logical_type::create_struct("latest_types", std::move(retval));
    }

    std::string computed_schema::storage_column_name(const std::string& field_name,
                                                      const types::complex_logical_type& type) {
        return "__" + field_name + "__" + std::to_string(static_cast<unsigned>(type.type()));
    }

    bool computed_schema::has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return false;
        }
        for (const auto& t : it->second) {
            if (t == type) {
                return true;
            }
        }
        return false;
    }

    // --- Sparse methods ---

    bool computed_schema::is_sparse(const std::pmr::string& phys_name) const {
        auto it = sparse_flags_.find(phys_name);
        return it != sparse_flags_.end() && it->second;
    }

    bool computed_schema::increment_non_null(const std::pmr::string& phys_name, uint64_t count) {
        auto it = non_null_counts_.find(phys_name);
        if (it == non_null_counts_.end()) {
            return false;
        }
        it->second += count;
        if (it->second >= sparse_threshold_) {
            promote_to_regular(phys_name);
            return true; // promoted
        }
        return false;
    }

    void computed_schema::promote_to_regular(const std::pmr::string& phys_name) {
        auto it = sparse_flags_.find(phys_name);
        if (it != sparse_flags_.end()) {
            it->second = false;
        }
        // Record promotion order to keep latest_types_struct() in sync with physical column order
        auto map_it = phys_to_field_type_.find(phys_name);
        if (map_it != phys_to_field_type_.end()) {
            promoted_order_.emplace_back(map_it->second);
        }
    }

    bool computed_schema::has_any_sparse() const {
        for (const auto& [phys, is_sp] : sparse_flags_) {
            if (is_sp) {
                return true;
            }
        }
        return false;
    }

    uint64_t computed_schema::get_non_null_count(const std::pmr::string& phys_name) const {
        auto it = non_null_counts_.find(phys_name);
        return it != non_null_counts_.end() ? it->second : 0;
    }

    std::vector<computed_schema::sparse_column_info> computed_schema::sparse_columns() const {
        std::vector<sparse_column_info> result;
        for (const auto& [name, type] : column_order_) {
            if (!has_type(name, type)) {
                continue;
            }
            std::string phys = storage_column_name(std::string(name), type);
            std::pmr::string pmr_phys(phys, fields_.get_allocator().resource());
            auto it = sparse_flags_.find(pmr_phys);
            if (it != sparse_flags_.end() && it->second) {
                result.push_back({std::string(name), type, phys});
            }
        }
        return result;
    }

    std::vector<computed_schema::sparse_column_info> computed_schema::take_newly_pinned() {
        std::vector<sparse_column_info> result = std::move(newly_pinned_);
        newly_pinned_.clear();
        return result;
    }

    std::string computed_schema::sparse_table_name(const std::string& collection,
                                                    const std::string& field_name,
                                                    const types::complex_logical_type& type) {
        return collection + "__sp__" + field_name + "__" +
               std::to_string(static_cast<unsigned>(type.type()));
    }

} // namespace components::catalog
