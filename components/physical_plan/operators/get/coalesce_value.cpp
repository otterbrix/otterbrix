#include "coalesce_value.hpp"

namespace components::operators::get {

    operator_get_ptr coalesce_value_t::create(std::vector<expressions::param_storage> params,
                                              const logical_plan::storage_parameters* storage_params) {
        std::vector<expressions::key_t> keys;
        std::vector<types::logical_value_t> constants;
        std::vector<coalesce_entry> entries;

        for (auto& param : params) {
            if (std::holds_alternative<expressions::key_t>(param)) {
                entries.push_back({coalesce_entry::kind::key, keys.size()});
                keys.push_back(std::get<expressions::key_t>(param));
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                entries.push_back({coalesce_entry::kind::constant, constants.size()});
                constants.push_back(storage_params->parameters.at(id));
            }
        }

        return operator_get_ptr(new coalesce_value_t(std::move(keys), std::move(constants), std::move(entries)));
    }

    coalesce_value_t::coalesce_value_t(std::vector<expressions::key_t> keys,
                                       std::vector<types::logical_value_t> constants,
                                       std::vector<coalesce_entry> entries)
        : operator_get_t()
        , keys_(std::move(keys))
        , constants_(std::move(constants))
        , entries_(std::move(entries)) {}

    std::vector<types::logical_value_t>
    coalesce_value_t::get_values_impl(const std::pmr::vector<types::logical_value_t>& row) {
        for (const auto& entry : entries_) {
            if (entry.type == coalesce_entry::kind::key) {
                const auto& key = keys_[entry.index];
                auto* local_values = row.data();
                size_t size = row.size();
                bool found = false;
                for (size_t i = 0; i < key.storage().size(); i++) {
                    auto it = std::find_if(local_values, local_values + size, [&](const types::logical_value_t& value) {
                        return core::pmr::operator==(value.type().alias(), key.storage()[i]);
                    });
                    if (it == local_values + size) {
                        break;
                    }
                    if (i + 1 != key.storage().size()) {
                        local_values = it->children().data();
                        size = it->children().size();
                    } else {
                        if (it->type().type() != types::logical_type::NA) {
                            return {*it};
                        }
                        found = true;
                    }
                }
                if (!found) {
                    continue;
                }
            } else {
                return {constants_[entry.index]};
            }
        }
        // All values are null
        auto* resource = row.empty() ? std::pmr::get_default_resource() : row.get_allocator().resource();
        return {types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA})};
    }

} // namespace components::operators::get
