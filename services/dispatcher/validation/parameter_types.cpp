#include "parameter_types.hpp"

#include <components/vector/vector_builders.hpp>

#include <algorithm>
#include <string>

namespace services::dispatcher::validation {

    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {
        // ON CONFLICT DO UPDATE answers the RETURNING of its update; the transformer gives the insert a copy too.
        const components::logical_plan::node_t* returning_node_of(const components::logical_plan::node_t* output) {
            if (output != nullptr && output->type() == components::logical_plan::node_type::insert_on_conflict_t &&
                output->children().size() > 1) {
                return output->children()[1].get();
            }
            return output;
        }
    } // namespace

    parameter_types_t::parameter_types_t(
        std::pmr::memory_resource* resource,
        const std::pmr::vector<components::logical_plan::external_parameter_t>& external,
        const components::vector::storage_parameters& parameters,
        const components::logical_plan::node_t* output)
        : resource_(resource)
        , slots_(resource)
        , output_(returning_node_of(output)) {
        for (const auto& parameter : external) {
            if (!parameter.slot || !parameters.is_valid(*parameter.slot) ||
                parameters.type(*parameter.slot).type() != logical_type::NA) {
                continue;
            }
            slots_.push_back(slot_t{*parameter.slot,
                                    parameter.number,
                                    complex_logical_type{logical_type::NA},
                                    false,
                                    0,
                                    false,
                                    false,
                                    std::pmr::vector<std::size_t>{parameter.insert_columns.begin(),
                                                                  parameter.insert_columns.end(),
                                                                  resource}});
        }
    }

    const parameter_types_t::slot_t* parameter_types_t::find(core::parameter_id_t slot) const {
        for (const auto& entry : slots_) {
            if (entry.slot == slot) {
                return &entry;
            }
        }
        return nullptr;
    }

    parameter_types_t::slot_t* parameter_types_t::find(core::parameter_id_t slot) {
        for (auto& entry : slots_) {
            if (entry.slot == slot) {
                return &entry;
            }
        }
        return nullptr;
    }

    bool parameter_types_t::holds(core::parameter_id_t slot) const { return find(slot) != nullptr; }

    bool parameter_types_t::is_untyped(core::parameter_id_t slot) const {
        const auto* entry = find(slot);
        return entry != nullptr && entry->type.type() == logical_type::NA;
    }

    complex_logical_type parameter_types_t::type_of(core::parameter_id_t slot) {
        auto* entry = find(slot);
        if (entry == nullptr) {
            return complex_logical_type{logical_type::NA};
        }
        entry->read = true;
        if (entry->type.type() == logical_type::NA) {
            ++entry->untyped_uses;
        }
        return entry->type;
    }

    void parameter_types_t::note_read(core::parameter_id_t slot) {
        if (auto* entry = find(slot); entry != nullptr) {
            entry->read = true;
        }
    }

    core::error_t parameter_types_t::inconsistent(const slot_t& entry) const {
        return core::error_t{
            core::error_code_t::ambiguous_parameter,
            std::pmr::string{"inconsistent types deduced for parameter $" + std::to_string(entry.number), resource_}};
    }

    core::error_t parameter_types_t::give(core::parameter_id_t slot, const complex_logical_type& type) {
        auto* entry = find(slot);
        if (entry == nullptr) {
            return core::error_t::no_error();
        }
        if (entry->untyped_uses > 0) {
            --entry->untyped_uses;
        }
        complex_logical_type given = type;
        given.set_alias(std::string{});
        if (entry->type.type() == logical_type::NA) {
            entry->type = std::move(given);
            entry->typed_later = later_;
            return core::error_t::no_error();
        }
        return entry->type == given ? core::error_t::no_error() : inconsistent(*entry);
    }

    core::error_t parameter_types_t::values_column(std::size_t column, const complex_logical_type& type) {
        complex_logical_type given = type;
        given.set_alias(std::string{});
        for (auto& entry : slots_) {
            if (std::find(entry.insert_columns.begin(), entry.insert_columns.end(), column) ==
                entry.insert_columns.end()) {
                continue;
            }
            if (entry.type.type() == logical_type::NA) {
                entry.type = given;
                entry.typed_later = false;
            } else if (!(entry.type == given)) {
                return inconsistent(entry);
            }
        }
        return core::error_t::no_error();
    }

    bool parameter_types_t::read_later(bool later) noexcept {
        const bool previous = later_;
        later_ = later;
        return previous;
    }

    complex_logical_type parameter_types_t::output_column(core::parameter_id_t slot) {
        auto* entry = find(slot);
        if (entry == nullptr) {
            return complex_logical_type{logical_type::NA};
        }
        // Read while untyped (a later column typed the $n since), or typed only by a clause PostgreSQL reads after
        // the select list: untyped when PostgreSQL reads it, so the final pass coerces this use to text.
        const bool read_untyped = entry->untyped_uses > 0;
        if (entry->type.type() != logical_type::NA && !entry->typed_later && !read_untyped) {
            return entry->type;
        }
        if (read_untyped) {
            --entry->untyped_uses;
        }
        entry->text_output = true;
        return complex_logical_type{logical_type::STRING_LITERAL};
    }

    bool parameter_types_t::is_output(const components::logical_plan::node_t* node) const noexcept {
        return node != nullptr && node == output_;
    }

    core::error_t parameter_types_t::check_all_typed() {
        const slot_t* clashing = nullptr;
        for (auto& entry : slots_) {
            if (!entry.text_output) {
                continue;
            }
            if (entry.type.type() == logical_type::NA) {
                entry.type = complex_logical_type{logical_type::STRING_LITERAL};
            } else if (entry.type.type() != logical_type::STRING_LITERAL &&
                       (clashing == nullptr || entry.number < clashing->number)) {
                clashing = &entry;
            }
        }
        if (clashing != nullptr) {
            return inconsistent(*clashing);
        }
        const slot_t* lowest = nullptr;
        for (const auto& entry : slots_) {
            const bool untyped = entry.read && entry.type.type() == logical_type::NA;
            const bool partly = entry.untyped_uses > 0;
            if ((untyped || partly) && (lowest == nullptr || entry.number < lowest->number)) {
                lowest = &entry;
            }
        }
        if (lowest == nullptr) {
            return core::error_t::no_error();
        }
        return core::error_t{
            lowest->type.type() == logical_type::NA ? core::error_code_t::indeterminate_datatype
                                                    : core::error_code_t::ambiguous_parameter,
            std::pmr::string{"could not determine data type of parameter $" + std::to_string(lowest->number),
                             resource_}};
    }

    void parameter_types_t::type_slots(components::logical_plan::parameter_node_t& parameters) const {
        for (const auto& entry : slots_) {
            if (entry.type.type() != logical_type::NA) {
                parameters.set_parameter(entry.slot, components::vector::make_null(resource_, entry.type));
            }
        }
    }

    const components::logical_plan::node_t* output_node_of(const components::logical_plan::node_t* root) {
        while (root != nullptr && root->type() == components::logical_plan::node_type::sequence_t) {
            const components::logical_plan::node_t* body = nullptr;
            for (auto it = root->children().rbegin(); it != root->children().rend(); ++it) {
                if (*it && (*it)->type() != components::logical_plan::node_type::catalog_resolve_t) {
                    body = it->get();
                    break;
                }
            }
            root = body;
        }
        return root;
    }

} // namespace services::dispatcher::validation
