#include "param_storage.hpp"

#include <core/pmr.hpp>

namespace components::logical_plan {

    const expr_value_t* get_parameter(const storage_parameters* storage, core::parameter_id_t id) noexcept {
        auto it = storage->parameters.find(id);
        return it != storage->parameters.end() ? &it->second : nullptr;
    }

    auto parameter_node_t::parameters() const -> const storage_parameters& { return values_; }

    storage_parameters parameter_node_t::take_parameters() { return std::move(values_); }

    auto parameter_node_t::set_parameters(const storage_parameters& parameters) -> void { values_ = parameters; }

    auto parameter_node_t::next_id() -> core::parameter_id_t {
        // The id must be FREE, not merely next. Three writers install ids this counter
        // never saw — the explicit-id add_parameter (plan API, python binding), the
        // view-body merge's set_parameter, and set_parameters — and the storage's
        // emplace silently keeps the OLD binding on a collision. A caller that minted
        // a colliding id here (constant folding is the one post-bind minter) would
        // rewrite its expression to read someone else's value.
        while (values_.parameters.find(core::parameter_id_t(counter_)) != values_.parameters.end()) {
            ++counter_;
        }
        auto tmp = counter_;
        ++counter_;
        return core::parameter_id_t(tmp);
    }

    auto parameter_node_t::parameter(core::parameter_id_t id) const noexcept -> const expr_value_t* {
        return get_parameter(&values_, id);
    }

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource) {
        return {new parameter_node_t(resource)};
    }
} // namespace components::logical_plan
