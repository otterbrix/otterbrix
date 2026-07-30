#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/logical_value.hpp>
#include <core/pmr.hpp>

#include <components/expressions/forward.hpp>

namespace components::logical_plan {

    using expr_value_t = types::logical_value_t;

    struct storage_parameters {
        std::pmr::unordered_map<core::parameter_id_t, expr_value_t> parameters;

        explicit storage_parameters(std::pmr::memory_resource* resource)
            : parameters(resource) {}

        std::pmr::memory_resource* resource() const { return parameters.get_allocator().resource(); }
    };

    template<class Value>
    void add_parameter(storage_parameters& storage, core::parameter_id_t id, Value&& value) {
        storage.parameters.emplace(id, expr_value_t(storage.resource(), std::forward<Value>(value)));
    }

    inline void add_parameter(storage_parameters& storage, core::parameter_id_t id, expr_value_t value) {
        storage.parameters.emplace(id, value);
    }

    // nullptr when `id` is not bound. Absence is deliberately outside the value domain: a
    // sentinel logical_value_t would have to name some memory resource, and every caller that
    // copied it would carry that resource into arbitrary later allocations.
    const expr_value_t* get_parameter(const storage_parameters* storage, core::parameter_id_t id) noexcept;

    class parameter_node_t : public boost::intrusive_ref_counter<parameter_node_t> {
    public:
        explicit parameter_node_t(std::pmr::memory_resource* resource)
            : values_(resource) {}

        auto parameters() const -> const storage_parameters&;
        auto take_parameters() -> storage_parameters;
        auto set_parameters(const storage_parameters& parameters) -> void;

        auto next_id() -> core::parameter_id_t;

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value&& value) {
            components::logical_plan::add_parameter(values_, id, std::forward<Value>(value));
        }

        template<class Value>
        [[nodiscard]] core::parameter_id_t add_parameter(Value&& value) {
            auto id = next_id();
            add_parameter(id, std::forward<Value>(value));
            return id;
        }

        // nullptr when `id` is not bound (see get_parameter).
        auto parameter(core::parameter_id_t id) const noexcept -> const expr_value_t*;

        void set_parameter(core::parameter_id_t id, expr_value_t value) {
            values_.parameters.insert_or_assign(id, std::move(value));
        }

    private:
        uint16_t counter_{0};
        storage_parameters values_;
    };

    using parameter_node_ptr = boost::intrusive_ptr<parameter_node_t>;

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource);

} // namespace components::logical_plan