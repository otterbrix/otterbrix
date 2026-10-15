#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/vector/storage_parameters.hpp>
#include <core/pmr.hpp>

#include <components/expressions/forward.hpp>

namespace components::logical_plan {

    using storage_parameters = vector::storage_parameters;
    using parameter_kind = vector::parameter_kind;

    class parameter_node_t : public boost::intrusive_ref_counter<parameter_node_t> {
    public:
        explicit parameter_node_t(std::pmr::memory_resource* resource)
            : values_(resource) {}

        auto parameters() const -> const storage_parameters& { return values_; }

        [[nodiscard]] core::parameter_id_t add_parameter(vector::vector_t&& value, parameter_kind kind) {
            return values_.add(std::move(value), kind);
        }

        auto parameter(core::parameter_id_t id) const -> const vector::vector_t& { return values_.at(id); }

        void set_parameter(core::parameter_id_t id, vector::vector_t&& value) { values_.set(id, std::move(value)); }

        void set_parameter_cell(core::parameter_id_t id, const vector::vector_t& column, uint64_t row) {
            values_.set_cell(id, column, row);
        }

    private:
        storage_parameters values_;
    };

    using parameter_node_ptr = boost::intrusive_ptr<parameter_node_t>;

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource);

} // namespace components::logical_plan
