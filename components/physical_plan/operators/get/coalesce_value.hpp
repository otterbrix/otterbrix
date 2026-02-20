#pragma once

#include "operator_get.hpp"

#include <expressions/expression.hpp>
#include <logical_plan/param_storage.hpp>

namespace components::operators::get {

    class coalesce_value_t final : public operator_get_t {
    public:
        static operator_get_ptr create(std::vector<expressions::param_storage> params,
                                       const logical_plan::storage_parameters* storage_params);

    private:
        std::vector<expressions::key_t> keys_;
        std::vector<types::logical_value_t> constants_;

        struct coalesce_entry {
            enum class kind { key, constant };
            kind type;
            size_t index;
        };
        std::vector<coalesce_entry> entries_;

        coalesce_value_t(std::vector<expressions::key_t> keys,
                         std::vector<types::logical_value_t> constants,
                         std::vector<coalesce_entry> entries);

        std::vector<types::logical_value_t>
        get_values_impl(const std::pmr::vector<types::logical_value_t>& row) override;
    };

} // namespace components::operators::get
