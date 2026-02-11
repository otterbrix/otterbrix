#pragma once

#include "operator_get.hpp"

#include <expressions/key.hpp>

namespace components::operators::get {

    class simple_value_t final : public operator_get_t {
    public:
        static operator_get_ptr create(const expressions::key_t& key);

    private:
        const expressions::key_t key_;

        explicit simple_value_t(const expressions::key_t& key);

        std::vector<types::logical_value_t>
        get_values_impl(const std::pmr::vector<types::logical_value_t>& row) override;
    };

} // namespace components::operators::get
