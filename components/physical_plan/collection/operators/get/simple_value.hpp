#pragma once

#include "operator_get.hpp"

#include <components/expressions/key.hpp>

namespace components::collection::operators::get {

    class simple_value_t final : public operator_get_t {
    public:
        static operator_get_ptr create(const expressions::key_t& key);

    private:
        const expressions::key_t key_;

        explicit simple_value_t(const expressions::key_t& key);

        document::value_t get_value_impl(const document::document_ptr& document);
    };

} // namespace components::collection::operators::get
