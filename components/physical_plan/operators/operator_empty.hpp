#pragma once

#include "operator.hpp"

namespace components::operators {

    class operator_empty_t final : public read_only_operator_t {
    public:
        operator_empty_t(std::pmr::memory_resource* resource, operator_data_ptr&& data);

    private:
        void on_execute_impl(pipeline::context_t*) override;
    };

} // namespace components::operators
