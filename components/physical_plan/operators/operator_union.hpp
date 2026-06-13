#pragma once

#include "operator.hpp"

namespace components::operators {

    class operator_union_t final : public read_only_operator_t {
    public:
        operator_union_t(std::pmr::memory_resource* resource, log_t log, bool all);

    private:
        bool all_;

        void on_execute_impl(pipeline::context_t* context) override;
    };

} // namespace components::operators