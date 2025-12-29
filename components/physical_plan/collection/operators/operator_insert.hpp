#pragma once

#include <components/document/document.hpp>
#include <components/expressions/key.hpp>
#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators {

    class operator_insert final : public read_write_operator_t {
    public:
        operator_insert(services::collection::context_collection_t* collection,
                        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> = {});

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> key_translation_;
    };

} // namespace components::collection::operators
