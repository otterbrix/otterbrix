#pragma once

#include <components/document/document.hpp>
#include <components/expressions/key.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_aggregate.hpp>

namespace components::collection::operators::aggregate {

    class operator_avg_t final : public operator_aggregate_t {
    public:
        explicit operator_avg_t(services::collection::context_collection_t* collection, expressions::key_t key);

    private:
        expressions::key_t key_;

        document::document_ptr aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::collection::operators::aggregate