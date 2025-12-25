#pragma once

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_aggregate.hpp>

namespace components::collection::operators::aggregate {

    class operator_count_t final : public operator_aggregate_t {
    public:
        explicit operator_count_t(services::collection::context_collection_t* collection);

    private:
        document::document_ptr aggregate_impl() override;
        std::string key_impl() const override;
    };

} // namespace components::collection::operators::aggregate