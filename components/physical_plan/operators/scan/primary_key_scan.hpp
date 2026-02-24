#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class primary_key_scan final : public read_only_operator_t {
    public:
        explicit primary_key_scan(std::pmr::memory_resource* resource, collection_full_name_t name = {});

        void append(size_t id);

        const collection_full_name_t& collection_name() const noexcept { return name_; }
        const vector::vector_t& rows() const { return rows_; }
        size_t row_count() const { return size_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        collection_full_name_t name_;
        vector::vector_t rows_;
        size_t size_{0};

        void on_execute_impl(pipeline::context_t* pipeline_context) override;
    };

} // namespace components::operators
