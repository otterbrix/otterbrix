#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_unique_constraint.hpp>
#include <components/physical_plan/operators/operator_update.hpp>

namespace components::operators {

    class operator_insert_on_conflict_t final : public read_write_operator_t {
    public:
        operator_insert_on_conflict_t(std::pmr::memory_resource* resource,
                                      log_t log,
                                      catalog::oid_t table_oid,
                                      bool has_returning);

        void set_parts(operator_ptr insert_part,
                       operator_unique_constraint_t* check,
                       boost::intrusive_ptr<operator_delete> removal) noexcept;

        void set_update(operator_ptr update_part, operator_update* update) noexcept;

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, catalog::INVALID_OID);
            if (insert_part_) {
                insert_part_->explain(s);
            }
            if (update_part_) {
                update_part_->explain(s);
            }
            s.end();
        }

        actor_zeta::unique_future<core::error_t> drive_(pipeline::context_t* ctx);
        actor_zeta::unique_future<core::error_t>
        fetch_holders_(pipeline::context_t* ctx, const operator_data_ptr& conflicts, chunks_vector_t* targets);
        void compose_output_(const operator_data_ptr& inserted,
                             const operator_data_ptr& removed,
                             const operator_data_ptr& updated);

        catalog::oid_t table_oid_;
        bool has_returning_;
        operator_ptr insert_part_{nullptr};
        operator_unique_constraint_t* check_{nullptr};
        boost::intrusive_ptr<operator_delete> removal_{nullptr};
        operator_ptr update_part_{nullptr};
        operator_update* update_{nullptr};
    };

} // namespace components::operators
