#include "operator_recursive_cte.hpp"
#include "operator_data.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::operators {

    operator_recursive_cte_t::operator_recursive_cte_t(std::pmr::memory_resource* resource, log_t log, bool all)
        : read_only_operator_t(resource, std::move(log), operator_type::recursive_cte)
        , all_(all) {}

    void operator_recursive_cte_t::reset_subtree(const operator_ptr& op) {
        if (!op) {
            return;
        }
        if (is_scan(op->type())) {
            return;
        }
        op->reset_for_reuse();
        reset_subtree(op->left());
        reset_subtree(op->right());
    }

    void operator_recursive_cte_t::on_execute_impl(pipeline::context_t* context) {
        if (!left_ || !left_->output()) {
            return;
        }

        auto* res = resource_;
        chunks_vector_t result(res);

        // Copy anchor rows into result and set as initial working set.
        for (const auto& chunk : left_->output()->chunks()) {
            if (chunk.size() > 0) {
                result.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
            }
        }
        working_set_ = left_->output();

        // Iterate: each pass, right_ reads from working_set_ via operator_cte_scan_t.
        while (true) {
            reset_subtree(right_);
            right_->on_execute(context);
            if (right_->has_error()) {
                set_error(right_->get_error());
                return;
            }
            if (!right_->output() || right_->output()->size() == 0) {
                break;
            }

            bool has_new = false;
            for (const auto& chunk : right_->output()->chunks()) {
                if (chunk.size() > 0) {
                    has_new = true;
                    result.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                }
            }
            if (!has_new) {
                break;
            }
            // Point working_set at right_'s output for the next iteration.
            working_set_ = right_->output();
        }

        if (result.empty() && left_->output() && !left_->output()->chunks().empty()) {
            result.emplace_back(res, left_->output()->chunks().front().types(), 0);
        }
        output_ = make_operator_data(res, std::move(result));
    }

} // namespace components::operators