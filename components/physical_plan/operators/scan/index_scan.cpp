#include "index_scan.hpp"

#include <components/index/index.hpp>
#include <components/index/index_engine.hpp>

namespace components::operators {

    index_scan::index_scan(std::pmr::memory_resource* resource, log_t* log,
                           collection_full_name_t name,
                           index::index_engine_ptr& index_engine,
                           const expressions::compare_expression_ptr& expr,
                           logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , name_(std::move(name))
        , index_engine_(&index_engine)
        , expr_(expr)
        , limit_(limit) {}

    void index_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (log_) {
            trace(log(), "index_scan by field \"{}\"", expr_->primary_key().as_string());
        }

        // In production, executor intercepts scans via intercept_scan_() and injects data
        // before on_execute_impl runs, so output_ is already set.
        // For unit tests (no executor), do local index lookup when index_engine_ is available
        // and left_ child provides the full table data.
        if (!index_engine_ || !*index_engine_ || !left_ || !left_->output()) {
            return;
        }

        auto* idx = index::search_index(*index_engine_, {expr_->primary_key()});
        if (!idx) return;

        // Get compare value from parameters
        auto& value = pipeline_context->parameters.parameters.at(expr_->value());

        // Search index based on compare type
        std::vector<int64_t> row_ids;
        using expressions::compare_type;
        switch (expr_->type()) {
            case compare_type::eq: {
                auto range = idx->find(value);
                for (auto it = range.first; it != range.second; ++it) {
                    row_ids.push_back(it->row_index);
                }
                break;
            }
            case compare_type::lt: {
                // lower_bound returns (cbegin, first_>=_value), iterate the range for < value
                auto range = idx->lower_bound(value);
                for (auto it = range.first; it != range.second; ++it) {
                    row_ids.push_back(it->row_index);
                }
                break;
            }
            case compare_type::lte: {
                // upper_bound returns (first_>_value, cend), iterate from cbegin to first_>_value for <= value
                auto ub = idx->upper_bound(value);
                for (auto it = idx->cbegin(); it != ub.first; ++it) {
                    row_ids.push_back(it->row_index);
                }
                break;
            }
            case compare_type::gt: {
                // upper_bound returns (first_>_value, cend), iterate the range for > value
                auto range = idx->upper_bound(value);
                for (auto it = range.first; it != range.second; ++it) {
                    row_ids.push_back(it->row_index);
                }
                break;
            }
            case compare_type::gte: {
                // lower_bound returns (cbegin, first_>=_value), iterate from first_>=_value to cend for >= value
                auto lb = idx->lower_bound(value);
                for (auto it = lb.second; it != idx->cend(); ++it) {
                    row_ids.push_back(it->row_index);
                }
                break;
            }
            case compare_type::ne: {
                auto eq_range = idx->find(value);
                for (auto it = idx->cbegin(); it != idx->cend(); ++it) {
                    bool in_eq = false;
                    for (auto eq_it = eq_range.first; eq_it != eq_range.second; ++eq_it) {
                        if (eq_it->row_index == it->row_index) {
                            in_eq = true;
                            break;
                        }
                    }
                    if (!in_eq) {
                        row_ids.push_back(it->row_index);
                    }
                }
                break;
            }
            default:
                break;
        }

        // Apply limit
        size_t count = row_ids.size();
        int limit_val = limit_.limit();
        if (limit_val >= 0) {
            count = std::min(count, static_cast<size_t>(limit_val));
        }

        // Build output from source data
        auto& source_chunk = left_->output()->data_chunk();
        auto types = source_chunk.types();
        output_ = operators::make_operator_data(resource_, types, count);
        auto& out_chunk = output_->data_chunk();

        for (size_t i = 0; i < count; i++) {
            auto row_id = static_cast<size_t>(row_ids[i]);
            for (size_t j = 0; j < source_chunk.column_count(); j++) {
                out_chunk.set_value(j, i, source_chunk.value(j, row_id));
            }
            out_chunk.row_ids.data<int64_t>()[i] = row_ids[i];
        }
        out_chunk.set_cardinality(count);
    }

} // namespace components::operators
