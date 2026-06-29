#include "operator_cte_scan.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::operators {

    operator_cte_scan_t::operator_cte_scan_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             operator_data_ptr* working_set)
        : read_only_operator_t(resource, std::move(log), operator_type::cte_scan)
        , working_set_(working_set) {}

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_cte_scan_t::source_next(pipeline::context_t* /*ctx*/) {
        // Walk the CURRENT working set (the recursive driver repoints *working_set_ each
        // iteration and resets this cursor via reset_source). Emit a COPY of each chunk —
        // the working set is owned by the recursive_cte and re-read by the next iteration,
        // so moving chunks out would empty it for that reader. A 0-column chunk past the
        // end is the drain sentinel that stops execute_pipeline's pump.
        if (working_set_ && *working_set_) {
            const auto& chunks = (*working_set_)->chunks();
            if (cursor_ < chunks.size()) {
                const auto& c = chunks[cursor_];
                ++cursor_;
                co_return core::result_wrapper_t<vector::data_chunk_t>(c.partial_copy(resource_, 0, c.size()));
            }
        }

        // No working set, or walked past the last chunk: emit the 0-column drain sentinel.
        std::pmr::vector<types::complex_logical_type> empty_types(resource_);
        co_return core::result_wrapper_t<vector::data_chunk_t>(vector::data_chunk_t{resource_, empty_types, 0});
    }

} // namespace components::operators
