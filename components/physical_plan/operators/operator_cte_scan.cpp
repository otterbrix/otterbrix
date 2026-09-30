#include "operator_cte_scan.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::operators {

    operator_cte_scan_t::operator_cte_scan_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             operator_data_ptr* working_set)
        : read_only_operator_t(resource, std::move(log), operator_type::cte_scan)
        , working_set_(working_set) {}

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_cte_scan_t::source_next(pipeline::context_t* /*ctx*/) {
        // Walk the CURRENT working set (the recursive driver repoints *working_set_ each
        // iteration and resets this cursor via reset_source). Emit a COPY of each chunk —
        // the working set is owned by the recursive_cte and re-read by the next iteration,
        // so moving chunks out would empty it for that reader.
        if (working_set_ && *working_set_) {
            const auto& chunks = (*working_set_)->chunks();
            if (cursor_ < chunks.size()) {
                const auto& c = chunks[cursor_];
                ++cursor_;
                co_return c.partial_copy(resource_, 0, c.size());
            }
        }
        co_return std::nullopt;
    }

} // namespace components::operators
