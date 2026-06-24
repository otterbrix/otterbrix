#include "operator_raw_data.hpp"

namespace components::operators {

    operator_raw_data_t::operator_raw_data_t(vector::data_chunk_t&& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        auto* resource = chunk.resource();
        output_ = make_operator_data(resource, std::move(chunk));
    }

    operator_raw_data_t::operator_raw_data_t(const vector::data_chunk_t& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        output_ = make_operator_data(chunk.resource(), chunk.types(), chunk.size());
        chunk.copy(output_->data_chunk(), 0);
    }

    std::pmr::memory_resource* operator_raw_data_t::resource() const noexcept { return output_->resource(); }

    vector::data_chunk_t operator_raw_data_t::make_drain_chunk() {
        std::pmr::vector<types::complex_logical_type> empty_types(resource());
        return vector::data_chunk_t{resource(), empty_types, 0};
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_raw_data_t::source_next(pipeline::context_t* /*ctx*/) {
        // The literal rows already live in output_ (set in the ctor): walk the
        // chunk vector by an index cursor and emit a COPY of each chunk (right
        // children re-read output_, so the chunks must not be moved out — mirrors
        // execute_pipeline's COPY-not-move note for materialized inputs).
        //
        // output_ always carries >=1 chunk (make_operator_data materializes one even
        // for a 0-row VALUES), and that chunk holds the VALUES value schema. A
        // schema'd 0-row chunk is REAL input (the pump stops only on a 0-COLUMN
        // chunk), so a 0-row VALUES first flows its one schema'd 0-row guard — letting
        // a scalar aggregate emit COUNT=0 and an OUTER join NULL-pad — and drains on
        // the next call (cursor past the end).
        const auto& chunks = output_->chunks();
        if (cursor_ < chunks.size()) {
            const auto& c = chunks[cursor_];
            ++cursor_;
            co_return core::result_wrapper_t<vector::data_chunk_t>(c.partial_copy(resource(), 0, c.size()));
        }

        // Past the last chunk: emit the 0-column drain sentinel so the pump stops.
        co_return core::result_wrapper_t<vector::data_chunk_t>(make_drain_chunk());
    }

} // namespace components::operators
