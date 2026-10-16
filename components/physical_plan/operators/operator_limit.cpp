#include "operator_limit.hpp"

#include <algorithm>
#include <cassert>

namespace components::operators {

    operator_limit_t::operator_limit_t(std::pmr::memory_resource* resource, log_t log, logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::limit)
        , limit_(limit)
        , shape_(resource) {}

    actor_zeta::unique_future<core::error_t> operator_limit_t::open_impl(pipeline::context_t* /*ctx*/) {
        auto invalid = limit_.validate(resource_);
        if (invalid.contains_error()) {
            co_return invalid;
        }
        begin_ = static_cast<uint64_t>(limit_.offset());
        end_ = limit_.read_cap().rows();
        opened_ = true;
        co_return core::error_t::no_error();
    }

    core::error_t
    operator_limit_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& input, chunks_vector_t& out) {
        assert(opened_ && "the executor opens an operator_limit_t before pushing into it");
        if (shape_.empty()) {
            const auto& types = input.types();
            shape_.assign(types.begin(), types.end());
        }
        const uint64_t batch = input.size();
        if (batch == 0) {
            return core::error_t::no_error();
        }

        // This batch occupies stream positions [chunk_start, chunk_end); intersect with
        // the window and emit that contiguous slice (every row passes a limit, so the
        // surviving rows of a batch are always a contiguous run — no per-row filtering).
        const uint64_t chunk_start = stream_pos_;
        const uint64_t chunk_end = stream_pos_ + batch;
        stream_pos_ = chunk_end;

        const uint64_t emit_start = std::max(begin_, chunk_start);
        const uint64_t emit_end = std::min(end_, chunk_end);
        if (emit_start < emit_end) {
            if (emit_start == chunk_start && emit_end == chunk_end) {
                // The whole batch lies inside the window — move it through untouched, no
                // per-row copy. Now that operator_limit is ALWAYS present (the sole
                // authoritative limiter), a large `LIMIT n` result must not pay a full
                // copy per surviving batch; only the ≤2 boundary batches (front-skip /
                // back-trim) need a partial_copy.
                out.emplace_back(std::move(input));
            } else {
                auto* res = resource_ ? resource_ : input.resource();
                out.emplace_back(input.partial_copy(res, emit_start - chunk_start, emit_end - emit_start));
            }
            note_emitted();
        }
        return core::error_t::no_error();
    }

    core::error_t operator_limit_t::finalize(pipeline::context_t* /*ctx*/, chunks_vector_t& out) {
        if (emitted() || shape_.empty()) {
            return core::error_t::no_error();
        }
        vector::data_chunk_t empty(resource_, shape_, 0);
        empty.set_cardinality(0);
        out.emplace_back(std::move(empty));
        return core::error_t::no_error();
    }

} // namespace components::operators
