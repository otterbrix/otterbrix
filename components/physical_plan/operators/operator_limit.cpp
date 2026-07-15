#include "operator_limit.hpp"

#include <algorithm>
#include <limits>

namespace components::operators {

    operator_limit_t::operator_limit_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::limit)
        , limit_(limit) {}

    core::error_t
    operator_limit_t::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& input, chunks_vector_t& out) {
        const int64_t batch = static_cast<int64_t>(input.size());
        if (batch == 0) {
            return core::error_t::no_error();
        }

        // Absolute window [win_start, win_end) over the whole merged stream, in stream
        // positions. offset = rows to skip; limit = rows to keep (-1 = unlimited). The
        // window is [offset, offset + limit), saturating on overflow, or [offset, inf)
        // when unlimited.
        const int64_t offset = limit_.offset();
        const int64_t lim = limit_.limit();
        const int64_t win_start = offset;
        const int64_t win_end = (lim < 0)                                        ? std::numeric_limits<int64_t>::max()
                                : (offset > std::numeric_limits<int64_t>::max() - lim)
                                    ? std::numeric_limits<int64_t>::max()
                                    : offset + lim;

        // This batch occupies stream positions [chunk_start, chunk_end); intersect with
        // the window and emit that contiguous slice (every row passes a limit, so the
        // surviving rows of a batch are always a contiguous run — no per-row filtering).
        const int64_t chunk_start = stream_pos_;
        const int64_t chunk_end = stream_pos_ + batch;
        stream_pos_ = chunk_end;

        const int64_t emit_start = std::max(win_start, chunk_start);
        const int64_t emit_end = std::min(win_end, chunk_end);
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
                out.emplace_back(input.partial_copy(res,
                                                    static_cast<uint64_t>(emit_start - chunk_start),
                                                    static_cast<uint64_t>(emit_end - emit_start)));
            }
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
