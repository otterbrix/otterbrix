#pragma once

#include "operator.hpp"
#include "operator_data.hpp"
#include <boost/intrusive_ptr.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {
    // A pre-materialized data carrier: holds a fixed set of chunks (set in the ctor,
    // mark_executed() immediately). It is only ever read synchronously via output() by
    // its holder (operator_group_t's non-vectorizable aggregator gather,
    // operator_aggregate_t's scalar path) — it is never a plan root nor an operator on a
    // driven left-chain, so it inherits the base SINK role and is never streamed.
    class operator_batch_t final : public read_only_operator_t {
    public:
        operator_batch_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);
    };

    using operator_batch_ptr = boost::intrusive_ptr<operator_batch_t>;

    inline operator_batch_ptr make_operator_batch(std::pmr::memory_resource* resource, chunks_vector_t&& chunks) {
        return {new operator_batch_t(resource, std::move(chunks))};
    }
} // namespace components::operators
