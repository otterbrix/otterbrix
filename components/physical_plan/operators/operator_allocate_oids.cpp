#include "operator_allocate_oids.hpp"

#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <utility>

namespace components::operators {

    operator_allocate_oids_t::operator_allocate_oids_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       std::size_t count)
        : read_write_operator_t(resource, std::move(log), operator_type::allocate_oids)
        , count_(count) {}

    actor_zeta::unique_future<void> operator_allocate_oids_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (count_ == 0 || ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }
        auto [_a, af] =
            actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::allocate_oids_batch, count_);
        auto batch = co_await std::move(af);

        // Publish on the data channel: a single UINTEGER column (catalog::oid_t is
        // uint32_t), one row per allocated OID, rows in allocation order. Split
        // across chunks of at most DEFAULT_VECTOR_CAPACITY rows, as the multi-chunk
        // operator_data contract requires.
        std::pmr::vector<types::complex_logical_type> col_types(resource());
        col_types.emplace_back(types::logical_type::UINTEGER);
        chunks_vector_t chunks(resource());
        for (std::size_t offset = 0; offset < batch.size(); offset += vector::DEFAULT_VECTOR_CAPACITY) {
            const std::size_t rows = std::min<std::size_t>(vector::DEFAULT_VECTOR_CAPACITY, batch.size() - offset);
            vector::data_chunk_t chunk(resource(), col_types, rows);
            for (std::size_t i = 0; i < rows; ++i) {
                chunk.set_value(uint64_t{0}, static_cast<uint64_t>(i), batch[offset + i]);
            }
            chunk.set_cardinality(static_cast<uint64_t>(rows));
            chunks.emplace_back(std::move(chunk));
        }
        set_output(make_operator_data(resource(), std::move(chunks)));
        mark_executed();
        co_return;
    }

} // namespace components::operators
