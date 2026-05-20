#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

#include <utility>
#include <vector>

namespace components::operators {

    // Writes pre-built pg_catalog rows for a CREATE INDEX statement: pg_class
    // (relkind='i') + pg_index (indisvalid=false) + pg_depend (index→table 'a' +
    // index→column 'i'). The rows are produced upstream by the planner via
    // catalog::build_create_index_writes; this operator just streams them through
    // disk.append_pg_catalog_row in the order received.
    //
    // Pairs with operator_create_index_backfill_t which performs the index
    // engine work (register/create/scan/insert_rows) and flips indisvalid=true.
    // Splitting the two means the metadata is durable before any backfill runs,
    // and the backfill phase can be retried/resumed independently.
    class operator_create_index_metadata_t final : public read_write_operator_t {
    public:
        using catalog_write_t = std::pair<components::catalog::oid_t, vector::data_chunk_t>;

        operator_create_index_metadata_t(std::pmr::memory_resource* resource,
                                         log_t log,
                                         std::vector<catalog_write_t> catalog_writes);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::vector<catalog_write_t> catalog_writes_;
    };

} // namespace components::operators
