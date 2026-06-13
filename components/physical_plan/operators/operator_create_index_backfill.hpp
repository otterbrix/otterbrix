#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <vector>

namespace components::operators {

    // Performs the runtime side of CREATE INDEX:
    //   1. ensure the collection is registered with the index manager
    //   2. create the in-memory index engine entry (manager_index::create_index)
    //   3. scan the table's existing rows from disk and feed them via insert_rows
    //   4. flip pg_index.indisvalid → true (delete the !valid row, write a valid one)
    //
    // The pg_class/pg_index(indisvalid=false)/pg_depend rows have already been
    // written by operator_create_index_metadata_t earlier in the sequence, so
    // recovery sees a half-built index as invalid until the backfill step
    // commits successfully.
    class operator_create_index_backfill_t final : public read_write_operator_t {
    public:
        operator_create_index_backfill_t(std::pmr::memory_resource* resource,
                                         log_t log,
                                         std::string index_name,
                                         components::logical_plan::index_type index_type,
                                         std::pmr::vector<components::expressions::key_t> keys,
                                         components::catalog::oid_t table_oid,
                                         components::catalog::oid_t index_oid,
                                         std::string indkey);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string index_name_;
        components::logical_plan::index_type index_type_;
        std::pmr::vector<components::expressions::key_t> keys_;
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t index_oid_;
        std::string indkey_;
    };

} // namespace components::operators
