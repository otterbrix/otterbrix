#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {

    // Creates physical storage, registers the collection with the index manager,
    // and writes pre-built pg_catalog rows (pg_class, pg_attribute, pg_depend).
    // All work is done in a single await_async_and_resume so the executor's
    // find_waiting_operator loop drives it correctly.
    class operator_create_collection_t final : public read_write_operator_t {
    public:
        using catalog_write_t = std::pair<components::catalog::oid_t, vector::data_chunk_t>;

        operator_create_collection_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     components::catalog::oid_t database_oid,
                                     std::vector<table::column_definition_t> columns,
                                     bool is_disk_storage,
                                     std::vector<catalog_write_t> catalog_writes);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        components::catalog::oid_t database_oid_;
        std::vector<table::column_definition_t> columns_;
        bool is_disk_storage_;
        std::vector<catalog_write_t> catalog_writes_;
    };

} // namespace components::operators
