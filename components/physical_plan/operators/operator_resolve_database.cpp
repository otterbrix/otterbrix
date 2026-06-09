#include "operator_resolve_database.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve_database.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // Direct write into the uint32_t column buffer, avoiding a logical_value_t round-trip.
        inline void set_uint32(vector::data_chunk_t& c, size_t col, size_t row, std::uint32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::uint32_t>()[row] = v;
            vec.validity().set(row, true);
        }
    } // namespace

    operator_resolve_database_t::operator_resolve_database_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             std::string name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_database)
        , name_(std::move(name)) {}

    operator_resolve_database_t::operator_resolve_database_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string name,
        components::logical_plan::node_catalog_resolve_database_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_database)
        , name_(std::move(name))
        , target_node_(target_node) {}

    void operator_resolve_database_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_resolve_database_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgDatabase = catalog::well_known_oid::pg_database_table;

        // Single-column UINTEGER (database_oid) chunk. Always emitted, even on
        // miss, so downstream operators see a consistent schema.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.emplace_back(types::logical_type::UINTEGER);
        out_types.back().set_alias("database_oid");

        vector::data_chunk_t out_chunk(resource_, out_types);

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            out_chunk.set_cardinality(0);
            set_output(make_operator_data(resource_, std::move(out_chunk)));
            mark_executed();
            co_return;
        }

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Look up pg_database by datname.
        types::logical_value_t name_lv(resource_, std::string_view{name_});
        std::pmr::vector<std::string> db_keys(resource_);
        db_keys.emplace_back("datname");
        std::pmr::vector<types::logical_value_t> db_vals(resource_);
        db_vals.emplace_back(name_lv);
        auto [_db, dbf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           kPgDatabase,
                                           std::move(db_keys),
                                           components::operators::make_key_chunk(resource_, std::move(db_vals)));
        auto db_batches = co_await std::move(dbf);

        bool resolved = false;
        if (!db_batches.empty() && db_batches[0].size() != 0 && db_batches[0].column_count() >= 1) {
            auto db_oid_v = db_batches[0].value(0, 0);
            if (!db_oid_v.is_null()) {
                const auto oid_val = static_cast<catalog::oid_t>(db_oid_v.value<std::uint32_t>());
                out_chunk.set_cardinality(1);
                set_uint32(out_chunk, 0, 0, static_cast<std::uint32_t>(oid_val));
                if (target_node_) {
                    target_node_->set_database_oid(oid_val);
                }
                resolved = true;
            }
        }
        if (!resolved) {
            out_chunk.set_cardinality(0);
        }

        set_output(make_operator_data(resource_, std::move(out_chunk)));
        mark_executed();
    }

} // namespace components::operators
