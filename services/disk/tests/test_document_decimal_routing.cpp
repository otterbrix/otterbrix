#include <catch2/catch_test_macros.hpp>

#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <thread>
#include <unistd.h>

// ЗАПИСЬ #364: the write path routes an incoming column onto an existing storage column
// BY NAME (plus the bare logical_type ENUM on a computed table), so DECIMAL(12,4) data
// landed inside a column whose storage is DECIMAL(10,2). The first tripwire was the
// statistics merge (logical_value_t comparison across two decimal parameterizations —
// SIGABRT in a debug build); under NDEBUG the same append went through silently, storing
// scale-4 raw integers into a scale-2 column — every later scan misread them ×100.
// The append must REFUSE the parameterization drift before WAL and materialization.

using namespace services::disk;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    using namespace disk_test_helpers;
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::data_chunk_t;

    std::string dec_dir() {
        static std::string p = "/tmp/test_otterbrix_dec_route_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(dec_dir()); }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = dec_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(dec_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }
    };

    complex_logical_type decimal_with_alias(uint8_t width, uint8_t scale, const char* alias) {
        auto created = complex_logical_type::create_decimal(width, scale);
        REQUIRE_FALSE(created.has_error());
        auto t = std::move(created.value());
        t.set_alias(alias);
        return t;
    }

    // One row, one decimal column named `alias`, raw (unscaled) value `raw`.
    std::pmr::vector<data_chunk_t>
    decimal_batch(std::pmr::memory_resource* r, uint8_t width, uint8_t scale, const char* alias, int64_t raw) {
        std::pmr::vector<complex_logical_type> types(r);
        types.push_back(decimal_with_alias(width, scale, alias));
        data_chunk_t chunk(r, types, 1);
        chunk.set_cardinality(1);
        chunk.data[0].data<int64_t>()[0] = raw;
        std::pmr::vector<data_chunk_t> batch(r);
        batch.emplace_back(std::move(chunk));
        return batch;
    }

    components::execution_context_t append_ctx(catalog::oid_t table_oid) {
        return components::execution_context_t{session_id_t{},
                                               components::table::transaction_data{0, 0},
                                               {},
                                               table_oid};
    }
} // namespace

TEST_CASE("services::disk::document_decimal::second_scale_refuses_instead_of_merging") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "docns");
    auto table_oid = test_create_computing_table(fx, ns_oid, "docs");
    REQUIRE(table_oid >= catalog::FIRST_USER_OID);
    std::vector<components::table::column_definition_t> no_columns;
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              no_columns,
              /*is_computed=*/true);

    // First document: x = 1.50 as DECIMAL(10,2). The computed table adopts the column.
    {
        auto appended = fx.invoke(&manager_disk_t::storage_append,
                                  append_ctx(table_oid),
                                  table_oid,
                                  decimal_batch(&fx.resource, 10, 2, "x", 150));
        REQUIRE_FALSE(appended.has_error());
        REQUIRE(appended.value().second == 1);
    }

    // Second document: x = 2.7182 as DECIMAL(12,4). RED before the fix: the by-name
    // (and enum-only) routing moved this vector into the (10,2) column and the
    // statistics merge died on the cross-parameterization comparison (SIGABRT in this
    // debug build; silent ×100 misread under NDEBUG). The honest answer is a refusal.
    {
        auto appended = fx.invoke(&manager_disk_t::storage_append,
                                  append_ctx(table_oid),
                                  table_oid,
                                  decimal_batch(&fx.resource, 12, 4, "x", 27182));
        REQUIRE(appended.has_error());
    }

    // The refusal costs one statement, not the table: the original parameterization
    // still appends, and nothing of the refused chunk landed.
    {
        auto appended = fx.invoke(&manager_disk_t::storage_append,
                                  append_ctx(table_oid),
                                  table_oid,
                                  decimal_batch(&fx.resource, 10, 2, "x", 275));
        REQUIRE_FALSE(appended.has_error());
        REQUIRE(appended.value().first == 1);
        REQUIRE(appended.value().second == 1);
    }
}

TEST_CASE("services::disk::document_decimal::regular_table_refuses_parameterization_drift") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "regns");
    std::vector<components::table::column_definition_t> cols;
    {
        auto created = complex_logical_type::create_decimal(10, 2);
        REQUIRE_FALSE(created.has_error());
        cols.emplace_back("x", std::move(created.value()));
    }
    auto table_oid = test_create_table(fx, ns_oid, "prices", cols);
    REQUIRE(table_oid >= catalog::FIRST_USER_OID);
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    {
        auto appended = fx.invoke(&manager_disk_t::storage_append,
                                  append_ctx(table_oid),
                                  table_oid,
                                  decimal_batch(&fx.resource, 10, 2, "x", 999));
        REQUIRE_FALSE(appended.has_error());
    }

    // The regular-table leg matches by name alone, so the same drift reached the same
    // storage column. Same refusal required.
    {
        auto appended = fx.invoke(&manager_disk_t::storage_append,
                                  append_ctx(table_oid),
                                  table_oid,
                                  decimal_batch(&fx.resource, 12, 4, "x", 12345));
        REQUIRE(appended.has_error());
    }
}
