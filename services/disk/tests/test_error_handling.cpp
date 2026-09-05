#include <catch2/catch_test_macros.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>

// Edge cases for ddl_*: missing parent, RESTRICT blocks with descriptive error,
// CASCADE through chains, malformed names, OID monotonicity, drop-by-unknown-oid no-op,
// dependency cycle detection, etc.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    using namespace disk_test_helpers;

    std::string err_dir() {
        static std::string p = "/tmp/test_otterbrix_err_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(err_dir()); }

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
                c.path = err_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(err_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the manager first: its dtor joins the internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
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

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. resolve_namespace on unknown name returns found=false, no error.
TEST_CASE("services::disk::error::resolve_unknown_namespace") {
    fixture fx;
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 2. resolve_table with valid namespace_oid but unknown table name returns found=false.
TEST_CASE("services::disk::error::resolve_unknown_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rt = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("not_a_table"));
    REQUIRE_FALSE(rt.found);
}

// 3. resolve_table with INVALID_OID namespace returns found=false.
TEST_CASE("services::disk::error::resolve_table_invalid_namespace") {
    fixture fx;
    auto rt = test_probe::probe_table(fx, fx.ctx(), INVALID_OID, std::string("any"));
    REQUIRE_FALSE(rt.found);
}

// 8. CREATE NAMESPACE allows duplicate names — name is not enforced unique at the
//    primitive-write layer (dispatcher checks via catalog_ before calling). Here we
//    just verify it produces distinct OIDs and pg_namespace ends up with two rows of
//    the same name.
TEST_CASE("services::disk::error::duplicate_namespace_name_two_rows") {
    fixture fx;
    auto a = test_create_namespace(fx, "dup");
    auto b = test_create_namespace(fx, "dup");
    REQUIRE(a != b);
    // resolve_namespace returns the first match by scan order — non-deterministic but found.
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("dup"), std::uint64_t{0});
    REQUIRE(r.found);
}

// 12. topological_drop_order on an empty seed returns empty vector — caller pushes the seed.
TEST_CASE("services::disk::error::topological_drop_empty") {
    core::pmr::otterbrix_resource resource;
    auto edges = [](std::pmr::memory_resource* mr, oid_t /*cls*/, oid_t /*oid*/) {
        return std::pmr::vector<dependency_t>{mr};
    };
    oid_t cycle_at = INVALID_OID;
    auto order = topological_drop_order(&resource, well_known_oid::pg_namespace_table, oid_t{16384}, edges, cycle_at);
    REQUIRE(order.empty());
    REQUIRE(cycle_at == INVALID_OID);
}

// 13. CREATE NAMESPACE with a long name (PostgreSQL's typical 63-byte limit isn't
//     enforced here — accept arbitrary length).
TEST_CASE("services::disk::error::long_namespace_name_accepted") {
    fixture fx;
    std::string long_name(200, 'x');
    auto ns_oid = test_create_namespace(fx, long_name);
    REQUIRE(ns_oid >= FIRST_USER_OID);
    auto rs = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), long_name, std::uint64_t{0});
    REQUIRE(rs.found);
}

// 14. CREATE NAMESPACE with empty name accepted (no validation at primitive-write layer).
TEST_CASE("services::disk::error::empty_name_accepted") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "");
    REQUIRE(ns_oid >= FIRST_USER_OID);
}

// 16. resolve_function on unknown name in valid namespace returns found=false.
TEST_CASE("services::disk::error::resolve_unknown_function") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rf = test_probe::probe_function(fx, fx.ctx(), ns_oid, std::string("unknown_fn"));
    REQUIRE_FALSE(rf.found);
}
// 17. storage_delete_rows separates "how many marks were set" from "the delete could
//     not be performed". These were the same value — 0 — until the reply got a wrapper,
//     and both operators that send it (operator_delete, operator_fk_cascade) simply
//     dropped the reply because there was nothing in it to read. A cascade could then
//     mark no child row at all and let its parent row go.
//
//     THE TWO ZEROS ARE THE POINT. A repeat of the same delete legitimately reports 0 —
//     chunk_vector_info::delete_rows skips a row that already carries a stamp, which is
//     also what duplicate ids in one request do — and that zero must stay a SUCCESS. An
//     oid no agent has storage for must not produce that same zero.
TEST_CASE("services::disk::error::delete_rows_refusal_is_not_a_zero_count") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::data_chunk_t;
    using components::vector::vector_t;

    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsdel");

    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", complex_logical_type{logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "rows", cols);
    REQUIRE(table_oid >= FIRST_USER_OID);
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    // Three committed rows.
    int64_t first_row = 0;
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        complex_logical_type t{logical_type::BIGINT};
        t.set_alias("a");
        types.push_back(std::move(t));
        data_chunk_t chunk(&fx.resource, types, 3);
        chunk.set_cardinality(3);
        for (uint64_t i = 0; i < 3; ++i) {
            chunk.set_value(0, i, static_cast<std::int64_t>(i + 1));
        }
        std::pmr::vector<data_chunk_t> batch(&fx.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto appended = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(appended.has_error());
        REQUIRE(appended.value().second == 3);
        first_row = static_cast<int64_t>(appended.value().first);
    }

    auto ids_of = [&](int64_t base, uint64_t n) {
        vector_t v(&fx.resource, logical_type::BIGINT, n);
        for (uint64_t i = 0; i < n; ++i) {
            v.data<int64_t>()[i] = base + static_cast<int64_t>(i);
        }
        return v;
    };

    INFO("the delete happened: three marks set");
    {
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows, txn_ctx(), table_oid, ids_of(first_row, 3), std::uint64_t{3});
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == 3);
    }

    INFO("the same rows again: zero marks set, and that is a SUCCESS, not a refusal");
    {
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows, txn_ctx(), table_oid, ids_of(first_row, 3), std::uint64_t{3});
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == 0);
    }

    INFO("an oid with no storage anywhere: the delete DID NOT HAPPEN, and says so");
    {
        const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows, txn_ctx(), nowhere, ids_of(0, 1), std::uint64_t{1});
        REQUIRE(r.has_error());
    }

    INFO("asking for nothing is not a refusal, whatever the oid");
    {
        const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows, txn_ctx(), nowhere, ids_of(0, 1), std::uint64_t{0});
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == 0);
    }
}
