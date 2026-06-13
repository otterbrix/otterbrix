#include <catch2/catch.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <filesystem>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string resolve_dir() {
        static std::string p = "/tmp/test_otterbrix_resolve_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(resolve_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = resolve_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(resolve_dir());
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
        auto invoke_async(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        // Alias used by disk_test_helpers templates.
        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return invoke_async(fn, std::forward<Args>(args)...);
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. After bootstrap, resolve_namespace finds the well-known "public" namespace.
TEST_CASE("services::disk::resolve::namespace_finds_bootstrap") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("public"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::public_namespace);
}

// 2. resolve_namespace misses on unknown name.
TEST_CASE("services::disk::resolve::namespace_misses_unknown") {
    fixture fx;
    auto r =
        fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 3. After CREATE TABLE, resolve_table finds the new relation + lists its column attoids.
TEST_CASE("services::disk::resolve::table_finds_after_create") {
    fixture fx;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});

    const auto table_oid = disk_test_helpers::test_create_table(fx,
                                                                well_known_oid::public_namespace,
                                                                std::string("users"),
                                                                cols,
                                                                catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    auto r = test_probe::probe_table(fx, fx.ctx(), well_known_oid::public_namespace, std::string("users"));
    REQUIRE(r.found);
    REQUIRE(r.oid == table_oid);
    REQUIRE(r.namespace_oid == well_known_oid::public_namespace);
    REQUIRE(r.relkind == components::catalog::relkind::regular);
    REQUIRE(r.columns.size() == 2);
}

// 4. resolve_table misses when the namespace doesn't match.
TEST_CASE("services::disk::resolve::table_misses_in_wrong_namespace") {
    fixture fx;
    disk_test_helpers::test_create_table(fx,
                                         well_known_oid::public_namespace,
                                         std::string("users"),
                                         std::vector<components::table::column_definition_t>{},
                                         catalog::relkind::regular);

    auto r = test_probe::probe_table(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("users"));
    REQUIRE_FALSE(r.found);
}

// 5. resolve_type finds the bootstrap "int64" type in pg_catalog.
TEST_CASE("services::disk::resolve::type_finds_bootstrap") {
    fixture fx;
    auto r = test_probe::probe_type(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("int64"));
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::int64_type);
}

// 6. resolve_function finds the bootstrap "count" aggregate.
TEST_CASE("services::disk::resolve::function_finds_bootstrap_count") {
    fixture fx;
    auto r = test_probe::probe_function(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("count"));
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::fn_count);
}

// 7. read_chunks_by_keys (batched, N-row columnar keys) == N independent
// read_chunks_by_key calls (parity). A single batched call carries an N-row
// keys data_chunk (column j = key_col_names[j], row i = i-th key-tuple) and
// returns vector<vector<data_chunk_t>> with result[k] == the singular
// read_chunks_by_key result for key k. Covers a no-match key (empty entry)
// and a multi-row-match key.
TEST_CASE("services::disk::resolve::read_chunks_by_keys_multi_key_parity") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;
    using components::vector::data_chunk_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_rbk");
    auto table_oid = disk_test_helpers::test_create_table(
        fx,
        ns_oid,
        "rbk_tbl",
        std::vector<components::table::column_definition_t>{},
        catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    // Regular IN_MEMORY storage with an explicit {k, payload} schema. The rows
    // appended below give:
    //   k=10 -> one row (payload 100)
    //   k=20 -> two rows (payload 200, 201)  [multi-row match]
    //   k=30 -> one row (payload 300)
    //   k=99 -> no row                        [no-match]
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("payload", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_with_columns,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols));
    }
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"k", "payload"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        constexpr std::uint64_t nrows = 4;
        auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, nrows);
        chunk->set_cardinality(nrows);
        const std::int64_t kvals[nrows] = {10, 20, 20, 30};
        const std::int64_t pvals[nrows] = {100, 200, 201, 300};
        for (std::uint64_t r = 0; r < nrows; ++r) {
            chunk->set_value(0, r, logical_value_t(&fx.resource, kvals[r]));
            chunk->set_value(1, r, logical_value_t(&fx.resource, pvals[r]));
        }
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto [start, count] =
            fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(chunk));
        REQUIRE(count == nrows);
        (void) start;
    }

    // The N distinct key values we probe (one no-match: 99).
    const std::vector<std::int64_t> probe_keys = {10, 20, 30, 99};
    const std::size_t N = probe_keys.size();

    auto total_rows = [](const auto& chunks) {
        std::uint64_t t = 0;
        for (const auto& c : chunks)
            t += c.size();
        return t;
    };

    // --- batched: ONE read_chunks_by_keys with an N-row keys chunk ---
    std::vector<std::vector<data_chunk_t>> batched;
    {
        std::pmr::vector<complex_logical_type> ktypes(&fx.resource);
        complex_logical_type kt{logical_type::BIGINT};
        ktypes.push_back(std::move(kt));
        data_chunk_t keys(&fx.resource, ktypes, N);
        for (std::size_t i = 0; i < N; ++i) {
            keys.set_value(0, i, logical_value_t(&fx.resource, probe_keys[i]));
        }
        keys.set_cardinality(N);
        std::pmr::vector<std::string> key_cols{&fx.resource};
        key_cols.emplace_back("k");
        auto res = fx.invoke(&manager_disk_t::read_chunks_by_keys,
                             fx.ctx(),
                             table_oid,
                             std::move(key_cols),
                             std::move(keys));
        REQUIRE(res.size() == N);
        // Copy into a std::vector for re-use in the parity loop (chunk-by-chunk
        // size/value comparison below).
        for (auto& entry : res) {
            std::vector<data_chunk_t> e;
            for (auto& c : entry)
                e.push_back(std::move(c));
            batched.push_back(std::move(e));
        }
    }

    // Batched cardinalities: 10->1, 20->2 (multi-row), 30->1, 99->0 (no-match).
    REQUIRE(total_rows(batched[0]) == 1);
    REQUIRE(total_rows(batched[1]) == 2);
    REQUIRE(total_rows(batched[2]) == 1);
    REQUIRE(total_rows(batched[3]) == 0);

    // --- parity: result[k] of the batched call == singular read_chunks_by_key(k) ---
    for (std::size_t i = 0; i < N; ++i) {
        std::pmr::vector<std::string> single_key_cols{&fx.resource};
        single_key_cols.emplace_back("k");
        std::pmr::vector<logical_value_t> single_vals{&fx.resource};
        single_vals.emplace_back(&fx.resource, probe_keys[i]);
        auto single = fx.invoke(&manager_disk_t::read_chunks_by_key,
                                fx.ctx(),
                                table_oid,
                                std::move(single_key_cols),
                                test_probe::build_key_chunk(&fx.resource, std::move(single_vals)));

        // Same total row count per key (the no-match key yields 0 on both paths).
        std::uint64_t single_total = total_rows(single);
        std::uint64_t batched_total = 0;
        for (auto& c : batched[i])
            batched_total += c.size();
        REQUIRE(batched_total == single_total);

        // Same set of (k, payload) pairs per key on both paths.
        auto collect_pairs = [](auto& chunks) {
            std::vector<std::pair<std::int64_t, std::int64_t>> pairs;
            for (auto& c : chunks) {
                for (std::uint64_t r = 0; r < c.size(); ++r) {
                    pairs.emplace_back(c.value(0, r).template value<std::int64_t>(),
                                       c.value(1, r).template value<std::int64_t>());
                }
            }
            std::sort(pairs.begin(), pairs.end());
            return pairs;
        };
        auto batched_pairs = collect_pairs(batched[i]);
        auto single_pairs = collect_pairs(single);
        REQUIRE(batched_pairs == single_pairs);

        // Every returned row actually carries the probed key value.
        for (auto& pr : single_pairs) {
            REQUIRE(pr.first == probe_keys[i]);
        }
    }
}
