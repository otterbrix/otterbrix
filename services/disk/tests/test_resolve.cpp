#include <catch2/catch_test_macros.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    // storage_append takes the whole chunk batch; wrap a single chunk for the
    // one-shot test call site.
    std::pmr::vector<components::vector::data_chunk_t>
    to_batch(std::pmr::memory_resource* resource, std::unique_ptr<components::vector::data_chunk_t> chunk) {
        std::pmr::vector<components::vector::data_chunk_t> batch(resource);
        if (chunk) {
            batch.emplace_back(std::move(*chunk));
        }
        return batch;
    }

    std::string resolve_dir() {
        static std::string p = "/tmp/test_otterbrix_resolve_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(resolve_dir()); }

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
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
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
    auto rr = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("public"));
    REQUIRE_FALSE(rr.has_error());
    auto& r = rr.value();
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::public_namespace);
}

// 2. resolve_namespace misses on unknown name.
TEST_CASE("services::disk::resolve::namespace_misses_unknown") {
    fixture fx;
    auto rr =
        fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"));
    REQUIRE_FALSE(rr.has_error());
    REQUIRE_FALSE(rr.value().found);
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

// 5. resolve_type finds the bootstrap "int8" type in pg_catalog. Type names count bytes,
// so the 8-byte integer is "int8" — int64_type is its OID constant, which counts bits.
TEST_CASE("services::disk::resolve::type_finds_bootstrap") {
    fixture fx;
    auto r = test_probe::probe_type(fx, fx.ctx(), well_known_oid::pg_catalog_namespace, std::string("int8"));
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
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "rbk_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    // Regular storage with an explicit {k, payload} schema. The rows
    // appended below give:
    //   k=10 -> one row (payload 100)
    //   k=20 -> two rows (payload 200, 201)  [multi-row match]
    //   k=30 -> one row (payload 300)
    //   k=99 -> no row                        [no-match]
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("payload", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
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
            chunk->set_value(0, r, kvals[r]);
            chunk->set_value(1, r, pvals[r]);
        }
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto append_r =
            fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, to_batch(&fx.resource, std::move(chunk)));
        REQUIRE_FALSE(append_r.has_error());
        auto [start, count] = append_r.value();
        REQUIRE(count == nrows);
        (void) start;
    }

    // The N distinct key values we probe (one no-match: 99).
    const std::vector<std::int64_t> probe_keys = {10, 20, 30, 99};
    const std::size_t N = probe_keys.size();

    auto total_rows = [](const auto& chunks) {
        std::uint64_t t = 0;
        for (const auto& c : chunks) t += c.size();
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
            keys.set_value(0, i, probe_keys[i]);
        }
        keys.set_cardinality(N);
        // Key column as a storage ORDINAL: "k" is column 0 of this test's {k, payload} schema.
        std::pmr::vector<std::uint64_t> key_cols{&fx.resource};
        key_cols.emplace_back(0);
        auto res = disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_keys,
                                                        fx.ctx(),
                                                        table_oid,
                                                        std::move(key_cols),
                                                        std::move(keys),
                                                        std::pmr::vector<std::uint64_t>{&fx.resource}));
        REQUIRE(res.size() == N);
        // Copy into a std::vector for re-use in the parity loop (chunk-by-chunk
        // size/value comparison below).
        for (auto& entry : res) {
            std::vector<data_chunk_t> e;
            for (auto& c : entry) e.push_back(std::move(c));
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
        std::pmr::vector<std::uint64_t> single_key_cols{&fx.resource};
        single_key_cols.emplace_back(0);
        std::pmr::vector<logical_value_t> single_vals{&fx.resource};
        single_vals.emplace_back(&fx.resource, probe_keys[i]);
        auto single =
            disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_key,
                                                 fx.ctx(),
                                                 table_oid,
                                                 std::move(single_key_cols),
                                                 test_probe::build_key_chunk(&fx.resource, std::move(single_vals)),
                                                 std::pmr::vector<std::uint64_t>{&fx.resource}));

        // Same total row count per key (the no-match key yields 0 on both paths).
        std::uint64_t single_total = total_rows(single);
        std::uint64_t batched_total = 0;
        for (auto& c : batched[i]) batched_total += c.size();
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

// 8. A keyed read that CANNOT BE PERFORMED must not be reported as "no rows".
//
// A bare vector with no error slot collapses every failure of a keyed catalog read — an
// unknown key column, a key-arity mismatch, and (the dangerous one) a scan_local
// io_error/data_corruption from a failed block pin — into "one empty entry per key",
// indistinguishable from a legitimate miss: operator_resolve_table
// read that as "Database does not exist", operator_resolve_constraint built FK metadata
// out of it. Test 7 above pins the other half: key 99, which genuinely matches nothing,
// yields an EMPTY entry — empty already means "not found".
//
// The injected failure here is an unknown key column: resolve_key_col_indices cannot map
// it, so no scan runs at all. No failpoint exists in this layer for forcing a real
// io_error, and this branch reaches the same error path.
TEST_CASE("services::disk::resolve::unperformable_keyed_read_is_an_error") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_err");
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "err_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
    }

    // A column ordinal this table does not have: the read is impossible, not empty.
    std::pmr::vector<std::uint64_t> bad_cols{&fx.resource};
    bad_cols.emplace_back(99);
    std::pmr::vector<logical_value_t> vals{&fx.resource};
    vals.emplace_back(&fx.resource, std::int64_t{10});
    auto res = fx.invoke(&manager_disk_t::read_chunks_by_key,
                         fx.ctx(),
                         table_oid,
                         std::move(bad_cols),
                         test_probe::build_key_chunk(&fx.resource, std::move(vals)),
                         std::pmr::vector<std::uint64_t>{&fx.resource});

    INFO("an unperformable keyed read must carry an error, not an empty result");
    REQUIRE(res.has_error());

    // And the batched entry point must behave identically — same failure, same channel.
    std::pmr::vector<std::uint64_t> bad_cols_b{&fx.resource};
    bad_cols_b.emplace_back(99);
    std::pmr::vector<logical_value_t> vals_b{&fx.resource};
    vals_b.emplace_back(&fx.resource, std::int64_t{10});
    auto res_b = fx.invoke(&manager_disk_t::read_chunks_by_keys,
                           fx.ctx(),
                           table_oid,
                           std::move(bad_cols_b),
                           test_probe::build_key_chunk(&fx.resource, std::move(vals_b)),
                           std::pmr::vector<std::uint64_t>{&fx.resource});
    REQUIRE(res_b.has_error());
}

// 9. A projected keyed read returns the same values in the columns it asked for.
//
// Projection is supplied by the CALLER, and the columns it leaves out come back as
// ordinal-stable placeholders rather than being removed — that is what lets a consumer keep
// addressing column 3 as column 3. The failure mode is therefore silent: project too narrowly
// and the consumer reads an empty placeholder where a value used to be, with no error anywhere.
TEST_CASE("services::disk::resolve::projected_read_matches_full_read") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::types::logical_value_t;

    fixture fx;
    auto ns_oid = disk_test_helpers::test_create_namespace(fx, "ns_proj");
    auto table_oid = disk_test_helpers::test_create_table(fx,
                                                          ns_oid,
                                                          "proj_tbl",
                                                          std::vector<components::table::column_definition_t>{},
                                                          catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);
    {
        std::vector<components::table::column_definition_t> scols;
        scols.emplace_back("k", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("a", complex_logical_type{logical_type::BIGINT});
        scols.emplace_back("b", complex_logical_type{logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  well_known_oid::main_database,
                  std::move(scols),
                  /*is_computed=*/false);
    }
    {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        for (auto n : {"k", "a", "b"}) {
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias(n);
            types.push_back(std::move(t));
        }
        auto chunk = std::make_unique<components::vector::data_chunk_t>(&fx.resource, types, 1);
        chunk->set_cardinality(1);
        chunk->set_value(0, 0, std::int64_t{7});
        chunk->set_value(1, 0, std::int64_t{70});
        chunk->set_value(2, 0, std::int64_t{700});
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto append_r =
            fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, to_batch(&fx.resource, std::move(chunk)));
        REQUIRE_FALSE(append_r.has_error());
    }

    auto read = [&](std::pmr::vector<std::uint64_t> projection) {
        std::pmr::vector<std::uint64_t> key_cols{&fx.resource};
        key_cols.emplace_back(0); // "k"
        std::pmr::vector<logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, std::int64_t{7});
        return disk_test_helpers::read_ok(fx.invoke(&manager_disk_t::read_chunks_by_key,
                                                    fx.ctx(),
                                                    table_oid,
                                                    std::move(key_cols),
                                                    test_probe::build_key_chunk(&fx.resource, std::move(vals)),
                                                    std::move(projection)));
    };

    auto full = read(std::pmr::vector<std::uint64_t>{&fx.resource});
    std::pmr::vector<std::uint64_t> only_b{&fx.resource};
    only_b.emplace_back(2); // "b"
    auto projected = read(std::move(only_b));

    REQUIRE(full.size() == 1);
    REQUIRE(projected.size() == 1);
    REQUIRE(full[0].size() == 1);
    REQUIRE(projected[0].size() == 1);
    // Same ordinals on both paths, and the projected column carries the same value.
    REQUIRE(projected[0].column_count() == full[0].column_count());
    REQUIRE(projected[0].value(2, 0).value<std::int64_t>() == full[0].value(2, 0).value<std::int64_t>());
    // The key column survives projection: the filter needs it, so the agent keeps it.
    REQUIRE(projected[0].value(0, 0).value<std::int64_t>() == std::int64_t{7});
}

// --- THE CATALOG-READ FUNNEL ------------------------------------------------------------
//
// manager_disk_t::scan_table is the single door every catalog read goes through
// (manager_disk_resolve.cpp) — namespace resolve, function resolve, cast lookup,
// namespace enumeration. Three of its legs can fail to perform the read at all: no agents, no
// owning agent, and a scan that came back with an error. NONE of them may answer with an EMPTY
// batch list, because an empty batch list is also what "there are no matching rows" looks like,
// and a caller handed one reads a failed read as a negative answer. The two cases below are the
// two shapes of that lie, each pinned against CONTENT that is provably on disk.
namespace {

    // The T3 interposer seam is process-wide and this fixture opens one .otbx per catalog
    // table, so filter by path: every handle whose path does not carry the marker is returned
    // unwrapped, i.e. not interposed at all. Same shape as the scope in
    // services/disk/tests/test_persistence.cpp and integration/cpp/test/test_catalog_write_refusal.cpp.
    class one_table_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        one_table_fault_scope_t(otterbrix_test::fault_plan_t& plan, std::string path_marker)
            : plan_(plan)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~one_table_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
        std::string marker_;
    };

    // A manager that can be torn down and reopened over the same directory — the fixture above
    // wipes its directory in the constructor, so it cannot express a restart.
    struct reopenable_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit reopenable_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~reopenable_disk() {
            // Destroy the manager first: its dtor joins the internal loop thread, which may
            // still enqueue children onto the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
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

        void checkpoint(services::wal::id_t wal_id) {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::checkpoint_all,
                                                       session_id_t{},
                                                       wal_id,
                                                       std::numeric_limits<uint64_t>::max());
            for (int i = 0; i < 100000 && !cf.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            // Bind the [[nodiscard]] reply and state something true of it: the sealed WAL
            // floor is the oldest root any table could still fall back to, so it can never
            // run ahead of the id this round was told the WAL had reached.
            auto sealed = std::move(cf).take_ready();
            REQUIRE(sealed <= wal_id);
        }
    };

} // namespace

// CASE 1. A pg_proc scan that CANNOT BE PERFORMED must not be reported as "no such function".
//
// Phase 1 lays down a clean database and CHECKPOINTS it, so the five builtin pg_proc rows
// live in the FILE and not merely in this process's memory. Phase 2 reopens it with the fault
// seam installed and the plan still CLEAN — the load reads only the header and the metadata
// chain, so it succeeds and pg_proc's handle comes back wrapped. Only then is the handle
// poisoned: from that point every read of pg_proc's data blocks fails, which is exactly a
// buffer-pool refill that cannot reach the platter.
TEST_CASE("services::disk::resolve::a_failed_catalog_scan_is_not_no_rows") {
    const auto dir = std::filesystem::path(resolve_dir() + "_readfail");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    const auto marker = "/" + std::to_string(static_cast<unsigned>(well_known_oid::pg_proc_table)) + "/";

    {
        reopenable_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        fd.checkpoint(services::wal::id_t{100});
    }

    otterbrix_test::fault_plan_t plan;
    one_table_fault_scope_t scope(plan, marker);

    reopenable_disk fd2(dir);
    fd2.manager->bootstrap_system_tables_sync();

    plan.crashed = true;
    auto poisoned =
        fd2.invoke(&manager_disk_t::resolve_function_by_name, fd2.ctx(), std::string("count"));
    INFO("a pg_proc scan that failed must not answer 'there is no function named count'");
    // Before the funnel carried an error channel this was an EMPTY vector, indistinguishable
    // from the honest negative answer — while the row was on the platter.
    REQUIRE(poisoned.has_error());

    // The proof that the emptiness was a LIE and not a fact about the catalog: clear the
    // poison and the same call over the same file answers with the row.
    plan.crashed = false;
    auto healthy =
        fd2.invoke(&manager_disk_t::resolve_function_by_name, fd2.ctx(), std::string("count"));
    REQUIRE_FALSE(healthy.has_error());
    REQUIRE(healthy.value().size() == 1);
    CHECK(healthy.value()[0].found);
    CHECK(healthy.value()[0].name == "count");

    std::filesystem::remove_all(dir);
}

// CASE 2. The same lie, one floor down and through a different reader: agent_disk_t::scan_local's
// "this agent does not own the oid" leg. A manager that never bootstrapped owns no pg_cast, so
// find_cast_oid's scan cannot run at all — and INVALID_OID is the value the caller
// (operator_unregister_cast, operator_register_cast.cpp) reads as "there is no such cast
// row", which is a statement about the catalog that nobody was in a position to make.
TEST_CASE("services::disk::resolve::an_unowned_catalog_scan_is_not_no_rows") {
    const auto dir = std::filesystem::path(resolve_dir() + "_unowned");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    reopenable_disk fd(dir); // deliberately NOT bootstrapped: no system storage exists

    auto cast_oid = fd.invoke(&manager_disk_t::find_cast_oid,
                              fd.ctx(),
                              components::catalog::oid_t{well_known_oid::pg_type_table},
                              components::catalog::oid_t{well_known_oid::pg_class_table});
    INFO("a pg_cast scan that could not be performed must not answer 'no such cast'");
    // Before scan_local refused by type this was INVALID_OID, i.e. "the catalog says no".
    REQUIRE(cast_oid.has_error());
    CHECK(cast_oid.error().type == core::error_code_t::missing_table);

    std::filesystem::remove_all(dir);
}
