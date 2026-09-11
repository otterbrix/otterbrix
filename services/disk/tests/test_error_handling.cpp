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
            // The manager's dtor joins its loop thread, which may still enqueue onto the scheduler; destroy it first.
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

TEST_CASE("services::disk::error::resolve_unknown_namespace") {
    fixture fx;
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"));
    REQUIRE_FALSE(r.has_error());
    REQUIRE_FALSE(r.value().found);
}

TEST_CASE("services::disk::error::resolve_unknown_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rt = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("not_a_table"));
    REQUIRE_FALSE(rt.found);
}

TEST_CASE("services::disk::error::resolve_table_invalid_namespace") {
    fixture fx;
    auto rt = test_probe::probe_table(fx, fx.ctx(), INVALID_OID, std::string("any"));
    REQUIRE_FALSE(rt.found);
}

// The write layer refuses the duplicate; the dispatcher's resolve snapshot can't see another txn's uncommitted row.
TEST_CASE("services::disk::error::duplicate_namespace_name_refused_at_write") {
    fixture fx;
    auto a = test_create_namespace(fx, "dup");
    REQUIRE(a >= FIRST_USER_OID);

    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    auto writes = catalog::build_create_namespace_writes(&fx.resource, std::string("dup"), oids[0]);
    REQUIRE(writes.size() == 1);
    auto second =
        fx.invoke(&manager_disk_t::append_pg_catalog_row, fx.ctx(), writes[0].table_oid, std::move(writes[0].row));
    REQUIRE(second.has_error());
    REQUIRE(second.error().type == core::error_code_t::database_already_exists);

    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("dup"));
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().found);
    REQUIRE(r.value().oid == a);
}

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

// PostgreSQL's 63-byte name limit isn't enforced here; arbitrary length is accepted.
TEST_CASE("services::disk::error::long_namespace_name_accepted") {
    fixture fx;
    std::string long_name(200, 'x');
    auto ns_oid = test_create_namespace(fx, long_name);
    REQUIRE(ns_oid >= FIRST_USER_OID);
    auto rs = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), long_name);
    REQUIRE_FALSE(rs.has_error());
    REQUIRE(rs.value().found);
}

TEST_CASE("services::disk::error::empty_name_accepted") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "");
    REQUIRE(ns_oid >= FIRST_USER_OID);
}

TEST_CASE("services::disk::error::resolve_unknown_function") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rf = test_probe::probe_function(fx, fx.ctx(), ns_oid, std::string("unknown_fn"));
    REQUIRE_FALSE(rf.found);
}
// A repeat delete legitimately reports 0 marks set; an oid with no storage must not produce that same zero.
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
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows,
                           txn_ctx(),
                           table_oid,
                           ids_of(first_row, 3),
                           std::uint64_t{3});
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == 3);
    }

    INFO("the same rows again: zero marks set, and that is a SUCCESS, not a refusal");
    {
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows,
                           txn_ctx(),
                           table_oid,
                           ids_of(first_row, 3),
                           std::uint64_t{3});
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

// Each case below pairs a routing miss (must error) with its empty-but-legitimate look-alike (must stay success).
namespace {
    using namespace disk_test_helpers;

    // first_row_out receives the first appended row id (0 when nrows == 0).
    template<typename Fx>
    catalog::oid_t make_one_column_table(Fx& fx, const std::string& ns, std::uint64_t nrows, int64_t& first_row_out) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        using components::vector::data_chunk_t;

        auto ns_oid = test_create_namespace(fx, ns);
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
        first_row_out = 0;
        if (nrows == 0) {
            return table_oid;
        }
        // Chunked at 1000 so a wide seed stays inside DEFAULT_VECTOR_CAPACITY; appends within
        // one txn are contiguous, so the first chunk's start_row is the whole range's start.
        constexpr std::uint64_t kChunk = 1000;
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        for (std::uint64_t done = 0; done < nrows; done += kChunk) {
            const std::uint64_t n = std::min<std::uint64_t>(kChunk, nrows - done);
            std::pmr::vector<complex_logical_type> types(&fx.resource);
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias("a");
            types.push_back(std::move(t));
            data_chunk_t chunk(&fx.resource, types, n);
            chunk.set_cardinality(n);
            for (uint64_t i = 0; i < n; ++i) {
                chunk.set_value(0, i, static_cast<std::int64_t>(done + i + 1));
            }
            std::pmr::vector<data_chunk_t> batch(&fx.resource);
            batch.emplace_back(std::move(chunk));
            auto appended = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(appended.has_error());
            REQUIRE(appended.value().second == n);
            if (done == 0) {
                first_row_out = static_cast<int64_t>(appended.value().first);
            }
        }
        return table_oid;
    }

    inline std::pmr::vector<components::vector::data_chunk_t> one_column_batch(std::pmr::memory_resource* r,
                                                                               std::uint64_t nrows) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        std::pmr::vector<complex_logical_type> types(r);
        complex_logical_type t{logical_type::BIGINT};
        t.set_alias("a");
        types.push_back(std::move(t));
        components::vector::data_chunk_t chunk(r, types, nrows == 0 ? std::uint64_t{1} : nrows);
        chunk.set_cardinality(nrows);
        for (uint64_t i = 0; i < nrows; ++i) {
            chunk.set_value(0, i, static_cast<std::int64_t>(i + 1));
        }
        std::pmr::vector<components::vector::data_chunk_t> batch(r);
        batch.emplace_back(std::move(chunk));
        return batch;
    }

    inline components::vector::vector_t ids_from(std::pmr::memory_resource* r, int64_t base, uint64_t n) {
        components::vector::vector_t v(r, components::types::logical_type::BIGINT, n == 0 ? uint64_t{1} : n);
        for (uint64_t i = 0; i < n; ++i) {
            v.data<int64_t>()[i] = base + static_cast<int64_t>(i);
        }
        return v;
    }
} // namespace

TEST_CASE("services::disk::error::fetch_refusal_is_not_an_empty_result") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nsfetch", 3, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    INFO("the fetch happened: three rows come back");
    {
        auto r = fx.invoke(&manager_disk_t::storage_fetch,
                           session_id_t{},
                           table_oid,
                           ids_from(&fx.resource, first_row, 3),
                           std::uint64_t{3},
                           std::vector<size_t>{},
                           with_open_snapshot(0, 0),
                           components::table::fetch_visibility_t::SNAPSHOT,
                           /*limit=*/std::int64_t{-1},
                           k_fetch_epoch_unchecked);
        REQUIRE_FALSE(r.has_error());
        std::uint64_t rows = 0;
        for (const auto& chunk : r.value()) {
            rows += chunk.size();
        }
        REQUIRE(rows == 3);
    }

    INFO("asking for no rows is not a refusal, whatever the oid");
    {
        auto r = fx.invoke(&manager_disk_t::storage_fetch,
                           session_id_t{},
                           nowhere,
                           ids_from(&fx.resource, 0, 0),
                           std::uint64_t{0},
                           std::vector<size_t>{},
                           with_open_snapshot(0, 0),
                           components::table::fetch_visibility_t::SNAPSHOT,
                           /*limit=*/std::int64_t{-1},
                           k_fetch_epoch_unchecked);
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().empty());
    }

    INFO("an oid with no storage anywhere: the fetch DID NOT HAPPEN, and says so");
    {
        auto r = fx.invoke(&manager_disk_t::storage_fetch,
                           session_id_t{},
                           nowhere,
                           ids_from(&fx.resource, 0, 1),
                           std::uint64_t{1},
                           std::vector<size_t>{},
                           with_open_snapshot(0, 0),
                           components::table::fetch_visibility_t::SNAPSHOT,
                           /*limit=*/std::int64_t{-1},
                           k_fetch_epoch_unchecked);
        REQUIRE(r.has_error());
    }
}

// Conflating (0,0)'s empty-batch meaning with "no agent owns this oid" let an INSERT report success over lost rows.
TEST_CASE("services::disk::error::append_refusal_is_not_a_zero_range") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nsapp", 0, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    components::execution_context_t append_ctx{session_id_t{},
                                               components::table::transaction_data{0, 0},
                                               {},
                                               table_oid};

    INFO("the append happened: two rows materialized");
    {
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, one_column_batch(&fx.resource, 2));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().second == 2);
    }

    INFO("appending no rows is not a refusal, whatever the oid");
    {
        components::execution_context_t nowhere_ctx{session_id_t{},
                                                    components::table::transaction_data{0, 0},
                                                    {},
                                                    nowhere};
        auto r = fx.invoke(&manager_disk_t::storage_append, nowhere_ctx, nowhere, one_column_batch(&fx.resource, 0));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().second == 0);
    }

    INFO("an oid with no storage anywhere: the append DID NOT HAPPEN, and says so");
    {
        components::execution_context_t nowhere_ctx{session_id_t{},
                                                    components::table::transaction_data{0, 0},
                                                    {},
                                                    nowhere};
        auto r = fx.invoke(&manager_disk_t::storage_append, nowhere_ctx, nowhere, one_column_batch(&fx.resource, 2));
        REQUIRE(r.has_error());
    }
}

TEST_CASE("services::disk::error::update_refusal_is_not_a_zero_range") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nsupd", 2, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    INFO("updating no rows is not a refusal, whatever the oid");
    {
        std::pmr::vector<components::vector::vector_t> ids(&fx.resource);
        ids.emplace_back(ids_from(&fx.resource, 0, 0));
        auto r = fx.invoke(&manager_disk_t::storage_update,
                           txn_ctx(),
                           nowhere,
                           std::move(ids),
                           one_column_batch(&fx.resource, 0));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().second == 0);
    }

    INFO("an oid with no storage anywhere: the update DID NOT HAPPEN, and says so");
    {
        std::pmr::vector<components::vector::vector_t> ids(&fx.resource);
        ids.emplace_back(ids_from(&fx.resource, first_row, 2));
        auto r = fx.invoke(&manager_disk_t::storage_update,
                           txn_ctx(),
                           nowhere,
                           std::move(ids),
                           one_column_batch(&fx.resource, 2));
        REQUIRE(r.has_error());
    }
}

// Advancing an unknown cursor stays drained on purpose: the drain path erases it, so "unknown" equals "finished".
TEST_CASE("services::disk::error::scan_open_refusal_is_not_a_drained_cursor") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nsscan", 0, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    INFO("an OPEN over a real but EMPTY table drains, and that is a SUCCESS");
    {
        auto r = fx.invoke(&manager_disk_t::storage_fetch_next_batch,
                           session_id_t{},
                           table_oid,
                           std::uint64_t{0},
                           std::unique_ptr<components::table::table_filter_t>(nullptr),
                           std::int64_t{-1},
                           std::vector<size_t>{},
                           with_open_snapshot(0, 0));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().batch);
        REQUIRE(r.value().batch->size() == 0);
    }

    INFO("an OPEN over an oid with no storage anywhere: the scan NEVER STARTED, and says so");
    {
        auto r = fx.invoke(&manager_disk_t::storage_fetch_next_batch,
                           session_id_t{},
                           nowhere,
                           std::uint64_t{0},
                           std::unique_ptr<components::table::table_filter_t>(nullptr),
                           std::int64_t{-1},
                           std::vector<size_t>{},
                           with_open_snapshot(0, 0));
        REQUIRE(r.has_error());
    }
}

// An empty type list leaves every column's chunk_position at -1 when resolve_table maps by name.
TEST_CASE("services::disk::error::storage_types_refusal_is_not_an_empty_schema") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nstypes", 0, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    INFO("a real storage answers with its schema");
    {
        auto r = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, table_oid);
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().size() == 1);
    }

    INFO("an oid with no storage anywhere: the schema read DID NOT HAPPEN, and says so");
    {
        auto r = fx.invoke(&manager_disk_t::storage_types, session_id_t{}, nowhere);
        REQUIRE(r.has_error());
    }
}

TEST_CASE("services::disk::error::total_rows_refusal_is_not_a_zero_count") {
    fixture fx;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nsrows", 0, first_row);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    INFO("a real EMPTY table counts zero rows, and that is a SUCCESS");
    {
        auto r = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid);
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == 0);
    }

    INFO("an oid with no storage anywhere: the count read DID NOT HAPPEN, and says so");
    {
        auto r = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, nowhere);
        REQUIRE(r.has_error());
    }
}

// Contract half of the index-scan read cap (end-to-end half: test_index_scan_limit_cap.cpp).
// Counts visible rows, not requested ids, and the capped reply is always the uncapped reply's prefix in order.
TEST_CASE("services::disk::error::fetch_limit_counts_visible_rows_not_requested_ids") {
    fixture fx;
    // Hidden head exceeds one fetch window (DEFAULT_VECTOR_CAPACITY == 1024) so the cap must survive into window two.
    constexpr std::uint64_t kRows = 1500;
    constexpr std::uint64_t kHidden = 1100;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nslimit", kRows, first_row);

    // Deleted under txn 88 itself, which then can't see its own uncommitted delete, unlike every other reader.
    {
        auto r = fx.invoke(&manager_disk_t::storage_delete_rows,
                           txn_ctx(),
                           table_oid,
                           ids_from(&fx.resource, first_row, kHidden),
                           kHidden);
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == kHidden);
    }

    auto fetch = [&](std::int64_t limit) {
        auto r = fx.invoke(&manager_disk_t::storage_fetch,
                           session_id_t{},
                           table_oid,
                           ids_from(&fx.resource, first_row, kRows),
                           kRows,
                           std::vector<size_t>{},
                           with_open_snapshot(88, 0),
                           components::table::fetch_visibility_t::SNAPSHOT,
                           limit,
                           k_fetch_epoch_unchecked);
        REQUIRE_FALSE(r.has_error());
        std::vector<std::int64_t> values;
        for (const auto& chunk : r.value()) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                values.push_back(chunk.get_value<std::int64_t>(0, i));
            }
        }
        return values;
    };

    INFO("uncapped: the rows this txn may still see, and only those");
    const auto uncapped = fetch(-1);
    REQUIRE(uncapped.size() == kRows - kHidden);

    INFO("a cap of 3 yields THREE VISIBLE rows — the first 1100 ids produced nothing at all");
    const auto capped = fetch(3);
    REQUIRE(capped.size() == 3);

    INFO("and they are the uncapped answer's first three, in order");
    for (std::size_t i = 0; i < capped.size(); ++i) {
        REQUIRE(capped[i] == uncapped[i]);
    }

    INFO("a cap wider than the visible set never binds");
    REQUIRE(fetch(static_cast<std::int64_t>(kRows) * 2) == uncapped);

    INFO("a cap of 0 asks for no rows and gets none — not a refusal");
    REQUIRE(fetch(0).empty());
}

// One floor above the per-agent refusal: with zero disk agents, every leg must still error, not answer empty.
TEST_CASE("services::disk::error::a_manager_with_no_agents_refuses_instead_of_answering_empty") {
    // No bootstrap: zero agents means no system tables to seed, and seeding isn't under test.
    core::pmr::otterbrix_resource resource;
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    auto* scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
    configuration::config_disk cfg;
    cfg.path = err_dir() + "/no_agents";
    cfg.agent = 0;
    std::filesystem::create_directories(cfg.path);
    {
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager(
            actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, cfg, log));

        auto call = [&](auto fn, auto&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::move(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        };

        const catalog::oid_t oid{FIRST_USER_OID};

        REQUIRE(call(&manager_disk_t::storage_types, session_id_t{}, oid).has_error());
        REQUIRE(call(&manager_disk_t::storage_total_rows, session_id_t{}, oid).has_error());
        REQUIRE(call(&manager_disk_t::storage_fetch,
                     session_id_t{},
                     oid,
                     ids_from(&resource, 0, 1),
                     std::uint64_t{1},
                     std::vector<size_t>{},
                     with_open_snapshot(0, 0),
                     components::table::fetch_visibility_t::SNAPSHOT,
                     std::int64_t{-1},
                     k_fetch_epoch_unchecked)
                    .has_error());
        REQUIRE(call(&manager_disk_t::storage_fetch_next_batch,
                     session_id_t{},
                     oid,
                     std::uint64_t{0},
                     std::unique_ptr<components::table::table_filter_t>(nullptr),
                     std::int64_t{-1},
                     std::vector<size_t>{},
                     with_open_snapshot(0, 0))
                    .has_error());
        {
            components::execution_context_t ctx{session_id_t{}, components::table::transaction_data{0, 0}, {}, oid};
            REQUIRE(call(&manager_disk_t::storage_append, ctx, oid, one_column_batch(&resource, 2)).has_error());
        }
        {
            std::pmr::vector<components::vector::vector_t> ids(&resource);
            ids.emplace_back(ids_from(&resource, 0, 2));
            REQUIRE(
                call(&manager_disk_t::storage_update, txn_ctx(), oid, std::move(ids), one_column_batch(&resource, 2))
                    .has_error());
        }
        REQUIRE(call(&manager_disk_t::storage_delete_rows, txn_ctx(), oid, ids_from(&resource, 0, 1), std::uint64_t{1})
                    .has_error());
    }
    scheduler->stop();
    delete scheduler;
    std::filesystem::remove_all(cfg.path);
}

// NOT NULL enforcement (storage_append_inner stage 2b) must refuse, not reuse append's (0,0) empty-batch reply.
TEST_CASE("services::disk::error::a_not_null_violation_is_a_refusal_not_an_empty_append") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::data_chunk_t;

    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_notnull");

    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", complex_logical_type{logical_type::BIGINT});
    cols.emplace_back("b", complex_logical_type{logical_type::BIGINT});
    cols[1].set_not_null(true);
    auto table_oid = test_create_table(fx, ns_oid, "t_notnull", cols);
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);

    auto two_col_chunk = [&](bool null_in_b) {
        std::pmr::vector<complex_logical_type> types(&fx.resource);
        complex_logical_type ta{logical_type::BIGINT};
        ta.set_alias("a");
        types.push_back(std::move(ta));
        complex_logical_type tb{logical_type::BIGINT};
        tb.set_alias("b");
        types.push_back(std::move(tb));
        data_chunk_t chunk(&fx.resource, types, 2);
        chunk.set_cardinality(2);
        for (uint64_t i = 0; i < 2; ++i) {
            chunk.set_value(0, i, static_cast<std::int64_t>(i + 1));
            chunk.set_value(1, i, static_cast<std::int64_t>(i + 10));
        }
        if (null_in_b) {
            chunk.data[1].validity().set_invalid(1);
        }
        std::pmr::vector<data_chunk_t> batch(&fx.resource);
        batch.emplace_back(std::move(chunk));
        return batch;
    };

    components::execution_context_t append_ctx{session_id_t{},
                                               components::table::transaction_data{0, 0},
                                               {},
                                               table_oid};

    INFO("a NULL in a NOT NULL column is a refusal, not a (0,0) success");
    {
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, two_col_chunk(true));
        REQUIRE(r.has_error());
    }

    INFO("nothing was materialized by the refused append");
    {
        auto total = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid);
        REQUIRE_FALSE(total.has_error());
        REQUIRE(total.value() == 0);
    }

    INFO("the channel is not over-broad: a valid batch still appends");
    {
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, two_col_chunk(false));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value().second == 2);
    }
}

// pool_idx_for_oid routes every range/oid to its owner, so a miss here is real, not a not-owned OID.
// These handlers return unique_future<void> (callers can only log), so this DEV tally is the only observable signal.
TEST_CASE("services::disk::error::a_publish_or_revert_that_finds_no_storage_says_so") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns_miss");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t_miss", cols);
    fx.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              cols,
              /*is_computed=*/false);
    const auto nowhere = static_cast<catalog::oid_t>(table_oid + 4242);

    services::disk::reset_publish_revert_misses();

    INFO("publish_commits for an oid with no storage anywhere is a miss");
    {
        std::vector<components::pg_catalog_append_range_t> ranges;
        ranges.push_back(components::pg_catalog_append_range_t{nowhere, 0, 3});
        fx.invoke(&manager_disk_t::storage_publish_commits, txn_ctx(), std::uint64_t{2000}, std::move(ranges));
        REQUIRE(services::disk::publish_revert_misses() == 1);
    }

    INFO("publish_deletes for it is a miss");
    {
        std::set<catalog::oid_t> tables{nowhere};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{2000}, std::move(tables));
        REQUIRE(services::disk::publish_revert_misses() == 2);
    }

    INFO("revert_deletes for it is a miss");
    {
        std::vector<catalog::oid_t> tables{nowhere};
        fx.invoke(&manager_disk_t::storage_revert_deletes, txn_ctx(), std::move(tables));
        REQUIRE(services::disk::publish_revert_misses() == 3);
    }

    INFO("revert_appends for it is a miss");
    {
        std::vector<components::pg_catalog_append_range_t> ranges;
        ranges.push_back(components::pg_catalog_append_range_t{nowhere, 0, 3});
        // An unowned oid is reported through publish_revert_misses and skipped, not refused, so the
        // handler itself must still answer no_error -- which is what the next line counts on.
        REQUIRE_FALSE(
            fx.invoke(&manager_disk_t::storage_revert_appends, txn_ctx(), std::move(ranges), false).contains_error());
        REQUIRE(services::disk::publish_revert_misses() == 4);
    }

    INFO("an oid WITH storage is no miss on any leg, and a zero-count range stays a no-op");
    {
        std::vector<components::pg_catalog_append_range_t> ranges;
        ranges.push_back(components::pg_catalog_append_range_t{table_oid, 0, 0});
        fx.invoke(&manager_disk_t::storage_publish_commits, txn_ctx(), std::uint64_t{2000}, std::move(ranges));
        std::set<catalog::oid_t> tables{table_oid};
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{2000}, std::move(tables));
        REQUIRE(services::disk::publish_revert_misses() == 4);
    }
}
