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
    REQUIRE_FALSE(r.has_error());
    REQUIRE_FALSE(r.value().found);
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
    REQUIRE_FALSE(r.has_error());
    REQUIRE(r.value().found);
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
    REQUIRE_FALSE(rs.has_error());
    REQUIRE(rs.value().found);
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

// 18-22. THE REST OF THE FAMILY case 17 belongs to.
//
// storage_delete_rows got an error channel because "0 marks set" and "the delete never
// reached a storage" were the same reply. Every other data leg on this contract still
// answers a ROUTING REFUSAL with the shape of a LEGITIMATELY EMPTY TABLE: an empty chunk
// vector (storage_fetch), a zero-length append range (storage_append / storage_update),
// a drained cursor (storage_fetch_next_batch), an empty schema (storage_types) and a zero
// row count (storage_total_rows). Each of those shapes is also the correct answer to a
// real question about a real table, so no caller can tell the two apart — which is the
// whole defect, restated once per leg.
//
// EVERY CASE BELOW PAIRS THE TWO. The legitimate empty answer must stay a SUCCESS; only
// "this oid names no storage anywhere" becomes an error. A test that asserted the refusal
// alone would be satisfied by a leg that refuses everything.
namespace {
    using namespace disk_test_helpers;

    // A table with one BIGINT column `a`, its storage created, and `nrows` committed rows.
    // Returns its oid; `first_row_out` receives the first appended row id.
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

    // One BIGINT column named `a`, `nrows` rows valued 1..nrows, as a one-chunk batch.
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

// 18. storage_fetch: an empty chunk vector is what a point-fetch of rows this snapshot may
//     not see legitimately returns. It must NOT also be what "no agent has a storage for
//     this oid" returns.
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
                           /*limit=*/std::int64_t{-1});
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
                           /*limit=*/std::int64_t{-1});
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
                           /*limit=*/std::int64_t{-1});
        REQUIRE(r.has_error());
    }
}

// 19. storage_append: (start_row=0, count=0) is what appending an EMPTY batch legitimately
//     answers. It must not also be what "no agent owns this oid" answers — that reading is
//     what let an INSERT report success over rows that reached no storage.
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

// 20. storage_update: the twin of 19 on the mutation side. (0,0) is the honest answer to an
//     empty request and must stay one; a routing miss is an UPDATE that did not happen.
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

// 21. storage_fetch_next_batch: a cardinality-0 batch is the DRAINED SENTINEL — the honest
//     end of a real scan, and the honest whole answer for an empty table. An OPEN (cursor
//     id 0) against an oid no agent has a storage for replied with that same sentinel, so
//     every scan source read "this table is empty" from a scan that never started.
//
//     ADVANCING an unknown cursor stays drained on purpose: the drain path erases the
//     entry itself, so "I do not know this cursor" IS "that cursor is finished".
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

// 22. storage_types: an oid nothing owns answers with an EMPTY type list — the same list a
//     storage whose schema has not been adopted yet answers with. resolve_table maps every
//     live column onto that list by name; an empty one leaves every column's chunk_position
//     at -1, i.e. a schema that describes nothing, derived from a read that never happened.
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

// 23. storage_total_rows: 0 is the honest row count of an empty table and must stay one.
//     It is also what an oid nothing owns answers, so a count read that never reached a
//     storage is indistinguishable from a table that really holds nothing.
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

// 24. storage_fetch's `limit` — the contract half of the index-scan read cap, pinned here
//     where the visibility can be arranged exactly (the end-to-end half is
//     integration/cpp/test/test_index_scan_limit_cap.cpp).
//
//     THE CAP COUNTS ROWS THE FETCH PRODUCED, NEVER IDS IT WAS HANDED. Under SNAPSHOT the
//     fetch drops every row the asking transaction may not see, so the two counts differ by
//     exactly the rows the reader never receives — and a budget deducted from the id count
//     spends itself on those. That is why the cap cannot live above this call: the index
//     answers with a superset of ids and only this leg knows which of them became rows.
//
//     And it is a TRUNCATION, not a selection: the capped reply is the uncapped reply's
//     prefix, same rows in the same order.
TEST_CASE("services::disk::error::fetch_limit_counts_visible_rows_not_requested_ids") {
    fixture fx;
    // The hidden head is LONGER THAN ONE FETCH WINDOW (DEFAULT_VECTOR_CAPACITY == 1024) on
    // purpose: the first window then produces ZERO rows, so a budget deducted per window —
    // or per id — is spent before a single row has been handed back, and the cap has to
    // survive into the second window to answer at all.
    constexpr std::uint64_t kRows = 1500;
    constexpr std::uint64_t kHidden = 1100;
    int64_t first_row = 0;
    const auto table_oid = make_one_column_table(fx, "nslimit", kRows, first_row);

    // Hide the head from txn 88 by deleting it UNDER txn 88: a transaction does not see its
    // own uncommitted delete, and every other reader still does.
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
                           limit);
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

// 25. THE ROUTER'S OWN REFUSAL, one floor above the agent's. Every case above reaches the
//     leg through a real agent that turns out to own no storage for the oid. This one
//     removes the agent: a manager configured with NO disk agents has nowhere to send
//     anything, and each leg used to answer that with its own natural empty value — an
//     empty type list, 0 rows, an empty chunk vector, a zero-length append range, a drained
//     cursor, an empty fold.
//
//     THIS TOPOLOGY IS NOT REACHED BY A STATEMENT TODAY, and the case does not pretend
//     otherwise: an agentless manager owns no storage at all, so no DML can find a row to
//     write and no scan a row to read — the same verdict the storage_delete_rows wave
//     recorded for its own routing legs. What it pins is that the refusal EXISTS and is
//     reachable through the public contract, so the day a topology can lose an agent slot
//     the answer is an error and not an empty table.
TEST_CASE("services::disk::error::a_manager_with_no_agents_refuses_instead_of_answering_empty") {
    // No bootstrap: with zero agents there are no system tables to seed, and seeding is not
    // what is under test.
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
                     std::int64_t{-1})
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
