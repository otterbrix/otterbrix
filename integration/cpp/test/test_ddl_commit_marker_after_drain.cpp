// ============================================================================
// THE DDL COMMIT MARKER MUST BE WRITTEN AFTER THE DRAIN, NOT BEFORE IT.
//
// Writing the WAL commit_txn marker (durable, wal_sync_mode::FULL) in
// operator_commit_transaction_t's DDL prefix — BEFORE the dispatcher drain that
// allocates the commit_id, and before the index insert-commit, the first of the two
// steps that may refuse the commit — is unsafe. Replay's first pass keys committed
// transactions off the MARKER's transaction_id (wal_reader.cpp pass 1), so a refusal
// anywhere after that prefix leaves a durable marker for a commit the live process
// rejected and reported as an error, and a RESTART resurrects every physical record of
// that transaction.
//
// The observable that pins the order is the marker's commit_id: it is allocated by the
// drain and by nothing else, so a marker carrying it can only have been written after
// the drain, while a marker emitted above the drain can only stamp 0 ("commit_id isn't
// allocated yet ... so pass 0"). The failure signature is every DDL statement's commit
// marker carrying commit_id == 0 while DML markers (always emitted below the drain)
// carry a real id.
//
// The journal is read through the same standalone reader bootstrap replays with, against
// the live space — the markers are behind FULL fsyncs, so they are on the device before
// their statements report success.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/wal/wal_reader.hpp>

#include <cstddef>
#include <filesystem>
#include <memory_resource>
#include <string>
#include <unistd.h>

using namespace test_helpers;

TEST_CASE("integration::cpp::ddl_commit_marker::carries_the_drained_commit_id") {
    auto config =
        make_test_config(std::filesystem::path("/tmp/otterbrix/integration/test_ddl_commit_marker_after_drain") /
                             std::to_string(::getpid()),
                         /*wal_on=*/true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Two DDL commits (each writes its marker through the DDL-commit mode of
    // operator_commit_transaction_t) and one DML commit as the contrast case.
    REQUIRE(exec(dispatcher, "CREATE DATABASE markers;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE markers.t (id bigint, v bigint);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO markers.t (id, v) VALUES (1, 10), (2, 20);")->is_success());

    std::pmr::synchronized_pool_resource pool;
    log_t quiet; // null logger: every log macro null-checks through should_log
    services::wal::wal_reader_t reader(&pool, config.wal, quiet);
    auto records = reader.read_committed_records(services::wal::id_t{0});
    REQUIRE_FALSE(records.has_error());

    std::size_t markers_total = 0;
    std::size_t markers_without_commit_id = 0;
    for (const auto& r : records.value()) {
        if (r.is_commit_marker() && r.transaction_id != 0) {
            ++markers_total;
            if (r.commit_id == 0) {
                ++markers_without_commit_id;
            }
        }
    }

    // The three statements above commit through three markers at least (DDL
    // txn markers + the autocommit DML marker).
    REQUIRE(markers_total >= 3);

    INFO("commit markers found: " << markers_total << ", of them carrying commit_id == 0: "
                                  << markers_without_commit_id);
    INFO("a marker with commit_id == 0 was written BEFORE the drain that allocates the id — i.e. "
         "before the first step that may still refuse the commit");
    CHECK(markers_without_commit_id == 0);
}
