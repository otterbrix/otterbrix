// ============================================================================
// A CREATE INDEX WHOSE WAL CATCHUP REFUSED MUST LEAVE NOTHING THE PLANNER CAN USE.
//
// operator_create_index_backfill_t registers the index engine with manager_index_t
// (create_index) BEFORE it backfills it, and the engine's registration -- not
// pg_index.indisvalid -- is what create_plan_match consults: get_indexed_keys reports the
// key, can_use_index says yes, and full_scan is replaced by index_scan.
//
// So every exit AFTER that registration has to take the engine back out. The executor does
// not do it for us: all three undo_create_index calls sit inside the
// `needs_ddl_txn && cursor->is_success()` block (executor.cpp:1968), which covers failures
// AFTER the operator succeeded -- accumulate, commit, inline index-commit. A failure of the
// OPERATOR ITSELF lands in the `else if (... is_error())` branch, which calls only
// revert_failed_txn: that reverts the catalog appends and the PENDING index entries, and
// leaves the engine REGISTERED AND EMPTY.
//
// The result is not a slow query, it is a WRONG ANSWER: the next equality predicate on the
// indexed column is planned as an index_scan over an engine whose entries were just
// reverted, and the table answers with nothing.
//
// THE INJECTION. The catchup's refusal is produced deterministically by the WAL's own
// DEV_MODE seam (services/wal/wal_page.hpp): a segment file that will not open makes
// wal_page_reader_t::read_all_records refuse, which makes wal_worker_t::load refuse, which
// is exactly the branch operator_create_index_backfill.cpp takes on a refused catchup. It is
// armed only around the CREATE INDEX, so the seeding traffic above it is untouched.
//
// This is the CONTENT-level witness for the wal_worker_t::load hole work: the unit proofs of
// the hole itself are in services/wal/tests/test_wal_load_hole.cpp, and they assert on id
// sets. This one asserts on the ROWS a table answers with afterwards.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/wal/wal_page.hpp>

#include <filesystem>
#include <memory>
#include <sstream>
#include <string>

using namespace test_helpers;

namespace {

    // Process-wide seam, scoped by this object and narrowed to WAL segment files by path.
    // Starts switched OFF: the seeding traffic must reach the journal, so that the only
    // thing the refusal can be about is the catchup read.
    class wal_open_refusal_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_open_refusal_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_open_refusal_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_open_refusal_t(const wal_open_refusal_t&) = delete;
        wal_open_refusal_t& operator=(const wal_open_refusal_t&) = delete;

        std::string refuse_open_marker; // segment files matching this do not open at all

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (!refuse_open_marker.empty() && path.string().find(refuse_open_marker) != std::string::npos) {
                return nullptr;
            }
            return inner;
        }
    };

    constexpr unsigned kRowCount = 40;
    constexpr int kGroups = 2;
    constexpr unsigned kInGroup0 = kRowCount / kGroups;

} // namespace

TEST_CASE("integration::cpp::create_index_catchup_refusal::a_refused_catchup_leaves_the_table_answering_in_full") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_create_index_catchup_refusal/refused",
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_open_refusal_t fault;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE CatchupDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE CatchupDb.t (id bigint, grp int, val bigint);")->is_success());
    {
        auto cur = seed_rows(dispatcher, "CatchupDb.t", "id, grp, val", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % kGroups) << ", " << (i * 10) << ")";
            return s.str();
        });
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // The full answer, established BEFORE any index exists, so the comparison below is
    // against the table itself and not against an expectation.
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0);
    }

    // Arm only now.
    fault.refuse_open_marker = "wal_";

    auto create = exec(dispatcher, "CREATE INDEX idx_grp ON CatchupDb.t (grp);");
    INFO("a CREATE INDEX whose WAL catchup could not be read must FAIL rather than publish "
         "an index built from whatever the journal happened to hand back");
    REQUIRE(create->is_error());

    // Disarm: everything below is about the state the failed statement left behind, not
    // about the journal.
    fault.refuse_open_marker.clear();

    INFO("the refusal the statement reported: " << create->get_error().what.c_str());

    // THE TABLE FIRST. A failed CREATE INDEX must not have touched a single base row.
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t;");
        INFO("unfiltered SELECT after the failed CREATE INDEX: "
             << (cur->is_error() ? cur->get_error().what.c_str() : "no error") << " , rows "
             << (cur->is_error() ? 0 : cur->size()) << " , expected " << kRowCount);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // THE ANSWER, NOT THE STATUS. The index does not exist as far as the user is concerned,
    // so the table must answer exactly as it did before the failed statement.
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        INFO("SELECT after the failed CREATE INDEX: "
             << (cur->is_error() ? cur->get_error().what.c_str() : "no error") << " , rows "
             << (cur->is_error() ? 0 : cur->size()) << " , expected " << kInGroup0);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM CatchupDb.t WHERE grp = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / kGroups));
    }

    // And the statement must be retryable: nothing of the failed build may be left claiming
    // the name or the table.
    {
        auto retry = exec(dispatcher, "CREATE INDEX idx_grp ON CatchupDb.t (grp);");
        INFO("retry after the journal recovered: "
             << (retry->is_error() ? retry->get_error().what.c_str() : "no error"));
        REQUIRE(retry->is_success());
    }
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        INFO("SELECT through the rebuilt index");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0);
    }
}
