// The CREATE INDEX build does not read the journal.
//
// It used to: after the snapshot scan, a WAL catchup replayed physical records to pick up
// concurrently committed rows, so an unreadable WAL segment had to FAIL the build (an empty
// load reply would have silently published an index missing every row that segment described).
// The build now feeds itself from a RAW read of every physical row plus the DML mirror /
// post-append reconciliation — the journal is not consulted, so a journal that cannot be
// opened is not the build's problem.
//
// The WAL-open refusal is produced deterministically by the WAL's own DEV_MODE seam
// (services/wal/wal_page.hpp): a segment file that will not open makes
// wal_page_reader_t::read_all_records refuse. It's armed only around the CREATE INDEX, so
// the seeding traffic above it is untouched — and since the current segment is already open,
// the build's own catalog writes keep landing.
//
// What this pins:
//   * the CREATE INDEX SUCCEEDS with the journal unopenable — the build reads no segment;
//   * the TABLE answers in full afterwards — the build touched no base row;
//   * the built index is the one doing the answering (Index Scan) and answers in full.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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
    // thing the arming can affect is the build's (absent) journal read.
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

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            out += std::string(cur->value(0, r).value<std::string_view>());
            out += '\n';
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::create_index_catchup_refusal::the_build_does_not_read_the_journal") {
    auto config = make_test_config(integration_fixture_path("test_create_index_catchup_refusal/refused"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_open_refusal_t fault;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE CatchupDb;")->is_success());
    // grp is bigint, not int: the Index Scan assertion below needs the literal's type to
    // match the key's, or the planner keeps the heap and the probe proves nothing.
    REQUIRE(exec(dispatcher, "CREATE TABLE CatchupDb.t (id bigint, grp bigint, val bigint);")->is_success());
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

    // Arm only now: from here no WAL segment file can be opened.
    fault.refuse_open_marker = "wal_";

    {
        auto create = exec(dispatcher, "CREATE INDEX idx_grp ON CatchupDb.t (grp);");
        INFO("a build that consulted the journal would refuse here; this one must not: "
             << (create->is_error() ? create->get_error().what.c_str() : "no error"));
        REQUIRE(create->is_success());
    }

    // Disarm: everything below is about the state the statement left behind.
    fault.refuse_open_marker.clear();

    // THE TABLE FIRST. The build must not have touched a single base row.
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t;");
        INFO("unfiltered SELECT after the build: " << (cur->is_error() ? cur->get_error().what.c_str() : "no error")
                                                   << " , rows " << (cur->is_error() ? 0 : cur->size())
                                                   << " , expected " << kRowCount);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    // THE ANSWER, THROUGH THE INDEX. Prove the index is the one reading, then that it
    // answers exactly what the table held before the build.
    {
        auto plan = exec(dispatcher, "EXPLAIN SELECT id FROM CatchupDb.t WHERE grp = 0;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan:\n" << text);
        INFO("a Seq Scan here would answer out of the heap and prove nothing about the build");
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        INFO("SELECT through the built index: " << (cur->is_error() ? cur->get_error().what.c_str() : "no error")
                                                << " , rows " << (cur->is_error() ? 0 : cur->size()) << " , expected "
                                                << kInGroup0);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM CatchupDb.t WHERE grp = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / kGroups));
    }

    // Rows written AFTER the build, with the journal healthy again, must reach the index too.
    REQUIRE(exec(dispatcher, "INSERT INTO CatchupDb.t (id, grp, val) VALUES (1000, 0, 10000);")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0 + 1);
    }
}
