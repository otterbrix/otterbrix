// The build feeds itself from a RAW read of every physical row plus the DML mirror; the journal is never
// consulted, so an unopenable WAL segment does not touch it. The open refusal is produced by the WAL's own
// DEV_MODE seam (services/wal/wal_page.hpp).

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

    class wal_open_refusal_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_open_refusal_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_open_refusal_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_open_refusal_t(const wal_open_refusal_t&) = delete;
        wal_open_refusal_t& operator=(const wal_open_refusal_t&) = delete;

        std::string refuse_open_marker;

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

    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0);
    }

    fault.refuse_open_marker = "wal_";

    {
        auto create = exec(dispatcher, "CREATE INDEX idx_grp ON CatchupDb.t (grp);");
        INFO("a build that consulted the journal would refuse here; this one must not: "
             << (create->is_error() ? create->get_error().what.c_str() : "no error"));
        REQUIRE(create->is_success());
    }

    fault.refuse_open_marker.clear();

    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t;");
        INFO("unfiltered SELECT after the build: " << (cur->is_error() ? cur->get_error().what.c_str() : "no error")
                                                   << " , rows " << (cur->is_error() ? 0 : cur->size())
                                                   << " , expected " << kRowCount);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

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

    REQUIRE(exec(dispatcher, "INSERT INTO CatchupDb.t (id, grp, val) VALUES (1000, 0, 10000);")->is_success());
    {
        auto cur = exec(dispatcher, "SELECT id, grp, val FROM CatchupDb.t WHERE grp = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kInGroup0 + 1);
    }
}
