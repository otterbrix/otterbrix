#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/table/test/fault_injection_file.hpp>
#include <services/wal/wal_binary.hpp>
#include <services/wal/wal_page_reader.hpp>

#include <filesystem>
#include <sstream>
#include <string>

// An INSERT / UPDATE past the part budget is journaled as a group of part records; replay joins each group into
// the operation it was, so the rows, and the row ids later records name, come back as they were written.

namespace {

    constexpr int ROWS = 3000;

    // The part budget for this scope; small enough that every chunk is a part of its own.
    class part_budget_scope_t final {
    public:
        explicit part_budget_scope_t(std::size_t bytes) { services::wal::dev_set_wal_part_payload_bytes(bytes); }
        ~part_budget_scope_t() { services::wal::dev_set_wal_part_payload_bytes(0); }
        part_budget_scope_t(const part_budget_scope_t&) = delete;
        part_budget_scope_t& operator=(const part_budget_scope_t&) = delete;
    };

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = dispatcher->execute_sql(otterbrix::session_id_t(), sql);
        INFO("SQL: " << sql.substr(0, 120));
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what : "none"));
        REQUIRE(cursor->is_success());
    }

    // The part records the journal under \p wal_root holds, read before any reader joins them.
    std::size_t journaled_parts(const std::filesystem::path& wal_root) {
        std::size_t parts = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(wal_root)) {
            if (!entry.is_regular_file() || entry.path().filename().string().rfind("wal_", 0) != 0) {
                continue;
            }
            services::wal::wal_page_reader_t reader(std::pmr::get_default_resource(), entry.path());
            REQUIRE(reader.is_open());
            auto records = reader.read_all_records(services::wal::id_t{0});
            REQUIRE_FALSE(records.has_error());
            for (const auto& record : records.value()) {
                if (record.is_part()) {
                    ++parts;
                }
            }
        }
        return parts;
    }

} // namespace

TEST_CASE("integration::cpp::test_wal_operation_parts::replay") {
    auto config = test_create_config(integration_fixture_path("test_wal_operation_parts/replay"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    part_budget_scope_t budget(4096);

    {
        test_spaces space(config);
        run_sql(space.dispatcher(), "CREATE SCHEMA s;");
        run_sql(space.dispatcher(), "CREATE TABLE s.t (id bigint, v text);");
        run_sql(space.dispatcher(), "CHECKPOINT;");
    }

    {
        // .otbx writes fail from the end of this scope, so the shutdown checkpoint folds nothing: the rows below
        // exist only in the journal.
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t fault(plan);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        std::ostringstream insert;
        insert << "INSERT INTO s.t (id, v) VALUES ";
        for (int id = 0; id < ROWS; ++id) {
            insert << (id == 0 ? "" : ", ") << "(" << id << ", 'v-" << id << "')";
        }
        run_sql(dispatcher, insert.str() + ";");
        // 2000 rows over two chunks; the DELETE then names row ids that replay must place where the INSERT did.
        run_sql(dispatcher, "UPDATE s.t SET v = 'u' WHERE id % 3 <> 0;");
        run_sql(dispatcher, "DELETE FROM s.t WHERE id % 5 = 0;");
        plan.fail_writes_from = 1;
    }
    CHECK(journaled_parts(config.wal.path) >= 5);

    {
        test_spaces space(config);
        auto cursor = space.dispatcher()->execute_sql(otterbrix::session_id_t(), "SELECT id, v FROM s.t ORDER BY id;");
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what : "none"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == ROWS - ROWS / 5);
        std::size_t row = 0;
        for (int id = 0; id < ROWS; ++id) {
            if (id % 5 == 0) {
                continue;
            }
            INFO("id " << id);
            auto stored_id = cursor->get<int64_t>(0, row);
            REQUIRE_FALSE(stored_id.has_error());
            REQUIRE(stored_id.value() == id);
            auto stored_v = cursor->get<std::string_view>(1, row);
            REQUIRE_FALSE(stored_v.has_error());
            REQUIRE(std::string(stored_v.value()) == (id % 3 != 0 ? std::string("u") : "v-" + std::to_string(id)));
            ++row;
        }
    }
}
