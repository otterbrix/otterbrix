#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

// Segments now size to the row group (was a flat 256 KiB), which could push more strings into
// overflow and cost text-heavy tables more; this bounds table-bytes-per-payload-byte three ways
// so a layout regression fails directly. Hidden by default ([.]); run with [textstorage].

namespace {
    uint64_t directory_bytes(const std::filesystem::path& root) {
        std::error_code ec;
        if (!std::filesystem::exists(root, ec)) {
            return 0;
        }
        uint64_t total = 0;
        for (std::filesystem::recursive_directory_iterator it(root, ec), end; it != end; it.increment(ec)) {
            if (ec) {
                break;
            }
            if (it->is_regular_file(ec)) {
                total += it->file_size(ec);
            }
        }
        return total;
    }

    // Summing the whole fixture root was rejected: on the short-value load (payload 2,560,000 B)
    // the root held 63,766,648 B (24.9x), of which the table was 25,440,256 B (9.9x), pg_catalog
    // btrees 35,037,184 B (13.7x, a fixed bootstrap cost), and WAL 3,289,088 B (1.3x, varies with
    // checkpoint timing) -- neither is text-column layout, and together they buried it.
    uint64_t user_table_bytes(const std::filesystem::path& disk_path) {
        std::error_code ec;
        uint64_t total = 0;
        for (std::filesystem::directory_iterator db(disk_path, ec), end; !ec && db != end; db.increment(ec)) {
            std::error_code entry_ec;
            if (!db->is_directory(entry_ec)) {
                continue;
            }
            const std::string name = db->path().filename().string();
            char* tail = nullptr;
            const unsigned long oid = std::strtoul(name.c_str(), &tail, 10);
            if (tail == name.c_str() || *tail != '\0' || oid < components::catalog::FIRST_USER_OID) {
                continue;
            }
            for (std::filesystem::directory_iterator table(db->path(), entry_ec), table_end;
                 !entry_ec && table != table_end;
                 table.increment(entry_ec)) {
                // Only table directories: their sibling files are the database's WAL segments.
                if (table->is_directory(entry_ec)) {
                    total += directory_bytes(table->path());
                }
            }
        }
        return total;
    }

    struct measurement_t {
        uint64_t payload_bytes{0};
        uint64_t table_bytes{0};
        uint64_t root_bytes{0};
        bool failed{false};
        std::string error;
    };

    measurement_t
    load_text_table(const std::filesystem::path& root, int rows, int value_length, int every_nth_big, int big_length) {
        measurement_t out;
        auto config = test_create_config(root);
        test_clear_directory(config);
        config.wal.on = true;
        config.log.level = log_t::level::off;

        const std::string small(static_cast<size_t>(value_length), 'x');
        const std::string big(static_cast<size_t>(big_length), 'y');
        auto expected_payload = [&](int id) -> const std::string& {
            const bool is_big = every_nth_big > 0 && (id % every_nth_big) == 0;
            return is_big ? big : small;
        };

        {
            test_spaces space(config);
            auto* d = space.dispatcher();
            auto exec = [&](const std::string& sql) {
                auto session = otterbrix::session_id_t();
                return d->execute_sql(session, sql);
            };

            REQUIRE(exec("CREATE DATABASE t;")->is_success());
            REQUIRE(exec("CREATE TABLE t.wide (id bigint, payload text);")->is_success());

            constexpr int kBatch = 50;
            for (int base = 0; base < rows; base += kBatch) {
                std::string sql = "INSERT INTO t.wide (id, payload) VALUES ";
                for (int i = 0; i < kBatch && base + i < rows; ++i) {
                    const int id = base + i;
                    const std::string& value = expected_payload(id);
                    if (i != 0) {
                        sql += ", ";
                    }
                    sql += "(" + std::to_string(id) + ", '" + value + "')";
                    out.payload_bytes += value.size();
                }
                sql += ";";
                auto session = otterbrix::session_id_t();
                auto cur = d->execute_sql(session, sql);
                if (cur->is_error()) {
                    out.failed = true;
                    out.error = cur->get_error().what;
                    return out;
                }
            }
            // CHECKPOINT so what is on disk is the settled layout, not whatever happened to be flushed.
            REQUIRE(exec("CHECKPOINT;")->is_success());
            out.table_bytes = user_table_bytes(config.disk.path);
            out.root_bytes = directory_bytes(root);
        }

        // Verifies the tighter layout didn't drop or garble bytes (which would pass the bound for the
        // wrong reason); covers dictionary-segment edges and, in the mixed shape, big-string markers.
        {
            test_spaces reopened(config);
            auto* d = reopened.dispatcher();
            const int probes[] = {0, 1, rows / 2, rows - 1};
            for (const int id : probes) {
                auto session = otterbrix::session_id_t();
                auto cur =
                    d->execute_sql(session, "SELECT payload FROM t.wide WHERE id = " + std::to_string(id) + ";");
                INFO("readback of row " << id << " after reopen");
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 1);
                const auto cell = cur->value(0, 0);
                REQUIRE(cell.value<std::string_view>() == expected_payload(id));
            }
        }
        return out;
    }
} // namespace

TEST_CASE("integration::cpp::test_text_column_storage::amplification_stays_bounded", "[.][textstorage]") {
    struct case_t {
        const char* name;
        int rows;
        int value_length;
        int every_nth_big;
        int big_length;
        double max_amplification;
    };

    // Three shapes: short (inlines), near a 16 KiB segment's capacity (stresses dictionary slack),
    // and mixed with occasional huge values forcing overflow blocks.
    //
    //   short 64 B      9.73x measured  — a short text value still costs about ten times itself
    //   inline 4090 B   1.74x measured  — was 2.36x (9,635 B/row) while checkpoint persisted every
    //                                     16 KiB segment at full allocation; segments now persist
    //                                     trimmed (compact_string_dictionary), bringing the live
    //                                     layout to 1.04x -- the rest of 1.74x is a superseded
    //                                     shadow-paging generation kept as free blocks (87 of 217,
    //                                     reusable but never truncated). The 2.2 bound is two
    //                                     checkpoint generations of ~1.05 each plus margin, not
    //                                     tuned to the metric.
    //   mixed           3.02x measured
    const case_t cases[] = {
        {"short values (64 B)", 40000, 64, 0, 0, 10.5},
        {"large inline values (4090 B)", 8000, 4090, 0, 0, 2.2},
        {"mixed: 200 B with every 100th at 8192 B", 40000, 200, 100, 8192, 3.5},
    };

    for (const auto& c : cases) {
        const std::filesystem::path root =
            integration_fixture_path("test_text_storage") / std::to_string(c.value_length);
        const auto m = load_text_table(root, c.rows, c.value_length, c.every_nth_big, c.big_length);

        INFO(c.name);
        REQUIRE_FALSE(m.failed);
        REQUIRE(m.payload_bytes > 0);
        REQUIRE(m.table_bytes > 0);
        const double amplification = static_cast<double>(m.table_bytes) / static_cast<double>(m.payload_bytes);
        WARN(c.name << ": payload " << (m.payload_bytes / 1024) << " KiB, table " << (m.table_bytes / 1024)
                    << " KiB, amplification " << amplification << "x (whole fixture root "
                    << (m.root_bytes / 1024) << " KiB)");

        CHECK(amplification < c.max_amplification);
    }
}
