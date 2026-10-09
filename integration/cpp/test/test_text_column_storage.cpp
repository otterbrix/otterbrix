#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>
#include <string>

// Segments now size to the row group (was a flat 256 KiB), which could push more strings into
// overflow and cost text-heavy tables more; this bounds table-bytes-per-payload-byte three ways
// so a layout regression fails directly. The reduced case runs under ctest; the full one is
// hidden ([.]) because it writes hundreds of megabytes -- run it with [textstorage].

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
                auto cur = d->execute_sql(session, "SELECT payload FROM t.wide WHERE id = " + std::to_string(id) + ";");
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

namespace {
    struct case_t {
        const char* name;
        int rows;
        int value_length;
        int every_nth_big;
        int big_length;
        double max_amplification;
    };

    // A checkpoint's metadata chain, free list and header cost the same 7 blocks whether the table
    // holds 316 or 2 696 segments (measured 1 839 104 bytes written per checkpoint on both), and
    // every CHECKPOINT of a changed table keeps the previous generation: a per-file cost, not a
    // per-payload-byte one, so it is allowed for outside the ratio (it is what kept the reduced
    // 4090-byte shape at 2.28x while the full one sits at 2.11x).
    constexpr uint64_t kCheckpointFixedBytes = 2 * 8 * 256 * 1024;

    // Three shapes: short (inlines), near a 16 KiB segment's capacity (stresses dictionary slack),
    // and mixed with occasional huge values forcing overflow blocks. The bounds are two checkpoint
    // generations (compact rewrites a changed table on every CHECKPOINT) of a tightly packed
    // layout plus margin, not tuned to the metric; the packer-per-append layout this replaced
    // measured 28.7x / 2.56x / 14.8x on the full shapes.
    template<size_t N>
    void check_amplification(const case_t (&cases)[N], const char* fixture) {
        for (const auto& c : cases) {
            const std::filesystem::path root = integration_fixture_path(fixture) / std::to_string(c.value_length);
            const auto m = load_text_table(root, c.rows, c.value_length, c.every_nth_big, c.big_length);

            INFO(c.name);
            REQUIRE_FALSE(m.failed);
            REQUIRE(m.payload_bytes > 0);
            REQUIRE(m.table_bytes > 0);
            const double amplification = static_cast<double>(m.table_bytes) / static_cast<double>(m.payload_bytes);
            const double bound_bytes =
                c.max_amplification * static_cast<double>(m.payload_bytes) + static_cast<double>(kCheckpointFixedBytes);
            INFO(c.name << ": payload " << (m.payload_bytes / 1024) << " KiB, table " << (m.table_bytes / 1024)
                        << " KiB, amplification " << amplification << "x, bound " << c.max_amplification
                        << "x + fixed = " << (static_cast<uint64_t>(bound_bytes) / 1024) << " KiB (whole fixture root "
                        << (m.root_bytes / 1024) << " KiB)");

            CHECK(static_cast<double>(m.table_bytes) < bound_bytes);
        }
    }
} // namespace

// The layout does not depend on scale (the same blocks-per-row shape at 4 096 and 40 000 rows), so
// this reduced run is the ctest guard (~4 s, ~50 MB). Measured: 6.68x / 2.28x / 3.17x raw, of which
// the fixed checkpoint cost is 8.2x / 0.5x / 1.9x.
TEST_CASE("integration::cpp::test_text_column_storage::amplification_stays_bounded_reduced", "[textstorage]") {
    const case_t cases[] = {
        {"short values (64 B)", 8000, 64, 0, 0, 10.5},
        {"large inline values (4090 B)", 2000, 4090, 0, 0, 2.2},
        {"mixed: 200 B with every 100th at 8192 B", 8000, 200, 100, 8192, 3.5},
    };
    check_amplification(cases, "test_text_storage_reduced");
}

// Measured with the shared append packer: 3.18x / 2.11x / 2.32x.
TEST_CASE("integration::cpp::test_text_column_storage::amplification_stays_bounded", "[.][textstorage]") {
    const case_t cases[] = {
        {"short values (64 B)", 40000, 64, 0, 0, 10.5},
        {"large inline values (4090 B)", 8000, 4090, 0, 0, 2.2},
        {"mixed: 200 B with every 100th at 8192 B", 40000, 200, 100, 8192, 3.5},
    };
    check_amplification(cases, "test_text_storage");
}
