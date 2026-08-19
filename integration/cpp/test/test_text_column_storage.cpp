#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// How much storage does a text column actually cost?
//
// Column segments used to be given a whole 256 KiB block regardless of how little they held; they
// are now sized to what a row group can contain (DEFAULT_VECTOR_CAPACITY * type_size). For a text
// column type_size is sizeof(std::string_view) = 16 bytes, so a segment went from 256 KiB to 16 KiB.
// The string BYTES do not live in that segment though — short values are inlined into the segment's
// buffer and long ones are pushed into separate overflow blocks. A much smaller segment could
// therefore push more values into overflow blocks and end up costing MORE, which would make the
// change a regression for text-heavy tables even while it is a large win for fixed-width ones.
//
// This measures the thing directly: bytes on disk per byte of payload, for three value sizes. It is
// a characterization test — it states a bound the storage layer must stay inside, so it fails
// whichever way the layout regresses.
//
// Hidden by default ([.]) because it writes hundreds of megabytes. Run it with [textstorage].

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

    struct measurement_t {
        uint64_t payload_bytes{0};
        uint64_t disk_bytes{0};
        bool failed{false};
        std::string error;
    };

    measurement_t load_text_table(const std::filesystem::path& root, int rows, int value_length, int every_nth_big,
                                  int big_length) {
        measurement_t out;
        auto config = test_create_config(root);
        test_clear_directory(config);
        config.disk.on = true;
        config.wal.on = true;
        config.log.level = log_t::level::off;
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE t;")->is_success());
        REQUIRE(exec("CREATE TABLE t.wide (id bigint, payload text);")->is_success());

        const std::string small(static_cast<size_t>(value_length), 'x');
        const std::string big(static_cast<size_t>(big_length), 'y');

        constexpr int kBatch = 50;
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO t.wide (id, payload) VALUES ";
            for (int i = 0; i < kBatch && base + i < rows; ++i) {
                const int id = base + i;
                const bool is_big = every_nth_big > 0 && (id % every_nth_big) == 0;
                const std::string& value = is_big ? big : small;
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
        out.disk_bytes = directory_bytes(root);
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

    // Three shapes: short values that inline comfortably, values near the point where a 16 KiB
    // segment can hold only a handful of them, and a mix where occasional huge values force
    // overflow blocks.
    //
    // The bounds are the MEASURED values with a little headroom, so this pins the layout rather than
    // guessing at it. Measured with the segment sized to the row group (current), against the same
    // load with the old whole-block sizing:
    //
    //   short 64 B      9.5x   (was 12.8x)
    //   inline 4090 B   1.85x  (was 2.29x)
    //   mixed           3.06x  (was 3.81x)
    //
    // So sizing the segment to the row group improved text columns too — the worry that it would
    // push more values into overflow blocks and cost more does not hold. What the numbers DO show is
    // a pre-existing inefficiency that has nothing to do with that change: a short text value costs
    // about nine times its own length on disk.
    const case_t cases[] = {
        {"short values (64 B)", 40000, 64, 0, 0, 10.5},
        {"large inline values (4090 B)", 8000, 4090, 0, 0, 2.2},
        {"mixed: 200 B with every 100th at 8192 B", 40000, 200, 100, 8192, 3.5},
    };

    for (const auto& c : cases) {
        const std::filesystem::path root =
            std::filesystem::path("/tmp/otterbrix/integration/test_text_storage") / std::to_string(c.value_length);
        const auto m = load_text_table(root, c.rows, c.value_length, c.every_nth_big, c.big_length);

        INFO(c.name);
        REQUIRE_FALSE(m.failed);
        REQUIRE(m.payload_bytes > 0);
        const double amplification = static_cast<double>(m.disk_bytes) / static_cast<double>(m.payload_bytes);
        WARN(c.name << ": payload " << (m.payload_bytes / 1024) << " KiB, on disk " << (m.disk_bytes / 1024)
                    << " KiB, amplification " << amplification << "x");

        CHECK(amplification < c.max_amplification);
    }
}
