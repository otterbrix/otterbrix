#include "test_config.hpp"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

namespace {
    std::string make_oid(int x) {
        auto s = std::to_string(x);
        if (s.size() < 24) {
            s = std::string(24 - s.size(), '0') + s;
        }
        return s;
    }

    void ensure_ok(const components::cursor::cursor_t_ptr& cur, const std::string& msg) {
        if (!cur->is_success()) {
            const auto err = cur->get_error();
            throw std::runtime_error("SQL failed: " + msg + ", code=" + std::to_string(static_cast<int>(err.type)) +
                                     ", what=" + std::string(err.what.c_str()));
        }
    }

    std::size_t exec_size(otterbrix::wrapper_dispatcher_t* dispatcher,
                          const otterbrix::session_id_t& session,
                          const std::string& sql) {
        auto cur = dispatcher->execute_sql(session, sql);
        ensure_ok(cur, sql);
        return cur->size();
    }

    std::uint64_t exec_count(otterbrix::wrapper_dispatcher_t* dispatcher,
                             const otterbrix::session_id_t& session,
                             const std::string& sql) {
        auto cur = dispatcher->execute_sql(session, sql);
        ensure_ok(cur, sql);
        if (cur->size() != 1) {
            throw std::runtime_error("unexpected cursor size for count query: " + std::to_string(cur->size()));
        }
        const auto v = cur->chunk_data().value(0, 0);
        const auto t = v.type().type();
        if (t == components::types::logical_type::UBIGINT) {
            return v.value<std::uint64_t>();
        }
        if (t == components::types::logical_type::BIGINT) {
            const auto x = v.value<std::int64_t>();
            if (x < 0) {
                throw std::runtime_error("negative COUNT(*) result");
            }
            return static_cast<std::uint64_t>(x);
        }
        throw std::runtime_error("unexpected COUNT(*) type");
    }

    void create_schema(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session) {
        exec_size(dispatcher, session, "CREATE DATABASE bench;");
        exec_size(dispatcher, session, "CREATE TABLE bench.l();");
        exec_size(dispatcher, session, "CREATE TABLE bench.r();");
    }

    void load_data(otterbrix::wrapper_dispatcher_t* dispatcher,
                   const otterbrix::session_id_t& session,
                   int rows_left,
                   int fanout_right) {

        constexpr int kBatch = 1000;

        {
            std::size_t total_inserted = 0;
            for (int base = 0; base < rows_left; base += kBatch) {
                const int limit = std::min(rows_left, base + kBatch);
                std::stringstream q;
                q << "INSERT INTO bench.l (_id, k, payload_l) VALUES ";
                for (int i = base; i < limit; ++i) {
                    q << "('" << make_oid(i + 1) << "', " << i << ", 'l" << i << "')"
                      << (i + 1 == limit ? ";" : ", ");
                }
                total_inserted += exec_size(dispatcher, session, q.str());
            }
            if (total_inserted != static_cast<std::size_t>(rows_left)) {
                throw std::runtime_error("unexpected inserted rows to l");
            }
        }

        {
            std::size_t total_inserted = 0;
            int rid = 1;
            for (int base = 0; base < rows_left; base += kBatch) {
                const int limit = std::min(rows_left, base + kBatch);
                std::stringstream q;
                q << "INSERT INTO bench.r (_id, k, payload_r) VALUES ";
                bool first = true;
                for (int i = base; i < limit; ++i) {
                    // exactly one matching row for k=i (keeps JOIN output bounded)
                    if (!first) {
                        q << ", ";
                    }
                    first = false;
                    int idn = 10'000'000 + rid++;
                    q << "('" << make_oid(idn) << "', " << i << ", 'r" << i << "_m')";

                    // additional non-matching rows inflate right table size
                    for (int j = 1; j < fanout_right; ++j) {
                        q << ", ";
                        idn = 10'000'000 + rid++;
                        const int non_match_key = rows_left * j + i;
                        q << "('" << make_oid(idn) << "', " << non_match_key << ", 'r" << i << "_x" << j << "')";
                    }
                }
                q << ";";
                total_inserted += exec_size(dispatcher, session, q.str());
            }
            if (total_inserted != static_cast<std::size_t>(rows_left * fanout_right)) {
                throw std::runtime_error("unexpected inserted rows to r");
            }
        }
    }

    void create_hash_index_on_right(otterbrix::wrapper_dispatcher_t* dispatcher,
                                    const otterbrix::session_id_t& session,
                                    const std::string& index_name) {
        exec_size(dispatcher, session, "CREATE INDEX " + index_name + " ON bench.r USING hash (k);");
    }

    void bootstrap_right_schema(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session) {
        exec_size(dispatcher,
                  session,
                  "INSERT INTO bench.r (_id, k, payload_r) VALUES ('000000000000000000009999', -1, 'bootstrap');");
    }

    struct stats_t {
        double min_ms{};
        double median_ms{};
        double avg_ms{};
        double max_ms{};
        std::size_t result_rows{};
    };

    stats_t run_case(const std::string& name, bool create_hash_index, int rows_left, int fanout_right, int repeats) {
        const std::filesystem::path base_path = std::filesystem::path("/tmp/test_join") / ("profile_index_join_" + name);
        auto config = test_create_config(base_path);
        config.main_path = base_path;
        config.wal.path = base_path / "wal";
        config.disk.path = base_path / "disk";
        config.log.path = base_path / "log";
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        config.log.level = log_t::level::off;
        std::cerr << "storage_path=" << config.main_path.string() << std::endl;

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto session = otterbrix::session_id_t();
        std::cout << "create_schema\n";
        create_schema(dispatcher, session);
        std::cout << "bootstrap_right_schema\n";
        bootstrap_right_schema(dispatcher, session);
        if (create_hash_index) {
            std::cout << "create_hash_index_on_right\n";

            create_hash_index_on_right(dispatcher, session, "idx_r_k_hash_" + name);
        }
        load_data(dispatcher, session, rows_left, fanout_right);

        std::cout << "warmup\n";
        const std::string join_sql = "SELECT COUNT(*) FROM bench.l l INNER JOIN bench.r r ON l.k = r.k;";
        const auto expected_count = static_cast<std::uint64_t>(rows_left);
        auto warmup_count = exec_count(dispatcher, session, join_sql);
        if (warmup_count != expected_count) {
            throw std::runtime_error("unexpected join COUNT(*) in warmup");
        }

        std::cout << "exec\n";
        std::vector<double> ms;
        ms.reserve(static_cast<std::size_t>(repeats));
        for (int i = 0; i < repeats; ++i) {
            auto t0 = std::chrono::steady_clock::now();
            auto count = exec_count(dispatcher, session, join_sql);
            auto t1 = std::chrono::steady_clock::now();
            if (count != expected_count) {
                throw std::runtime_error("unexpected join COUNT(*) in measured run");
            }
            auto dur = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
            ms.push_back(static_cast<double>(dur) / 1000.0);
        }

        std::sort(ms.begin(), ms.end());
        stats_t out{};
        out.min_ms = ms.front();
        out.max_ms = ms.back();
        out.median_ms = ms[ms.size() / 2];
        out.avg_ms = std::accumulate(ms.begin(), ms.end(), 0.0) / static_cast<double>(ms.size());
        out.result_rows = static_cast<std::size_t>(warmup_count);
        return out;
    }
} // namespace

int main(int argc, char** argv) {
    int rows_left = 30'000;
    int fanout_right = 3;
    int repeats = 7;

    if (argc > 1) {
        rows_left = std::stoi(argv[1]);
    }

    if (argc > 2) {
        fanout_right = std::stoi(argv[2]);
    }
    if (argc > 3) {
        repeats = std::stoi(argv[3]);
    }

    std::cout << "profile_index_join\n";
    std::cout << "rows_left=" << rows_left << ", fanout_right=" << fanout_right << ", repeats=" << repeats << "\n\n";

    std::cout << "hash_stats\n";

    const auto hash_stats = run_case("hash", true, rows_left, fanout_right, repeats);
    std::cout << "no_idx_stats\n";

    const auto no_idx_stats = run_case("no_index", false, rows_left, fanout_right, repeats);

    auto print_stats = [](const char* label, const stats_t& s) {
        std::cout << std::left << std::setw(14) << label << " rows=" << s.result_rows << " min=" << std::fixed
                  << std::setprecision(2) << s.min_ms << "ms median=" << s.median_ms << "ms avg=" << s.avg_ms
                  << "ms max=" << s.max_ms << "ms\n";
    };

    print_stats("hash_index", hash_stats);
    print_stats("no_index", no_idx_stats);

    const auto ratio = no_idx_stats.median_ms / std::max(1.0, hash_stats.median_ms);
    std::cout << "\nmedian(no_index)/median(hash_index) = " << std::fixed << std::setprecision(2) << ratio << "x\n";
    return 0;
}
