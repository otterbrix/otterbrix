// End-to-end SQL benchmark: latency, p95, QPS, RSS/CPU over N/D/K and filters.

#include "integration/cpp/test/test_config.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <vector>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <vector_search/distance_metrics.hpp>

using namespace components;

namespace {

    static const database_name_t db_name = "testdatabase";
    static const collection_name_t coll_name = "testcollection";

    std::string vector_to_string(const std::vector<double>& vec) {
        std::stringstream ss;
        ss << "ARRAY[";
        ss << std::setprecision(8);
        for (std::size_t i = 0; i < vec.size(); ++i) {
            ss << vec[i] << (i + 1 == vec.size() ? "]" : ", ");
        }
        return ss.str();
    }

    std::vector<double> generate_random_vector(std::size_t dim, std::mt19937& rng) {
        std::uniform_real_distribution<double> dist(-1.0, 1.0);
        std::vector<double> data(dim);
        for (auto& v : data) v = dist(rng);
        return data;
    }

    // Vector columns require a typed schema (DOUBLE[dim]); schemaless (relkind='g')
    // collections reject ARRAY inserts. Create the typed table explicitly.
    void create_typed_collection(otterbrix::wrapper_dispatcher_t* dispatcher,
                                 components::session::session_id_t session,
                                 std::size_t dim) {
        std::stringstream q;
        q << "CREATE TABLE " << db_name << "." << coll_name
          << " (id BIGINT, category BIGINT, embedding DOUBLE[" << dim << "]);";
        auto cur = dispatcher->execute_sql(session, q.str());
        REQUIRE(cur->is_success());
    }

    void fill_database(otterbrix::wrapper_dispatcher_t* dispatcher,
                       components::session::session_id_t session,
                       std::size_t count,
                       std::size_t dim) {
        std::mt19937 rng(42);
        for (std::size_t i = 0; i < count; i += 100) {
            std::stringstream query;
            query << "INSERT INTO " << db_name << "." << coll_name
                  << " (id, category, embedding) VALUES ";
            std::size_t batch_size = std::min<std::size_t>(100, count - i);
            for (std::size_t j = 0; j < batch_size; ++j) {
                auto vec = generate_random_vector(dim, rng);
                int category = static_cast<int>(rng() % 100) + 1;
                query << "(" << (i + j) << ", " << category << ", " << vector_to_string(vec) << ")";
                if (j + 1 < batch_size) query << ", ";
            }
            query << ";";
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
        }
    }

    struct latency_stats_t {
        double avg_ms = 0.0;
        double p50_ms = 0.0;
        double p95_ms = 0.0;
        double p99_ms = 0.0;
        double min_ms = 0.0;
        double max_ms = 0.0;
        std::size_t samples = 0;
    };

    latency_stats_t summarize(std::vector<double> samples_ms) {
        latency_stats_t s;
        if (samples_ms.empty()) return s;
        std::sort(samples_ms.begin(), samples_ms.end());
        s.samples = samples_ms.size();
        s.min_ms = samples_ms.front();
        s.max_ms = samples_ms.back();
        double sum = 0.0;
        for (double v : samples_ms) sum += v;
        s.avg_ms = sum / static_cast<double>(samples_ms.size());
        auto pct = [&](double p) {
            std::size_t idx = static_cast<std::size_t>(std::ceil(p * samples_ms.size())) - 1;
            if (idx >= samples_ms.size()) idx = samples_ms.size() - 1;
            return samples_ms[idx];
        };
        s.p50_ms = pct(0.50);
        s.p95_ms = pct(0.95);
        s.p99_ms = pct(0.99);
        return s;
    }

    struct rusage_snapshot_t {
        long max_rss_kb = 0;       // peak resident set size, kilobytes
        double user_cpu_sec = 0.0; // user CPU time, seconds
        double sys_cpu_sec = 0.0;  // system CPU time, seconds
    };

    rusage_snapshot_t take_rusage() {
        struct rusage r;
        getrusage(RUSAGE_SELF, &r);
        rusage_snapshot_t s;
#ifdef __APPLE__
        // macOS reports ru_maxrss in bytes; Linux reports in kilobytes.
        s.max_rss_kb = r.ru_maxrss / 1024;
#else
        s.max_rss_kb = r.ru_maxrss;
#endif
        s.user_cpu_sec = r.ru_utime.tv_sec + r.ru_utime.tv_usec / 1e6;
        s.sys_cpu_sec = r.ru_stime.tv_sec + r.ru_stime.tv_usec / 1e6;
        return s;
    }

    void print_header(const std::string& title) {
        std::cout << "\n" << std::string(72, '=') << "\n";
        std::cout << title << "\n";
        std::cout << std::string(72, '=') << "\n";
    }

    void print_stats_row(const std::string& label,
                         const latency_stats_t& s,
                         double qps,
                         long rss_delta_kb,
                         double user_cpu_delta_sec,
                         double sys_cpu_delta_sec) {
        std::cout << std::left << std::setw(28) << label << " | " << std::fixed << std::setprecision(3)
                  << "avg=" << std::setw(8) << s.avg_ms << "ms"
                  << "  p50=" << std::setw(8) << s.p50_ms << "ms"
                  << "  p95=" << std::setw(8) << s.p95_ms << "ms"
                  << "  p99=" << std::setw(8) << s.p99_ms << "ms"
                  << "  qps=" << std::setw(8) << std::setprecision(1) << qps
                  << "  rss+=" << std::setw(6) << rss_delta_kb << "KB"
                  << "  cpu(u/s)=" << std::setprecision(3) << user_cpu_delta_sec << "/"
                  << sys_cpu_delta_sec << "s\n";
    }

    // Run a query N times; report latency, QPS and resource deltas.
    void benchmark_query(otterbrix::wrapper_dispatcher_t* dispatcher,
                         components::session::session_id_t session,
                         const std::string& label,
                         const std::string& query,
                         std::size_t iterations) {
        for (std::size_t i = 0; i < 3; ++i) { // warmup
            auto cur = dispatcher->execute_sql(session, query);
            (void) cur;
        }

        std::vector<double> samples_ms;
        samples_ms.reserve(iterations);

        auto rusage_before = take_rusage();
        auto window_start = std::chrono::high_resolution_clock::now();

        for (std::size_t i = 0; i < iterations; ++i) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto cur = dispatcher->execute_sql(session, query);
            auto t1 = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> dt = t1 - t0;
            samples_ms.push_back(dt.count());
            (void) cur;
        }

        auto window_end = std::chrono::high_resolution_clock::now();
        auto rusage_after = take_rusage();

        auto stats = summarize(samples_ms);
        std::chrono::duration<double> window = window_end - window_start;
        double qps = static_cast<double>(iterations) / window.count();
        long rss_delta = rusage_after.max_rss_kb - rusage_before.max_rss_kb;
        double user_delta = rusage_after.user_cpu_sec - rusage_before.user_cpu_sec;
        double sys_delta = rusage_after.sys_cpu_sec - rusage_before.sys_cpu_sec;

        print_stats_row(label, stats, qps, rss_delta, user_delta, sys_delta);
    }

    // Like benchmark_query but builds the plan directly (filter strategy has no SQL surface).
    void benchmark_vector_plan(otterbrix::wrapper_dispatcher_t* dispatcher,
                               components::session::session_id_t session,
                               const std::string& label,
                               const std::vector<double>& query_vec,
                               std::size_t k,
                               int category_threshold, // <0 — no filter
                               components::vector_search::filter_strategy strategy,
                               std::size_t iterations) {
        auto run_once = [&]() {
            components::expressions::compare_expression_ptr filter;
            auto params_node = components::logical_plan::make_parameter_node(dispatcher->resource());
            if (category_threshold >= 0) {
                filter = components::expressions::make_compare_expression(
                    dispatcher->resource(),
                    components::expressions::compare_type::gte,
                    components::expressions::key_t{dispatcher->resource(), "category"},
                    core::parameter_id_t{0});
                params_node->add_parameter(core::parameter_id_t{0}, category_threshold);
            }
            auto node =
                components::logical_plan::make_node_vector_search(dispatcher->resource(),
                                                                  core::dbname_t{db_name},
                                                                  core::relname_t{coll_name},
                                                                  "embedding",
                                                                  query_vec,
                                                                  k,
                                                                  components::vector_search::metric_type::l2,
                                                                  filter,
                                                                  strategy);
            return dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), std::move(node), std::move(params_node)});
        };

        for (std::size_t i = 0; i < 3; ++i) {
            auto cur = run_once();
            (void) cur;
        }

        std::vector<double> samples_ms;
        samples_ms.reserve(iterations);

        auto rusage_before = take_rusage();
        auto window_start = std::chrono::high_resolution_clock::now();

        for (std::size_t i = 0; i < iterations; ++i) {
            auto t0 = std::chrono::high_resolution_clock::now();
            auto cur = run_once();
            auto t1 = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> dt = t1 - t0;
            samples_ms.push_back(dt.count());
            (void) cur;
        }

        auto window_end = std::chrono::high_resolution_clock::now();
        auto rusage_after = take_rusage();

        auto stats = summarize(samples_ms);
        std::chrono::duration<double> window = window_end - window_start;
        double qps = static_cast<double>(iterations) / window.count();
        long rss_delta = rusage_after.max_rss_kb - rusage_before.max_rss_kb;
        double user_delta = rusage_after.user_cpu_sec - rusage_before.user_cpu_sec;
        double sys_delta = rusage_after.sys_cpu_sec - rusage_before.sys_cpu_sec;

        print_stats_row(label, stats, qps, rss_delta, user_delta, sys_delta);
    }

} // namespace

// Scenario 1: scalability over N.
TEST_CASE("vector_search_bench::scalability_over_N") {
    const std::vector<std::size_t> row_counts = {
        200,    400,    700,    1000,   1400,   2000,   2700,   3600,
        4800,   6300,   8200,   10500,  13500,  17000,  21500,  27000,
        33500,  41000,  50000,  60000,  72000,  85000,  100000
    };
    const std::size_t dim = 128;
    const std::size_t k = 10;
    const std::size_t iterations = 30;

    std::mt19937 rng(1337);
    auto query_vec = generate_random_vector(dim, rng);
    std::string q_str = vector_to_string(query_vec);

    for (std::size_t N : row_counts) {
        print_header("Scalability: N=" + std::to_string(N) + " D=" + std::to_string(dim) +
                     " K=" + std::to_string(k));

        auto config = test_create_config("/tmp/custom_bench_vs/N_" + std::to_string(N));
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto session = components::session::session_id_t();

        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(db_name) + ";");
        create_typed_collection(dispatcher, session, dim);
        fill_database(dispatcher, session, N, dim);

        std::string base_q = "SELECT * FROM " + db_name + "." + coll_name + " ORDER BY id LIMIT " +
                             std::to_string(k) + ";";
        std::string vs_q = "SELECT * FROM " + db_name + "." + coll_name +
                           " ORDER BY embedding <-> " + q_str + " LIMIT " +
                           std::to_string(k) + ";";

        benchmark_query(dispatcher, session, "Baseline (ORDER BY id)", base_q, iterations);
        benchmark_query(dispatcher, session, "Vector Search (L2)", vs_q, iterations);
    }
}

// Scenario 2: scalability over D.
TEST_CASE("vector_search_bench::scalability_over_D") {
    const std::vector<std::size_t> dims = {
        4,   8,   12,  16,  24,  32,  48,  64,  80,   96,   128,  160,
        192, 224, 256, 320, 384, 448, 512, 640, 768,  896,  1024, 1280,
        1536, 1792, 2048
    };
    const std::size_t N = 3000;
    const std::size_t k = 10;
    const std::size_t iterations = 30;

    for (std::size_t dim : dims) {
        print_header("Scalability: N=" + std::to_string(N) + " D=" + std::to_string(dim) +
                     " K=" + std::to_string(k));

        std::mt19937 rng(7);
        auto query_vec = generate_random_vector(dim, rng);
        std::string q_str = vector_to_string(query_vec);

        auto config = test_create_config("/tmp/custom_bench_vs/D_" + std::to_string(dim));
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto session = components::session::session_id_t();

        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(db_name) + ";");
        create_typed_collection(dispatcher, session, dim);
        fill_database(dispatcher, session, N, dim);

        std::string vs_q = "SELECT * FROM " + db_name + "." + coll_name +
                           " ORDER BY embedding <-> " + q_str + " LIMIT " +
                           std::to_string(k) + ";";
        benchmark_query(dispatcher, session, "Vector Search D=" + std::to_string(dim), vs_q, iterations);
    }
}

// Scenario 3: scalability over K.
TEST_CASE("vector_search_bench::scalability_over_K") {
    const std::vector<std::size_t> ks = {
        1,    2,    3,    5,    7,    10,   15,   20,   30,   45,   65,   90,
        125,  175,  250,  350,  500,  700,  1000, 1400, 1800, 2300, 2800
    };
    const std::size_t N = 3000;
    const std::size_t dim = 128;
    const std::size_t iterations = 30;

    std::mt19937 rng(11);
    auto query_vec = generate_random_vector(dim, rng);
    std::string q_str = vector_to_string(query_vec);

    print_header("Scalability over K: N=" + std::to_string(N) + " D=" + std::to_string(dim));

    auto config = test_create_config("/tmp/custom_bench_vs/K");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto session = components::session::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(db_name) + ";");
    create_typed_collection(dispatcher, session, dim);
    fill_database(dispatcher, session, N, dim);

    for (std::size_t k : ks) {
        std::string vs_q = "SELECT * FROM " + db_name + "." + coll_name +
                           " ORDER BY embedding <-> " + q_str + " LIMIT " +
                           std::to_string(k) + ";";
        benchmark_query(dispatcher, session, "K=" + std::to_string(k), vs_q, iterations);
    }
}

// Scenario 4: filter strategy (pre vs post).
TEST_CASE("vector_search_bench::filter_strategy_comparison") {
    const std::size_t N = 15000;
    const std::size_t dim = 128;
    const std::size_t k = 10;
    const std::size_t iterations = 30;

    std::mt19937 rng(99);
    auto query_vec = generate_random_vector(dim, rng);

    auto config = test_create_config("/tmp/custom_bench_vs/strategy");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto session = components::session::session_id_t();

    dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(db_name) + ";");
    create_typed_collection(dispatcher, session, dim);
    fill_database(dispatcher, session, N, dim);

    struct sel_t {
        const char* label;
        int threshold; // category >= threshold; <0 — no filter
    };
    const std::vector<sel_t> selectivities = {
        {"~100%", -1}, {"~98%", 2},  {"~95%", 5},  {"~92%", 8},  {"~88%", 12}, {"~83%", 17}, {"~78%", 22},
        {"~72%", 28},  {"~66%", 34}, {"~60%", 40}, {"~54%", 46}, {"~48%", 52}, {"~42%", 58}, {"~36%", 64},
        {"~30%", 70},  {"~25%", 75}, {"~20%", 80}, {"~16%", 84}, {"~12%", 88}, {"~9%", 91},  {"~7%", 93},
        {"~5%", 95},   {"~3%", 97},  {"~2%", 98},  {"~1%", 99},
    };

    for (const auto& sel : selectivities) {
        print_header("Filter strategy comparison @ selectivity " + std::string(sel.label));

        benchmark_vector_plan(dispatcher,
                              session,
                              "pre_filter",
                              query_vec,
                              k,
                              sel.threshold,
                              components::vector_search::filter_strategy::pre_filter,
                              iterations);
        benchmark_vector_plan(dispatcher,
                              session,
                              "post_filter",
                              query_vec,
                              k,
                              sel.threshold,
                              components::vector_search::filter_strategy::post_filter,
                              iterations);
    }
}
