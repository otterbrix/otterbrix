// Dynamic-workload benchmarks: inserts, deletes, churn, rebuilds, recall@k.
// CSV lines (prefixed "CSV,") are emitted for plotting.

#include "integration/cpp/test/test_config.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>

#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>
#include <components/session/session.hpp>
#include <vector_search/distance_metrics.hpp>

using namespace components;

namespace {

    static const database_name_t db_name = "benchdb";
    static const collection_name_t coll_name = "vectors";

    using clock_t_ = std::chrono::high_resolution_clock;
    inline double ms_since(clock_t_::time_point t0) {
        return std::chrono::duration<double, std::milli>(clock_t_::now() - t0).count();
    }

    // Clustered generator — mimics real embeddings.
    struct vector_gen_t {
        std::size_t dim;
        std::vector<std::vector<double>> centers;
        std::normal_distribution<double> noise{0.0, 0.08};

        vector_gen_t(std::size_t d, std::size_t n_clusters, std::mt19937& rng)
            : dim(d) {
            std::uniform_real_distribution<double> u(-1.0, 1.0);
            centers.resize(n_clusters, std::vector<double>(d));
            for (auto& c : centers)
                for (auto& x : c) x = u(rng);
        }
        std::vector<double> sample(std::mt19937& rng) {
            const auto& c = centers[rng() % centers.size()];
            std::vector<double> v(dim);
            for (std::size_t i = 0; i < dim; ++i) v[i] = c[i] + noise(rng);
            return v;
        }
    };

    // std::fixed so the literal parser sees no INT elements.
    std::string vec_to_array(const std::vector<double>& v) {
        std::stringstream ss;
        ss << "ARRAY[" << std::fixed << std::setprecision(8);
        for (std::size_t i = 0; i < v.size(); ++i) ss << v[i] << (i + 1 == v.size() ? "]" : ", ");
        return ss.str();
    }

    std::string vec_to_literal(const std::vector<double>& v) {
        std::stringstream ss;
        ss << "'[" << std::fixed << std::setprecision(8);
        for (std::size_t i = 0; i < v.size(); ++i) ss << v[i] << (i + 1 == v.size() ? "]'" : ", ");
        return ss.str();
    }

    // ---- latency / resource summaries ----
    struct stats_t {
        double avg = 0, p50 = 0, p95 = 0, p99 = 0, max = 0;
        std::size_t n = 0;
    };
    stats_t summarize(std::vector<double> s) {
        stats_t r;
        if (s.empty()) return r;
        std::sort(s.begin(), s.end());
        r.n = s.size();
        r.max = s.back();
        double sum = 0;
        for (double v : s) sum += v;
        r.avg = sum / s.size();
        auto pct = [&](double p) {
            std::size_t i = static_cast<std::size_t>(std::ceil(p * s.size()));
            if (i > 0) --i;
            if (i >= s.size()) i = s.size() - 1;
            return s[i];
        };
        r.p50 = pct(0.50);
        r.p95 = pct(0.95);
        r.p99 = pct(0.99);
        return r;
    }

    long peak_rss_kb() {
        struct rusage r;
        getrusage(RUSAGE_SELF, &r);
#ifdef __APPLE__
        return r.ru_maxrss / 1024; // macOS: bytes
#else
        return r.ru_maxrss; // Linux: kilobytes
#endif
    }

    void header(const std::string& t) {
        std::cout << "\n" << std::string(78, '=') << "\n" << t << "\n" << std::string(78, '=') << "\n";
    }

    // ---- harness with a live mirror for recall ground truth ----
    struct workload_t {
        otterbrix::wrapper_dispatcher_t* dispatcher;
        components::session::session_id_t session;
        std::size_t dim;
        std::mt19937 rng{12345};
        vector_gen_t gen;
        std::map<int, std::vector<double>> live; // id -> vector (mirror of committed live rows)
        int next_id = 0;

        explicit workload_t(otterbrix::wrapper_dispatcher_t* d, std::size_t dim_)
            : dispatcher(d)
            , session(components::session::session_id_t())
            , dim(dim_)
            , gen(dim_, 64, rng) {
            dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(db_name) + ";");
            // Typed schema (DOUBLE[dim]) — schemaless (relkind='g') collections
            // reject ARRAY inserts.
            std::stringstream create_q;
            create_q << "CREATE TABLE " << db_name << "." << coll_name
                     << " (id BIGINT, embedding DOUBLE[" << dim << "]);";
            auto create_cur = dispatcher->execute_sql(session, create_q.str());
            REQUIRE(create_cur->is_success());
        }

        void create_index() {
            // CREATE INDEX needs the column to exist — seed one row.
            if (live.empty()) {
                insert(1);
            }
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE INDEX ix ON " + std::string(db_name) + "." +
                                                   std::string(coll_name) +
                                                   " USING hnsw (embedding vector_l2_ops) WITH (m=16, ef_construction=200);");
            REQUIRE(cur->is_success());
        }

        // Insert `count` rows in batches; returns total wall time (ms).
        double insert_traced(std::size_t count, std::vector<double>* batch_lat, std::size_t batch = 200) {
            auto t0 = clock_t_::now();
            std::size_t done = 0;
            while (done < count) {
                std::size_t b = std::min(batch, count - done);
                std::stringstream q;
                q << "INSERT INTO " << db_name << "." << coll_name << " (id, embedding) VALUES ";
                std::vector<std::pair<int, std::vector<double>>> staged;
                staged.reserve(b);
                for (std::size_t j = 0; j < b; ++j) {
                    int id = next_id++;
                    auto v = gen.sample(rng);
                    q << "(" << id << ", " << vec_to_array(v) << ")" << (j + 1 == b ? ";" : ", ");
                    staged.emplace_back(id, std::move(v));
                }
                auto tb = clock_t_::now();
                auto cur = dispatcher->execute_sql(session, q.str());
                if (batch_lat) batch_lat->push_back(ms_since(tb) / static_cast<double>(b)); // per-row ms
                if (!cur->is_success()) {
                    std::cout << "INSERT FAILED at id~" << staged.front().first
                              << " (live=" << live.size() << "): " << std::string(cur->get_error().what) << "\n";
                }
                REQUIRE(cur->is_success());
                for (auto& [id, v] : staged) live[id] = std::move(v);
                done += b;
            }
            return ms_since(t0);
        }

        double insert(std::size_t count, std::size_t batch = 200) {
            return insert_traced(count, nullptr, batch);
        }

        // Delete the `count` lowest-id rows; returns wall time (ms).
        double delete_n(std::size_t count) {
            if (live.empty()) return 0.0;
            count = std::min(count, live.size());
            std::vector<int> victims;
            victims.reserve(count);
            for (auto it = live.begin(); it != live.end() && victims.size() < count; ++it)
                victims.push_back(it->first);
            int hi = victims.back() + 1; // everything below hi that's still live == the victims

            auto t0 = clock_t_::now();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM " + std::string(db_name) + "." +
                                                   std::string(coll_name) + " WHERE id < " + std::to_string(hi) +
                                                   ";");
            REQUIRE(cur->is_success());
            double dt = ms_since(t0);
            for (int id : victims) live.erase(id);
            return dt;
        }

        std::vector<double> random_query() { return gen.sample(rng); }

        // Exact top-k ids over the live mirror (ground truth).
        std::vector<int> exact_topk(const std::vector<double>& q, std::size_t k) {
            std::vector<std::pair<double, int>> d;
            d.reserve(live.size());
            for (auto& [id, v] : live) {
                double s = 0;
                for (std::size_t i = 0; i < dim; ++i) {
                    double diff = v[i] - q[i];
                    s += diff * diff;
                }
                d.emplace_back(s, id);
            }
            std::partial_sort(d.begin(), d.begin() + static_cast<std::ptrdiff_t>(std::min(k, d.size())), d.end());
            std::vector<int> ids;
            for (std::size_t i = 0; i < std::min(k, d.size()); ++i) ids.push_back(d[i].second);
            return ids;
        }

        // Index top-k ids via the <-> (L2) operator.
        std::vector<int> index_topk(const std::vector<double>& q, std::size_t k) {
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM " + std::string(db_name) + "." +
                                                   std::string(coll_name) + " ORDER BY embedding <-> " +
                                                   vec_to_literal(q) + " LIMIT " + std::to_string(k) + ";");
            std::vector<int> ids;
            if (!cur->is_success()) return ids;
            auto col = cur->chunk_data().column_index("id");
            if (col == static_cast<size_t>(-1)) return ids;
            for (std::size_t r = 0; r < cur->size(); ++r)
                ids.push_back(static_cast<int>(
                    cur->value(static_cast<uint64_t>(col), static_cast<uint64_t>(r)).value<int64_t>()));
            return ids;
        }

        // `n_lat` searches for latency; recall@k on the first `n_rec`.
        std::pair<stats_t, double> search_burst(std::size_t n_lat, std::size_t k, std::size_t n_rec = 25) {
            std::vector<double> lat;
            lat.reserve(n_lat);
            double recall_sum = 0;
            std::size_t recall_n = 0;
            for (std::size_t i = 0; i < n_lat; ++i) {
                auto q = random_query();
                bool check = i < n_rec;
                std::vector<int> truth;
                if (check) truth = exact_topk(q, k);
                auto t0 = clock_t_::now();
                auto got = index_topk(q, k);
                lat.push_back(ms_since(t0));
                if (check) {
                    std::unordered_set<int> t(truth.begin(), truth.end());
                    std::size_t hit = 0;
                    for (int id : got)
                        if (t.count(id)) ++hit;
                    recall_sum += truth.empty() ? 1.0 : static_cast<double>(hit) / truth.size();
                    ++recall_n;
                }
            }
            return {summarize(lat), recall_sum / std::max<std::size_t>(1, recall_n)};
        }
    };

    void print_search(const std::string& label, const stats_t& s, double recall, double qps = -1) {
        std::cout << std::left << std::setw(34) << label << std::fixed << std::setprecision(3)
                  << " | avg=" << std::setw(8) << s.avg << "ms  p95=" << std::setw(8) << s.p95
                  << "ms  p99=" << std::setw(8) << s.p99 << "ms  recall@k=" << std::setprecision(4) << recall;
        if (qps >= 0) std::cout << "  qps=" << std::setprecision(1) << qps;
        std::cout << "\n";
    }

    test_spaces make_space(const std::string& tag) {
        auto config = test_create_config("/tmp/dyn_bench_vs/" + tag);
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        return test_spaces(config);
    }

} // namespace

// Scenario A: insert throughput & maintenance overhead.
TEST_CASE("dynamic::insert_throughput") {
    header("A. Insert throughput: indexed vs non-indexed (D=128, batch=200)");
    const std::size_t dim = 128, N = 100000, k = 10;

    std::cout << "CSV,scenario,mode,rows,insert_ms,rows_per_sec,search_p95_ms,recall\n";

    for (bool indexed : {false, true}) {
        auto space = make_space(std::string("insert_") + (indexed ? "idx" : "noidx"));
        workload_t w(space.dispatcher(), dim);
        if (indexed) w.create_index();

        double t = w.insert(N);
        double rps = N / (t / 1000.0);

        // Brute-force is O(N)/query — few samples; HNSW is ~1ms — many.
        auto [s, recall] = w.search_burst(indexed ? 200 : 20, k);
        print_search(std::string("insert ") + (indexed ? "[HNSW]   " : "[no index]") +
                         "  " + std::to_string(N) + " rows in " + std::to_string(static_cast<long>(t)) + "ms (" +
                         std::to_string(static_cast<long>(rps)) + "/s)",
                     s,
                     recall);
        std::cout << "CSV,insert_throughput," << (indexed ? "indexed" : "plain") << "," << N << ","
                  << static_cast<long>(t) << "," << static_cast<long>(rps) << "," << s.p95 << "," << recall << "\n";
    }
    std::cout << "(non-indexed search is exact brute force; indexed uses the HNSW graph)\n";
}

// Scenario B: tombstone degradation and rebuild recovery.
TEST_CASE("dynamic::delete_and_rebuild") {
    header("B. Delete churn -> search p95 sawtooth + auto-rebuild (N=100000, D=128)");
    const std::size_t dim = 128, N = 100000, k = 10;
    const std::size_t step = 5000; // delete 5% of N each step
    const std::size_t steps = 16;  // up to 80% deleted -> several rebuilds

    auto space = make_space("delete_rebuild");
    workload_t w(space.dispatcher(), dim);
    w.create_index();
    w.insert(N);

    std::cout << "CSV,scenario,deleted_pct,live_rows,search_p95_ms,search_avg_ms,recall\n";
    auto [s0, r0] = w.search_burst(150, k);
    print_search("0% deleted (" + std::to_string(w.live.size()) + " live)", s0, r0);
    std::cout << "CSV,delete_and_rebuild,0," << w.live.size() << "," << s0.p95 << "," << s0.avg << "," << r0 << "\n";

    for (std::size_t i = 1; i <= steps && w.live.size() > step; ++i) {
        w.delete_n(step); // auto-rebuild may fire inside commit when >20% tombstones
        auto [s, r] = w.search_burst(150, k);
        double pct = 100.0 * (1.0 - static_cast<double>(w.live.size()) / N);
        print_search(std::to_string(static_cast<long>(pct)) + "% deleted (" + std::to_string(w.live.size()) + " live)", s, r);
        std::cout << "CSV,delete_and_rebuild," << static_cast<long>(pct) << "," << w.live.size() << "," << s.p95 << ","
                  << s.avg << "," << r << "\n";
    }
    std::cout << "peak RSS: " << peak_rss_kb() << " KB\n";
}

// Scenario C: steady-state mixed churn.
TEST_CASE("dynamic::mixed_churn") {
    header("C. Steady-state churn: delete+insert per round, N constant (N=100000, D=96)");
    const std::size_t dim = 96, N = 100000, k = 10;
    const std::size_t churn = 10000; // 10% of N replaced each round
    const std::size_t rounds = 12;

    auto space = make_space("mixed_churn");
    workload_t w(space.dispatcher(), dim);
    w.create_index();
    w.insert(N);

    std::cout << "CSV,scenario,round,live_rows,churn_ms,insert_ms,search_p95_ms,recall\n";
    long rss0 = peak_rss_kb();
    for (std::size_t round = 1; round <= rounds; ++round) {
        double dt_del = w.delete_n(churn);
        double dt_ins = w.insert(churn);
        auto [s, r] = w.search_burst(150, k);
        print_search("round " + std::to_string(round) + " (" + std::to_string(w.live.size()) + " live)", s, r);
        std::cout << "CSV,mixed_churn," << round << "," << w.live.size() << "," << static_cast<long>(dt_del) << ","
                  << static_cast<long>(dt_ins) << "," << s.p95 << "," << r << "\n";
    }
    std::cout << "RSS start=" << rss0 << "KB  end=" << peak_rss_kb() << "KB  (bounded under churn)\n";
}

// Scenario D: realistic multi-phase user session.
TEST_CASE("dynamic::realistic_session") {
    header("D. Realistic session: load / search / cleanup / ingest / churn (D=128)");
    const std::size_t dim = 128, k = 10;

    auto space = make_space("realistic");
    workload_t w(space.dispatcher(), dim);
    w.create_index();

    std::cout << "CSV,scenario,phase,live_rows,op_ms,search_p95_ms,recall\n";
    auto report = [&](const std::string& phase, double op_ms) {
        auto [s, r] = w.search_burst(120, k);
        print_search(phase + " (" + std::to_string(w.live.size()) + " live)", s, r);
        std::cout << "CSV,realistic," << phase << "," << w.live.size() << "," << static_cast<long>(op_ms) << "," << s.p95
                  << "," << r << "\n";
    };

    // Phase 1: initial bulk load of 100k vectors.
    double t = w.insert(100000);
    report("1.bulk_load_100k", t);

    // Phase 2: a quiet read-heavy period (search burst already in report).
    report("2.read_heavy", 0);

    // Phase 3: cleanup — delete 25% stale data (triggers a rebuild).
    t = w.delete_n(25000);
    report("3.cleanup_del25pct", t);

    // Phase 4: fresh ingest of 40k new vectors.
    t = w.insert(40000);
    report("4.ingest_40k", t);

    // Phase 5: heavy interleaved churn with reads (3 mini-rounds, 15k each).
    double churn_ms = 0;
    for (int i = 0; i < 3; ++i) {
        churn_ms += w.delete_n(15000);
        churn_ms += w.insert(15000);
    }
    report("5.interleaved_churn", churn_ms);

    std::cout << "final live rows: " << w.live.size() << "   peak RSS: " << peak_rss_kb() << " KB\n";
}

// Scenario E: insert latency vs index size.
TEST_CASE("dynamic::insert_latency_scaling") {
    header("E. Insert latency vs index size (per-row, windows of 10k, D=128)");
    const std::size_t dim = 128, total = 100000, window = 10000, batch = 200;

    std::cout << "CSV,scenario,mode,rows_after,ins_p95_us,ins_avg_us\n";
    for (bool indexed : {false, true}) {
        auto space = make_space(std::string("ins_scale_") + (indexed ? "idx" : "plain"));
        workload_t w(space.dispatcher(), dim);
        if (indexed) w.create_index();

        for (std::size_t done = 0; done < total; done += window) {
            std::vector<double> lat; // per-row ms for each batch in this window
            w.insert_traced(window, &lat, batch);
            auto s = summarize(lat);
            std::size_t rows = w.live.size();
            std::cout << std::left << std::setw(28)
                      << (std::string(indexed ? "HNSW" : "plain") + " @ " + std::to_string(rows) + " rows")
                      << std::fixed << std::setprecision(2) << " | insert avg=" << std::setw(7) << s.avg * 1000
                      << "us/row  p95=" << std::setw(7) << s.p95 * 1000 << "us/row\n";
            std::cout << "CSV,insert_scaling," << (indexed ? "indexed" : "plain") << "," << rows << ","
                      << s.p95 * 1000 << "," << s.avg * 1000 << "\n"; // microseconds per row
        }
    }
}

// Scenario F: delete throughput & rebuild spikes.
TEST_CASE("dynamic::delete_throughput") {
    header("F. Delete throughput & rebuild spikes (batch=5000, N=100000, D=128)");
    const std::size_t dim = 128, N = 100000, k = 10;
    const std::size_t step = 5000;

    auto space = make_space("del_thru");
    workload_t w(space.dispatcher(), dim);
    w.create_index();
    w.insert(N);
    auto [s0, r0] = w.search_burst(60, k); // confirm index healthy before churn
    std::cout << "baseline search p95=" << std::fixed << std::setprecision(3) << s0.p95
              << "ms recall=" << r0 << "\n";

    std::cout << "CSV,scenario,step,live_after,del_batch_ms,del_rows_per_sec\n";
    std::size_t step_i = 0;
    double total_del_ms = 0;
    std::size_t total_deleted = 0;
    while (w.live.size() > step) {
        double dt = w.delete_n(step); // a rebuild may fire here (spike)
        total_del_ms += dt;
        total_deleted += step;
        double rps = step / (dt / 1000.0);
        ++step_i;
        std::cout << std::left << std::setw(28)
                  << ("delete step " + std::to_string(step_i) + " (" + std::to_string(w.live.size()) + " live)")
                  << std::fixed << std::setprecision(2) << " | " << std::setw(8) << dt << "ms  "
                  << std::setprecision(0) << std::setw(8) << rps << " rows/s"
                  << (dt > 200 ? "   <-- rebuild spike" : "") << "\n";
        std::cout << "CSV,delete_throughput," << step_i << "," << w.live.size() << "," << dt << ","
                  << static_cast<long>(rps) << "\n";
    }
    std::cout << "deleted " << total_deleted << " rows in " << std::setprecision(0) << total_del_ms
              << "ms  (avg " << static_cast<long>(total_deleted / (total_del_ms / 1000.0)) << " rows/s overall)\n";
}
