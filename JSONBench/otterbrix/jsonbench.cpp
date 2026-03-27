#include <boost/json/src.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <integration/cpp/base_spaces.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <components/catalog/catalog.hpp>
#include <components/catalog/computed_schema.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/disk/manager_disk.hpp>

// Subclass to expose internals for diagnostics
class bench_spaces final : public otterbrix::base_otterbrix_t {
public:
    explicit bench_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {}

    services::dispatcher::manager_dispatcher_t* mgr() {
        return manager_dispatcher_.get();
    }
};

#ifndef JSONBENCH_DATA_FILE
#define JSONBENCH_DATA_FILE "file_0001.json"
#endif

static constexpr bool USE_SPARSE = true;

static constexpr size_t N_ROWS = 10'000;
static constexpr const char* DB_NAME = "bench";
static constexpr const char* TABLE_NAME = "events";
static constexpr size_t INSERT_BATCH = 1000;
static constexpr size_t SCHEMA_SAMPLE = 2000;
static constexpr size_t SPARSE_THRESHOLD = USE_SPARSE ? N_ROWS / 10 : 0;

namespace bj = boost::json;

// ----------------------------------------------------------------------------
// JSON flattening
// ----------------------------------------------------------------------------

static void flatten_object(const bj::object& obj,
                           const std::string& prefix,
                           std::map<std::string, std::string>& out) {
    for (const auto& kv : obj) {
        std::string path = prefix.empty()
            ? std::string(kv.key())
            : prefix + '.' + std::string(kv.key());
        const auto& val = kv.value();
        if (val.is_object()) {
            flatten_object(val.as_object(), path, out);
        } else if (val.is_array()) {
            out[path] = bj::serialize(val);
        } else if (val.is_string()) {
            out[path] = std::string(val.as_string());
        } else if (val.is_int64()) {
            out[path] = std::to_string(val.as_int64());
        } else if (val.is_uint64()) {
            out[path] = std::to_string(val.as_uint64());
        } else if (val.is_double()) {
            out[path] = std::to_string(val.as_double());
        } else if (val.is_bool()) {
            out[path] = val.as_bool() ? "true" : "false";
        }
        // null → omit
    }
}

// ----------------------------------------------------------------------------
// Schema discovery
// ----------------------------------------------------------------------------

struct ColDef {
    std::string path;
    bool        is_int;
};

static std::vector<ColDef> discover_schema(const char* filename, size_t sample) {
    std::map<std::string, bool> path_non_int;

    std::ifstream f(filename);
    size_t count = 0;
    std::string line;
    while (count < sample && std::getline(f, line)) {
        if (line.empty()) continue;
        boost::system::error_code ec;
        auto jv = bj::parse(line, ec);
        if (ec || !jv.is_object()) continue;

        std::map<std::string, std::string> flat;
        flatten_object(jv.as_object(), {}, flat);
        for (const auto& [path, val] : flat) {
            if (!path_non_int.count(path)) path_non_int[path] = false;
            if (!path_non_int[path]) {
                try {
                    size_t pos = 0;
                    std::stoll(val, &pos);
                    if (pos != val.size()) path_non_int[path] = true;
                } catch (...) {
                    path_non_int[path] = true;
                }
            }
        }
        ++count;
    }

    std::vector<ColDef> cols;
    cols.reserve(path_non_int.size());
    for (const auto& [path, non_int] : path_non_int) {
        cols.push_back({path, !non_int});
    }
    return cols;
}

// ----------------------------------------------------------------------------
// Misc helpers
// ----------------------------------------------------------------------------

static size_t rss_kb() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.compare(0, 6, "VmRSS:") == 0) {
            size_t val = 0;
            std::istringstream ss(line.substr(6));
            ss >> val;
            return val;
        }
    }
    return 0;
}

using Clock = std::chrono::steady_clock;
static double ms_since(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

static void print_cursor(components::cursor::cursor_t_ptr& cur) {
    if (cur->is_error()) {
        std::cout << "  ERROR: " << cur->get_error().what << "\n";
        return;
    }
    auto& chunk = cur->chunk_data();
    size_t ncols = chunk.column_count();

    std::cout << "  ";
    for (size_t c = 0; c < ncols; ++c) {
        if (c) std::cout << " | ";
        std::cout << chunk.data[c].type().alias();
    }
    std::cout << "\n  " << std::string(40, '-') << "\n";

    size_t nrows = cur->size();
    for (size_t r = 0; r < nrows; ++r) {
        std::cout << "  ";
        for (size_t c = 0; c < ncols; ++c) {
            if (c) std::cout << " | ";
            auto val = cur->value(c, r);
            auto lt  = val.type().type();
            using LT = components::types::logical_type;
            switch (lt) {
                case LT::BIGINT:
                case LT::INTEGER:
                case LT::SMALLINT:
                case LT::HUGEINT:
                case LT::UTINYINT:
                case LT::USMALLINT:
                case LT::UINTEGER:
                case LT::UBIGINT:
                    std::cout << val.value<int64_t>();
                    break;
                case LT::STRING_LITERAL:
                case LT::ANY: {
                    auto* sp = val.value<std::string*>();
                    std::cout << (sp ? *sp : "NULL");
                    break;
                }
                case LT::NA:
                    std::cout << "";
                    break;
                default:
                    std::cout << "?";
                    break;
            }
        }
        std::cout << "\n";
    }
    std::cout << "  (" << nrows << " rows)\n";
}

static double run_query(otterbrix::base_otterbrix_t* space,
                        const std::string& label,
                        const std::string& sql,
                        bool print_rows = true) {
    std::cout << "\n=== " << label << " ===\n";
    std::cout << "  SQL: " << sql.substr(0, 120)
              << (sql.size() > 120 ? "..." : "") << "\n\n";

    auto t0      = Clock::now();
    auto session = otterbrix::session_id_t();
    auto cur     = space->dispatcher()->execute_sql(session, sql);
    double ms    = ms_since(t0);

    if (print_rows) print_cursor(cur);
    else if (cur->is_error())
        std::cout << "  ERROR: " << cur->get_error().what << "\n";
    else
        std::cout << "  (" << cur->size() << " rows)\n";

    std::cout << "  Time: " << ms << " ms\n";
    return ms;
}

// ----------------------------------------------------------------------------
// Benchmark run
// ----------------------------------------------------------------------------

struct BenchResult {
    double insert_ms;
    size_t rss_delta_kb;
    double q1_ms, q2_ms, q3_ms, q4_ms, q5_ms;
};

static BenchResult run_bench(const std::string& label,
                             const std::vector<ColDef>& cols,
                             const std::vector<std::map<std::string, std::string>>& records,
                             size_t sparse_threshold) {
    std::cout << "\n\n";
    std::cout << "########################################\n";
    std::cout << "# " << label << "\n";
    if (sparse_threshold > 0)
        std::cout << "# sparse_threshold = " << sparse_threshold << "\n";
    else
        std::cout << "# sparse disabled\n";
    std::cout << "########################################\n";

    std::filesystem::remove_all("/tmp/jsonbench_otterbrix");
    std::filesystem::create_directories("/tmp/jsonbench_otterbrix");

    auto cfg      = configuration::config::create_config("/tmp/jsonbench_otterbrix");
    cfg.disk.on   = false;
    cfg.wal.on    = false;
    cfg.log.level = log_t::level::warn;

    bench_spaces space(cfg);
    auto* dispatcher = space.dispatcher();
    auto* resource   = dispatcher->resource();

    collection_full_name_t full_name{DB_NAME, TABLE_NAME};

    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_database(session, DB_NAME);
    }
    {
        auto node = components::logical_plan::make_node_create_collection(
            resource, full_name, static_cast<uint64_t>(sparse_threshold));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, node);
        if (cur->is_error()) {
            std::cerr << "CREATE TABLE error: " << cur->get_error().what << "\n";
        }
    }

    // Build path → column index map
    std::map<std::string, size_t> path_to_idx;
    for (size_t i = 0; i < cols.size(); ++i) path_to_idx[cols[i].path] = i;

    using CT = components::types::complex_logical_type;
    using LT = components::types::logical_type;
    using LV = components::types::logical_value_t;

    std::pmr::vector<CT> types(resource);
    for (const auto& col : cols)
        types.emplace_back(col.is_int ? LT::BIGINT : LT::STRING_LITERAL, col.path);

    // ---- Insert --------------------------------------------------------------
    std::cout << "\n=== Inserting " << records.size() << " rows"
              << " (batch=" << INSERT_BATCH << ") ===\n";
    size_t rss_before = rss_kb();
    auto t_insert = Clock::now();

    for (size_t start = 0; start < records.size(); start += INSERT_BATCH) {
        size_t end   = std::min(start + INSERT_BATCH, records.size());
        size_t batch = end - start;

        components::vector::data_chunk_t chunk(resource, types, batch);
        chunk.set_cardinality(batch);

        // Mark all columns as invalid (NULL) first; only set valid where JSON has a value
        for (size_t c = 0; c < cols.size(); ++c) {
            chunk.data[c].validity().set_all_invalid(batch);
        }

        for (size_t i = 0; i < batch; ++i) {
            const auto& flat = records[start + i];
            for (const auto& [path, val] : flat) {
                auto it = path_to_idx.find(path);
                if (it == path_to_idx.end()) continue;
                size_t col_idx = it->second;
                if (cols[col_idx].is_int) {
                    try {
                        chunk.set_value(col_idx, i, LV{resource, std::stoll(val)});
                    } catch (...) {
                        chunk.set_value(col_idx, i, LV{resource, val});
                    }
                } else {
                    chunk.set_value(col_idx, i, LV{resource, val});
                }
            }
        }

        auto ins = components::logical_plan::make_node_insert(resource, full_name, std::move(chunk));
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_plan(session, ins);
        if (cur->is_error()) {
            std::cerr << "Insert error at row " << start << ": " << cur->get_error().what << "\n";
        }
    }

    double insert_ms  = ms_since(t_insert);
    size_t rss_after  = rss_kb();
    size_t rss_delta  = rss_after > rss_before ? rss_after - rss_before : 0;
    std::cout << "Insert time : " << insert_ms << " ms\n";
    std::cout << "Memory used : " << rss_delta << " KB"
              << "  (" << rss_after << " KB RSS total)\n";

    // ---- Sparse diagnostics --------------------------------------------------
    if (sparse_threshold > 0) {
        auto* mgr = space.mgr();
        auto* res = mgr->resource();
        components::catalog::table_id tid(res, full_name);

        if (mgr->mutable_catalog().table_computes(tid)) {
            const auto& schema = mgr->mutable_catalog().get_computing_table_schema(tid);
            auto sparse_cols = schema.sparse_columns();

            size_t n_sparse = sparse_cols.size();
            std::cout << "\n--- Sparse diagnostics ---\n";
            std::cout << "  Sparse columns still remaining : " << n_sparse << "\n";

            // Count total rows in all sparse tables
            size_t total_sparse_rows = 0;
            size_t max_sparse_rows   = 0;
            std::string max_col_name;
            for (const auto& sp : sparse_cols) {
                uint64_t nnc = schema.get_non_null_count(
                    std::pmr::string(
                        components::catalog::computed_schema::storage_column_name(
                            sp.field_name, sp.type).c_str(),
                        res));
                total_sparse_rows += nnc;
                if (nnc > max_sparse_rows) {
                    max_sparse_rows = nnc;
                    max_col_name    = sp.field_name;
                }
            }
            std::cout << "  Total non-null rows in sparse  : " << total_sparse_rows << "\n";
            std::cout << "  Largest sparse col             : " << max_col_name
                      << " (" << max_sparse_rows << " rows)\n";
            std::cout << "  sparse_threshold               : " << schema.sparse_threshold() << "\n";
        }
        std::cout << "--------------------------\n";
    }

    // ---- Diagnostic -------------------------------------------------------------
    run_query(&space, "Diag: feed.like direct count",
        "SELECT COUNT(*) as cnt FROM bench.events "
        "WHERE \"commit.collection\" = 'app.bsky.feed.like';");

    run_query(&space, "Diag: raw first 5 rows",
        "SELECT _id, time_us, kind, \"commit.operation\" FROM bench.events LIMIT 5;");

    run_query(&space, "Diag: kind x operation for feed.post",
        "SELECT kind, \"commit.operation\", COUNT(*) as cnt "
        "FROM bench.events "
        "WHERE \"commit.collection\" = 'app.bsky.feed.post' "
        "GROUP BY kind, \"commit.operation\";");

    // ---- Queries -------------------------------------------------------------
    double q1 = run_query(&space, "Q1: Top event types",
        "SELECT \"commit.collection\", COUNT(*) as count "
        "FROM bench.events "
        "GROUP BY \"commit.collection\" "
        "ORDER BY count DESC;");

    double q2 = run_query(&space, "Q2: Unique users (kind=commit, op=create)",
        "SELECT \"commit.collection\", COUNT(*) as count, COUNT(DISTINCT did) as users "
        "FROM bench.events "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "GROUP BY \"commit.collection\" "
        "ORDER BY count DESC;");

    double q3 = run_query(&space, "Q3: Post / repost / like counts",
        "SELECT \"commit.collection\", COUNT(*) as count "
        "FROM bench.events "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "  AND (\"commit.collection\" = 'app.bsky.feed.post' "
        "       OR \"commit.collection\" = 'app.bsky.feed.repost' "
        "       OR \"commit.collection\" = 'app.bsky.feed.like') "
        "GROUP BY \"commit.collection\" "
        "ORDER BY count DESC;");

    double q4 = run_query(&space, "Q4: First 3 users to post",
        "SELECT did, MIN(time_us) as first_post "
        "FROM bench.events "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "  AND \"commit.collection\" = 'app.bsky.feed.post' "
        "GROUP BY did "
        "ORDER BY first_post ASC "
        "LIMIT 3;");

    double q5 = run_query(&space, "Q5: Top 3 users by activity span",
        "SELECT did, MIN(time_us) as first_ts, MAX(time_us) as last_ts "
        "FROM bench.events "
        "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
        "  AND \"commit.collection\" = 'app.bsky.feed.post' "
        "GROUP BY did "
        "ORDER BY (last_ts - first_ts) DESC "
        "LIMIT 3;");

    return {insert_ms, rss_delta, q1, q2, q3, q4, q5};
}

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------

int main() {
    // ---- Schema discovery + data load (shared) ------------------------------
    std::cout << "=== Discovering schema from " << JSONBENCH_DATA_FILE << " ===\n";
    auto cols = discover_schema(JSONBENCH_DATA_FILE, SCHEMA_SAMPLE);
    std::cout << "Discovered " << cols.size() << " columns.\n";

    std::cout << "\n=== Reading up to " << N_ROWS << " rows ===\n";
    std::vector<std::map<std::string, std::string>> records;
    records.reserve(N_ROWS);
    {
        std::ifstream f(JSONBENCH_DATA_FILE);
        std::string line;
        while (records.size() < N_ROWS && std::getline(f, line)) {
            if (line.empty()) continue;
            boost::system::error_code ec;
            auto jv = bj::parse(line, ec);
            if (ec || !jv.is_object()) continue;
            std::map<std::string, std::string> flat;
            flatten_object(jv.as_object(), {}, flat);
            records.push_back(std::move(flat));
        }
    }
    std::cout << "Parsed " << records.size() << " records.\n";

    // Sort by (kind, commit.operation, commit.collection) for zone-map pruning
    std::sort(records.begin(), records.end(), [](const auto& a, const auto& b) {
        auto get = [](const auto& m, const char* k) -> const std::string& {
            static const std::string empty;
            auto it = m.find(k);
            return it != m.end() ? it->second : empty;
        };
        const auto& ka  = get(a, "kind");
        const auto& kb  = get(b, "kind");
        if (ka != kb) return ka < kb;
        const auto& opa = get(a, "commit.operation");
        const auto& opb = get(b, "commit.operation");
        if (opa != opb) return opa < opb;
        return get(a, "commit.collection") < get(b, "commit.collection");
    });

    // ---- Run selected variant ------------------------------------------------
    const std::string label = USE_SPARSE
        ? ("SPARSE (threshold=" + std::to_string(SPARSE_THRESHOLD) + ")")
        : "BASELINE (no sparse)";
    run_bench(label, cols, records, SPARSE_THRESHOLD);

    return 0;
}
