#include <boost/json/src.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <integration/cpp/base_spaces.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <components/logical_plan/node_insert.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

// Thin subclass to expose the protected constructor
class bench_spaces final : public otterbrix::base_otterbrix_t {
public:
    explicit bench_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {}
};

#ifndef JSONBENCH_DATA_FILE
#define JSONBENCH_DATA_FILE "file_0001.json"
#endif

static constexpr size_t N_ROWS       = 100'000;
static constexpr const char* DB_NAME    = "bench";
static constexpr const char* TABLE_NAME = "events";
static constexpr size_t INSERT_BATCH = 500;
static constexpr size_t SCHEMA_SAMPLE = 2000;

namespace bj = boost::json;

// ----------------------------------------------------------------------------
// JSON flattening
// ----------------------------------------------------------------------------

// Recursively flatten a JSON object into { dot-path → string value }.
// Arrays are serialized as JSON strings. Null values are omitted.
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
    std::string path;    // dot-separated JSON path (= column name), e.g. "commit.collection"
    bool        is_int;  // BIGINT if all sampled values parse as int64, else STRING_LITERAL
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
                default:
                    std::cout << "?";
                    break;
            }
        }
        std::cout << "\n";
    }
    std::cout << "  (" << nrows << " rows)\n";
}

static void run_query(otterbrix::base_otterbrix_t* space,
                      const std::string& label,
                      const std::string& sql) {
    std::cout << "\n=== " << label << " ===\n";
    std::cout << "  SQL: " << sql.substr(0, 120)
              << (sql.size() > 120 ? "..." : "") << "\n\n";

    auto t0      = Clock::now();
    auto session = otterbrix::session_id_t();
    auto cur     = space->dispatcher()->execute_sql(session, sql);
    double ms    = ms_since(t0);

    print_cursor(cur);
    std::cout << "  Time: " << ms << " ms\n";
}

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------

int main() {
    std::filesystem::remove_all("/tmp/jsonbench_otterbrix");
    std::filesystem::create_directories("/tmp/jsonbench_otterbrix");

    auto cfg      = configuration::config::create_config("/tmp/jsonbench_otterbrix");
    cfg.disk.on   = false;
    cfg.wal.on    = false;
    cfg.log.level = log_t::level::warn;

    bench_spaces space(cfg);
    auto* dispatcher = space.dispatcher();
    auto* resource   = dispatcher->resource();

    // ---- Create DB and table (dynamic schema) --------------------------------
    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_database(session, DB_NAME);
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->create_collection(session, DB_NAME, TABLE_NAME);
    }

    // ---- Schema discovery ---------------------------------------------------
    std::cout << "=== Discovering schema from " << JSONBENCH_DATA_FILE << " ===\n";
    auto cols = discover_schema(JSONBENCH_DATA_FILE, SCHEMA_SAMPLE);
    std::cout << "Discovered " << cols.size() << " columns.\n";

    // Build path → column index map for fast lookup during insert
    std::map<std::string, size_t> path_to_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        path_to_idx[cols[i].path] = i;
    }

    // Build type vector  (column name = dot-path, e.g. "commit.collection")
    using CT = components::types::complex_logical_type;
    using LT = components::types::logical_type;
    using LV = components::types::logical_value_t;

    std::pmr::vector<CT> types(resource);
    for (const auto& col : cols) {
        types.emplace_back(col.is_int ? LT::BIGINT : LT::STRING_LITERAL, col.path);
    }

    collection_full_name_t full_name{DB_NAME, TABLE_NAME};

    // ---- Read and sort records ----------------------------------------------
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
        const auto& ca  = get(a, "commit.collection");
        const auto& cb  = get(b, "commit.collection");
        return ca < cb;
    });

    // ---- Insert in batches --------------------------------------------------
    std::cout << "\n=== Inserting " << records.size() << " rows (batch=" << INSERT_BATCH << ") ===\n";
    size_t rss_before = rss_kb();
    auto t_insert = Clock::now();

    for (size_t start = 0; start < records.size(); start += INSERT_BATCH) {
        size_t end   = std::min(start + INSERT_BATCH, records.size());
        size_t batch = end - start;

        components::vector::data_chunk_t chunk(resource, types, batch);
        chunk.set_cardinality(batch);

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
            std::cerr << "Insert error: " << cur->get_error().what << "\n";
            return 1;
        }
    }

    double insert_ms = ms_since(t_insert);
    size_t rss_after = rss_kb();
    std::cout << "Insert time : " << insert_ms << " ms\n";
    std::cout << "Memory used : " << (rss_after - rss_before) << " KB"
              << "  (" << rss_after << " KB RSS total)\n";

    // ---- Queries (column names with dots require double-quote quoting) -------

    // Q1: Top event types by count
    run_query(&space,
              "Q1: Top event types",
              "SELECT \"commit.collection\", COUNT(did) as count "
              "FROM bench.events "
              "GROUP BY \"commit.collection\" "
              "ORDER BY count DESC;");

    // Q2: Unique users per event type (kind=commit, operation=create)
    run_query(&space,
              "Q2: Unique users per event type (kind=commit, op=create)",
              "SELECT \"commit.collection\", COUNT(did) as count, COUNT(DISTINCT did) as users "
              "FROM bench.events "
              "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
              "GROUP BY \"commit.collection\" "
              "ORDER BY count DESC;");

    // Q3: Post/repost/like counts
    run_query(&space,
              "Q3: Post / repost / like counts",
              "SELECT \"commit.collection\", COUNT(did) as count "
              "FROM bench.events "
              "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
              "  AND (\"commit.collection\" = 'app.bsky.feed.post' "
              "       OR \"commit.collection\" = 'app.bsky.feed.repost' "
              "       OR \"commit.collection\" = 'app.bsky.feed.like') "
              "GROUP BY \"commit.collection\" "
              "ORDER BY count DESC;");

    // Q4: First 3 users to post
    run_query(&space,
              "Q4: First 3 users to post",
              "SELECT did, MIN(time_us) as first_post "
              "FROM bench.events "
              "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
              "  AND \"commit.collection\" = 'app.bsky.feed.post' "
              "GROUP BY did "
              "ORDER BY first_post ASC "
              "LIMIT 3;");

    // Q5: Top 3 users by activity span
    run_query(&space,
              "Q5: Top 3 users by activity span",
              "SELECT did, MIN(time_us) as first_ts, MAX(time_us) as last_ts "
              "FROM bench.events "
              "WHERE kind = 'commit' AND \"commit.operation\" = 'create' "
              "  AND \"commit.collection\" = 'app.bsky.feed.post' "
              "GROUP BY did "
              "ORDER BY last_ts DESC "
              "LIMIT 3;");

    return 0;
}
