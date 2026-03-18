#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <integration/cpp/base_spaces.hpp>
#include <integration/cpp/otterbrix.hpp>

// Thin subclass to expose the protected constructor
class bench_spaces final : public otterbrix::base_otterbrix_t {
public:
    explicit bench_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {}
};

#ifndef JSONBENCH_DATA_FILE
#define JSONBENCH_DATA_FILE "file_0001_filtered.json"
#endif

// Number of rows to load from the dataset
static constexpr int N_ROWS = 10'000;

static constexpr const char* DB_NAME = "bench";
static constexpr const char* TABLE_NAME = "events";
static constexpr int INSERT_BATCH = 500;

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

struct Record {
    std::string did;
    int64_t     time_us{0};
    std::string kind;
    std::string collection;  // commit.collection
    std::string operation;   // commit.operation
};

// Extract the value of a top-level string field from a JSON line.
static std::string extract_str(const std::string& json, const char* key) {
    std::string needle = std::string("\"") + key + "\":\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos)
        return {};
    pos += needle.size();
    std::string out;
    for (; pos < json.size(); ++pos) {
        char c = json[pos];
        if (c == '"')
            break;
        if (c == '\\' && pos + 1 < json.size()) {
            ++pos;
            switch (json[pos]) {
                case '"': out += '"'; break;
                case '\\': out += '\\'; break;
                case '/': out += '/'; break;
                case 'n': out += '\n'; break;
                case 't': out += '\t'; break;
                default: out += json[pos]; break;
            }
        } else {
            out += c;
        }
    }
    return out;
}

// Extract the value of a top-level integer field.
static int64_t extract_int64(const std::string& json, const char* key) {
    std::string needle = std::string("\"") + key + "\":";
    auto pos = json.find(needle);
    if (pos == std::string::npos)
        return 0;
    pos += needle.size();
    auto end = json.find_first_of(",}", pos);
    try {
        return std::stoll(json.substr(pos, end - pos));
    } catch (...) {
        return 0;
    }
}

// Extract a string field from inside a JSON sub-object: "parent":{"key":"VALUE",...}
static std::string extract_nested_str(const std::string& json,
                                      const char* parent,
                                      const char* key) {
    std::string parent_needle = std::string("\"") + parent + "\":{";
    auto ppos = json.find(parent_needle);
    if (ppos == std::string::npos)
        return {};
    // Find the matching closing brace (simplified: first '}' after parent start)
    auto end = json.find('}', ppos + parent_needle.size());
    auto sub = json.substr(ppos, end - ppos + 1);
    return extract_str(sub, key);
}

static Record parse_line(const std::string& line) {
    Record r;
    r.did        = extract_str(line, "did");
    r.time_us    = extract_int64(line, "time_us");
    r.kind       = extract_str(line, "kind");
    r.collection = extract_nested_str(line, "commit", "collection");
    r.operation  = extract_nested_str(line, "commit", "operation");
    return r;
}

// Escape a SQL string literal.
static std::string sql_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '\'')
            out += "''";
        else
            out += c;
    }
    return out;
}

// Read RSS from /proc/self/status (Linux only).
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

// Print all rows from a cursor.
static void print_cursor(components::cursor::cursor_t_ptr& cur) {
    if (cur->is_error()) {
        std::cout << "  ERROR: " << cur->get_error().what << "\n";
        return;
    }
    auto& chunk = cur->chunk_data();
    size_t ncols = chunk.column_count();

    // Header: use type aliases as column names
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

// Run a query, print results, timing.
static void run_query(otterbrix::base_otterbrix_t* space,
                      const std::string& label,
                      const std::string& sql) {
    std::cout << "\n=== " << label << " ===\n";
    std::cout << "  SQL: " << sql.substr(0, 120)
              << (sql.size() > 120 ? "..." : "") << "\n\n";

    auto t0  = Clock::now();
    auto session = otterbrix::session_id_t();
    auto cur = space->dispatcher()->execute_sql(session, sql);
    double ms = ms_since(t0);

    print_cursor(cur);
    std::cout << "  Time: " << ms << " ms\n";
}

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------

int main() {
    // ---- Setup space --------------------------------------------------------
    std::filesystem::remove_all("/tmp/jsonbench_otterbrix");
    std::filesystem::create_directories("/tmp/jsonbench_otterbrix");

    auto cfg      = configuration::config::create_config("/tmp/jsonbench_otterbrix");
    cfg.disk.on   = false;
    cfg.wal.on    = false;
    cfg.log.level = log_t::level::warn; // suppress trace/info noise

    bench_spaces space(cfg);
    auto* dispatcher = space.dispatcher();

    // ---- Create DB and table ------------------------------------------------
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + DB_NAME + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        std::string full = std::string(DB_NAME) + "." + TABLE_NAME;
        dispatcher->execute_sql(session, "CREATE TABLE " + full + " ();");
    }

    // ---- Parse data ---------------------------------------------------------
    std::cout << "=== Parsing " << N_ROWS << " rows from " << JSONBENCH_DATA_FILE << " ===\n";
    std::vector<Record> records;
    records.reserve(N_ROWS);
    {
        std::ifstream f(JSONBENCH_DATA_FILE);
        if (!f) {
            std::cerr << "Cannot open " << JSONBENCH_DATA_FILE << "\n";
            return 1;
        }
        std::string line;
        while (static_cast<int>(records.size()) < N_ROWS && std::getline(f, line)) {
            if (line.empty()) continue;
            records.push_back(parse_line(line));
        }
    }
    std::cout << "Parsed " << records.size() << " records.\n";

    // ---- Insert in batches --------------------------------------------------
    std::cout << "\n=== Inserting " << records.size() << " rows (batch=" << INSERT_BATCH << ") ===\n";
    size_t rss_before = rss_kb();
    auto t_insert = Clock::now();

    std::string full_table = std::string(DB_NAME) + "." + TABLE_NAME;
    for (size_t start = 0; start < records.size(); start += INSERT_BATCH) {
        size_t end = std::min(start + INSERT_BATCH, records.size());
        std::ostringstream sql;
        sql << "INSERT INTO " << full_table
            << " (did, time_us, kind, collection, operation) VALUES ";
        for (size_t i = start; i < end; ++i) {
            if (i != start) sql << ", ";
            const auto& r = records[i];
            sql << "('"  << sql_escape(r.did)        << "', "
                <<          r.time_us                 << ", '"
                <<          sql_escape(r.kind)        << "', '"
                <<          sql_escape(r.collection)  << "', '"
                <<          sql_escape(r.operation)   << "')";
        }
        sql << ";";
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql.str());
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

    // ---- Queries ------------------------------------------------------------

    // Q1: Top event types by count
    run_query(&space,
              "Q1: Top event types",
              "SELECT collection, COUNT(did) as count "
              "FROM bench.events "
              "GROUP BY collection "
              "ORDER BY count DESC;");

    // Q2: Unique users per event type (kind=commit, operation=create)
    run_query(&space,
              "Q2: Unique users per event type (kind=commit, op=create)",
              "SELECT collection, COUNT(did) as count, COUNT(DISTINCT did) as users "
              "FROM bench.events "
              "WHERE kind = 'commit' AND operation = 'create' "
              "GROUP BY collection "
              "ORDER BY count DESC;");

    // Q3: Post/repost/like counts (subset of event types)
    run_query(&space,
              "Q3: Post / repost / like counts",
              "SELECT collection, COUNT(did) as count "
              "FROM bench.events "
              "WHERE kind = 'commit' AND operation = 'create' "
              "  AND (collection = 'app.bsky.feed.post' "
              "       OR collection = 'app.bsky.feed.repost' "
              "       OR collection = 'app.bsky.feed.like') "
              "GROUP BY collection "
              "ORDER BY count DESC;");

    // Q4: First 3 users to post
    run_query(&space,
              "Q4: First 3 users to post",
              "SELECT did, MIN(time_us) as first_post "
              "FROM bench.events "
              "WHERE kind = 'commit' AND operation = 'create' "
              "  AND collection = 'app.bsky.feed.post' "
              "GROUP BY did "
              "ORDER BY first_post ASC "
              "LIMIT 3;");

    // Q5: Top 3 users by activity span (latest - earliest post time)
    run_query(&space,
              "Q5: Top 3 users by activity span",
              "SELECT did, MIN(time_us) as first_ts, MAX(time_us) as last_ts "
              "FROM bench.events "
              "WHERE kind = 'commit' AND operation = 'create' "
              "  AND collection = 'app.bsky.feed.post' "
              "GROUP BY did "
              "ORDER BY last_ts DESC "
              "LIMIT 3;");

    return 0;
}
