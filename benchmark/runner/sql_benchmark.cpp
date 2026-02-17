#include "sql_benchmark.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace otterbrix::benchmark {

namespace {

std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

std::string strip_comments_and_directives(const std::string& raw) {
    std::string result;
    result.reserve(raw.size());

    size_t i = 0;
    while (i < raw.size()) {
        // Block comments: /* ... */
        if (i + 1 < raw.size() && raw[i] == '/' && raw[i + 1] == '*') {
            auto end = raw.find("*/", i + 2);
            if (end == std::string::npos) {
                break; // unterminated block comment, skip rest
            }
            i = end + 2;
            continue;
        }

        // Line comments: -- ...
        if (i + 1 < raw.size() && raw[i] == '-' && raw[i + 1] == '-') {
            while (i < raw.size() && raw[i] != '\n') {
                ++i;
            }
            continue;
        }

        // TPC-H directives: lines starting with :
        if (raw[i] == ':' && (i == 0 || raw[i - 1] == '\n')) {
            while (i < raw.size() && raw[i] != '\n') {
                ++i;
            }
            continue;
        }

        result += raw[i];
        ++i;
    }

    return result;
}

std::vector<std::string> split_queries(const std::string& sql) {
    std::vector<std::string> queries;
    std::string current;

    for (char ch : sql) {
        if (ch == ';') {
            auto stmt = trim(current);
            if (!stmt.empty()) {
                queries.push_back(std::move(stmt));
            }
            current.clear();
        } else {
            current += ch;
        }
    }

    auto stmt = trim(current);
    if (!stmt.empty()) {
        queries.push_back(std::move(stmt));
    }

    return queries;
}

std::string make_relative_name(const std::filesystem::path& path, const std::filesystem::path& base_dir) {
    auto rel = std::filesystem::relative(path, base_dir);
    auto name = rel.string();
    // Remove extension
    auto dot = name.rfind('.');
    if (dot != std::string::npos) {
        name = name.substr(0, dot);
    }
    return name;
}

std::string make_group(const std::filesystem::path& path, const std::filesystem::path& base_dir) {
    auto rel = std::filesystem::relative(path, base_dir);
    if (rel.has_parent_path()) {
        return rel.parent_path().string();
    }
    return "sql";
}

// --- Setup file parsing ---

struct setup_data_t {
    std::string sql;
    std::vector<sql_csv_entry_t> csv_entries;
    std::string database;
};

setup_data_t parse_setup_file(const std::filesystem::path& path) {
    setup_data_t data;

    std::ifstream file(path);
    if (!file.is_open()) {
        return data;
    }

    std::string line;
    std::string sql_lines;

    while (std::getline(file, line)) {
        auto trimmed = trim(line);

        // Parse @database directive
        if (trimmed.starts_with("-- @database ")) {
            data.database = trim(trimmed.substr(13)); // strlen("-- @database ")
            continue;
        }

        // Parse @load_csv directives from comments
        if (trimmed.starts_with("-- @load_csv ")) {
            auto args = trimmed.substr(13); // strlen("-- @load_csv ")
            std::istringstream iss(args);
            sql_csv_entry_t entry;
            iss >> entry.path >> entry.table;
            std::string delim;
            if (iss >> delim && !delim.empty()) {
                entry.delimiter = delim[0];
            }
            if (!entry.path.empty() && !entry.table.empty()) {
                data.csv_entries.push_back(std::move(entry));
            }
            continue;
        }

        sql_lines += line + "\n";
    }

    // Strip comments from the SQL portion
    data.sql = strip_comments_and_directives(sql_lines);
    data.sql = trim(data.sql);

    return data;
}

// --- CSV helpers (same logic as interpreted_benchmark.cpp) ---

std::vector<std::string> split_csv_line(const std::string& line, char delimiter) {
    std::vector<std::string> fields;
    std::string field;
    for (char ch : line) {
        if (ch == delimiter) {
            fields.push_back(trim(field));
            field.clear();
        } else {
            field += ch;
        }
    }
    auto trimmed = trim(field);
    if (!trimmed.empty() || !fields.empty()) {
        fields.push_back(trimmed);
    }
    // Remove trailing empty field (TPC-H .tbl files end with delimiter)
    if (!fields.empty() && fields.back().empty()) {
        fields.pop_back();
    }
    return fields;
}

std::string escape_sql_string(const std::string& s) {
    std::string result;
    result.reserve(s.size());
    for (char ch : s) {
        if (ch == '\'') {
            result += "''";
        } else {
            result += ch;
        }
    }
    return result;
}

bool is_numeric(const std::string& s) {
    if (s.empty()) return false;
    size_t start = 0;
    if (s[0] == '-' || s[0] == '+') start = 1;
    if (start >= s.size()) return false;
    bool has_dot = false;
    for (size_t i = start; i < s.size(); ++i) {
        if (s[i] == '.') {
            if (has_dot) return false;
            has_dot = true;
        } else if (!std::isdigit(static_cast<unsigned char>(s[i]))) {
            return false;
        }
    }
    return true;
}

} // namespace

sql_benchmark_t::sql_benchmark_t(std::string name,
                                 std::string group,
                                 std::string sql,
                                 std::string setup_sql,
                                 std::vector<sql_csv_entry_t> csv_entries,
                                 std::filesystem::path benchmark_dir,
                                 std::string database)
    : name_(std::move(name))
    , group_(std::move(group))
    , sql_(std::move(sql))
    , setup_sql_(std::move(setup_sql))
    , csv_entries_(std::move(csv_entries))
    , benchmark_dir_(std::move(benchmark_dir))
    , database_(std::move(database)) {}

std::string sql_benchmark_t::name() const { return name_; }
std::string sql_benchmark_t::group() const { return group_; }
std::string sql_benchmark_t::description() const { return "SQL: " + name_; }
std::string sql_benchmark_t::query() const { return sql_; }

void sql_benchmark_t::execute_sql_block(benchmark_state_t& state, const std::string& sql) {
    std::string current;

    for (char ch : sql) {
        if (ch == ';') {
            auto stmt = trim(current);
            if (!stmt.empty()) {
                auto cursor = state.dispatcher->execute_sql(state.session, stmt);
                if (cursor->is_error()) {
                    throw std::runtime_error("SQL error: " + cursor->get_error().what);
                }
            }
            current.clear();
        } else {
            current += ch;
        }
    }

    auto stmt = trim(current);
    if (!stmt.empty()) {
        auto cursor = state.dispatcher->execute_sql(state.session, stmt);
        if (cursor->is_error()) {
            throw std::runtime_error("SQL error: " + cursor->get_error().what);
        }
    }
}

void sql_benchmark_t::load_csv_file(benchmark_state_t& state, const sql_csv_entry_t& entry) {
    auto csv_path = std::filesystem::path(entry.path);
    if (!csv_path.is_absolute()) {
        csv_path = benchmark_dir_ / csv_path;
    }

    std::ifstream file(csv_path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open CSV file: " + csv_path.string());
    }

    // Read header line to get column names
    std::string header_line;
    if (!std::getline(file, header_line)) {
        throw std::runtime_error("CSV file is empty: " + csv_path.string());
    }
    auto columns = split_csv_line(header_line, entry.delimiter);
    if (columns.empty()) {
        throw std::runtime_error("CSV file has no columns: " + csv_path.string());
    }

    // Build column list string
    std::string col_list;
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) col_list += ", ";
        col_list += columns[i];
    }

    // Read data lines and batch INSERT
    constexpr size_t batch_size = 100;
    std::vector<std::string> value_tuples;
    uint64_t row_num = 0;
    std::string line;

    auto qualified_table = database_.empty() ? entry.table : database_ + "." + entry.table;

    auto flush_batch = [&]() {
        if (value_tuples.empty()) return;
        std::string sql = "INSERT INTO " + qualified_table + " (" + col_list + ") VALUES ";
        for (size_t i = 0; i < value_tuples.size(); ++i) {
            if (i > 0) sql += ", ";
            sql += value_tuples[i];
        }
        auto cursor = state.dispatcher->execute_sql(state.session, sql);
        if (cursor->is_error()) {
            throw std::runtime_error("CSV load SQL error for " + entry.table + ": " + cursor->get_error().what);
        }
        value_tuples.clear();
    };

    while (std::getline(file, line)) {
        auto trimmed_line = trim(line);
        if (trimmed_line.empty()) continue;

        auto fields = split_csv_line(trimmed_line, entry.delimiter);
        ++row_num;

        std::string tuple = "(";
        for (size_t i = 0; i < columns.size(); ++i) {
            if (i > 0) tuple += ", ";
            if (i < fields.size() && !fields[i].empty()) {
                if (is_numeric(fields[i])) {
                    tuple += fields[i];
                } else {
                    tuple += "'" + escape_sql_string(fields[i]) + "'";
                }
            } else {
                tuple += "NULL";
            }
        }
        tuple += ")";
        value_tuples.push_back(std::move(tuple));

        if (value_tuples.size() >= batch_size) {
            flush_batch();
        }
    }
    flush_batch();

    std::cout << "  Loaded " << row_num << " rows from " << csv_path.filename().string() << " into " << entry.table
              << "\n";
}

std::string sql_benchmark_t::qualify_sql(const std::string& sql) const {
    if (database_.empty()) return sql;

    std::string result = sql;

    // Collect table names from csv_entries and qualify whole-word occurrences
    for (const auto& entry : csv_entries_) {
        const auto& tbl = entry.table;
        std::string qualified = database_ + "." + tbl;
        size_t pos = 0;

        while ((pos = result.find(tbl, pos)) != std::string::npos) {
            // Skip if already qualified (preceded by '.')
            if (pos > 0 && result[pos - 1] == '.') {
                pos += tbl.size();
                continue;
            }
            // Check whole-word boundaries
            auto is_ident = [](char c) {
                return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
            };
            bool start_ok = (pos == 0 || !is_ident(result[pos - 1]));
            bool end_ok = (pos + tbl.size() >= result.size() || !is_ident(result[pos + tbl.size()]));

            if (start_ok && end_ok) {
                result.replace(pos, tbl.size(), qualified);
                pos += qualified.size();
            } else {
                pos += tbl.size();
            }
        }
    }

    return result;
}

void sql_benchmark_t::load(benchmark_state_t& state) {
    // Create database if specified
    if (!database_.empty()) {
        auto create_db = "CREATE DATABASE " + database_;
        auto cursor = state.dispatcher->execute_sql(state.session, create_db);
        if (cursor->is_error()) {
            throw std::runtime_error("Cannot create database: " + cursor->get_error().what);
        }
    }

    if (!setup_sql_.empty()) {
        execute_sql_block(state, qualify_sql(setup_sql_));
    }
    for (const auto& entry : csv_entries_) {
        load_csv_file(state, entry);
    }
}

void sql_benchmark_t::run(benchmark_state_t& state) {
    auto qualified = qualify_sql(sql_);
    auto cursor = state.dispatcher->execute_sql(state.session, qualified);
    if (cursor->is_error()) {
        throw std::runtime_error("SQL error: " + cursor->get_error().what);
    }
}

std::vector<std::unique_ptr<sql_benchmark_t>>
sql_benchmark_t::load_from_file(const std::filesystem::path& path, const std::filesystem::path& base_dir) {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open SQL file: " + path.string());
    }

    std::ostringstream ss;
    ss << file.rdbuf();
    auto raw = ss.str();

    auto cleaned = strip_comments_and_directives(raw);
    auto queries = split_queries(cleaned);

    if (queries.empty()) {
        throw std::runtime_error("No SQL queries found in: " + path.string());
    }

    // Look for _setup.sql in same directory
    auto setup_path = path.parent_path() / "_setup.sql";
    setup_data_t setup;
    if (std::filesystem::exists(setup_path)) {
        setup = parse_setup_file(setup_path);
    }

    auto benchmark_dir = path.parent_path();

    std::vector<std::unique_ptr<sql_benchmark_t>> result;
    auto base_name = make_relative_name(path, base_dir);
    auto group = make_group(path, base_dir);

    if (queries.size() == 1) {
        result.push_back(std::unique_ptr<sql_benchmark_t>(
            new sql_benchmark_t(base_name, group, std::move(queries[0]),
                                setup.sql, setup.csv_entries, benchmark_dir,
                                setup.database)));
    } else {
        for (size_t i = 0; i < queries.size(); ++i) {
            auto name = base_name + "/q" + std::to_string(i + 1);
            result.push_back(std::unique_ptr<sql_benchmark_t>(
                new sql_benchmark_t(name, group, std::move(queries[i]),
                                    setup.sql, setup.csv_entries, benchmark_dir,
                                    setup.database)));
        }
    }

    return result;
}

} // namespace otterbrix::benchmark
