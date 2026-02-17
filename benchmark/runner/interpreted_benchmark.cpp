#include "interpreted_benchmark.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

namespace otterbrix::benchmark {

namespace {

const std::vector<std::string> directives = {
    "name", "group", "description", "runs", "timeout", "load", "run", "result", "cleanup", "load_csv"};

bool is_directive(const std::string& line) {
    for (const auto& d : directives) {
        if (line == d || line.starts_with(d + " ")) {
            return true;
        }
    }
    return false;
}

std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

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

csv_load_entry_t parse_load_csv_args(const std::string& args) {
    csv_load_entry_t entry;
    std::istringstream iss(args);
    iss >> entry.path >> entry.table;
    std::string delim;
    if (iss >> delim && !delim.empty()) {
        entry.delimiter = delim[0];
    }
    if (entry.path.empty() || entry.table.empty()) {
        throw std::runtime_error("load_csv requires: <file_path> <db.table> [delimiter]");
    }
    return entry;
}

} // namespace

interpreted_benchmark_t::interpreted_benchmark_t(const std::filesystem::path& path) { parse(path); }

std::string interpreted_benchmark_t::name() const { return name_; }
std::string interpreted_benchmark_t::group() const { return group_; }
std::string interpreted_benchmark_t::description() const { return description_; }
std::string interpreted_benchmark_t::query() const { return run_sql_; }
uint64_t interpreted_benchmark_t::nruns() const { return nruns_; }
uint64_t interpreted_benchmark_t::timeout_seconds() const { return timeout_; }

void interpreted_benchmark_t::parse(const std::filesystem::path& path) {
    benchmark_dir_ = path.parent_path();

    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open benchmark file: " + path.string());
    }

    std::string current_section;
    std::string current_body;
    std::string line;

    auto flush_section = [&]() {
        if (current_section.empty()) return;
        auto body = trim(current_body);

        if (current_section == "name") {
            name_ = body;
        } else if (current_section == "group") {
            group_ = body;
        } else if (current_section == "description") {
            description_ = body;
        } else if (current_section == "runs") {
            nruns_ = std::stoull(body);
        } else if (current_section == "timeout") {
            timeout_ = std::stoull(body);
        } else if (current_section == "load") {
            load_sql_ = body;
        } else if (current_section == "run") {
            run_sql_ = body;
        } else if (current_section == "result") {
            expected_rows_ = std::stoll(body);
        } else if (current_section == "cleanup") {
            cleanup_sql_ = body;
        } else if (current_section == "load_csv") {
            load_csv_entries_.push_back(parse_load_csv_args(body));
        }

        current_section.clear();
        current_body.clear();
    };

    while (std::getline(file, line)) {
        auto trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            if (!current_section.empty() &&
                (current_section == "load" || current_section == "run" || current_section == "cleanup")) {
                current_body += "\n";
            }
            continue;
        }

        if (is_directive(trimmed)) {
            flush_section();
            auto space_pos = trimmed.find(' ');
            if (space_pos != std::string::npos) {
                current_section = trimmed.substr(0, space_pos);
                current_body = trimmed.substr(space_pos + 1);
            } else {
                current_section = trimmed;
                current_body.clear();
            }
        } else {
            if (!current_body.empty()) {
                current_body += "\n";
            }
            current_body += trimmed;
        }
    }
    flush_section();

    if (name_.empty()) {
        throw std::runtime_error("Benchmark file missing 'name': " + path.string());
    }
    if (run_sql_.empty()) {
        throw std::runtime_error("Benchmark file missing 'run': " + path.string());
    }
}

void interpreted_benchmark_t::execute_sql_block(benchmark_state_t& state, const std::string& sql) {
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

void interpreted_benchmark_t::load_csv_file(benchmark_state_t& state, const csv_load_entry_t& entry) {
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

    auto flush_batch = [&]() {
        if (value_tuples.empty()) return;
        std::string sql = "INSERT INTO " + entry.table + " (" + col_list + ") VALUES ";
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

void interpreted_benchmark_t::load(benchmark_state_t& state) {
    if (!load_sql_.empty()) {
        execute_sql_block(state, load_sql_);
    }
    for (const auto& entry : load_csv_entries_) {
        load_csv_file(state, entry);
    }
}

void interpreted_benchmark_t::run(benchmark_state_t& state) { execute_sql_block(state, run_sql_); }

void interpreted_benchmark_t::cleanup(benchmark_state_t& state) {
    if (!cleanup_sql_.empty()) {
        execute_sql_block(state, cleanup_sql_);
    }
}

std::string interpreted_benchmark_t::verify(benchmark_state_t& state) {
    if (expected_rows_ < 0) return "";

    auto cursor = state.dispatcher->execute_sql(state.session, run_sql_);
    if (cursor->is_error()) {
        return "Verification SQL error: " + cursor->get_error().what;
    }

    auto actual = static_cast<int64_t>(cursor->size());
    if (actual != expected_rows_) {
        return "Expected " + std::to_string(expected_rows_) + " rows, got " + std::to_string(actual);
    }
    return "";
}

} // namespace otterbrix::benchmark
