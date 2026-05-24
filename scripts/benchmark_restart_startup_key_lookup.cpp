#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

[[noreturn]] void die(const std::string& msg) {
    std::cerr << "ERROR: " << msg << '\n';
    std::exit(1);
}

struct Options {
    std::string runner;
    fs::path workspace = "/tmp/otterbrix_restart_startup_bench";
    int rows = 0;
    int payload_bytes = 512;
    int runs = 7;
    bool shuffle_ids = false;
    int seed = 1234567;
    bool show_runner_output = false;
};

Options parse_args(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto read_value = [&](const std::string& name) -> std::string {
            if (i + 1 >= argc) {
                die("missing value for " + name);
            }
            return argv[++i];
        };

        if (arg == "--runner") opt.runner = read_value(arg);
        else if (arg == "--workspace") opt.workspace = read_value(arg);
        else if (arg == "--rows") opt.rows = std::stoi(read_value(arg));
        else if (arg == "--payload-bytes") opt.payload_bytes = std::stoi(read_value(arg));
        else if (arg == "--runs") opt.runs = std::stoi(read_value(arg));
        else if (arg == "--seed") opt.seed = std::stoi(read_value(arg));
        else if (arg == "--shuffle-ids") opt.shuffle_ids = true;
        else if (arg == "--show-runner-output") opt.show_runner_output = true;
        else if (arg == "--help") {
            std::cout
                << "Usage: benchmark_restart_startup_key_lookup [options]\n"
                << "  --runner PATH\n"
                << "  --workspace PATH\n"
                << "  --rows N\n"
                << "  --payload-bytes N\n"
                << "  --runs N\n"
                << "  --shuffle-ids\n"
                << "  --seed N\n"
                << "  --show-runner-output\n";
            std::exit(0);
        } else {
            die("unknown argument: " + arg);
        }
    }
    return opt;
}

void generate_csv(const fs::path& csv_path, int rows, int payload_bytes, bool shuffle_ids) {
    if (rows <= 0) die("--rows must be > 0");
    std::ofstream out(csv_path);
    if (!out) die("failed to open " + csv_path.string());

    const std::string payload(static_cast<size_t>(payload_bytes), 'x');
    out << "id,payload\n";
    if (shuffle_ids) {
        const int step = rows - 1;
        for (int i = 0; i < rows; ++i) {
            const int row_id = ((i * step) % rows) + 1;
            out << row_id << ',' << payload << '\n';
        }
    } else {
        for (int i = 0; i < rows; ++i) {
            out << (i + 1) << ',' << payload << '\n';
        }
    }
}

void create_benchmark_layout(const fs::path& directory, const std::string& setup_sql) {
    fs::create_directories(directory);
    std::ofstream out(directory / "_setup.sql");
    if (!out) die("failed to open _setup.sql in " + directory.string());
    out << setup_sql << '\n';
}

void generate_lookup_sql(const fs::path& query_path, const std::string& db_name, int rows, int seed) {
    std::mt19937 rng(static_cast<uint32_t>(seed));
    std::uniform_int_distribution<int> dist(1, rows);
    const int key = dist(rng);
    std::ofstream out(query_path);
    if (!out) die("failed to open " + query_path.string());
    out << "-- @expected_rows 1\n";
    out << "SELECT * FROM " << db_name << ".kv WHERE id = " << key << ";\n";
}

void run_process(const std::vector<std::string>& args, const fs::path& cwd, bool suppress_output) {
    pid_t pid = fork();
    if (pid < 0) die("fork failed");

    if (pid == 0) {
        if (chdir(cwd.c_str()) != 0) _exit(127);
        if (suppress_output) {
            const int devnull = open("/dev/null", O_WRONLY);
            if (devnull >= 0) {
                dup2(devnull, STDOUT_FILENO);
                dup2(devnull, STDERR_FILENO);
                close(devnull);
            }
        }

        std::vector<char*> cargs;
        cargs.reserve(args.size() + 1);
        for (const auto& s : args) cargs.push_back(const_cast<char*>(s.c_str()));
        cargs.push_back(nullptr);
        execvp(cargs[0], cargs.data());
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) die("waitpid failed");
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        std::ostringstream os;
        os << "process failed: " << args[0];
        die(os.str());
    }
}

struct RestartMetrics {
    double avg_ms = 0;
    double median_ms = 0;
    std::string verified = "FAIL";
    double restart_wall_ms = 0;
    double startup_overhead_ms = 0;
};

std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string part;
    while (std::getline(ss, part, ',')) fields.push_back(part);
    return fields;
}

RestartMetrics run_restart_and_measure(
    const fs::path& runner,
    const fs::path& scenario_dir,
    int runs,
    bool suppress_output) {
    const fs::path out_csv = scenario_dir / "restart_result.csv";
    std::vector<std::string> cmd{
        runner.string(),
        "--file=lookup.sql",
        "--runs=" + std::to_string(runs),
        "--disk",
        "--skip-load",
        "--out=" + out_csv.string(),
    };
    const auto wall_start = std::chrono::steady_clock::now();
    run_process(cmd, scenario_dir, suppress_output);
    const auto wall_end = std::chrono::steady_clock::now();
    const double restart_wall_ms =
        std::chrono::duration<double, std::milli>(wall_end - wall_start).count();

    std::ifstream in(out_csv);
    if (!in) die("cannot open restart csv: " + out_csv.string());
    std::string header;
    if (!std::getline(in, header)) die("cannot parse restart csv header");
    const auto cols = split_csv_line(header);
    std::map<std::string, int> col_idx;
    for (int i = 0; i < static_cast<int>(cols.size()); ++i) col_idx[cols[i]] = i;

    if (!col_idx.count("avg_ms") || !col_idx.count("median_ms") || !col_idx.count("nruns") || !col_idx.count("verified")) {
        die("restart csv missing required columns");
    }

    int nrows = 0;
    double avg_sum = 0;
    double median_sum = 0;
    double timed_total_ms = 0;
    bool all_ok = true;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        const auto f = split_csv_line(line);
        const auto need = std::max({col_idx["avg_ms"], col_idx["median_ms"], col_idx["nruns"], col_idx["verified"]});
        if (static_cast<int>(f.size()) <= need) continue;
        const double avg = std::stod(f[col_idx["avg_ms"]]);
        const double med = std::stod(f[col_idx["median_ms"]]);
        const double nruns = std::stod(f[col_idx["nruns"]]);
        avg_sum += avg;
        median_sum += med;
        timed_total_ms += avg * nruns;
        all_ok = all_ok && (f[col_idx["verified"]] == "OK");
        ++nrows;
    }

    if (nrows == 0) die("Cannot parse restart benchmark output for scenario '" + scenario_dir.filename().string() + "'");

    RestartMetrics m;
    m.avg_ms = avg_sum / nrows;
    m.median_ms = median_sum / nrows;
    m.verified = all_ok ? "OK" : "FAIL";
    m.restart_wall_ms = restart_wall_ms;
    m.startup_overhead_ms = std::max(0.0, restart_wall_ms - timed_total_ms);
    return m;
}

double run_load_and_shutdown(const fs::path& runner, const fs::path& scenario_dir, bool suppress_output) {
    std::vector<std::string> cmd{
        runner.string(),
        "--file=lookup.sql",
        "--disk",
        "--load-only",
    };
    const auto wall_start = std::chrono::steady_clock::now();
    run_process(cmd, scenario_dir, suppress_output);
    const auto wall_end = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(wall_end - wall_start).count();
}

std::string human_size(const fs::path& p) {
    const auto size = fs::file_size(p);
    constexpr std::string_view units[] = {"B", "K", "M", "G", "T"};
    double value = static_cast<double>(size);
    for (size_t i = 0; i < std::size(units); ++i) {
        if (value < 1024.0 || i + 1 == std::size(units)) {
            std::ostringstream os;
            if (units[i] == "B") os << static_cast<long long>(value) << units[i];
            else os << std::fixed << std::setprecision(1) << value << units[i];
            return os.str();
        }
        value /= 1024.0;
    }
    return std::to_string(size) + "B";
}

void print_table_header() {
    std::cout
        << "scenario                 | load+shutdown  | restart avg  | restart median | restart wall  | startup overhead | status  \n"
        << "-------------------------+----------------+--------------+----------------+---------------+------------------+---------\n";
}

void print_table_row(
    const std::string& scenario,
    double load_shutdown_ms,
    double restart_avg_ms,
    double restart_median_ms,
    double restart_wall_ms,
    double startup_overhead_ms,
    const std::string& status) {
    std::cout << std::left << std::setw(24) << scenario << " | "
              << std::right << std::setw(11) << std::fixed << std::setprecision(3) << load_shutdown_ms << " ms | "
              << std::setw(9) << restart_avg_ms << " ms | "
              << std::setw(11) << restart_median_ms << " ms | "
              << std::setw(10) << restart_wall_ms << " ms | "
              << std::setw(13) << startup_overhead_ms << " ms | "
              << std::left << ' ' << std::setw(8) << status << '\n';
}

int main(int argc, char** argv) {
    const Options args = parse_args(argc, argv);
    if (args.runner.empty()) die("benchmark_runner not found. Use --runner PATH.");
    const fs::path runner = fs::absolute(args.runner);
    if (!fs::exists(runner)) die("runner does not exist: " + runner.string());

    if (fs::exists(args.workspace)) fs::remove_all(args.workspace);
    fs::create_directories(args.workspace);

    try {
        const fs::path csv_path = args.workspace / "data.csv";
        std::cout << "Generating dataset: rows=" << args.rows << ", payload_bytes=" << args.payload_bytes << '\n';
        const auto gen_start = std::chrono::steady_clock::now();
        generate_csv(csv_path, args.rows, args.payload_bytes, args.shuffle_ids);
        const auto gen_end = std::chrono::steady_clock::now();
        const double gen_ms = std::chrono::duration<double, std::milli>(gen_end - gen_start).count();
        std::cout << "Dataset file: " << csv_path.string() << " (" << human_size(csv_path) << "), generated in "
                  << std::fixed << std::setprecision(1) << gen_ms << " ms\n";

        const auto ts = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::ostringstream bench_tag;
        bench_tag << ts << '_' << getpid();
        const std::string db_name = "benchdb_" + bench_tag.str();

        const fs::path no_index_dir = args.workspace / "scenario_no_index";
        const fs::path single_index_dir = args.workspace / "scenario_single_field_index";
        const fs::path hash_index_dir = args.workspace / "scenario_hash_single_field_index";

        const std::string load_setup_sql =
            "-- @database " + db_name + "\n"
            "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
            "-- @load_csv " + csv_path.string() + " kv ,";

        create_benchmark_layout(no_index_dir, load_setup_sql);
        generate_lookup_sql(no_index_dir / "lookup.sql", db_name, args.rows, args.seed);
        create_benchmark_layout(single_index_dir, load_setup_sql + "\nCREATE INDEX idx_id ON " + db_name + ".kv (id);");
        generate_lookup_sql(single_index_dir / "lookup.sql", db_name, args.rows, args.seed);
        create_benchmark_layout(hash_index_dir, load_setup_sql + "\nCREATE INDEX idx_id_hash ON " + db_name + ".kv USING hash (id);");
        generate_lookup_sql(hash_index_dir / "lookup.sql", db_name, args.rows, args.seed);

        const std::vector<std::pair<std::string, fs::path>> scenarios{
            {"no_index", no_index_dir},
            {"single_field_index", single_index_dir},
            {"hash_single_field_index", hash_index_dir},
        };

        std::cout << "\nRunning restart startup benchmark: phase1(load+shutdown) -> phase2(restart with --skip-load)...\n";
        print_table_header();

        for (const auto& [name, dir] : scenarios) {
            const double load_shutdown_ms = run_load_and_shutdown(runner, dir, !args.show_runner_output);
            const RestartMetrics m = run_restart_and_measure(runner, dir, args.runs, !args.show_runner_output);
            print_table_row(
                name,
                load_shutdown_ms,
                m.avg_ms,
                m.median_ms,
                m.restart_wall_ms,
                m.startup_overhead_ms,
                m.verified);
        }
    } catch (const std::exception& e) {
        fs::remove_all(args.workspace);
        die(e.what());
    }

    fs::remove_all(args.workspace);
    return 0;
}
