// Write-path profiling driver: measures INSERT / UPDATE / DELETE throughput and
// per-statement latency across the variable grid (interface, index, primary key,
// table width, catalog size, log level).
// Opens the engine the way base_otterbrix_t::open does, but with a counting resource between the
// engine and its pool, and talks to it ONLY through that host's wrapper_dispatcher_t — execute_sql
// for the SQL path and execute_plan for the logical_plan path. All chunks and plans are built on
// dispatcher()->resource(), i.e. on the counting resource.

#include <components/configuration/configuration.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>
#include <integration/cpp/wrapper_dispatcher.hpp>
#include <services/engine/engine.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>

#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

    using clock_t_ = std::chrono::steady_clock;

    // Global heap traffic, counted by the operator new/delete replacements below.
    std::atomic<std::uint64_t> heap_allocations{0};
    std::atomic<std::uint64_t> heap_bytes{0};

    struct options_t {
        std::string workload{"bulk"}; // single | bulk | update | delete | mixed
        std::string iface{"sql"};     // sql | plan
        std::string log{"off"};       // off | trace
        std::uint64_t rows{100000};
        std::uint64_t batch{1000};
        std::uint64_t width{4};
        bool index{false};
        bool pk{false};
        bool csv{false};
        std::uint64_t repeat{1}; // repeats the measured UPDATE (long enough to profile)
        // Filler tables created alongside the target one. The catalog reads every
        // statement performs are full scans of pg_class / pg_attribute, so their cost
        // grows with the number of catalog rows — this makes that growth measurable.
        std::uint64_t tables{1};
        // Milliseconds to sleep AFTER the seed load and BEFORE the measured statements,
        // so a sampling profiler can be aimed at the measured window alone: profiling
        // the whole process mixes the seed INSERT into the DELETE/UPDATE profile.
        std::uint64_t pause_before_dml_ms{0};
    };

    bool starts_with(const std::string& s, const char* prefix) { return s.rfind(prefix, 0) == 0; }

    std::string tail_of(const std::string& s, const char* prefix) { return s.substr(std::strlen(prefix)); }

    bool parse_options(int argc, char** argv, options_t& o) {
        for (int i = 1; i < argc; ++i) {
            const std::string a = argv[i];
            if (starts_with(a, "--workload=")) {
                o.workload = tail_of(a, "--workload=");
            } else if (starts_with(a, "--iface=")) {
                o.iface = tail_of(a, "--iface=");
            } else if (starts_with(a, "--log=")) {
                o.log = tail_of(a, "--log=");
            } else if (starts_with(a, "--rows=")) {
                o.rows = std::strtoull(tail_of(a, "--rows=").c_str(), nullptr, 10);
            } else if (starts_with(a, "--batch=")) {
                o.batch = std::strtoull(tail_of(a, "--batch=").c_str(), nullptr, 10);
            } else if (starts_with(a, "--tables=")) {
                o.tables = std::strtoull(tail_of(a, "--tables=").c_str(), nullptr, 10);
            } else if (starts_with(a, "--pause-before-dml=")) {
                o.pause_before_dml_ms = std::strtoull(tail_of(a, "--pause-before-dml=").c_str(), nullptr, 10);
            } else if (starts_with(a, "--repeat=")) {
                o.repeat = std::strtoull(tail_of(a, "--repeat=").c_str(), nullptr, 10);
            } else if (starts_with(a, "--width=")) {
                o.width = std::strtoull(tail_of(a, "--width=").c_str(), nullptr, 10);
            } else if (a == "--index") {
                o.index = true;
            } else if (a == "--pk") {
                o.pk = true;
            } else if (a == "--csv") {
                o.csv = true;
            } else {
                std::cerr << "unknown argument: " << a << "\n";
                return false;
            }
        }
        if (o.batch == 0) {
            std::cerr << "--batch must be > 0\n";
            return false;
        }
        return true;
    }

    configuration::config make_config(const options_t& o) {
        // One named base dir via create_config -- hand-assigning `current_path()/"disk"`
        // and `.../"wal"` instead scatters both into whatever directory the profiler was
        // launched from.
        auto cfg = configuration::config::create_config(std::filesystem::current_path() /
                                                        "otterbrix_write_profile_data");
        cfg.log.level = (o.log == "trace") ? log_t::level::trace : log_t::level::off;
        // The WAL is NOT optional here: a run without it misleads -- DELETE column
        // pruning measured 3.6x without durable writes and exactly nothing with them.
        return cfg;
    }

    // Per-statement latency samples, in microseconds.
    struct stats_t {
        std::vector<double> samples;

        void add(double us) { samples.push_back(us); }

        double percentile(double p) {
            if (samples.empty()) {
                return 0.0;
            }
            std::sort(samples.begin(), samples.end());
            const auto idx = static_cast<std::size_t>(p * static_cast<double>(samples.size() - 1));
            return samples[idx];
        }
    };

    bool report_failure(const components::cursor::cursor_t_ptr& cur, const std::string& what) {
        if (cur->is_success()) {
            return true;
        }
        std::cerr << "FAILED: " << what.substr(0, 160) << "\n  " << cur->get_error().what << "\n";
        return false;
    }

    bool run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, stats_t* stats) {
        auto session = otterbrix::session_id_t();
        const auto t0 = clock_t_::now();
        auto cur = dispatcher->execute_sql(session, sql);
        const auto t1 = clock_t_::now();
        if (stats != nullptr) {
            stats->add(std::chrono::duration<double, std::micro>(t1 - t0).count());
        }
        return report_failure(cur, sql);
    }

    std::string column_list(const options_t& o) {
        std::string cols = "id, k";
        for (std::uint64_t c = 0; c < o.width; ++c) {
            cols += ", v" + std::to_string(c);
        }
        return cols;
    }

    // Column types mirror column_list(): all BIGINT, so the plan path writes cells
    // through typed accessors.
    std::pmr::vector<components::types::complex_logical_type> column_types(const options_t& o,
                                                                          std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types{resource};
        types.emplace_back(components::types::logical_type::BIGINT, std::string("id"));
        types.emplace_back(components::types::logical_type::BIGINT, std::string("k"));
        for (std::uint64_t c = 0; c < o.width; ++c) {
            types.emplace_back(components::types::logical_type::BIGINT, std::string("v" + std::to_string(c)));
        }
        return types;
    }

    bool create_schema(otterbrix::wrapper_dispatcher_t* dispatcher, const options_t& o) {
        if (!run_sql(dispatcher, "CREATE SCHEMA wp;", nullptr)) {
            return false;
        }
        std::string ddl = "CREATE TABLE wp.t (id bigint, k bigint";
        for (std::uint64_t c = 0; c < o.width; ++c) {
            ddl += ", v" + std::to_string(c) + " bigint";
        }
        ddl += ") ;";
        if (!run_sql(dispatcher, ddl, nullptr)) {
            return false;
        }
        // Inline CREATE TABLE constraints are currently dropped by the planner, so the
        // primary key has to be declared through ALTER TABLE to actually exist.
        if (o.pk && !run_sql(dispatcher, "ALTER TABLE wp.t ADD CONSTRAINT t_pk PRIMARY KEY (id);", nullptr)) {
            return false;
        }
        if (o.index && !run_sql(dispatcher, "CREATE INDEX t_k_idx ON wp.t (k);", nullptr)) {
            return false;
        }
        // Filler tables: never written to, they only enlarge the catalog (see options_t::tables).
        for (std::uint64_t t = 1; t < o.tables; ++t) {
            std::string filler = "CREATE TABLE wp.f" + std::to_string(t) + " (id bigint, k bigint";
            for (std::uint64_t c = 0; c < o.width; ++c) {
                filler += ", v" + std::to_string(c) + " bigint";
            }
            filler += ");";
            if (!run_sql(dispatcher, filler, nullptr)) {
                return false;
            }
        }
        return true;
    }

    std::string insert_statement(const options_t& o, std::uint64_t first_row, std::uint64_t count) {
        std::string sql = "INSERT INTO wp.t (" + column_list(o) + ") VALUES ";
        for (std::uint64_t r = 0; r < count; ++r) {
            const std::uint64_t id = first_row + r;
            if (r != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(id) + ", " + std::to_string(id % 1000);
            for (std::uint64_t c = 0; c < o.width; ++c) {
                sql += ", " + std::to_string(id + c);
            }
            sql += ")";
        }
        sql += ";";
        return sql;
    }

    bool insert_batch_plan(otterbrix::wrapper_dispatcher_t* dispatcher,
                           const options_t& o,
                           std::uint64_t first_row,
                           std::uint64_t count,
                           stats_t* stats) {
        auto* resource = dispatcher->resource();
        auto types = column_types(o, resource);
        components::vector::data_chunk_t chunk{resource, types, count};
        for (std::uint64_t r = 0; r < count; ++r) {
            const auto id = static_cast<std::int64_t>(first_row + r);
            chunk.data[0].data<std::int64_t>()[r] = id;
            chunk.data[1].data<std::int64_t>()[r] = id % 1000;
            for (std::uint64_t c = 0; c < o.width; ++c) {
                chunk.data[2 + c].data<std::int64_t>()[r] = id + static_cast<std::int64_t>(c);
            }
        }
        chunk.set_cardinality(count);

        auto node = components::sql::transform::name_catalog_target(
            core::dbname_t{"wp"},
            core::relname_t{"t"},
            components::logical_plan::make_node_insert(resource, std::move(chunk)));

        auto session = otterbrix::session_id_t();
        const auto t0 = clock_t_::now();
        auto cur = dispatcher->execute_plan(session,
                                            components::logical_plan::execution_plan_t{resource, node, nullptr});
        const auto t1 = clock_t_::now();
        if (stats != nullptr) {
            stats->add(std::chrono::duration<double, std::micro>(t1 - t0).count());
        }
        return report_failure(cur, "plan insert");
    }

    bool load_rows(otterbrix::wrapper_dispatcher_t* dispatcher,
                   const options_t& o,
                   std::uint64_t rows,
                   stats_t* stats) {
        for (std::uint64_t done = 0; done < rows; done += o.batch) {
            const std::uint64_t count = std::min(o.batch, rows - done);
            const bool ok = (o.iface == "plan") ? insert_batch_plan(dispatcher, o, done, count, stats)
                                                : run_sql(dispatcher, insert_statement(o, done, count), stats);
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    struct alloc_snapshot_t {
        std::uint64_t allocations{0};
        std::uint64_t bytes{0};
        std::uint64_t deallocations{0};
        std::uint64_t heap_allocations{0};
        std::uint64_t heap_bytes{0};
    };

    alloc_snapshot_t alloc_now(const core::pmr::counting_resource_t& counter) {
        return {counter.allocations(),
                counter.allocated_bytes(),
                counter.deallocations(),
                heap_allocations.load(std::memory_order_relaxed),
                heap_bytes.load(std::memory_order_relaxed)};
    }

    // Mirrors base_otterbrix_t::open (integration/cpp/base_spaces.cpp), with the counter between
    // the engine and the pool. Member order is the destruction order: wrapper, engine, pools.
    struct host_t final {
        core::pmr::otterbrix_resource pool;
        core::pmr::counting_resource_t counter{&pool};
        log_t log;
        actor_zeta::scheduler_ptr scheduler_general{new actor_zeta::shared_work(3, 1000)};
        actor_zeta::scheduler_ptr scheduler_exec{new actor_zeta::shared_work(3, 1000)};
        actor_zeta::scheduler_ptr scheduler_disk{new actor_zeta::shared_work(3, 1000)};
        std::optional<services::engine::engine_t> engine;
        std::unique_ptr<otterbrix::wrapper_dispatcher_t, actor_zeta::pmr::deleter_t> wrapper{
            nullptr,
            actor_zeta::pmr::deleter_t(&counter)};
    };

    bool open_host(host_t& host, const configuration::config& config) {
        auto log = make_log("otterbrix", config.log.path);
        if (log.has_error()) {
            std::cerr << "make_log failed: " << log.error().what << '\n';
            return false;
        }
        host.log = std::move(log.value());
        host.log.set_level(config.log.level);
        auto engine = services::engine::open_engine(&host.counter,
                                                    services::engine::schedulers_t{host.scheduler_general.get(),
                                                                                   host.scheduler_exec.get(),
                                                                                   host.scheduler_disk.get()},
                                                    config,
                                                    host.log,
                                                    components::planner::primitives_t{});
        if (engine.has_error()) {
            std::cerr << "otterbrix refused to start: " << engine.error().what << '\n';
            return false;
        }
        host.engine.emplace(std::move(engine.value()));
        host.wrapper = actor_zeta::spawn<otterbrix::wrapper_dispatcher_t>(&host.counter,
                                                                          host.engine->dispatcher_address(),
                                                                          host.log);
        return true;
    }

    // The WAL segments and the table tree share one directory (config_disk keeps `<base>/wal`), so the
    // two figures are told apart by the segment file name, not by the directory.
    enum class store_t
    {
        wal,
        disk
    };

    std::uintmax_t directory_bytes(const std::filesystem::path& dir, store_t store) {
        std::error_code ec;
        if (!std::filesystem::exists(dir, ec)) {
            return 0;
        }
        std::uintmax_t total = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir, ec)) {
            if (entry.is_regular_file(ec)) {
                const bool is_wal_segment = starts_with(entry.path().filename().string(), "wal_");
                if (is_wal_segment != (store == store_t::wal)) {
                    continue;
                }
                const auto size = std::filesystem::file_size(entry.path(), ec);
                if (!ec) {
                    total += size;
                }
            }
        }
        return total;
    }

    // CPU consumed by the whole process so far. Wall time on this write path is dominated by
    // waiting, so CPU is the only figure that reacts to the actual work.
    struct cpu_time_t {
        double user_ms{0.0};
        double sys_ms{0.0};
    };

    cpu_time_t cpu_now() {
        rusage usage{};
        if (getrusage(RUSAGE_SELF, &usage) != 0) {
            return {};
        }
        const auto to_ms = [](const timeval& tv) {
            return static_cast<double>(tv.tv_sec) * 1000.0 + static_cast<double>(tv.tv_usec) / 1000.0;
        };
        return {to_ms(usage.ru_utime), to_ms(usage.ru_stime)};
    }

    long peak_rss_kb() {
        rusage usage{};
        if (getrusage(RUSAGE_SELF, &usage) != 0) {
            return 0;
        }
#if defined(__APPLE__)
        return usage.ru_maxrss / 1024; // Darwin reports bytes
#else
        return usage.ru_maxrss;        // Linux reports kilobytes
#endif
    }

} // namespace

// Replacements of the global allocation functions. They live in this translation unit only, so they
// reach this benchmark binary and nothing else. Failure aborts: the project allows no exceptions.
namespace {

    void* counted_alloc(std::size_t size) noexcept {
        heap_allocations.fetch_add(1, std::memory_order_relaxed);
        heap_bytes.fetch_add(size, std::memory_order_relaxed);
        return std::malloc(size != 0 ? size : 1);
    }

    void* counted_alloc_aligned(std::size_t size, std::size_t alignment) noexcept {
        heap_allocations.fetch_add(1, std::memory_order_relaxed);
        heap_bytes.fetch_add(size, std::memory_order_relaxed);
        void* p = nullptr;
        if (alignment < sizeof(void*)) {
            alignment = sizeof(void*);
        }
        if (posix_memalign(&p, alignment, size != 0 ? size : 1) != 0) {
            return nullptr;
        }
        return p;
    }

    void* must(void* p) noexcept {
        if (p == nullptr) {
            std::abort();
        }
        return p;
    }

} // namespace

void* operator new(std::size_t size) { return must(counted_alloc(size)); }
void* operator new[](std::size_t size) { return must(counted_alloc(size)); }
void* operator new(std::size_t size, const std::nothrow_t&) noexcept { return counted_alloc(size); }
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept { return counted_alloc(size); }
void* operator new(std::size_t size, std::align_val_t alignment) {
    return must(counted_alloc_aligned(size, static_cast<std::size_t>(alignment)));
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return must(counted_alloc_aligned(size, static_cast<std::size_t>(alignment)));
}
void* operator new(std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return counted_alloc_aligned(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment, const std::nothrow_t&) noexcept {
    return counted_alloc_aligned(size, static_cast<std::size_t>(alignment));
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept { std::free(p); }
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept { std::free(p); }

int main(int argc, char** argv) {
    options_t options;
    if (!parse_options(argc, argv, options)) {
        return 2;
    }

    const auto config = make_config(options);
    host_t host;
    if (!open_host(host, config)) {
        return 1;
    }
    auto* dispatcher = host.wrapper.get();

    if (!create_schema(dispatcher, options)) {
        return 1;
    }

    stats_t setup_stats;
    stats_t measured;
    // The seed load of the update/delete/mixed workloads is SETUP, not measurement:
    // the clock starts after it, so wall_ms covers only the statements under test.
    auto measured_start = clock_t_::now();
    auto cpu_start = cpu_now();
    auto alloc_start = alloc_now(host.counter);
    std::uint64_t measured_statements = 0;
    std::uint64_t measured_rows = 0;

    if (options.workload == "bulk" || options.workload == "single") {
        if (!load_rows(dispatcher, options, options.rows, &measured)) {
            return 1;
        }
        measured_statements = measured.samples.size();
        measured_rows = options.rows;
    } else if (options.workload == "update" || options.workload == "delete") {
        if (!load_rows(dispatcher, options, options.rows, &setup_stats)) {
            return 1;
        }
        if (options.pause_before_dml_ms != 0) {
            std::cerr << "seed done, sleeping " << options.pause_before_dml_ms << " ms before the measured DML\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(options.pause_before_dml_ms));
        }
        measured_start = clock_t_::now();
        cpu_start = cpu_now();
        alloc_start = alloc_now(host.counter);
        // Touch roughly a quarter of the table in one statement.
        const std::uint64_t hi = options.rows / 4;
        const std::string predicate = " WHERE id < " + std::to_string(hi) + ";";
        const std::string sql = (options.workload == "update")
                                    ? "UPDATE wp.t SET v0 = v0 + 1" + predicate
                                    : "DELETE FROM wp.t" + predicate;
        const std::uint64_t reps = (options.workload == "update") ? std::max<std::uint64_t>(1, options.repeat) : 1;
        for (std::uint64_t i = 0; i < reps; ++i) {
            if (!run_sql(dispatcher, sql, &measured)) {
                return 1;
            }
        }
        measured_statements = reps;
        measured_rows = hi * reps;
    } else if (options.workload == "mixed") {
        if (!load_rows(dispatcher, options, options.rows, &setup_stats)) {
            return 1;
        }
        if (options.pause_before_dml_ms != 0) {
            std::cerr << "seed done, sleeping " << options.pause_before_dml_ms << " ms before the measured DML\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(options.pause_before_dml_ms));
        }
        measured_start = clock_t_::now();
        cpu_start = cpu_now();
        alloc_start = alloc_now(host.counter);
        const std::uint64_t cycles = options.rows / 4;
        for (std::uint64_t i = 0; i < cycles; ++i) {
            const std::uint64_t id = options.rows + i;
            if (!run_sql(dispatcher, insert_statement(options, id, 1), &measured)) {
                return 1;
            }
            if (!run_sql(dispatcher,
                         "UPDATE wp.t SET v0 = v0 + 1 WHERE id = " + std::to_string(i) + ";",
                         &measured)) {
                return 1;
            }
            if (!run_sql(dispatcher, "DELETE FROM wp.t WHERE id = " + std::to_string(i) + ";", &measured)) {
                return 1;
            }
        }
        measured_statements = measured.samples.size();
        measured_rows = cycles * 3;
    } else {
        std::cerr << "unknown --workload: " << options.workload << "\n";
        return 2;
    }

    const auto measured_end = clock_t_::now();
    const auto cpu_end = cpu_now();
    const auto alloc_end = alloc_now(host.counter);
    const std::uint64_t allocs = alloc_end.allocations - alloc_start.allocations;
    const std::uint64_t alloc_bytes = alloc_end.bytes - alloc_start.bytes;
    const std::uint64_t deallocs = alloc_end.deallocations - alloc_start.deallocations;
    const std::uint64_t heap_allocs = alloc_end.heap_allocations - alloc_start.heap_allocations;
    const std::uint64_t heap_alloc_bytes = alloc_end.heap_bytes - alloc_start.heap_bytes;
    const double cpu_user_ms = cpu_end.user_ms - cpu_start.user_ms;
    const double cpu_sys_ms = cpu_end.sys_ms - cpu_start.sys_ms;
    const double wall_ms = std::chrono::duration<double, std::milli>(measured_end - measured_start).count();
    const double wall_s = wall_ms / 1000.0;

    const auto wal_bytes = directory_bytes(config.wal.path, store_t::wal);
    const auto disk_bytes = directory_bytes(config.disk.path, store_t::disk);

    if (options.csv) {
        std::cout << options.workload << ',' << options.iface << ',' << (options.index ? 1 : 0) << ','
                  << (options.pk ? 1 : 0) << ',' << options.log << ',' << options.tables << ',' << options.width
                  << ',' << options.rows << ','
                  << options.batch << ',' << wall_ms << ','
                  << (wall_s > 0.0 ? static_cast<double>(measured_rows) / wall_s : 0.0) << ','
                  << (wall_s > 0.0 ? static_cast<double>(measured_statements) / wall_s : 0.0) << ','
                  << measured.percentile(0.50) << ',' << measured.percentile(0.90) << ','
                  << measured.percentile(0.99) << ',' << measured.percentile(1.0) << ',' << cpu_user_ms << ','
                  << cpu_sys_ms << ',' << peak_rss_kb() << ','
                  << wal_bytes << ',' << disk_bytes << ',' << allocs << ',' << alloc_bytes << ',' << deallocs << ','
                  << heap_allocs << ',' << heap_alloc_bytes << '\n';
    } else {
        std::cout << "workload=" << options.workload << " iface=" << options.iface
                  << " storage=disk wal=on"
                  << " index=" << options.index << " pk=" << options.pk << " log=" << options.log
                  << " tables=" << options.tables << " width=" << options.width << " rows=" << options.rows
                  << " batch=" << options.batch << '\n'
                  << "  wall_ms=" << wall_ms << " rows_per_s=" << (wall_s > 0.0 ? static_cast<double>(measured_rows) / wall_s : 0.0)
                  << " stmts_per_s=" << (wall_s > 0.0 ? static_cast<double>(measured_statements) / wall_s : 0.0) << '\n'
                  << "  lat_us p50=" << measured.percentile(0.50) << " p90=" << measured.percentile(0.90)
                  << " p99=" << measured.percentile(0.99) << " max=" << measured.percentile(1.0) << '\n'
                  << "  cpu_user_ms=" << cpu_user_ms << " cpu_sys_ms=" << cpu_sys_ms << '\n'
                  << "  peak_rss_kb=" << peak_rss_kb() << " wal_bytes=" << wal_bytes
                  << " disk_bytes=" << disk_bytes << '\n'
                  << "  allocs=" << allocs << " alloc_bytes=" << alloc_bytes << " deallocs=" << deallocs << '\n'
                  << "  heap_allocs=" << heap_allocs << " heap_bytes=" << heap_alloc_bytes << '\n';
    }
    return 0;
}
