// Exhaustive crash-point enumeration: for every k, the reopened file must equal root N or root
// N+1, never a third state or an error, proving shadow paging holds at every crash point.
// Fault-seam gotchas (rediscovered at cost): fault_injection_scope_t must wrap the handle BEFORE
// the manager is constructed, or counters read zero; arming aims at base_writes + k, not k alone,
// since a blanket fail_after_writes would also kill data writes; fail_writes_from is 1-based
// (can't express k=0).

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <vector>
#include <unistd.h>

#include "block_reachability_walker.hpp"
#include "fault_injection_file.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t BASE_ROWS = 6000;
    constexpr uint64_t EXTRA_ROWS = 800;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    std::string matrix_db_path(const char* tag) {
        return "/tmp/test_otterbrix_crash_matrix_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct matrix_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        matrix_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(matrix_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "crash_matrix_table");
    }

    std::string row_name(uint64_t row) { return "crash_matrix_row_payload_padding_" + std::to_string(row); }

    void append_rows(data_table_t& table, matrix_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                auto name = row_name(row);
                chunk.set_value(1, i, std::string_view{name});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    struct round_trace_t {
        bool committed{false};
        bool compact_ok{true};
        bool had_error{false};
        const char* stage{"committed"};
        uint64_t writes_after_compact{0};
        uint64_t writes_after_table_ckpt{0};
        uint64_t writes_after_free_list{0};
        uint64_t writes_before_header{0};
        uint64_t writes_total{0};
        uint64_t syncs_total{0};
    };

    round_trace_t run_round(tstorage::single_file_block_manager_t& bm,
                            data_table_t& table,
                            otterbrix_test::fault_plan_t& plan,
                            bool do_compact,
                            bool stop_before_header) {
        round_trace_t t;
        auto snap = [&plan, &t]() {
            t.writes_total = plan.writes_seen;
            t.syncs_total = plan.syncs_seen;
        };
        if (do_compact) {
            t.compact_ok = table.compact(WATERMARK);
        }
        t.writes_after_compact = plan.writes_seen;
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (auto cp = table.checkpoint(writer); cp.has_error()) {
            t.had_error = true;
            t.stage = "table-checkpoint";
            snap();
            return t;
        }
        if (auto flushed = writer.flush(); flushed.has_error()) {
            t.had_error = true;
            t.stage = "metadata-flush";
            snap();
            return t;
        }
        t.writes_after_table_ckpt = plan.writes_seen;
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            t.had_error = true;
            t.stage = "free-list";
            snap();
            return t;
        }
        t.writes_after_free_list = plan.writes_seen;
        if (auto barrier = bm.file_sync(); barrier.has_error()) {
            t.had_error = true;
            t.stage = "barrier-fsync";
            snap();
            return t;
        }
        t.writes_before_header = plan.writes_seen;
        if (stop_before_header) {
            t.stage = "stopped-before-header";
            snap();
            return t;
        }
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        if (auto committed = bm.write_header(header); committed.has_error()) {
            t.had_error = true;
            t.stage = "header";
            snap();
            return t;
        }
        t.committed = true;
        snap();
        return t;
    }

    struct recovery_outcome_t {
        bool open_ok{false};
        bool load_ok{false};
        bool scan_clean{false};
        bool walker_ok{false};
        uint64_t unexplained{0};
        uint64_t overlap{0};
        uint64_t iteration{0};
        uint64_t rows{0};
        std::string note;
    };

    recovery_outcome_t recover_and_judge(const std::string& path) {
        recovery_outcome_t out;
        matrix_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        if (auto opened = bm.load_existing_database(); opened.has_error()) {
            out.note = std::string("open failed: ") + opened.error().what.c_str();
            return out;
        }
        out.open_ok = true;

        tstorage::database_header_t header{};
        if (!otterbrix_test::read_active_durable_header(path, header)) {
            out.note = "no valid durable header slot after a successful open";
            return out;
        }
        out.iteration = header.iteration;

        if (bm.meta_block() == tstorage::INVALID_INDEX) {
            out.note = "recovered root has no metadata pointer";
            return out;
        }
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        if (loaded.has_error()) {
            out.note = std::string("load failed: ") + loaded.error().what.c_str();
            return out;
        }
        out.load_ok = true;
        auto table = std::move(loaded.value());

        std::vector<storage_index_t> column_ids{storage_index_t(0), storage_index_t(1)};
        table_scan_state state(&env.resource);
        table->initialize_scan(state, column_ids, nullptr);
        auto types = table->copy_types();
        data_chunk_t chunk(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        std::set<uint64_t> ids;
        uint64_t rows = 0;
        bool clean = true;
        while (clean) {
            chunk.reset();
            table->scan(chunk, state);
            if (state.table_state.has_error()) {
                clean = false;
                out.note = std::string("scan error: ") + state.table_state.scan_error.what.c_str();
                break;
            }
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto id_cell = chunk.value(0, i);
                auto name_cell = chunk.value(1, i);
                const auto id = id_cell.value<int64_t>();
                const auto name = name_cell.value<std::string_view>();
                if (id < 0 || name != row_name(static_cast<uint64_t>(id)) ||
                    !ids.insert(static_cast<uint64_t>(id)).second) {
                    clean = false;
                    out.note = "row content mismatch at id " + std::to_string(id);
                    break;
                }
                rows++;
            }
        }
        if (clean && rows != 0 && (*ids.begin() != 0 || *ids.rbegin() != rows - 1)) {
            clean = false;
            out.note = "recovered id set is not a contiguous prefix";
        }
        out.rows = rows;
        out.scan_clean = clean;

        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        out.walker_ok = report.ok;
        if (!report.ok) {
            out.note += (out.note.empty() ? "" : "; ") + std::string("walker: ") + report.error;
        }
        out.unexplained = report.unexplained.size();
        out.overlap = report.reachable_free_overlap.size();
        return out;
    }

    enum class crash_kind_t { clean_writes, torn_write, sync_fail, before_header };
    enum class survival_t { persisted, reverted };

    struct round_shape_t {
        uint64_t base_writes{0};
        uint64_t round_writes{0};  // W: the header write is write W
        uint64_t compact_end{0};
        uint64_t table_ckpt_end{0};
        uint64_t free_list_end{0};
        uint64_t iter_before{0};
    };

    struct cell_result_t {
        bool committed{false};
        round_trace_t trace;
        recovery_outcome_t out;
    };

    cell_result_t run_cell(const std::string& base_path,
                           const std::string& work_path,
                           bool do_compact,
                           const round_shape_t& shape,
                           crash_kind_t kind,
                           survival_t survival,
                           uint64_t k) {
        cell_result_t cell;
        std::error_code ec;
        std::filesystem::copy_file(base_path, work_path, std::filesystem::copy_options::overwrite_existing, ec);
        REQUIRE_FALSE(ec);
        {
            matrix_env_t env;
            otterbrix_test::fault_plan_t plan;
            otterbrix_test::fault_injection_scope_t scope(plan);
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, work_path);
            REQUIRE_FALSE(bm.load_existing_database().has_error());

            tstorage::metadata_manager_t meta_mgr(bm);
            tstorage::meta_block_pointer_t ptr;
            ptr.block_pointer = bm.meta_block();
            tstorage::metadata_reader_t reader(meta_mgr, ptr);
            auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
            REQUIRE_FALSE(loaded.has_error());
            auto table = std::move(loaded.value());
            append_rows(*table, env, BASE_ROWS, EXTRA_ROWS);

            REQUIRE(plan.writes_seen == shape.base_writes);

            switch (kind) {
                case crash_kind_t::clean_writes:
                    plan.fail_writes_from = shape.base_writes + k + 1;
                    break;
                case crash_kind_t::torn_write:
                    plan.torn_at_write = shape.base_writes + k;
                    break;
                case crash_kind_t::sync_fail:
                    plan.fail_syncs_from = k;
                    break;
                case crash_kind_t::before_header:
                    break;
            }

            cell.trace = run_round(bm, *table, plan, do_compact, kind == crash_kind_t::before_header);
            cell.committed = cell.trace.committed;

            // `reverted` drops everything since the last fsync; `persisted` keeps every acknowledged
            // write and forbids further I/O so no destructor can write after the "crash".
            if (survival == survival_t::reverted) {
                REQUIRE(scope.last() != nullptr);
                scope.last()->crash_revert();
            } else {
                plan.crashed = true;
            }
        }
        cell.out = recover_and_judge(work_path);
        return cell;
    }

    std::string outcome_str(const recovery_outcome_t& out, const round_shape_t& shape) {
        std::string tag;
        if (!out.open_ok || !out.load_ok) {
            tag = "OPEN-ERROR(" + out.note + ")";
        } else if (out.iteration == shape.iter_before && out.rows == BASE_ROWS) {
            tag = "rootN";
        } else if (out.iteration == shape.iter_before + 1 && out.rows == BASE_ROWS + EXTRA_ROWS) {
            tag = "rootN+1";
        } else {
            tag = "THIRD-STATE(iter=" + std::to_string(out.iteration) + ",rows=" + std::to_string(out.rows) + ")";
        }
        if (out.open_ok && out.load_ok && !out.scan_clean) {
            tag += "+SCAN-MISMATCH(" + out.note + ")";
        }
        if (!out.walker_ok) {
            tag += "+WALKER-ERROR";
        }
        if (out.unexplained != 0) {
            tag += "+UNEXPLAINED=" + std::to_string(out.unexplained);
        }
        if (out.overlap != 0) {
            tag += "+OVERLAP=" + std::to_string(out.overlap);
        }
        return tag;
    }

    void judge_cell(const cell_result_t& cell, const round_shape_t& shape, const std::string& label) {
        INFO(label << " -> " << outcome_str(cell.out, shape) << (cell.out.note.empty() ? "" : " | " + cell.out.note));
        CHECK(cell.out.open_ok);
        CHECK(cell.out.load_ok);
        CHECK(cell.out.scan_clean);
        CHECK(cell.out.walker_ok);
        CHECK(cell.out.unexplained == 0);
        CHECK(cell.out.overlap == 0);
        const bool is_n = cell.out.iteration == shape.iter_before && cell.out.rows == BASE_ROWS;
        const bool is_n1 = cell.out.iteration == shape.iter_before + 1 && cell.out.rows == BASE_ROWS + EXTRA_ROWS;
        CHECK((is_n || is_n1));
        if (cell.committed) {
            CHECK(is_n1);
        }
    }

    std::string rle_digest(const std::vector<std::string>& outcomes, uint64_t first_k) {
        std::string digest;
        size_t i = 0;
        while (i < outcomes.size()) {
            size_t j = i;
            while (j + 1 < outcomes.size() && outcomes[j + 1] == outcomes[i]) {
                ++j;
            }
            if (!digest.empty()) {
                digest += " | ";
            }
            const uint64_t ka = first_k + i;
            const uint64_t kb = first_k + j;
            digest += (ka == kb ? "k=" + std::to_string(ka) : "k=" + std::to_string(ka) + ".." + std::to_string(kb)) +
                      " -> " + outcomes[i];
            i = j + 1;
        }
        return digest;
    }

    void build_base(const std::string& base_path) {
        matrix_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, base_path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, BASE_ROWS);
        otterbrix_test::fault_plan_t unused_plan;
        for (int warmup = 0; warmup < 3; ++warmup) {
            auto t = run_round(bm, *table, unused_plan, true, false);
            REQUIRE(t.compact_ok);
            REQUIRE(t.committed);
        }
    }

    round_shape_t measure_round(const std::string& base_path, const std::string& work_path, bool do_compact) {
        round_shape_t shape;
        {
            tstorage::database_header_t base_header{};
            REQUIRE(otterbrix_test::read_active_durable_header(base_path, base_header));
            shape.iter_before = base_header.iteration;
        }
        std::error_code ec;
        std::filesystem::copy_file(base_path, work_path, std::filesystem::copy_options::overwrite_existing, ec);
        REQUIRE_FALSE(ec);
        matrix_env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, work_path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        auto table = std::move(loaded.value());
        append_rows(*table, env, BASE_ROWS, EXTRA_ROWS);
        shape.base_writes = plan.writes_seen;

        auto t = run_round(bm, *table, plan, do_compact, false);
        REQUIRE(t.compact_ok);
        REQUIRE(t.committed);
        shape.round_writes = t.writes_total - shape.base_writes;
        shape.compact_end = t.writes_after_compact - shape.base_writes;
        shape.table_ckpt_end = t.writes_after_table_ckpt - shape.base_writes;
        shape.free_list_end = t.writes_after_free_list - shape.base_writes;
        REQUIRE(t.writes_before_header == t.writes_after_free_list);
        REQUIRE(shape.round_writes == shape.free_list_end + 1);
        REQUIRE(t.syncs_total == 2);
        return shape;
    }

    void run_matrix(bool do_compact, const char* family) {
        const auto base_path = matrix_db_path((std::string(family) + "_base").c_str());
        const auto work_path = matrix_db_path((std::string(family) + "_work").c_str());
        remove_file(base_path);
        remove_file(work_path);

        build_base(base_path);
        const auto shape = measure_round(base_path, work_path, do_compact);
        const uint64_t W = shape.round_writes;
        WARN("[a7.4 " << family << "] root N iteration=" << shape.iter_before << " base_writes=" << shape.base_writes
                      << " round: W=" << W << " writes (compact:1.." << shape.compact_end << ", table+meta:"
                      << shape.compact_end + 1 << ".." << shape.table_ckpt_end << ", free-list:"
                      << shape.table_ckpt_end + 1 << ".." << shape.free_list_end
                      << ", 1st fsync, header:" << W << ", 2nd fsync)");

        for (auto survival : {survival_t::persisted, survival_t::reverted}) {
            const char* sname = survival == survival_t::persisted ? "persisted" : "reverted";
            std::vector<std::string> outcomes;
            for (uint64_t k = 0; k <= W; ++k) {
                auto cell =
                    run_cell(base_path, work_path, do_compact, shape, crash_kind_t::clean_writes, survival, k);
                const std::string label =
                    std::string("[a7.4 ") + family + "] clean/" + sname + " k=" + std::to_string(k);
                {
                    INFO(label << " (stage=" << cell.trace.stage << ")");
                    CHECK(cell.committed == (k == W));
                }
                judge_cell(cell, shape, label);
                outcomes.push_back(outcome_str(cell.out, shape));
            }
            WARN("[a7.4 " << family << "] clean/" << sname << ": " << rle_digest(outcomes, 0));
        }

        // One survival shape only: crash_revert after a tear would undo the tear itself and
        // collapse into the reverted clean shape above.
        {
            std::vector<std::string> outcomes;
            for (uint64_t k = 1; k <= W; ++k) {
                auto cell =
                    run_cell(base_path, work_path, do_compact, shape, crash_kind_t::torn_write, survival_t::persisted, k);
                const std::string label = std::string("[a7.4 ") + family + "] torn k=" + std::to_string(k);
                {
                    INFO(label << " (stage=" << cell.trace.stage << ")");
                    CHECK(cell.committed == (k == W));
                }
                judge_cell(cell, shape, label);
                outcomes.push_back(outcome_str(cell.out, shape));
            }
            WARN("[a7.4 " << family << "] torn/persisted: " << rle_digest(outcomes, 1));
        }

        // s=1: data/metadata barrier, round dies before the header. s=2: header commit itself,
        // write landed but durability unproven (reconcile's indeterminate latch).
        {
            std::string digest;
            for (uint64_t s = 1; s <= 2; ++s) {
                for (auto survival : {survival_t::persisted, survival_t::reverted}) {
                    const char* sname = survival == survival_t::persisted ? "persisted" : "reverted";
                    auto cell = run_cell(base_path, work_path, do_compact, shape, crash_kind_t::sync_fail, survival, s);
                    const std::string label =
                        std::string("[a7.4 ") + family + "] sync-fail s=" + std::to_string(s) + "/" + sname;
                    {
                        INFO(label << " (stage=" << cell.trace.stage << ")");
                        CHECK_FALSE(cell.committed);
                    }
                    judge_cell(cell, shape, label);
                    if (!digest.empty()) {
                        digest += " | ";
                    }
                    digest += "s=" + std::to_string(s) + "/" + sname + " -> " + outcome_str(cell.out, shape);
                }
            }
            WARN("[a7.4 " << family << "] sync-fail: " << digest);
        }

        {
            std::string digest;
            for (auto survival : {survival_t::persisted, survival_t::reverted}) {
                const char* sname = survival == survival_t::persisted ? "persisted" : "reverted";
                auto cell =
                    run_cell(base_path, work_path, do_compact, shape, crash_kind_t::before_header, survival, 0);
                const std::string label = std::string("[a7.4 ") + family + "] after-1st-fsync/" + sname;
                {
                    INFO(label << " (stage=" << cell.trace.stage << ")");
                    CHECK_FALSE(cell.committed);
                    CHECK_FALSE(cell.trace.had_error);
                }
                judge_cell(cell, shape, label);
                if (!digest.empty()) {
                    digest += " | ";
                }
                digest += std::string(sname) + " -> " + outcome_str(cell.out, shape);
            }
            WARN("[a7.4 " << family << "] after-1st-fsync (no header attempt): " << digest);
        }

        remove_file(base_path);
        remove_file(work_path);
    }

}

TEST_CASE("crash_matrix: a COMPACTING checkpoint round recovers to root N or N+1 at every crash point", "[a7.4]") {
    // Only this family exercises that compaction never overwrites a block the durable root still reads.
    run_matrix(true, "compacting");
}

TEST_CASE("crash_matrix: a non-compacting checkpoint round recovers to root N or N+1 at every crash point",
          "[a7.4]") {
    // Root N's data blocks stay live and shared with the successor; the incremental round must not touch them.
    run_matrix(false, "noncompact");
}


// Regression: without publishing compact's write-through blocks in the free list, a committed
// round leaked 8 blocks (2 MiB at 6k rows) that no root names, orphaned since reclaim only walks
// roots.
TEST_CASE("crash_matrix: a restart does not orphan the previous process's live tree", "[a7.4]") {
    const auto path = matrix_db_path("restart_leak");
    remove_file(path);
    build_base(path);

    {
        matrix_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        auto table = std::move(loaded.value());
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        INFO("fresh reopen: unexplained=" << report.unexplained.size()
                                          << " live_superseded=" << report.live_superseded.size()
                                          << " overlap=" << report.reachable_free_overlap.size());
        CHECK(report.unexplained.empty());
        CHECK(report.reachable_free_overlap.empty());
        CHECK(report.live_superseded.empty());
    }

    uint64_t prev_blocks = 0;
    for (int cycle = 1; cycle <= 3; ++cycle) {
        matrix_env_t env;
        otterbrix_test::fault_plan_t plan;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = bm.meta_block();
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        auto table = std::move(loaded.value());
        auto t = run_round(bm, *table, plan, true, false);
        REQUIRE(t.compact_ok);
        REQUIRE(t.committed);
        const uint64_t blocks = bm.total_blocks();
        INFO("restart cycle " << cycle << ": block_count " << prev_blocks << " -> " << blocks);
        if (cycle >= 2) {
            CHECK(blocks <= prev_blocks);
        }
        prev_blocks = blocks;
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        CHECK(report.unexplained.empty());
        CHECK(report.reachable_free_overlap.empty());
    }
    remove_file(path);
}
