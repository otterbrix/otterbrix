// A7.4 — exhaustive crash-point enumeration for the checkpoint.
//
// The gate, verbatim from the plan: for each k, the opened state equals root N or root N+1 —
// never an error, never a third state. This is the proof that shadow paging (A7.1 two-slot
// header, A7.2 split free pool, A7.3 superseded-root reclaim, A7.7 failed-round rollback)
// actually holds at EVERY point a crash can land, and it is what authorises deleting the
// .prev/.broken machinery in A7.5.
//
// WHAT ONE ROUND IS. The exact sequence of table_storage_t::checkpoint
// (services/disk/manager_disk.cpp), preceded by data_table_t::compact when the round
// compacts — the same order agent_disk_t::checkpoint_inner runs:
//
//   [compact: rebuild + write-through]                 (compacting rounds only)
//   table.checkpoint(writer)   — column flushes, metadata chain, A7.3 reclaim
//   writer.flush()
//   set_meta_block
//   serialize_free_list()      — free-list chain
//   file_sync()                — 1st fsync: data/metadata durable BEFORE the root names them
//   write_header()             — ONE slot write + 2nd fsync: the atomic commit point
//
// THE MATRIX. A healthy round is measured first (writes_seen / syncs_seen through the T3
// fault seam), which gives W = writes in the round and pins the two-barrier shape. Then, for
// a round that COMPACTED and for one that did not:
//
//   * clean crash after k successful writes, k = 0..W, in BOTH persistence shapes:
//       - persisted: writes 1..k are on the device, write k+1 and everything later never
//         happened (the device kept what it acknowledged);
//       - reverted:  crash_revert() — everything since the last fsync is gone (kill -9 with
//         a page cache that never reached the platter; the conservative extreme);
//   * a TORN write at k, k = 1..W: the seam persists only the first half of write k, then
//     fails it and everything after. A tear is a different shape from a clean failure — for
//     every k < W it leaves a half-written block with a broken CRC; at k = W (the header) the
//     tear reassembles into a byte-exact new-generation header (all differing bytes live in
//     the first hardware sector) and the round COMMITS through reconcile case 1;
//   * a failed fsync at s = 1 (the barrier) and s = 2 (the header commit), each followed by
//     the crash, in both persistence shapes — the two barriers mean different things and get
//     crashed separately;
//   * a crash after the 1st fsync with NO header attempt at all (the round stops between the
//     barriers).
//
// For every cell the file is reopened by a FRESH manager (fresh env, seam removed) and judged
// on three axes, all required:
//   1. the open itself succeeds — never data_corruption, never io_error;
//   2. the DATA reads back and matches one root exactly: iteration N ⇒ ids {0..BASE-1},
//      iteration N+1 ⇒ ids {0..BASE+EXTRA-1}, every row's payload matching its id, no
//      duplicates, no strays. Not merely "open returned success" — this branch has shipped
//      tests that asserted only the open and hid real corruption behind it;
//   3. the block-reachability walker reports ZERO unexplained ids and ZERO
//      reachable-free overlap — the accounting half of "the state is one of exactly two".
// Additionally: a round that reported COMMITTED must recover as root N+1 (a committed
// checkpoint may not be lost).
//
// THE .otbx IS JUDGED ALONE — by construction, not by cleanup. This test lives below
// services/disk: no agent_disk_t exists here, so no external file could ever mask what the
// matrix measures. (When this matrix was first taken, checkpoint_inner still kept a `.prev`
// whole-file backup and a restore path over the `.otbx`; the matrix was run with those
// physically absent, which is what authorised A7.5 to delete them everywhere.) The reopen
// is a bare load_existing_database + load_from_disk on the crashed file itself.
//
// SEAM FACTS the arithmetic depends on (rediscovered at cost, do not lose):
//   * fault_injection_scope_t wraps the handle at OPEN time — it is installed BEFORE the
//     manager is constructed, or every counter reads zero;
//   * arming is ABSOLUTE over the plan's life, so each scenario records base_writes (writes
//     seen before the round starts — proven equal to the measurement run's) and aims at
//     base_writes + k. A blanket fail_after_writes would also kill DATA writes, latch
//     durability_error_, and hide the point under test behind the degraded() gate;
//   * fail_writes_from (1-based) exists because fail_after_writes counts allowed successes
//     and cannot express k = 0 (its zero is the off switch).
//
// Scenario base state: one steady-state table (BASE_ROWS rows, three committed
// compact+checkpoint rounds — the A7.3 closed cycle), copied per cell; the crashed round
// appends EXTRA_ROWS first, so root N and root N+1 differ in observable data and "which root
// did we get" is decidable from the rows alone. Copying an engine-produced file is the shape
// fault_injection_file.hpp explicitly sanctions ("reopening the reverted file — or a
// filesystem copy of it — under a fresh environment IS the state after kill"); no file
// content is ever hand-placed.

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

    // One checkpoint round, staged, with the seam's write/sync counters snapshotted at every
    // stage boundary so a crash point k can be named by the stage it lands in. Mirrors
    // table_storage_t::checkpoint exactly (see the file header); `do_compact` prepends the
    // production compact, whose write-through is part of the round's write sequence and
    // therefore part of the matrix. Errors are RECORDED, not asserted: in a crash scenario the
    // round failing is the expected event.
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
            // Best-effort in production too: agent_disk_t::checkpoint_inner still attempts the
            // checkpoint when the compact refused, so the round continues here as well.
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

    // --- Recovery + judgement (the three axes of the gate) ---

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

        // Exactness without a million catch assertions: ids must be unique and, with the count,
        // form exactly {0..rows-1}; every payload must match its id. Aggregated booleans only.
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

        // Walker with the table still loaded (the registry bin needs the live handles).
        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        out.walker_ok = report.ok;
        if (!report.ok) {
            out.note += (out.note.empty() ? "" : "; ") + std::string("walker: ") + report.error;
        }
        out.unexplained = report.unexplained.size();
        out.overlap = report.reachable_free_overlap.size();
        return out;
    }

    // --- Scenario driver ---

    enum class crash_kind_t { clean_writes, torn_write, sync_fail, before_header };
    enum class survival_t { persisted, reverted };

    struct round_shape_t {
        uint64_t base_writes{0};   // writes before the round (reload + append): proven stable
        uint64_t round_writes{0};  // W — the header write is write W
        uint64_t compact_end{0};   // stage boundaries, in round-relative write numbers
        uint64_t table_ckpt_end{0};
        uint64_t free_list_end{0};
        uint64_t iter_before{0};   // root N's iteration, read from the base file
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
            otterbrix_test::fault_injection_scope_t scope(plan); // BEFORE the manager: wraps at open
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

            // Determinism guard: the scenario must stand exactly where the measurement run
            // stood, or the absolute write numbers below aim at the wrong writes.
            REQUIRE(plan.writes_seen == shape.base_writes);

            switch (kind) {
                case crash_kind_t::clean_writes:
                    plan.fail_writes_from = shape.base_writes + k + 1; // k successes, then failure
                    break;
                case crash_kind_t::torn_write:
                    plan.torn_at_write = shape.base_writes + k;
                    break;
                case crash_kind_t::sync_fail:
                    plan.fail_syncs_from = k; // syncs happen only inside the round
                    break;
                case crash_kind_t::before_header:
                    break; // no fault: the crash IS stopping between the two barriers
            }

            cell.trace = run_round(bm, *table, plan, do_compact, kind == crash_kind_t::before_header);
            cell.committed = cell.trace.committed;

            // The kill. `reverted` drops everything since the last fsync (the undo journal);
            // `persisted` keeps every write the device acknowledged and forbids all further
            // I/O, so nothing a destructor might attempt can land after the "crash".
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

    // The three axes, checked per cell. CHECK rather than REQUIRE on purpose: a hole at one k
    // must not hide the rest of the matrix — every failing k is reported with its shape.
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
            // A checkpoint that reported success is a durability promise: the caller advances
            // its WAL bookkeeping on it, so the crash must recover the NEW root.
            CHECK(is_n1);
        }
    }

    // Run-length compression for the digest: consecutive k with the same outcome collapse to
    // one range, so the matrix is readable in the test log.
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
        // Three committed compact+checkpoint rounds: the A7.3 steady state, where an unchanged
        // table is a closed cycle. No fault scope here — the seam counters are not needed and
        // the base must be a file the engine itself produced, healthy end to end.
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
        // The round's fixed anatomy, pinned rather than adapted to: exactly one header write
        // after the barrier, and exactly two fsyncs — the 1st makes data/metadata durable, the
        // 2nd commits the header. If either ever changes, this matrix must be RE-DERIVED, not
        // silently re-shaped around it.
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

        // Clean crash after k successful writes, both persistence shapes.
        for (auto survival : {survival_t::persisted, survival_t::reverted}) {
            const char* sname = survival == survival_t::persisted ? "persisted" : "reverted";
            std::vector<std::string> outcomes;
            for (uint64_t k = 0; k <= W; ++k) {
                auto cell =
                    run_cell(base_path, work_path, do_compact, shape, crash_kind_t::clean_writes, survival, k);
                const std::string label =
                    std::string("[a7.4 ") + family + "] clean/" + sname + " k=" + std::to_string(k);
                {
                    // Determinism of the round is the k arithmetic's foundation: with k < W
                    // armed the round MUST fail, with k = W it MUST commit.
                    INFO(label << " (stage=" << cell.trace.stage << ")");
                    CHECK(cell.committed == (k == W));
                }
                judge_cell(cell, shape, label);
                outcomes.push_back(outcome_str(cell.out, shape));
            }
            WARN("[a7.4 " << family << "] clean/" << sname << ": " << rle_digest(outcomes, 0));
        }

        // Torn write at k. Reopened as the device left it: prefix persisted, half of write k
        // persisted, nothing after. (crash_revert after a tear would undo the tear itself and
        // collapse into the reverted clean shape above, so the torn lane has one survival.)
        {
            std::vector<std::string> outcomes;
            for (uint64_t k = 1; k <= W; ++k) {
                auto cell =
                    run_cell(base_path, work_path, do_compact, shape, crash_kind_t::torn_write, survival_t::persisted, k);
                const std::string label = std::string("[a7.4 ") + family + "] torn k=" + std::to_string(k);
                {
                    // Every torn write below the header fails the round; the torn HEADER
                    // reassembles into a valid new-generation slot and commits through
                    // reconcile case 1 (fsync succeeded, read-back proves the device).
                    INFO(label << " (stage=" << cell.trace.stage << ")");
                    CHECK(cell.committed == (k == W));
                }
                judge_cell(cell, shape, label);
                outcomes.push_back(outcome_str(cell.out, shape));
            }
            WARN("[a7.4 " << family << "] torn/persisted: " << rle_digest(outcomes, 1));
        }

        // Failed fsync at each barrier, then the crash — both persistence shapes. s=1 is the
        // data/metadata barrier (round dies before the header), s=2 is the header commit
        // (write landed, durability unproven: reconcile case 3, the indeterminate latch).
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

        // Crash strictly BETWEEN the two barriers: 1st fsync done, header never attempted.
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

} // namespace

TEST_CASE("crash_matrix: a COMPACTING checkpoint round recovers to root N or N+1 at every crash point", "[a7.4]") {
    // Compaction is what puts the superseded root's blocks in play: the outgoing collection's
    // blocks go to pending_free_ (A7.2) and root N's chains are reclaimed mid-round (A7.3), so
    // only this family exercises the promise that a block the durable root still reads is
    // never overwritten by the round that crashes.
    run_matrix(true, "compacting");
}

TEST_CASE("crash_matrix: a non-compacting checkpoint round recovers to root N or N+1 at every crash point",
          "[a7.4]") {
    // Without the compact, root N's data blocks stay live in the registry and are SHARED with
    // the root under construction — the round writes only the appended tail plus fresh chains.
    // The matrix here proves the incremental round never touches the shared blocks in place.
    run_matrix(false, "noncompact");
}


// ---------------------------------------------------------------------------------------
// R-LEAK — the pre-existing defect the matrix flushed out, pinned by name (rule 19).
//
// RED before the serialize_free_list fix (the run is recorded in the A7.4 matrix's first
// execution): a committed round leaves the live tree holding blocks no root names — compact's
// write-through and the re-pointed tail segments — while the durable root's pointer stream
// names only the packed copy. In-process the registry protects them and the next compact
// recycles them; but nothing on disk has ever heard of them, so the moment the process ends
// (crash OR clean exit) they are orphans: a fresh open walked them as unexplained (8 blocks =
// 2 MiB at 6k rows) and NOTHING could ever free them — the reclaim only walks roots — so the
// file leaked one full table copy per process lifetime, forever.
//
// The fix publishes them in the serialized free list (the root's own statement of what it
// does not reference). This gate proves both halves across REAL process boundaries
// (fresh managers over the same file):
//   1. a fresh open of a steady-state file explains every block — zero unexplained;
//   2. restart cycles do not grow the file: reopen -> one compact+checkpoint round -> close,
//      repeated, with a stable block_count from the second cycle on.
// ---------------------------------------------------------------------------------------
TEST_CASE("crash_matrix: a restart does not orphan the previous process's live tree", "[a7.4]") {
    const auto path = matrix_db_path("restart_leak");
    remove_file(path);
    build_base(path);

    // Half 1: the reopened file is fully explained.
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
        // On a fresh open the registry holds only what the root names, so the deliberate
        // live-tree overlap must be EMPTY here — its ids became plain free-list content.
        CHECK(report.live_superseded.empty());
    }

    // Half 2: restart cycles are a closed cycle, like the in-process steady state (A7.3).
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
