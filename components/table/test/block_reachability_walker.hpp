#pragma once

// Block-reachability walker (test-side): classifies every block id into chain / registry /
// free-list / unexplained, to prove leaked old-root blocks are attributable garbage and never
// live data (the safety condition the old-root freeing formula relies on).
// Reads the durable root straight from the file's double header, NOT the in-memory manager
// state, because it judges what a crash would recover. Call only after a completed checkpoint
// or right after load; before the first checkpoint, use the registry-only classification of
// the issued-id journal instead.

#include <algorithm>
#include <cstring>
#include <fstream>
#include <set>
#include <string>

#include <components/table/data_table.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/single_file_block_manager.hpp>

namespace otterbrix_test {

    namespace storage = components::table::storage;

    struct walk_report_t {
        bool ok{false};
        std::string error;
        uint64_t iteration{0};
        uint64_t block_count{0};
        uint64_t meta_root{storage::INVALID_INDEX};      // block_pointer of the table-metadata chain
        uint64_t free_list_root{storage::INVALID_INDEX}; // block_pointer of the free-list chain
        std::set<uint64_t> chain_blocks;                 // bin 1a: metadata chains
        std::set<uint64_t> durable_data;                 // bin 1b: data blocks the durable root
                                                         //         references (via a scratch load,
                                                         //         so reachability can't drift
                                                         //         from the on-disk format)
        std::set<uint64_t> registry_live;                // bin 2: live table's blocks
        std::set<uint64_t> free_list_content;            // bin 3
        std::set<uint64_t> unexplained;                  // bin 4
        // Complete named-data set from the manager's own snapshot (root re-adopted it on
        // scratch load). durable_data above is only the registry DELTA, so it can't tell
        // "loaded from root" apart from "live private copy root doesn't name" — root_data can
        // (see live_superseded).
        std::set<uint64_t> root_data;
        // Blocks the walker's own scratch load allocated. Measured: loading a 12k-row table
        // and scanning it allocates zero blocks. Excluded from durable_data; callers must
        // treat these as explained when classifying issued-id journals from a walk.
        std::set<uint64_t> scratch_issued;
        // Must stay empty: a block the durable root reads (chains or named data) that its own
        // free list also publishes would be reissued over live data on the next open.
        std::set<uint64_t> reachable_free_overlap;
        // free ∩ (registry-only): live tree's blocks the root doesn't name — the free list's
        // deliberate third term, so a restart can reclaim write-through copies. Empty right
        // after reopen, since then the registry holds only what the root names.
        std::set<uint64_t> live_superseded;

        bool explains(uint64_t id) const {
            return chain_blocks.count(id) != 0 || durable_data.count(id) != 0 || root_data.count(id) != 0 ||
                   registry_live.count(id) != 0 || free_list_content.count(id) != 0;
        }
    };

    // Picks the active header slot: valid (checksum ok) with the greater iteration wins.
    // Returns false if neither slot validates (load_existing_database's data_corruption case).
    // The losing slot legitimately holds the PREVIOUS root, not a stale copy of the winner —
    // a checkpoint writes only the slot its own iteration owns.
    inline bool read_active_durable_header(const std::string& path, storage::database_header_t& out) {
        std::ifstream f(path, std::ios::binary);
        if (!f) {
            return false;
        }
        storage::database_header_t h1{};
        storage::database_header_t h2{};
        f.seekg(static_cast<std::streamoff>(storage::SECTOR_SIZE));
        f.read(reinterpret_cast<char*>(&h1), sizeof(h1));
        const bool h1_valid = static_cast<bool>(f) && h1.checksum_ok();
        f.clear(); // a short read on slot 1 must not poison the slot-2 read
        f.seekg(static_cast<std::streamoff>(2 * storage::SECTOR_SIZE));
        f.read(reinterpret_cast<char*>(&h2), sizeof(h2));
        const bool h2_valid = static_cast<bool>(f) && h2.checksum_ok();
        if (!h1_valid && !h2_valid) {
            return false;
        }
        out = (h1_valid && (!h2_valid || h1.iteration >= h2.iteration)) ? h1 : h2;
        return true;
    }

    // Follow a metadata sub-block chain from `start`, collecting the underlying block ids.
    // Delegates to metadata_manager_t::chain_blocks — the same code the reclaim path uses to
    // find a superseded root's chains, so this walker can't silently diverge from what it's
    // meant to be checking.
    inline bool walk_chain(storage::metadata_manager_t& mgr,
                           storage::meta_block_pointer_t start,
                           std::set<uint64_t>& out_blocks,
                           std::string& error,
                           std::pmr::memory_resource* resource) {
        std::pmr::vector<uint64_t> blocks(resource);
        auto walked = mgr.chain_blocks(start, blocks);
        if (walked.has_error()) {
            error = std::string(walked.error().what.c_str());
            return false;
        }
        out_blocks.insert(blocks.begin(), blocks.end());
        return true;
    }

    // `bm` must be the open file's manager with the table loaded (so the registry reflects
    // live data blocks). Reachability is computed by loading a scratch table from the durable
    // root (initialize_column registers each segment) rather than re-parsing the format
    // directly: an earlier version skipped this and mis-reported fresh checkpoint copies as
    // unexplained.
    inline walk_report_t walk_blocks(storage::single_file_block_manager_t& bm,
                                     const std::string& path,
                                     std::pmr::memory_resource* scratch_resource) {
        walk_report_t report;

        storage::database_header_t header;
        if (!read_active_durable_header(path, header)) {
            report.error = "cannot read durable header from " + path;
            return report;
        }
        report.iteration = header.iteration;
        report.block_count = header.block_count;
        report.meta_root = header.meta_block;
        report.free_list_root = header.free_list;

        storage::metadata_manager_t chain_mgr(bm);
        if (header.meta_block != storage::INVALID_INDEX) {
            if (!walk_chain(chain_mgr,
                            storage::meta_block_pointer_t(header.meta_block, 0),
                            report.chain_blocks,
                            report.error,
                            scratch_resource)) {
                return report;
            }
        }
        if (header.free_list != storage::INVALID_INDEX) {
            if (!walk_chain(chain_mgr,
                            storage::meta_block_pointer_t(header.free_list, 0),
                            report.chain_blocks,
                            report.error,
                            scratch_resource)) {
                return report;
            }
            storage::metadata_reader_t reader(chain_mgr, storage::meta_block_pointer_t(header.free_list, 0));
            auto count = reader.read<uint64_t>();
            for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
                report.free_list_content.insert(reader.read<uint64_t>());
            }
            if (reader.has_error()) {
                report.error = "free-list stream corrupt: read past end of chain";
                return report;
            }
        }

        for (auto id : bm.dev_live_registry_ids()) {
            report.registry_live.insert(id);
        }

        if (header.meta_block != storage::INVALID_INDEX) {
            // Scratch-load allocations are noise, not root data, so excluded from the delta.
            // Measured: empty for a plain reload (validity is persistent).
            const size_t issued_before_scratch = bm.dev_issued_ids().size();
            storage::metadata_manager_t load_mgr(bm);
            storage::meta_block_pointer_t root_ptr;
            root_ptr.block_pointer = header.meta_block;
            storage::metadata_reader_t reader(load_mgr, root_ptr);
            auto scratch = components::table::data_table_t::load_from_disk(scratch_resource, bm, reader);
            if (scratch.has_error()) {
                report.error = "scratch load from durable root failed: " +
                               std::string(scratch.error().what.c_str());
                return report;
            }
            {
                const auto& journal = bm.dev_issued_ids();
                for (size_t i = issued_before_scratch; i < journal.size(); ++i) {
                    report.scratch_issued.insert(journal[i]);
                }
            }
            for (auto id : bm.dev_live_registry_ids()) {
                if (report.registry_live.count(id) == 0 && report.scratch_issued.count(id) == 0) {
                    report.durable_data.insert(id);
                }
            }
            // Loader's own answer (load_from_disk re-adopted this set from the pointer stream
            // it just read), not a second, hand-rolled walk of the same data.
            report.root_data = bm.dev_durable_root_data_snapshot();
            // scratch table destroyed here; its temporary registrations drop out of the
            // registry with it (shared ids keep the live table's handles).
        }

        // max(durable block_count, high-water mark), not block_count alone: after a round
        // whose header never committed, the high-water mark can sit past block_count, and
        // leaked ids live exactly in that gap.
        const uint64_t high_water = std::max(report.block_count, bm.total_blocks());
        for (uint64_t id = 0; id < high_water; ++id) {
            const bool root_needed = report.chain_blocks.count(id) != 0 || report.root_data.count(id) != 0 ||
                                     report.durable_data.count(id) != 0;
            const bool live = report.registry_live.count(id) != 0;
            const bool in_free = report.free_list_content.count(id) != 0;
            if (in_free && root_needed) {
                report.reachable_free_overlap.insert(id);
            } else if (in_free && live) {
                report.live_superseded.insert(id);
            }
            if (!root_needed && !live && !in_free) {
                report.unexplained.insert(id);
            }
        }

        report.ok = true;
        return report;
    }

} // namespace otterbrix_test
