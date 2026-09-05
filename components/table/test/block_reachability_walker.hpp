#pragma once

// Block-reachability walker (test-side).
//
// Classifies every block id of a single-file database into exactly one of four bins:
//   1. chain      — reachable from the DURABLE root: a block of the table-metadata chain or of
//                   the free-list chain (the chains themselves, walked via the 12-byte
//                   sub-block headers written by metadata_writer_t);
//   2. registry   — a live block_handle_t in the block registry. For a LOADED table this is
//                   exactly the set of data blocks (initialize_column registers every segment
//                   by its disk id), so the walker MUST run with the table loaded/open;
//   3. free list  — an id listed in the durable free list's CONTENT. This legitimately
//                   includes blocks the LIVE tree still holds but the
//                   root does not name (reported separately as live_superseded); an id both
//                   free-listed and needed BY THE ROOT is still fatal (reachable_free_overlap);
//   4. unexplained — none of the above: leaked old-root blocks. The walker exists to prove
//                   they are attributable garbage (previous rounds' chains) and never live
//                   data — that is the safety condition of the old-root freeing formula, which
//                   this walker tries to REFUTE, not confirm.
//
// The durable root is read straight from the file's double header (iteration-max slot),
// NOT from the in-memory manager state: the walker judges what a crash would recover.
// Contract: call only after a completed checkpoint (both fsyncs done) or right after load;
// for the pre-checkpoint (virgin-header) case use the registry-only classification of the
// issued-id journal.

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
                                                         //         references (via a scratch load —
                                                         //         the LOADER defines reachability,
                                                         //         so it cannot drift from the format)
        std::set<uint64_t> registry_live;                // bin 2: live table's blocks
        std::set<uint64_t> free_list_content;            // bin 3
        std::set<uint64_t> unexplained;                  // bin 4
        // The durable root's COMPLETE named data set, taken from the manager's own record
        // (dev_durable_root_data_snapshot) right after the scratch load re-adopted it from the
        // file. durable_data above is the scratch REGISTRY DELTA and deliberately excludes ids
        // the live registry already held, so it alone cannot distinguish "live because the
        // loader loaded it from the root" from "live private copy the root does not name" —
        // and that distinction is what separates the fatal free-list overlap from the
        // deliberate one (see live_superseded).
        std::set<uint64_t> root_data;
        // Blocks the walker's OWN scratch load allocated. Measured: opening a 12k-row table
        // allocates ZERO blocks (validity is persistent, so loading is not writing), and a full
        // scan of the freshly loaded table allocates zero as well.
        // The accounting stays: it is what proves that, and a future load that starts
        // allocating again would show up here as an observer effect instead of as mystery
        // garbage. Excluded from durable_data, and callers must treat these as explained when
        // classifying issued-id journals taken after a walk.
        std::set<uint64_t> scratch_issued;
        // Invariant violations (must stay empty): a block the DURABLE ROOT reads (its chains
        // or its named data) that its own free list also publishes would be reissued over live
        // data with a valid CRC on the very next open.
        std::set<uint64_t> reachable_free_overlap;
        // free ∩ (registry-only): blocks the live in-memory tree holds that the judged root
        // does NOT name. This is the published list's deliberate
        // third term — the write-through/re-pointed copies whose only owner is the live tree,
        // named in the list so a RESTART can ever reclaim them. In-process they stay protected
        // by the registry (the in-memory pools never contain them); on a freshly reopened file
        // this set is empty, because there the registry holds only what the root names.
        std::set<uint64_t> live_superseded;

        bool explains(uint64_t id) const {
            return chain_blocks.count(id) != 0 || durable_data.count(id) != 0 || root_data.count(id) != 0 ||
                   registry_live.count(id) != 0 || free_list_content.count(id) != 0;
        }
    };

    // Read the durable double header straight from the file and pick the active slot by the
    // same rule production uses: a slot counts only if it reads back whole AND its
    // checksum validates, and among the surviving candidates the greater iteration wins.
    // Returns false if the file cannot be read or if NEITHER slot is valid — the state
    // load_existing_database reports as data_corruption. Since a checkpoint now writes only
    // the slot its iteration owns, the loser slot legitimately holds the PREVIOUS root; it is
    // not a copy of the winner and must never be read as one.
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

    // Follow a metadata sub-block chain from `start`, collecting the underlying BLOCK ids.
    //
    // Delegates to metadata_manager_t::chain_blocks -- the SAME implementation the
    // reclaim uses to find the superseded root's chains. That is deliberate: this walker
    // exists to judge that reclaim, and a private copy of "what a chain is" here would be a
    // second notion of it, free to agree with the reclaim while both were wrong. Cycle
    // detection and the 12-byte sub-block header layout live there, once.
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

    // Full durable-state walk + classification. `bm` must be the manager of the open file at
    // `path`; the table must be loaded so the registry reflects the live data blocks.
    //
    // Data-pointer reachability is computed by LOADING a scratch table from the durable root:
    // initialize_column registers every referenced segment block in the registry, so the
    // registry delta during the scratch load IS the set of data blocks the durable root
    // references. The first (shallow) version of this walker skipped this and immediately
    // mis-reported the fresh checkpoint copies as unexplained — the loader is the only
    // definition of reachability that cannot drift from the on-disk format.
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
            // Free-list CONTENT: the ids the durable free list says are reusable.
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

        // Scratch load: the durable root's data blocks, via the real loader.
        if (header.meta_block != storage::INVALID_INDEX) {
            // Anything the scratch load allocates is scratch noise, not root-referenced data,
            // so it is excluded from the delta. Measured: that set is EMPTY for a
            // plain reload (validity is persistent), but the exclusion stays so the walker
            // stays correct if any load path starts allocating again.
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
            // The COMPLETE named set: load_from_disk just re-adopted it into the manager from
            // the very pointer stream it loaded, so this is the loader's answer, not a second
            // walk of this file's own invention.
            report.root_data = bm.dev_durable_root_data_snapshot();
            // scratch table destroyed here; its temporary registrations drop out of the
            // registry with it (shared ids keep the live table's handles).
        }

        // The classification RANGE, not the bins: the wider of the durable block_count and the
        // manager's high-water mark.
        //
        // Stopping at the DURABLE root's block_count is exactly right after a COMMITTED round —
        // write_header records block_count = max_block_, so the two agree — and blind after a
        // FAILED one: a round whose header never committed can have pushed the high-water mark
        // PAST the durable block_count, and the ids in between are the leak. A walker that
        // cannot see them cannot gate them. After a successful checkpoint the two bounds are
        // equal and nothing about the report changes.
        const uint64_t high_water = std::max(report.block_count, bm.total_blocks());
        for (uint64_t id = 0; id < high_water; ++id) {
            // Two different free-list overlaps, split on WHO needs the block:
            //   * the ROOT needs it (chains or named data) and its own list frees it — fatal,
            //     the next open would reissue it over data that root reads;
            //   * only the LIVE TREE holds it — the published list names it ON PURPOSE, so a
            //     restart can reclaim what would otherwise be orphaned with the process.
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
