#pragma once

// Classifies each block id into chain / registry / free-list / unexplained, to prove leaked old-root
// blocks are garbage, never live data; reads the on-disk header, so call only after a checkpoint or load.

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
        uint64_t meta_root{storage::INVALID_INDEX};      // root of the table-metadata chain
        uint64_t free_list_root{storage::INVALID_INDEX}; // root of the free-list chain
        std::set<uint64_t> chain_blocks;                 // bin 1a: metadata chains
        std::set<uint64_t> durable_data;                 // bin 1b: data the durable root references (scratch load)
        std::set<uint64_t> registry_live;                // bin 2: live table's blocks
        std::set<uint64_t> free_list_content;            // bin 3
        std::set<uint64_t> unexplained;                  // bin 4
        // Loader's own snapshot; durable_data above is only the registry delta and can't tell
        // root-loaded apart from live-private-copy.
        std::set<uint64_t> root_data;
        // Walker's own scratch-load allocations, measured zero for a 12k-row table load+scan.
        std::set<uint64_t> scratch_issued;
        // Must stay empty: overlap here means the free list would reissue live data on the next open.
        std::set<uint64_t> reachable_free_overlap;
        std::set<uint64_t> live_superseded;

        bool explains(uint64_t id) const {
            return chain_blocks.count(id) != 0 || durable_data.count(id) != 0 || root_data.count(id) != 0 ||
                   registry_live.count(id) != 0 || free_list_content.count(id) != 0;
        }
    };

    // The losing slot legitimately holds the previous root, not a stale copy of the winner — a
    // checkpoint writes only the slot its own iteration owns.
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

    // `bm` must be the open file's manager with the table loaded. Reachability comes from a
    // scratch-load of the durable root, not direct re-parsing, which mis-reports fresh checkpoint
    // copies as unexplained.
    inline walk_report_t walk_blocks(storage::single_file_block_manager_t& bm,
                                     const std::string& path,
                                     std::pmr::memory_resource* scratch_resource) {
        walk_report_t report;

        storage::database_header_t header{};
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
            const size_t issued_before_scratch = bm.dev_issued_ids().size();
            storage::metadata_manager_t load_mgr(bm);
            storage::meta_block_pointer_t root_ptr;
            root_ptr.block_pointer = header.meta_block;
            storage::metadata_reader_t reader(load_mgr, root_ptr);
            auto scratch = components::table::data_table_t::load_from_disk(scratch_resource, bm, reader);
            if (scratch.has_error()) {
                report.error = "scratch load from durable root failed: " + std::string(scratch.error().what.c_str());
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
            report.root_data = bm.dev_durable_root_data_snapshot();
        }

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
