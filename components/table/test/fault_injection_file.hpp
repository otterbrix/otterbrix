#pragma once

// Fault-injection file handle + crash simulation (test-side).
//
// A faulty_file_handle_t wraps the real database file handle (installed through the
// DEV_MODE interposer seam in single_file_block_manager_t) and can:
//   - fail every positional write after the Nth one (fail_after_writes);
//   - tear one write in half and fail everything after it (torn_at_write), on the
//     positional AND the sequential overload alike;
//   - fail every read of ONE block location (fail_reads_at_location) — a rotten block, i.e.
//     the failure a walk of the durable root's metadata chain has to survive;
//   - simulate kill -9: an undo journal records the pre-image of every write — positional and
//     sequential alike — since the last successful sync(); crash_revert() rolls the file back
//     to exactly its state at the last fsync — the conservative crash semantics (nothing
//     unsynced survived). After crash_revert() every further I/O on the handle fails.
//
// No test may lay out crash states by hand; reopening the reverted file — or a filesystem
// copy of it — under a fresh environment IS the "state after kill".
//
// The wrapper must always delegate to the wrapped inner handle: the filesystem free
// functions reinterpret_cast their handle argument to the platform handle type, so passing
// the wrapper itself into them would read a garbage fd.

#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include <components/table/storage/single_file_block_manager.hpp>
#include <core/file/file_handle.hpp>

namespace otterbrix_test {

    struct fault_plan_t {
        // Fail the (N+1)th and every later positional write. 0 = off.
        uint64_t fail_after_writes{0};
        // Fail the Nth and every later positional write (1-BASED, the fail_syncs_from
        // convention). 0 = off. Exists because fail_after_writes counts ALLOWED successes and
        // its zero is the off switch, so it cannot express "no write succeeds at all" — which
        // is exactly the k=0 point of the crash matrix. The two knobs compose by OR:
        // whichever names the current write first fails it.
        uint64_t fail_writes_from{0};
        // Tear the Nth positional write (1-based): persist only its first half, then fail
        // it and everything after. 0 = off.
        uint64_t torn_at_write{0};
        // Fail the Nth sync() and every later one (1-BASED, unlike fail_after_writes above,
        // which is a count-of-successes). 0 = off. 1 fails the very first sync, which is the
        // only one create_new_database issues. Models the write that reached the page cache
        // and never reached the device — the failure mode the checkpoint's second fsync
        // exists to catch.
        uint64_t fail_syncs_from{0};
        // Fail every positional write that lands in a HEADER SLOT (offset SECTOR_SIZE or
        // 2 * SECTOR_SIZE — the two slots write_header alternates between), and nothing else.
        // The other write knobs above are COUNTED, so arming them for a checkpoint's header
        // also fails the data and metadata writes that precede it; those latch
        // durability_error_ and leave the block manager degraded, which is a different state
        // with a different caller (agent_disk_t::checkpoint_inner defers a degraded entry
        // instead of retrying it). This knob names the commit point directly, so it produces
        // the RECOVERABLE checkpoint failure — write_header's case 2, where both slots read
        // back as the iteration this manager already believed and nothing is latched.
        //
        // IT NAMES THE OFFSETS, NOT THE CALLER. create_new_database writes the very first
        // header into header_slot_offset(0) == 2 * SECTOR_SIZE as well, so a plan armed while a
        // file is being CREATED fails the creation instead ("Failed to write the initial
        // database header"), which is a different failure with a different caller. Arm it
        // around the round that must fail, not around the fixture.
        bool fail_writes_at_header_slots{false};
        // Fail every positional READ that starts at this exact file offset. That offset is a
        // block location (single_file_block_manager_t::block_location), so this models ONE
        // rotten/unreadable block rather than a dead file — the shape a metadata-chain walk
        // has to survive. UINT64_MAX = off, because offset 0 is the main header and therefore
        // a legitimate read target that cannot serve as the sentinel.
        uint64_t fail_reads_at_location{std::numeric_limits<uint64_t>::max()};
        // Set by crash_revert(): every further I/O fails.
        bool crashed{false};
        // Diagnostics.
        uint64_t writes_seen{0};
        uint64_t syncs_seen{0};
        uint64_t reads_failed{0};
        uint64_t header_writes_failed{0};
    };

    class faulty_file_handle_t final : public core::filesystem::file_handle_t {
    public:
        faulty_file_handle_t(std::unique_ptr<core::filesystem::file_handle_t> inner, fault_plan_t& plan)
            : core::filesystem::file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner))
            , plan_(plan)
            , synced_size_(inner_->file_size()) {}

        ~faulty_file_handle_t() override = default;

        bool write(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            if (plan_.crashed) {
                return false;
            }
            plan_.writes_seen++;
            if (plan_.fail_after_writes != 0 && plan_.writes_seen > plan_.fail_after_writes) {
                return false;
            }
            if (plan_.fail_writes_from != 0 && plan_.writes_seen >= plan_.fail_writes_from) {
                return false;
            }
            if (plan_.fail_writes_at_header_slots &&
                (location == components::table::storage::SECTOR_SIZE ||
                 location == 2 * components::table::storage::SECTOR_SIZE)) {
                plan_.header_writes_failed++;
                return false;
            }
            record_undo(location, nr_bytes);
            if (plan_.torn_at_write != 0 && plan_.writes_seen == plan_.torn_at_write) {
                // Persist only the first half, report failure: a torn sector train.
                uint64_t half = nr_bytes / 2;
                if (half != 0) {
                    inner_->write(buffer, half, location);
                }
                // Everything after a torn write is lost too.
                plan_.fail_after_writes = plan_.writes_seen;
                return false;
            }
            return inner_->write(buffer, nr_bytes, location);
        }

        core::filesystem::write_result_t write(void* buffer, uint64_t nr_bytes) override {
            // The block manager and the WAL both write POSITIONALLY, so this overload used
            // to only count and delegate. The bitcask index appends SEQUENTIALLY -- its
            // record writer and its txn-log writer both take this door -- so the same
            // knobs answer here. Nothing that used this header before reaches this overload,
            // so nothing that passed before changes.
            //
            // THE THIRD KNOB WAS MISSING HERE, and its absence hid the defect this seam is
            // meant to stage: torn_at_write was honoured only by the POSITIONAL overload, so
            // the one consumer that appends -- the bitcask index -- could be refused outright
            // but could never be torn. A torn SEQUENTIAL write is precisely the case in which
            // bytes land, the descriptor moves, and the answer must still say "refused";
            // without it, nothing in the suite could ask a caller whether it learns N.
            if (plan_.crashed) {
                return core::filesystem::write_result_t::refused(0);
            }
            plan_.writes_seen++;
            if (plan_.fail_after_writes != 0 && plan_.writes_seen > plan_.fail_after_writes) {
                return core::filesystem::write_result_t::refused(0);
            }
            if (plan_.fail_writes_from != 0 && plan_.writes_seen >= plan_.fail_writes_from) {
                return core::filesystem::write_result_t::refused(0);
            }
            // THE JOURNAL COVERS THIS OVERLOAD TOO. crash_revert() promises the file exactly
            // as it stood at the last fsync, and it keeps that promise in two halves: the
            // pre-images undo overwrites, the synced-length truncate undoes growth. A
            // sequential write that APPENDS is covered by the second half alone, which is why
            // the omission has never fired -- but a sequential write after a seek() into the
            // middle of the file overwrites, and nothing would have put those bytes back. The
            // position is the INNER handle's: this wrapper has no descriptor of its own.
            record_undo(inner_->seek_position(), nr_bytes);
            if (plan_.torn_at_write != 0 && plan_.writes_seen == plan_.torn_at_write) {
                // Persist only the first half and report the refusal WITH that count -- the
                // shape write(2) itself produces when it short-counts and then refuses.
                const uint64_t half = nr_bytes / 2;
                core::filesystem::write_result_t landed{};
                if (half != 0) {
                    landed = inner_->write(buffer, half);
                }
                // Everything after a torn write is lost too.
                plan_.fail_after_writes = plan_.writes_seen;
                return core::filesystem::write_result_t::refused(landed.bytes_written);
            }
            return inner_->write(buffer, nr_bytes);
        }

        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            if (plan_.crashed) {
                return false;
            }
            if (location == plan_.fail_reads_at_location) {
                plan_.reads_failed++;
                return false;
            }
            return inner_->read(buffer, nr_bytes, location);
        }

        int64_t read(void* buffer, uint64_t nr_bytes) override {
            if (plan_.crashed) {
                return -1;
            }
            return inner_->read(buffer, nr_bytes);
        }

        bool sync() override {
            if (plan_.crashed) {
                return false;
            }
            plan_.syncs_seen++;
            if (plan_.fail_syncs_from != 0 && plan_.syncs_seen >= plan_.fail_syncs_from) {
                return false;
            }
            bool ok = inner_->sync();
            if (ok) {
                undo_.clear();
                synced_size_ = inner_->file_size();
            }
            return ok;
        }

        bool truncate(int64_t new_size) override {
            if (plan_.crashed) {
                return false;
            }
            return inner_->truncate(new_size);
        }

        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            if (plan_.crashed) {
                return false;
            }
            return inner_->trim(offset_bytes, length_bytes);
        }

        // APPEND-STYLE CALLERS MOVE THE DESCRIPTOR, and the descriptor that matters is the
        // inner one: the free functions behind these read the handle they are given as a
        // platform handle, so answering from `this` would read a garbage fd (see the note at
        // the top of this file). Nothing is injected here -- a seek is not a device
        // operation a plan knob models -- they exist only to keep the wrapper transparent.
        bool seek(uint64_t location) override { return inner_->seek(location); }

        uint64_t seek_position() override { return inner_->seek_position(); }

        uint64_t file_size() override { return inner_->file_size(); }

        void close() override { inner_->close(); }

        // Simulate kill -9: revert every positional write since the last successful sync
        // (restore pre-images newest-first, then restore the synced length) and kill the
        // handle. The on-disk file is then exactly what a crash would conservatively leave.
        void crash_revert() {
            for (auto it = undo_.rbegin(); it != undo_.rend(); ++it) {
                if (!it->old_bytes.empty()) {
                    inner_->write(it->old_bytes.data(), it->old_bytes.size(), it->location);
                }
            }
            inner_->truncate(static_cast<int64_t>(synced_size_));
            inner_->sync();
            undo_.clear();
            plan_.crashed = true;
        }

    private:
        struct undo_entry_t {
            uint64_t location;
            std::vector<char> old_bytes; // pre-image; may be shorter than the write when the
                                         // write extended the file (the tail is handled by
                                         // the synced-length truncate)
        };

        void record_undo(uint64_t location, uint64_t nr_bytes) {
            undo_entry_t entry;
            entry.location = location;
            uint64_t size = inner_->file_size();
            if (location < size) {
                uint64_t readable = std::min(nr_bytes, size - location);
                entry.old_bytes.resize(readable);
                if (!inner_->read(entry.old_bytes.data(), readable, location)) {
                    entry.old_bytes.clear();
                }
            }
            undo_.push_back(std::move(entry));
        }

        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        fault_plan_t& plan_;
        std::vector<undo_entry_t> undo_; // pre-images since the last successful sync
        uint64_t synced_size_{0};
    };

    // Interposer + RAII installer. The seam is process-wide, so tests MUST scope it.
    class fault_injection_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        explicit fault_injection_scope_t(fault_plan_t& plan)
            : plan_(plan) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~fault_injection_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            auto wrapped = std::make_unique<faulty_file_handle_t>(std::move(inner), plan_);
            last_wrapped_ = wrapped.get();
            return wrapped;
        }

        // The most recently wrapped handle (the block manager's current one).
        faulty_file_handle_t* last() const { return last_wrapped_; }

    private:
        fault_plan_t& plan_;
        faulty_file_handle_t* last_wrapped_{nullptr};
    };

} // namespace otterbrix_test
