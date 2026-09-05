#pragma once

// T3: fault-injection file handle + crash simulation (test-side).
//
// A faulty_file_handle_t wraps the real database file handle (installed through the
// DEV_MODE interposer seam in single_file_block_manager_t) and can:
//   - fail every positional write after the Nth one (fail_after_writes);
//   - tear one write in half and fail everything after it (torn_at_write);
//   - fail every read of ONE block location (fail_reads_at_location) — a rotten block, i.e.
//     the failure a walk of the durable root's metadata chain has to survive;
//   - simulate kill -9: an undo journal records the pre-image of every positional write
//     since the last successful sync(); crash_revert() rolls the file back to exactly its
//     state at the last fsync — the conservative crash semantics (nothing unsynced
//     survived). After crash_revert() every further I/O on the handle fails.
//
// No test may lay out files by hand (plan rule for T3); reopening the reverted file — or a
// filesystem copy of it — under a fresh environment IS the "state after kill".
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
        // is exactly the k=0 point of the A7.4 crash matrix. The two knobs compose by OR:
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

        int64_t write(void* buffer, uint64_t nr_bytes) override {
            // The block manager and the WAL both write POSITIONALLY, so this overload used
            // to only count and delegate. The bitcask index appends SEQUENTIALLY -- its
            // record writer and its txn-log writer both take this door -- so the same two
            // knobs answer here. Nothing that used this header before reaches this overload,
            // so nothing that passed before changes.
            //
            // A short count is the refusal, matching what write(2) reports on a full device
            // and what every caller of the sequential overload compares against.
            if (plan_.crashed) {
                return -1;
            }
            plan_.writes_seen++;
            if (plan_.fail_after_writes != 0 && plan_.writes_seen > plan_.fail_after_writes) {
                return -1;
            }
            if (plan_.fail_writes_from != 0 && plan_.writes_seen >= plan_.fail_writes_from) {
                return -1;
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
