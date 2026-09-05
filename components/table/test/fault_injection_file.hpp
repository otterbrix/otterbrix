#pragma once

// Fault-injection file handle + crash simulation (test-side): wraps the real database file
// handle (installed through the DEV_MODE interposer seam in single_file_block_manager_t) to
// fail writes/reads/syncs on a plan, and to simulate kill -9 via crash_revert() (an undo
// journal rolls the file back to exactly its state at the last fsync).
// Must always delegate to the wrapped inner handle: the filesystem free functions
// reinterpret_cast their handle argument to the platform handle type, so passing the wrapper
// itself into them would read a garbage fd.

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
        // Fail the Nth and every later positional write (1-based). 0 = off. Exists because
        // fail_after_writes counts ALLOWED successes and its 0 means "off", so it can't
        // express "zero writes succeed". The two knobs compose by OR.
        uint64_t fail_writes_from{0};
        // Tear the Nth positional write (1-based): persist only its first half, then fail
        // it and everything after. 0 = off.
        uint64_t torn_at_write{0};
        // Fail the Nth sync() and every later one (1-based). 0 = off; 1 fails the very first
        // sync, the only one create_new_database issues. Models a write that reached the page
        // cache but never the device — what the checkpoint's second fsync exists to catch.
        uint64_t fail_syncs_from{0};
        // Fails only positional writes landing in a header slot (write_header's two
        // alternating offsets), producing the RECOVERABLE checkpoint failure (write_header's
        // case 2) instead of the degraded state the COUNTED knobs above would cause by also
        // failing the preceding data/metadata writes. Names the offsets, not the caller:
        // create_new_database's first header write lands at the same offset, so arming this
        // during file creation fails creation instead — arm it around the round under test.
        bool fail_writes_at_header_slots{false};
        // Fails the one positional READ at this exact offset — models a single rotten block,
        // the case a metadata-chain walk must survive. UINT64_MAX = off; 0 can't be the
        // sentinel since it's the main header's legitimate offset.
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
            // The block manager and WAL write positionally; the bitcask index's record and
            // txn-log writers append sequentially through this overload instead, so it shares
            // the same knobs. torn_at_write previously was honoured only by the positional
            // overload, so a torn sequential append could be refused but never torn — the
            // case where bytes land, the descriptor moves, yet the caller must still see
            // "refused".
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
            // crash_revert() undoes overwrites via pre-images and growth via the
            // synced-length truncate; a sequential write after seek() into the middle relies
            // on the pre-image half, unlike a plain append. Uses the INNER handle's position
            // — this wrapper keeps no descriptor of its own.
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

        // Must delegate to the inner descriptor (see the file-header note); nothing to
        // inject here, a seek isn't a device operation any plan knob models.
        bool seek(uint64_t location) override { return inner_->seek(location); }

        uint64_t seek_position() override { return inner_->seek_position(); }

        uint64_t file_size() override { return inner_->file_size(); }

        // Must forward the refusal: answering "no error" while the wrapped handle refused
        // would make this wrapper a liar too.
        core::error_t close() override { return inner_->close(); }

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
