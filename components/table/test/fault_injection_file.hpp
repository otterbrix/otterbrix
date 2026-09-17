#pragma once

// Wraps the real file handle to inject faults; must always delegate to inner_, since filesystem
// free functions reinterpret_cast the handle to the platform type.

#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include <components/table/storage/single_file_block_manager.hpp>
#include <core/file/file_handle.hpp>

namespace otterbrix_test {

    struct fault_plan_t {
        // Fails from N+1 vs from N: only fail_writes_from can express zero successes; 0 = off for both.
        uint64_t fail_after_writes{0};
        uint64_t fail_writes_from{0};
        uint64_t torn_at_write{0};
        uint64_t fail_syncs_from{0};
        // Fails only header-slot writes; arm around the tested round, since creation writes the same offset.
        bool fail_writes_at_header_slots{false};
        // Fails the one read at this exact offset (models a rotten block); UINT64_MAX = off, since 0 is legitimate.
        uint64_t fail_reads_at_location{std::numeric_limits<uint64_t>::max()};
        bool crashed{false};
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
            std::lock_guard lock(plan_mutex_);
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
            if (plan_.fail_writes_at_header_slots && (location == components::table::storage::SECTOR_SIZE ||
                                                      location == 2 * components::table::storage::SECTOR_SIZE)) {
                plan_.header_writes_failed++;
                return false;
            }
            record_undo(location, nr_bytes);
            if (plan_.torn_at_write != 0 && plan_.writes_seen == plan_.torn_at_write) {
                uint64_t half = nr_bytes / 2;
                if (half != 0) {
                    inner_->write(buffer, half, location);
                }
                plan_.fail_after_writes = plan_.writes_seen;
                return false;
            }
            return inner_->write(buffer, nr_bytes, location);
        }

        core::filesystem::write_result_t write(void* buffer, uint64_t nr_bytes) override {
            std::lock_guard lock(plan_mutex_);
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
            record_undo(inner_->seek_position(), nr_bytes);
            if (plan_.torn_at_write != 0 && plan_.writes_seen == plan_.torn_at_write) {
                const uint64_t half = nr_bytes / 2;
                core::filesystem::write_result_t landed{};
                if (half != 0) {
                    landed = inner_->write(buffer, half);
                }
                plan_.fail_after_writes = plan_.writes_seen;
                return core::filesystem::write_result_t::refused(landed.bytes_written);
            }
            return inner_->write(buffer, nr_bytes);
        }

        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            std::lock_guard lock(plan_mutex_);
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
            std::lock_guard lock(plan_mutex_);
            if (plan_.crashed) {
                return -1;
            }
            return inner_->read(buffer, nr_bytes);
        }

        bool sync() override {
            std::lock_guard lock(plan_mutex_);
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
            std::lock_guard lock(plan_mutex_);
            if (plan_.crashed) {
                return false;
            }
            return inner_->truncate(new_size);
        }

        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            std::lock_guard lock(plan_mutex_);
            if (plan_.crashed) {
                return false;
            }
            return inner_->trim(offset_bytes, length_bytes);
        }

        bool seek(uint64_t location) override { return inner_->seek(location); }

        uint64_t seek_position() override { return inner_->seek_position(); }

        uint64_t file_size() override { return inner_->file_size(); }

        core::error_t close() override { return inner_->close(); }

        // Undoes writes newest-first and truncates to the last synced length, as a real crash would.
        void crash_revert() {
            std::lock_guard lock(plan_mutex_);
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
            std::vector<char> old_bytes;
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

        // One plan serves every wrapped file, and the disk agents write their files from different threads.
        static inline std::mutex plan_mutex_;
        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        fault_plan_t& plan_;
        std::vector<undo_entry_t> undo_;
        uint64_t synced_size_{0};
    };

    // RAII install/uninstall of the process-wide interposer; tests must scope it.
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

        faulty_file_handle_t* last() const { return last_wrapped_; }

    private:
        fault_plan_t& plan_;
        faulty_file_handle_t* last_wrapped_{nullptr};
    };

} // namespace otterbrix_test
