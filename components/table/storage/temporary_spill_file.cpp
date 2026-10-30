#include "temporary_spill_file.hpp"

#include <array>
#include <cstdlib>
#include <string>
#include <system_error>
#include <unistd.h>

namespace components::table::storage {

    namespace {
        // The variables std::filesystem::temp_directory_path() consults, in its order (libc++ and libstdc++).
        std::string temporary_directory_source() {
            for (const char* name : std::array{"TMPDIR", "TMP", "TEMP", "TEMPDIR"}) {
                if (const char* value = std::getenv(name); value != nullptr) {
                    return std::string{name} + "=" + value;
                }
            }
            return "/tmp";
        }
    } // namespace

    temporary_spill_file_t::temporary_spill_file_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , fs_()
        , free_slots_(resource) {
        std::error_code ec;
        const auto directory = std::filesystem::temp_directory_path(ec);
        if (ec) {
            construction_error_ = core::error_t(core::error_code_t::io_error,
                                                std::pmr::string{"temporary_spill_file_t: the temporary directory (" +
                                                                     temporary_directory_source() +
                                                                     ") cannot hold spill files: " + ec.message(),
                                                                 resource_});
            return;
        }
        // One file per pool instance. The pid keeps two processes apart; the address keeps two pools
        // inside one process apart (tests build several). Nothing reads these files but the pool that
        // wrote them, and only while it is alive.
        path_ = directory / ("otterbrix_spill_" + std::to_string(static_cast<long>(::getpid())) + "_" +
                             std::to_string(reinterpret_cast<uintptr_t>(this)) + ".tmp");
    }

    temporary_spill_file_t::~temporary_spill_file_t() {
        file_.reset();
        if (path_.empty()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    bool temporary_spill_file_t::ensure_open() {
        if (file_ != nullptr) {
            return true;
        }
        // Created on first use: a pool that never overflows never touches the filesystem.
        file_ = core::filesystem::open_file(fs_,
                                            path_,
                                            core::filesystem::file_flags::READ | core::filesystem::file_flags::WRITE |
                                                core::filesystem::file_flags::FILE_CREATE);
        return file_ != nullptr;
    }

    core::result_wrapper_t<uint64_t> temporary_spill_file_t::write(const std::byte* data, uint64_t size) {
        if (size == 0) {
            return core::error_t(core::error_code_t::internal_error,
                                 std::pmr::string{"temporary_spill_file_t::write: zero-length buffer", resource_});
        }
        if (construction_error_.contains_error()) {
            return core::error_on(resource_, construction_error_);
        }
        if (!ensure_open()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"temporary_spill_file_t: cannot open the spill file", resource_});
        }

        uint64_t slot = INVALID_SLOT;
        auto bucket = free_slots_.find(size);
        if (bucket != free_slots_.end() && !bucket->second.empty()) {
            slot = bucket->second.back();
            bucket->second.pop_back();
        } else {
            slot = cursor_;
            cursor_ += size;
        }

        if (!file_->write(const_cast<std::byte*>(data), size, slot)) {
            // Hand the slot back: nothing was written to it, and the caller is about to keep the
            // buffer in memory.
            free_slots_[size].push_back(slot);
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"temporary_spill_file_t: write failed", resource_});
        }
        bytes_in_use_ += size;
        return slot;
    }

    bool temporary_spill_file_t::read(uint64_t slot, std::byte* data, uint64_t size) {
        if (slot == INVALID_SLOT || file_ == nullptr || size == 0) {
            return false;
        }
        return file_->read(static_cast<void*>(data), size, slot);
    }

    void temporary_spill_file_t::release(uint64_t slot, uint64_t size) {
        if (slot == INVALID_SLOT || size == 0) {
            return;
        }
        auto it = free_slots_.find(size);
        if (it == free_slots_.end()) {
            it = free_slots_.emplace(size, std::pmr::vector<uint64_t>(resource_)).first;
        }
        it->second.push_back(slot);
        bytes_in_use_ -= size < bytes_in_use_ ? size : bytes_in_use_;
    }

} // namespace components::table::storage
