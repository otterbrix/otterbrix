#pragma once

#include <core/file/file_system.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory_resource>
#include <vector>

namespace components::table::storage {

    // Spill target for transient buffers with no block yet; unlinked at destruction, so nothing
    // in it may ever be read after a restart.
    class temporary_spill_file_t {
    public:
        static constexpr uint64_t INVALID_SLOT = UINT64_MAX;

        explicit temporary_spill_file_t(std::pmr::memory_resource* resource);
        temporary_spill_file_t(const temporary_spill_file_t&) = delete;
        temporary_spill_file_t& operator=(const temporary_spill_file_t&) = delete;
        ~temporary_spill_file_t();

        // io_error means the buffer was NOT written -- the caller must keep it in memory, not drop it.
        [[nodiscard]] core::result_wrapper_t<uint64_t> write(const std::byte* data, uint64_t size);

        // False means unrecoverable bytes (data loss); callers report it rather than returning an empty buffer.
        [[nodiscard]] bool read(uint64_t slot, std::byte* data, uint64_t size);

        // Safe to call with INVALID_SLOT.
        void release(uint64_t slot, uint64_t size);

        uint64_t bytes_in_use() const noexcept { return bytes_in_use_; }

    private:
        [[nodiscard]] bool ensure_open();

        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        core::filesystem::path_t path_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        // size -> free offsets of exactly that size
        std::pmr::map<uint64_t, std::pmr::vector<uint64_t>> free_slots_;
        uint64_t cursor_{0};
        uint64_t bytes_in_use_{0};
    };

} // namespace components::table::storage
