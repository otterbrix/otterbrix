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

    // Scratch space for buffers the pool has to push out of memory but that have nowhere else to go.
    //
    // A column segment being appended into is transient: it has no block in the .otbx yet, so the
    // pool cannot simply drop it — dropping would lose the rows. Without somewhere to put it, the
    // only options are to keep it resident (which is what made a load fail at 24M rows holding 9 MiB
    // on disk) or to lose data. This is that somewhere.
    //
    // Deliberately simple: one file, slots addressed by byte offset, a free list bucketed by exact
    // allocation size. Slots are only ever reused for a request of the same size, so a slot never
    // partially covers a later buffer. The file is unlinked when the object dies — nothing in it
    // survives the process, and nothing may ever be read from it after a restart.
    class temporary_spill_file_t {
    public:
        static constexpr uint64_t INVALID_SLOT = UINT64_MAX;

        explicit temporary_spill_file_t(std::pmr::memory_resource* resource);
        temporary_spill_file_t(const temporary_spill_file_t&) = delete;
        temporary_spill_file_t& operator=(const temporary_spill_file_t&) = delete;
        ~temporary_spill_file_t();

        // Writes `size` bytes and returns the slot they landed in. An io_error here means the buffer
        // was NOT written: the caller must keep it in memory rather than dropping it.
        [[nodiscard]] core::result_wrapper_t<uint64_t> write(const std::byte* data, uint64_t size);

        // Reads a slot back. False means the bytes could not be recovered, which for a transient
        // buffer is data loss — the caller reports it rather than handing back an empty buffer.
        [[nodiscard]] bool read(uint64_t slot, std::byte* data, uint64_t size);

        // Returns the slot to the free list. Safe to call with INVALID_SLOT.
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
