#pragma once

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <string>

#if defined(_WIN32) || defined(_WIN64)
#define PLATFORM_WINDOWS
#elif defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__))
#define PLATFORM_POSIX
#endif

#undef create_directory
#undef move_files
#undef remove_directory

namespace core::filesystem {

    using path_t = std::filesystem::path;

    class local_file_system_t;

    // An int64_t return can't answer a sequential write: a write that short-counts and THEN
    // refuses has already moved the descriptor over the bytes it did write, and packing count
    // and error into one integer throws that count away in favour of the failing iteration's own
    // -1. Both fields are needed and neither derives from the other -- complete can't be
    // recomputed as `bytes_written == requested` since a zero-byte request makes both full
    // success and outright refusal `bytes_written == 0`. Not core::result_wrapper_t either: it
    // holds a value OR an error, so it can't express "this much landed AND it failed".
    struct [[nodiscard]] write_result_t {
        uint64_t bytes_written{0};
        bool complete{false};

        // The state that has no name in an int64_t: a stump on disk after a refusal.
        [[nodiscard]] bool partial() const noexcept { return !complete && bytes_written != 0; }

        static write_result_t done(uint64_t written) noexcept { return write_result_t{written, true}; }
        static write_result_t refused(uint64_t written) noexcept { return write_result_t{written, false}; }
    };

    enum class file_type_t
    {
        REGULAR,
        DIR,
        FIFO,
        SOCKET,
        LINK,
        BLOCKDEV,
        CHARDEV,
        INVALID
    };

    struct file_handle_t {
    public:
        file_handle_t(local_file_system_t& fs, path_t path);
        file_handle_t(const file_handle_t&) = delete;
        virtual ~file_handle_t();

        // The I/O entry points are virtual so a test-side wrapper can interpose fault
        // injection / crash simulation. Production handles inherit the
        // default bodies, which delegate to the filesystem free functions; those free
        // functions reinterpret_cast the handle to the PLATFORM handle type, so a wrapper
        // must always override and delegate to its wrapped inner handle, never pass itself.
        virtual int64_t read(void* buffer, uint64_t nr_bytes);
        // SEQUENTIAL WRITE. Returns what landed AND whether it finished -- see write_result_t.
        virtual write_result_t write(void* buffer, uint64_t nr_bytes);
        virtual bool read(void* buffer, uint64_t nr_bytes, uint64_t location);
        virtual bool write(void* buffer, uint64_t nr_bytes, uint64_t location);
        // Seek and its query are virtual for the same reason reads/writes are: the .otbx block
        // manager and the WAL address files positionally and never move the descriptor, but the
        // bitcask index APPENDS (seeks to the end, asks the position back per record). A wrapper
        // that couldn't override them ran the free function against the WRAPPER's garbage fd, so
        // records went to the wrong offset and the keydir recorded that as fact.
        virtual bool seek(uint64_t location);
        void reset();
        virtual uint64_t seek_position();
        virtual bool sync();
        virtual bool truncate(int64_t new_size);
        virtual bool trim(uint64_t offset_bytes, uint64_t length_bytes);
        std::string read_line();

        bool can_seek();
        bool is_pipe();
        virtual uint64_t file_size();
        file_type_t type();

        // Used to be `virtual void close() = 0;`: ::close(2) can fail, and on a write-back
        // filesystem that's where a deferred write error (EIO) is finally reported, so a refused
        // close is a lost write, not a cosmetic detail. The five delegating test wrappers
        // (core/b_plus_tree/tests/test_b_plus_tree.cpp, components/table/test/
        // fault_injection_file.hpp, services/wal/tests/test_wal_truncate_header_race.cpp,
        // integration/cpp/test/test_udf_refusal_registry_state.cpp,
        // integration/cpp/test/test_catalog_read_refusal.cpp) now read
        // `core::error_t close() override { return inner_->close(); }`, so a refusal travels out
        // through them -- a separate parallel `close_status()` would let a wrapper's own slot
        // answer "no error" while the wrapped handle held the refusal.
        //
        // `what` is EMPTY: core::error_t's message is a std::pmr::string needing an arena, this
        // layer has none, and binding it to the handle's own arena would hand back a string that
        // dies with the handle. So the refusal is `io_error` with an empty message (built on
        // std::pmr::null_memory_resource()), and path/errno are printed to stderr instead.
        //
        // The destructor is the one caller that can't act on it -- not a rule-6 violation: the
        // only upward channel a destructor has is a throw into std::terminate, trading one lost
        // write's report for the loss of every other handle still to be flushed. It prints and
        // drops instead (see ~unix_file_handle_t in local_file_system.cpp).
        virtual core::error_t close() = 0;

        path_t path() const { return path_; }

    public:
        local_file_system_t& fs_;
        path_t path_;
    };

    enum class file_lock_type : uint8_t
    {
        NO_LOCK = 0,
        READ_LOCK = 1,
        WRITE_LOCK = 2
    };
    static constexpr file_lock_type DEFAULT_LOCK = file_lock_type::NO_LOCK;
    enum class file_compression_type : uint8_t
    {
        AUTO_DETECT = 0,
        UNCOMPRESSED = 1,
        GZIP = 2,
        ZSTD = 3
    };

    enum class file_flags : uint16_t
    {
        EMPTY = 0,
        READ = 1 << 0,
        WRITE = 1 << 1,
        DIRECT_IO = 1 << 2,
        FILE_CREATE = 1 << 3,
        FILE_CREATE_NEW = 1 << 4,
        APPEND = 1 << 5,
        PRIVATE = 1 << 6,
        NULL_IF_NOT_EXISTS = 1 << 7,
        PARALLEL_ACCESS = 1 << 8
    };
    constexpr file_flags operator|(file_flags a, file_flags b) {
        return static_cast<file_flags>(static_cast<uint16_t>(a) | static_cast<uint16_t>(b));
    }
    constexpr file_flags operator&(file_flags a, file_flags b) {
        return static_cast<file_flags>(static_cast<uint16_t>(a) & static_cast<uint16_t>(b));
    }
    constexpr file_flags& operator|=(file_flags& a, file_flags b) {
        return a = static_cast<file_flags>(static_cast<uint16_t>(a) | static_cast<uint16_t>(b));
    }
    constexpr file_flags& operator&=(file_flags& a, file_flags b) {
        return a = static_cast<file_flags>(static_cast<uint16_t>(a) & static_cast<uint16_t>(b));
    }

} // namespace core::filesystem