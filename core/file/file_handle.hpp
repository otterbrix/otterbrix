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

    // Neither an int64_t nor result_wrapper_t (value OR error) can express both landed-and-failed.
    struct [[nodiscard]] write_result_t {
        uint64_t bytes_written{0};
        bool complete{false};

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

        // Production free functions reinterpret_cast the handle to the PLATFORM type, so any
        // wrapper must delegate to its wrapped inner handle, never pass itself.
        virtual int64_t read(void* buffer, uint64_t nr_bytes);
        // SEQUENTIAL WRITE. Returns what landed AND whether it finished -- see write_result_t.
        virtual write_result_t write(void* buffer, uint64_t nr_bytes);
        virtual bool read(void* buffer, uint64_t nr_bytes, uint64_t location);
        virtual bool write(void* buffer, uint64_t nr_bytes, uint64_t location);
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

        // ::close(2) can fail, and on a write-back filesystem that is where a deferred write error
        // (EIO) surfaces, so a refused close is a lost write; delegating wrappers must forward it.
        // Not a rule-6 violation: a destructor's only upward channel is std::terminate, trading one
        // lost write's report for every other handle still to flush; prints and drops instead.
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