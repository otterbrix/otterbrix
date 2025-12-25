#pragma once

#include <filesystem>
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

        int64_t read(void* buffer, uint64_t nr_bytes);
        int64_t write(void* buffer, uint64_t nr_bytes);
        bool read(void* buffer, uint64_t nr_bytes, uint64_t location);
        bool write(void* buffer, uint64_t nr_bytes, uint64_t location);
        bool seek(uint64_t location);
        void reset();
        uint64_t seek_position();
        bool sync();
        bool truncate(int64_t new_size);
        bool trim(uint64_t offset_bytes, uint64_t length_bytes);
        std::string read_line();

        bool can_seek();
        bool is_pipe();
        uint64_t file_size();
        file_type_t type();

        virtual void close() = 0;

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