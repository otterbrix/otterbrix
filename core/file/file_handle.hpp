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

    // THE ANSWER OF A SEQUENTIAL WRITE, which an int64_t could not give.
    //
    // Packing a byte count and an error code into one integer loses the only fact a caller can
    // act on: a write that short-counts and THEN refuses has already put bytes on the device and
    // already moved the descriptor over them. The count accumulated across the loop's iterations
    // is then thrown away in favour of the failing iteration's own return -- -1, from the POSIX
    // write(2) loop, which since the Windows arm was removed from local_file_system.cpp is the
    // only loop there is. So "nothing was written" and "12 of the 25 bytes were written" arrive
    // as the same answer, and a caller that cannot tell them apart can neither truncate the
    // stump nor rewind to it.
    //
    // Both fields are needed, and neither derives from the other:
    //   - bytes_written is how much REACHED the file and therefore how far the descriptor moved.
    //     It is meaningful on refusal, which is the whole point;
    //   - complete says whether the request finished. It cannot be recomputed as
    //     `bytes_written == requested` at every call site, because a zero-byte request makes the
    //     full success and the outright refusal both `bytes_written == 0`.
    //
    // core::result_wrapper_t is deliberately NOT the vehicle: it holds a value OR an error
    // (value() asserts !has_error()), so it cannot express "this much landed AND it failed".
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
        // SEEK AND ITS QUERY ARE VIRTUAL FOR THE SAME REASON THE READS AND WRITES ARE.
        // They were left non-virtual because the two consumers of the interposer at the time
        // (the .otbx block manager and the WAL) address their files POSITIONALLY and never
        // move the descriptor. The bitcask index APPENDS: it seeks to the end when it opens
        // a segment and asks the position back for every record it writes. On a wrapper that
        // could not override them, both calls ran the free function against the WRAPPER --
        // whose fd is the garbage the comment above warns about -- so records went to the
        // wrong offset and the keydir recorded that offset as fact.
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

        // THE CLOSE THAT CAN BE REPORTED. It used to be `virtual void close() = 0;`, and that
        // `void` was the whole defect: ::close(2) can fail, and on a write-back filesystem it is
        // where a deferred write error (EIO) is finally reported, so a refused close is a lost
        // write and not a cosmetic detail. A void return could only lose it.
        //
        // The five delegating test wrappers -- core/b_plus_tree/tests/test_b_plus_tree.cpp,
        // components/table/test/fault_injection_file.hpp,
        // services/wal/tests/test_wal_truncate_header_race.cpp,
        // integration/cpp/test/test_udf_refusal_registry_state.cpp and
        // integration/cpp/test/test_catalog_read_refusal.cpp -- now read
        // `core::error_t close() override { return inner_->close(); }`, so a refusal raised by the
        // wrapped handle travels OUT through them. That is why the channel is this return value
        // and not a second, parallel `close_status()` beside a void close(): the wrappers
        // DELEGATE, so a separate slot of their own would answer "no error" while the wrapped
        // handle held the refusal -- a new liar in place of an honest gap.
        //
        // WHAT `what` CARRIES, AND WHY IT IS EMPTY. core::error_t's message is a std::pmr::string
        // and therefore needs an arena; this layer has none (file_handle_t holds a filesystem
        // reference and a path, nothing more), rule 14 leaves no process-global to borrow, and
        // binding the message to an arena the handle owns would hand the caller a string that
        // dies with the handle. So the refusal is returned as `io_error` with an EMPTY message,
        // built on std::pmr::null_memory_resource() -- allocation-free by construction, the same
        // move error_t::no_error() already makes -- and the variable half (path and errno) is
        // printed to stderr by the implementation that knows it. The value says THAT it failed,
        // the stderr line says WHICH file and WHY; neither is silent (rule 6).
        //
        // THE DESTRUCTOR IS THE ONE CALLER THAT CANNOT ACT ON IT, and that is not a rule-6
        // violation. See ~unix_file_handle_t in local_file_system.cpp: rule 6 asks a refusal to be
        // LOUD, not FATAL, and the only upward channel a destructor has is a throw, which crosses
        // a destructor into std::terminate -- trading a report about one lost write for the loss
        // of every other handle still to be flushed. It therefore prints and drops, deliberately
        // and by name.
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