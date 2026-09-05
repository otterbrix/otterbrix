#include <catch2/catch_test_macros.hpp>

#include "file_system.hpp"
#include <components/log/log.hpp>
#include <algorithm>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/resource.h>
#include <unistd.h>

using namespace std;
using namespace core::filesystem;

// Rooted under the system temp dir (not process CWD) and keyed by pid so concurrent runs
// don't collide; create_directory() below is a bare mkdir(2), so the base must pre-exist.
static path_t make_testing_directory() {
    const auto base =
        std::filesystem::temp_directory_path() / ("otterbrix_file_system_" + std::to_string(::getpid()));
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    return base / "filesystem_test";
}

path_t testing_directory = make_testing_directory();

static void create_dummy_file(string fname1) {
    ofstream outfile(fname1);
    outfile << "test_string" << endl;
    outfile.close();
}

TEST_CASE("core::file::filesystem") {
    INFO("initialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (!directory_exists(fs, testing_directory)) {
            create_directory(fs, testing_directory);
        }
    }

    INFO("operators");
    {
        local_file_system_t fs = local_file_system_t();
        auto dname = testing_directory;
        dname /= "TEST_DIR";
        path_t fname1 = "TEST_FILE";
        path_t fname2 = "TEST_FILE_TWO";

        if (directory_exists(fs, dname)) {
            remove_directory(fs, dname);
        }

        create_directory(fs, dname);
        REQUIRE(directory_exists(fs, dname));
        REQUIRE_FALSE(file_exists(fs, dname));

        create_directory(fs, dname);

        auto fname_in_dir1 = dname;
        fname_in_dir1 /= fname1;
        auto fname_in_dir2 = dname;
        fname_in_dir2 /= fname2;

        create_dummy_file(fname_in_dir1);
        REQUIRE(file_exists(fs, fname_in_dir1));
        REQUIRE_FALSE(directory_exists(fs, fname_in_dir1));

        size_t n_files = 0;
        REQUIRE(list_files(fs, dname, [&n_files](const path_t&, bool) { n_files++; }));

        REQUIRE(n_files == 1);

        REQUIRE(file_exists(fs, fname_in_dir1));
        REQUIRE_FALSE(file_exists(fs, fname_in_dir2));

        move_files(fs, fname_in_dir1, fname_in_dir2);

        REQUIRE_FALSE(file_exists(fs, fname_in_dir1));
        REQUIRE(file_exists(fs, fname_in_dir2));

        remove_directory(fs, dname);

        REQUIRE_FALSE(directory_exists(fs, dname));
        REQUIRE_FALSE(file_exists(fs, fname_in_dir1));
        REQUIRE_FALSE(file_exists(fs, fname_in_dir2));
    }

    constexpr size_t size = 512;

    INFO("write_close_read");
    {
        local_file_system_t fs = local_file_system_t();
        unique_ptr<file_handle_t> handle;
        int64_t test_data[size];
        for (size_t i = 0; i < size; i++) {
            test_data[i] = static_cast<int64_t>(i);
        }

        auto fname = testing_directory;
        fname /= "test_file";

        // standard reading/writing test

        // open file for writing
        handle = open_file(fs, fname, file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
        // write 10 integers
        handle->write(test_data, sizeof(int64_t) * size, 0);
        // close the file
        handle.reset();

        for (size_t i = 0; i < size; i++) {
            test_data[i] = 0;
        }
        // now open the file for reading
        handle = open_file(fs, fname, file_flags::READ, file_lock_type::NO_LOCK);
        // read the 10 integers back
        handle->read(test_data, sizeof(int64_t) * size, 0);
        // check the values of the integers
        for (int i = 0; i < 10; i++) {
            REQUIRE(test_data[i] == i);
        }
        handle.reset();
        remove_file(fs, fname);
    }
    INFO("write_read without closing");
    {
        local_file_system_t fs = local_file_system_t();
        unique_ptr<file_handle_t> handle;
        int64_t test_data[size];
        for (size_t i = 0; i < size; i++) {
            test_data[i] = static_cast<int64_t>(i);
        }

        auto fname = testing_directory;
        fname /= "test_file";

        // standard reading/writing test

        // open file for writing
        handle = open_file(fs,
                           fname,
                           file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                           file_lock_type::NO_LOCK);
        // write 10 integers
        handle->write(test_data, sizeof(int64_t) * size, 0);
        handle->sync();

        for (size_t i = 0; i < size; i++) {
            test_data[i] = 0;
        }
        // read the 10 integers back
        handle->read(test_data, sizeof(int64_t) * size, 0);
        // check the values of the integers
        for (int i = 0; i < 10; i++) {
            REQUIRE(test_data[i] == i);
        }
        handle.reset();
        remove_file(fs, fname);
    }

    INFO("absolute_paths");
    {
        local_file_system_t fs;

#ifdef PLATFORM_WINDOWS
        const path_t long_path = "\\\\?\\D:\\very long network\\";
        REQUIRE(fs.is_path_absolute(network));
        REQUIRE(fs.normalize_path_absolute("C:/folder\\filename.csv") == "c:\\folder\\filename.csv");
        REQUIRE(fs.normalize_path_absolute(network) == network);
        REQUIRE(fs.normalize_path_absolute(long_path) == "\\\\?\\d:\\very long network\\");
#endif
    }

    INFO("deinitialization");
    {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}
// RLIMIT_FSIZE stages a real short-count-then-refuse write with no mock: the kernel writes up
// to the limit (EFBIG), the same shape as a full volume but reproducible, unlike ENOSPC.
// SIGXFSZ must be ignored or the refusal kills the test binary instead of being reported.
//
// Both the limit and the signal disposition are PROCESS-WIDE, so the guard restores them
// before the case returns; safe only because this binary is single-threaded and touches no
// other file while armed. The zero-ceiling case below uses a read-only fd instead, since an
// RLIMIT_FSIZE of 0 would ban writes process-wide.
namespace {
    struct fsize_limit_guard_t {
        struct rlimit previous {};
        struct sigaction previous_action {};
        bool armed{false};

        explicit fsize_limit_guard_t(rlim_t bytes) {
            if (::getrlimit(RLIMIT_FSIZE, &previous) != 0) {
                return;
            }
            struct sigaction ignore {};
            ignore.sa_handler = SIG_IGN;
            sigemptyset(&ignore.sa_mask);
            if (::sigaction(SIGXFSZ, &ignore, &previous_action) != 0) {
                return;
            }
            struct rlimit narrowed = previous;
            narrowed.rlim_cur = bytes;
            if (::setrlimit(RLIMIT_FSIZE, &narrowed) != 0) {
                ::sigaction(SIGXFSZ, &previous_action, nullptr);
                return;
            }
            armed = true;
        }

        ~fsize_limit_guard_t() {
            if (armed) {
                ::setrlimit(RLIMIT_FSIZE, &previous);
                ::sigaction(SIGXFSZ, &previous_action, nullptr);
            }
        }

        fsize_limit_guard_t(const fsize_limit_guard_t&) = delete;
        fsize_limit_guard_t& operator=(const fsize_limit_guard_t&) = delete;
    };
} // namespace

// Guards against a self-recursive forwarder: `return read(fs, ...)` inside
// `read(file_system<FSC>&, ...)` calls itself (fs is the wrapper, so overload resolution picks
// it again), compiles clean since templates are only diagnosed on instantiation, and blows the
// stack on first real use. Exercises open/write/size/seek/read/unlink through the wrapper.
TEST_CASE("core::file::filesystem::the_wrapper_forwards_to_its_backend") {
    file_system<local_file_system_t> fs{local_file_system_t()};
    std::error_code ec;
    std::filesystem::create_directories(testing_directory, ec);

    auto fname = testing_directory;
    fname /= "wrapper_forwarding";
    remove_file(fs, fname);

    auto handle =
        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
    REQUIRE(handle != nullptr);

    char payload[] = {'w', 'r', 'a', 'p'};
    const auto written = write(fs, *handle, payload, static_cast<int64_t>(sizeof(payload)));
    REQUIRE(written.complete);
    REQUIRE(written.bytes_written == sizeof(payload));
    REQUIRE(file_size(fs, *handle) == static_cast<int64_t>(sizeof(payload)));

    REQUIRE(seek(fs, *handle, uint64_t{0}));
    REQUIRE(seek_position(fs, *handle) == 0);
    char echoed[sizeof(payload)] = {};
    REQUIRE(read(fs, *handle, echoed, static_cast<int64_t>(sizeof(echoed))) ==
            static_cast<int64_t>(sizeof(echoed)));
    REQUIRE(std::equal(std::begin(payload), std::end(payload), std::begin(echoed)));

    handle.reset();
    REQUIRE(file_exists(fs, fname));
    REQUIRE(remove_file(fs, fname));
    REQUIRE_FALSE(file_exists(fs, fname));
}

TEST_CASE("core::file::filesystem::sequential_write_reports_what_it_wrote") {
    local_file_system_t fs = local_file_system_t();
    std::error_code ec;
    std::filesystem::create_directories(testing_directory, ec);

    auto fname = testing_directory;
    fname /= "partial_write";
    remove_file(fs, fname);

    constexpr uint64_t limit = 12;
    constexpr uint64_t requested = 25;
    char payload[requested];
    std::fill(std::begin(payload), std::end(payload), 'A');

    auto handle =
        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
    REQUIRE(handle != nullptr);

    write_result_t written{};
    {
        fsize_limit_guard_t guard{limit};
        REQUIRE(guard.armed);
        written = handle->write(payload, requested);
    }

    handle->sync();

    // Checked against file_size(), not `limit`: `limit` is a property of the staging, and a
    // kernel that refused the write whole (0 bytes) would satisfy the real contract just as
    // well as one that kept 12.
    REQUIRE_FALSE(written.complete);
    REQUIRE(written.bytes_written == handle->file_size());
    REQUIRE(written.bytes_written < requested);
    REQUIRE(written.partial() == (handle->file_size() != 0));

    // Descriptor must move by exactly what landed, so a caller's rewind lands correctly.
    REQUIRE(handle->seek_position() == written.bytes_written);

    // CHECK, not REQUIRE: pins that this kernel truncates rather than refusing whole (the
    // partial case, not the empty one) without failing the contract assertions above it.
    CHECK(written.bytes_written == limit);

    handle.reset();
    remove_file(fs, fname);
}

// A zero-byte request and a refusal that got nowhere are both `0` as a count, so `complete`
// carries the distinction that a bare int64_t return could not.
TEST_CASE("core::file::filesystem::sequential_write_separates_empty_from_refused") {
    local_file_system_t fs = local_file_system_t();
    std::error_code ec;
    std::filesystem::create_directories(testing_directory, ec);

    auto fname = testing_directory;
    fname /= "empty_write";
    remove_file(fs, fname);

    auto handle =
        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
    REQUIRE(handle != nullptr);

    char nothing = 0;
    const auto empty = handle->write(&nothing, 0);
    REQUIRE(empty.complete);
    REQUIRE(empty.bytes_written == 0);
    REQUIRE_FALSE(empty.partial());

    // Staged on a read-only handle, not RLIMIT_FSIZE=0: a zero ceiling would be a process-wide
    // write ban, reaching the suite's own logging; a read-only fd reaches only this descriptor.
    char payload[8];
    std::fill(std::begin(payload), std::end(payload), 'B');
    auto read_only = open_file(fs, fname, file_flags::READ, file_lock_type::NO_LOCK);
    REQUIRE(read_only != nullptr);
    const auto refused = read_only->write(payload, sizeof(payload));
    REQUIRE_FALSE(refused.complete);
    REQUIRE(refused.bytes_written == 0);
    REQUIRE_FALSE(refused.partial());
    // Nothing reached the file, which is the whole claim: `bytes_written == 0` has to mean it.
    REQUIRE(read_only->file_size() == 0);

    read_only.reset();
    handle.reset();
    remove_file(fs, fname);
}

// close() used to be `virtual void`, so a refused ::close(2) (a deferred write-back EIO) was
// unreportable; it now returns core::error_t.
//
// Refusal is staged by finding the handle's fd by IDENTITY (fstat every open fd, match
// st_dev/st_ino -- unique since nothing else has this path open) and closing it out from
// under the handle, so the handle's own close() answers EBADF.
static int descriptor_of(const path_t& path) {
    struct stat want = {};
    if (::stat(path.c_str(), &want) != 0) {
        return -1;
    }
    long ceiling = 256;
    struct rlimit limit = {};
    if (::getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur != RLIM_INFINITY &&
        static_cast<long>(limit.rlim_cur) < ceiling) {
        ceiling = static_cast<long>(limit.rlim_cur);
    }
    for (long candidate = 0; candidate < ceiling; ++candidate) {
        struct stat got = {};
        if (::fstat(static_cast<int>(candidate), &got) != 0) {
            continue;
        }
        if (got.st_dev == want.st_dev && got.st_ino == want.st_ino) {
            return static_cast<int>(candidate);
        }
    }
    return -1;
}

namespace {
    // Shape of the delegating wrappers elsewhere in the tree (test_b_plus_tree.cpp,
    // fault_injection_file.hpp, test_wal_truncate_header_race.cpp,
    // test_udf_refusal_registry_state.cpp, test_catalog_read_refusal.cpp): a wrapper that
    // swallowed close()'s refusal would report "no error" over a lost write.
    class forwarding_handle_t final : public file_handle_t {
    public:
        explicit forwarding_handle_t(std::unique_ptr<file_handle_t> inner)
            : file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner)) {}
        ~forwarding_handle_t() override = default;

        int64_t read(void* buffer, uint64_t nr_bytes) override { return inner_->read(buffer, nr_bytes); }
        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            return inner_->read(buffer, nr_bytes, location);
        }
        write_result_t write(void* buffer, uint64_t nr_bytes) override { return inner_->write(buffer, nr_bytes); }
        bool write(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            return inner_->write(buffer, nr_bytes, location);
        }
        bool seek(uint64_t location) override { return inner_->seek(location); }
        uint64_t seek_position() override { return inner_->seek_position(); }
        bool sync() override { return inner_->sync(); }
        bool truncate(int64_t new_size) override { return inner_->truncate(new_size); }
        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            return inner_->trim(offset_bytes, length_bytes);
        }
        uint64_t file_size() override { return inner_->file_size(); }
        core::error_t close() override { return inner_->close(); }

    private:
        std::unique_ptr<file_handle_t> inner_;
    };
} // namespace

TEST_CASE("core::file::filesystem::close_reports_its_refusal") {
    local_file_system_t fs = local_file_system_t();
    std::error_code ec;
    std::filesystem::create_directories(testing_directory, ec);

    auto fname = testing_directory;
    fname /= "close_refusal";
    remove_file(fs, fname);

    INFO("a close that worked says so, and a second one does not invent a refusal");
    {
        auto handle = open_file(fs,
                                fname,
                                file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                file_lock_type::NO_LOCK);
        REQUIRE(handle != nullptr);

        const core::error_t closed = handle->close();
        REQUIRE_FALSE(closed.contains_error());

        // Must be idempotent: the destructor also calls close(), which must not re-close a
        // descriptor number the kernel may have already handed to another opener.
        const core::error_t again = handle->close();
        REQUIRE_FALSE(again.contains_error());
        handle.reset();
    }

    INFO("a refused close reaches the caller as core::error_code_t::io_error");
    {
        auto handle = open_file(fs, fname, file_flags::READ, file_lock_type::NO_LOCK);
        REQUIRE(handle != nullptr);

        const int fd = descriptor_of(fname);
        REQUIRE(fd != -1);
        REQUIRE(::close(fd) == 0);

        const core::error_t refused = handle->close();
        REQUIRE(refused.contains_error());
        REQUIRE(refused.type == core::error_code_t::io_error);

        // fd is still cleared on refusal (::close(2) consumes it regardless), so this proves
        // no live fd is left for the destructor to double-close.
        const core::error_t after = handle->close();
        REQUIRE_FALSE(after.contains_error());
        handle.reset();
    }

    INFO("and it travels out through a delegating wrapper");
    {
        auto inner = open_file(fs, fname, file_flags::READ, file_lock_type::NO_LOCK);
        REQUIRE(inner != nullptr);
        std::unique_ptr<file_handle_t> wrapped = std::make_unique<forwarding_handle_t>(std::move(inner));

        const int fd = descriptor_of(fname);
        REQUIRE(fd != -1);
        REQUIRE(::close(fd) == 0);

        const core::error_t refused = wrapped->close();
        REQUIRE(refused.contains_error());
        REQUIRE(refused.type == core::error_code_t::io_error);
        wrapped.reset();
    }

    remove_file(fs, fname);
}
