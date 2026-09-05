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

// Rooted under the system temp directory, not the process CWD: a bare relative name drops
// `filesystem_test/` into whatever directory ctest launched the binary from. The pid keeps
// two concurrent runs apart. create_directory() below is a bare mkdir(2), so the base has to
// exist before the test asks for the leaf.
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
// A PARTIAL WRITE IS NOT A FAILED WRITE, and the difference is the only thing that lets a caller
// repair itself. The sequential write loop below it (local_file_system.cpp) issues ::write
// repeatedly; when one of those calls short-counts and the next one refuses, the bytes of the
// short count ARE on the device and the descriptor HAS moved over them.
//
// RLIMIT_FSIZE stages exactly that, with no mock anywhere in the path: the kernel writes up to
// the limit, returns the short count, and refuses everything after with EFBIG. That is the same
// shape a full volume produces, and it is reproducible, which ENOSPC is not. SIGXFSZ has to be
// ignored for the duration or the refusal kills the test binary instead of being reported.
//
// THE CAVEAT IS THAT BOTH ARE PROCESS-WIDE: while a guard is armed, EVERY file this binary
// writes is held to the same ceiling, so the limit and the disposition are restored before the
// case returns. That is safe here only because this binary is single-threaded and writes nothing
// but the file under test — adding a file-backed log sink, or a thread that writes while a case
// runs, would make unrelated code fail inside somebody else's assertion. So the armed window is
// kept to the single write it is staging, no case arms a ceiling it does not need, and there is
// no ceiling of ZERO anywhere below: an outright refusal is staged with a read-only descriptor
// instead (see the second case), which reaches only that descriptor.
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

// THE WRAPPER'S FORWARDERS ACTUALLY FORWARD, AND SOMETHING HAS TO INSTANTIATE THEM.
//
// file_system<FSC> holds a backend by PRIVATE inheritance and offers the same free-function
// surface over it. A forwarder written as `return read(fs, ...)` inside
// `read(file_system<FSC>&, ...)` calls ITSELF -- the first parameter is the wrapper, so the
// wrapper is what overload resolution picks -- and a template is only diagnosed when it is
// instantiated, so such a header compiles clean and blows the stack on its first real use.
// The private base is why forwarding needs an accessor rather than a cast.
//
// It goes end to end through the wrapper alone: open, sequential write, size, seek, read
// back, unlink. A regression does not fail an assertion here -- it exhausts the stack -- and
// that is the honest signature of the defect.
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

    // THE INVARIANT, SAID DIRECTLY: what the caller is told is what is on the device. It is
    // spelled against file_size() rather than against `limit` because `limit` is a property of
    // the STAGING, not of the contract -- a kernel that refuses an over-limit write whole
    // instead of short-counting it to the ceiling keeps 0 bytes and satisfies this line
    // exactly as one that keeps 12 does. A bare int64_t return cannot carry this: it answers
    // the refusing ::write's -1 and discards the accumulated count, so "nothing landed" and
    // "12 of 25 landed" become the same answer and neither can be repaired.
    REQUIRE_FALSE(written.complete);
    REQUIRE(written.bytes_written == handle->file_size());
    REQUIRE(written.bytes_written < requested);
    // partial() is that same fact under its own name, and it is derived rather than asserted
    // flat for the same reason: a stump exists only if something landed.
    REQUIRE(written.partial() == (handle->file_size() != 0));

    // The descriptor moved by exactly what landed, which is the other half of the contract:
    // a caller that rewinds has to rewind to the same place the count names.
    REQUIRE(handle->seek_position() == written.bytes_written);

    // SENSITIVITY OF THE INJECTION, beside the invariant and not in place of it: on every
    // kernel this suite runs on, RLIMIT_FSIZE truncates the write to the ceiling rather than
    // refusing it whole, so this is the partial case and not the empty one. A CHECK, because
    // it pins the staging: were it to stop holding, the case above would still be testing the
    // contract, just no longer this half of it.
    CHECK(written.bytes_written == limit);

    handle.reset();
    remove_file(fs, fname);
}

// THE OTHER TWO POINTS OF THE SAME CONTRACT, which an int64_t could not separate at all: a
// request of zero bytes SUCCEEDS having written nothing, and a refusal that got nowhere FAILS
// having written nothing. Both are `0` as a count, so the count alone cannot answer -- which
// is why the answer carries `complete` beside it rather than leaving every call site to
// re-derive it from a length it may not have kept.
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

    // A DESCRIPTOR THAT WILL NOT TAKE A WRITE AT ALL: the very first ::write returns -1 with
    // nothing short-counted, which is the refusal that got nowhere. Staged on a READ-ONLY
    // handle rather than under an RLIMIT_FSIZE of zero, and the difference is not stylistic --
    // a ceiling of zero is a PROCESS-WIDE ban on writing to any file, armed inside a suite
    // whose other cases and whose logging are one edit away from writing one. A read-only fd
    // reaches this descriptor and nothing else.
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

// THE CLOSE THAT CAN BE REPORTED.
//
// file_handle_t::close() used to be `virtual void`, so a refused ::close(2) -- which on a
// write-back filesystem is exactly where a deferred write error (EIO) surfaces -- could not be
// handed to anyone: a lost write arrived as a clean close. The signature now carries a
// core::error_t, and these cases pin the three facts that makes true.
//
// Staging the refusal: the descriptor is pulled out from under the handle and closed, so the
// handle's own ::close(2) answers EBADF. The descriptor is found by IDENTITY rather than by
// number -- fstat every open fd and keep the one whose (st_dev, st_ino) is this file's -- and
// the match is unique because nothing else in this process has this path open. There is no
// window for the number to be recycled: the ::close and the handle's close() are adjacent
// statements in a single-threaded test with nothing between them that can open a file.
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
    // THE SHAPE OF THE FIVE DELEGATING WRAPPERS IN THE TREE (test_b_plus_tree.cpp,
    // fault_injection_file.hpp, test_wal_truncate_header_race.cpp,
    // test_udf_refusal_registry_state.cpp, test_catalog_read_refusal.cpp), reproduced here so
    // that the forwarding itself is under test and not merely assumed. A wrapper that swallowed
    // the refusal would be the "new liar" the declaration warns about: its own slot answering
    // "no error" while the wrapped handle held a lost write.
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

        // Idempotence is part of the contract and not an accident: the destructor calls close()
        // too, so a second call has to be a no-op rather than a second ::close(2) of a number
        // the kernel may already have handed to another opener.
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

        // THE STATE IS STILL CLEARED, and deliberately so: ::close(2) consumes the descriptor
        // before it can report, so the handle must not keep it. This line is the proof that the
        // refusal above did not leave a live fd behind for the destructor to close twice.
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
