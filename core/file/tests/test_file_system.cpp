#include <catch2/catch.hpp>

#include "file_system.hpp"
#include <algorithm>
#include <components/log/log.hpp>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#if defined(__linux__)
#include <unistd.h>
#endif

using namespace std;
using namespace core::filesystem;

path_t testing_directory = "filesystem_test";

static void create_dummy_file(string fname1) {
    ofstream outfile(fname1);
    outfile << "test_string" << endl;
    outfile.close();
}

TEST_CASE("core::file::filesystem") {
    INFO("initialization") {
        local_file_system_t fs = local_file_system_t();
        if (!directory_exists(fs, testing_directory)) {
            create_directory(fs, testing_directory);
        }
    }

    INFO("operators") {
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

    INFO("write_close_read") {
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
    INFO("write_read without closing") {
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

    INFO("absolute_paths") {
        local_file_system_t fs;

#ifdef PLATFORM_WINDOWS
        const path_t long_path = "\\\\?\\D:\\very long network\\";
        REQUIRE(fs.is_path_absolute(network));
        REQUIRE(fs.normalize_path_absolute("C:/folder\\filename.csv") == "c:\\folder\\filename.csv");
        REQUIRE(fs.normalize_path_absolute(network) == network);
        REQUIRE(fs.normalize_path_absolute(long_path) == "\\\\?\\d:\\very long network\\");
#endif
    }

    INFO("deinitialization") {
        local_file_system_t fs = local_file_system_t();
        if (directory_exists(fs, testing_directory)) {
            remove_directory(fs, testing_directory);
        }
    }
}

#if defined(DEV_MODE) && defined(PLATFORM_POSIX)
namespace {
    struct posix_io_hook_guard_t {
        ~posix_io_hook_guard_t() { testing::reset_posix_positioned_io_hooks(); }
    };

    std::string g_test_pread_source;
    std::vector<uint64_t> g_test_pread_offsets;
    std::string g_test_pwrite_sink;
    std::vector<uint64_t> g_test_pwrite_offsets;

    int64_t test_partial_pread(int, void* buffer, size_t nr_bytes, uint64_t location) {
        g_test_pread_offsets.push_back(location);
        if (location >= g_test_pread_source.size()) {
            return 0;
        }
        const auto available = g_test_pread_source.size() - static_cast<size_t>(location);
        const auto chunk = std::min<size_t>({size_t{3}, nr_bytes, available});
        std::memcpy(buffer, g_test_pread_source.data() + static_cast<size_t>(location), chunk);
        return static_cast<int64_t>(chunk);
    }

    int64_t test_partial_pwrite(int, const void* buffer, size_t nr_bytes, uint64_t location) {
        g_test_pwrite_offsets.push_back(location);
        const auto chunk = std::min<size_t>(size_t{2}, nr_bytes);
        const auto required = static_cast<size_t>(location) + chunk;
        if (g_test_pwrite_sink.size() < required) {
            g_test_pwrite_sink.resize(required, '\0');
        }
        std::memcpy(g_test_pwrite_sink.data() + static_cast<size_t>(location), buffer, chunk);
        return static_cast<int64_t>(chunk);
    }
} // namespace

TEST_CASE("core::file::filesystem_posix_positioned_io_advances_offset_on_partial_io") {
    local_file_system_t fs;
    if (!directory_exists(fs, testing_directory)) {
        create_directory(fs, testing_directory);
    }

    auto fname = testing_directory;
    fname /= "positioned_partial_io";
    posix_io_hook_guard_t hook_guard;

    auto handle =
        open_file(fs, fname, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE, file_lock_type::NO_LOCK);
    REQUIRE(handle);

    SECTION("pread retries from the advanced location") {
        g_test_pread_source = "abcdefgh";
        g_test_pread_offsets.clear();
        std::vector<char> buffer(g_test_pread_source.size(), '\0');

        testing::set_posix_pread_hook(&test_partial_pread);
        REQUIRE(handle->read(buffer.data(), buffer.size(), 0));

        REQUIRE(std::string(buffer.data(), buffer.size()) == g_test_pread_source);
        REQUIRE(g_test_pread_offsets == std::vector<uint64_t>{0, 3, 6});
        testing::set_posix_pread_hook(nullptr);
    }

    SECTION("pwrite retries from the advanced location") {
        const std::string payload = "uvwxyz";
        g_test_pwrite_sink.clear();
        g_test_pwrite_offsets.clear();

        testing::set_posix_pwrite_hook(&test_partial_pwrite);
        REQUIRE(handle->write(const_cast<char*>(payload.data()), payload.size(), 0));

        REQUIRE(g_test_pwrite_sink == payload);
        REQUIRE(g_test_pwrite_offsets == std::vector<uint64_t>{0, 2, 4});
        testing::set_posix_pwrite_hook(nullptr);
    }

    handle.reset();
    remove_file(fs, fname);
}
#endif
