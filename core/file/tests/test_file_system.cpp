#include <catch2/catch.hpp>

#include "file_system.hpp"
#include <components/log/log.hpp>
#include <fstream>
#include <string>

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