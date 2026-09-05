#include "file_handle.hpp"
#include "local_file_system.hpp"

#include "path_utils.hpp"

#include <cstdint>
#include <cstdio>

#ifndef PLATFORM_WINDOWS
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __MVS__
#define _XOPEN_SOURCE_EXTENDED 1
#include <sys/resource.h>
#define PATH_MAX _XOPEN_PATH_MAX
#endif

#else
#include <string>
#include <sysinfoapi.h>

#ifdef __MINGW32__
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE
#endif

namespace core::filesystem {

    file_handle_t::file_handle_t(local_file_system_t& fs, path_t path_p)
        : fs_(fs)
        , path_(std::move(path_p)) {}

    file_handle_t::~file_handle_t() {}

    int64_t file_handle_t::read(void* buffer, uint64_t nr_bytes) {
        return ::core::filesystem::read(fs_, *this, buffer, static_cast<int64_t>(nr_bytes));
    }

    bool file_handle_t::read(void* buffer, uint64_t nr_bytes, uint64_t location) {
        return ::core::filesystem::read(fs_, *this, buffer, static_cast<int64_t>(nr_bytes), location);
    }

    write_result_t file_handle_t::write(void* buffer, uint64_t nr_bytes) {
        return ::core::filesystem::write(fs_, *this, buffer, static_cast<int64_t>(nr_bytes));
    }

    bool file_handle_t::write(void* buffer, uint64_t nr_bytes, uint64_t location) {
        return ::core::filesystem::write(fs_, *this, buffer, static_cast<int64_t>(nr_bytes), location);
    }

    bool file_handle_t::seek(uint64_t location) { return ::core::filesystem::seek(fs_, *this, location); }

    void file_handle_t::reset() { fs_.reset(*this); }

    uint64_t file_handle_t::seek_position() { return ::core::filesystem::seek_position(fs_, *this); }

    bool file_handle_t::can_seek() { return fs_.can_seek(); }

    bool file_handle_t::is_pipe() { return ::core::filesystem::is_pipe(fs_, path_); }

    std::string file_handle_t::read_line() {
        std::string result;
        char buffer[1];
        while (true) {
            uint64_t tuples_read = static_cast<uint64_t>(read(buffer, 1));
            if (tuples_read == 0 || buffer[0] == '\n') {
                return result;
            }
            if (buffer[0] != '\r') {
                result += buffer[0];
            }
        }
    }

    uint64_t file_handle_t::file_size() { return static_cast<uint64_t>(::core::filesystem::file_size(fs_, *this)); }

    bool file_handle_t::sync() { return ::core::filesystem::file_sync(fs_, *this); }

    bool file_handle_t::truncate(int64_t new_size) { return ::core::filesystem::truncate(fs_, *this, new_size); }

    bool file_handle_t::trim(uint64_t offset_bytes, uint64_t length_bytes) {
        return ::core::filesystem::trim(fs_, *this, offset_bytes, length_bytes);
    }

    file_type_t file_handle_t::type() { return ::core::filesystem::file_type(fs_, *this); }

} // namespace core::filesystem
