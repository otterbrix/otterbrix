#include "local_file_system.hpp"

#include "path_utils.hpp"
#include <algorithm>
#include <cassert>
#include <limits>

#include <cstdint>
#include <cstdio>
#include <sys/stat.h>

#ifndef PLATFORM_WINDOWS
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#else

#include <io.h>
#include <string>

#ifdef __MINGW32__
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

#if defined(__linux__) || defined(__APPLE__)
#include <pwd.h>
#endif

#if defined(__linux__)
#include <libgen.h>
#elif defined(__APPLE__)
#include <TargetConditionals.h>
#if not(defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE == 1)
#include <libproc.h>
#endif
#elif defined(PLATFORM_WINDOWS)
#include <restartmanager.h>
#endif

namespace core::filesystem {

    static constexpr uint64_t INVALID_INDEX = uint64_t(-1);

#ifndef PLATFORM_WINDOWS

    std::string local_file_system_t::enviroment_variable(const std::string& name) {
        const char* env = getenv(name.c_str());
        if (!env) {
            return {};
        }
        return env;
    }

    bool local_file_system_t::set_working_directory(const path_t& path) {
        if (chdir(path.c_str()) != 0) {
            return false;
        }
        return true;
    }

    uint64_t local_file_system_t::available_memory() {
        errno = 0;

#ifdef __MVS__
        struct rlimit limit;
        int rlim_rc = getrlimit(RLIMIT_AS, &limit);
        uint64_t max_memory = std::min<uint64_t>(limit.rlim_max, UINTPTR_MAX);
#else
        uint64_t max_memory = std::min<uint64_t>(static_cast<uint64_t>(sysconf(_SC_PHYS_PAGES)) *
                                                     static_cast<uint64_t>(sysconf(_SC_PAGESIZE)),
                                                 UINTPTR_MAX);
#endif
        if (errno != 0) {
            return INVALID_INDEX;
        }
        return max_memory;
    }

    path_t local_file_system_t::working_directory() {
        auto buffer = std::make_unique<char[]>(PATH_MAX);
        char* ret = getcwd(buffer.get(), PATH_MAX);
        if (!ret) {
            return path_t();
        }
        return path_t(buffer.get());
    }

    path_t local_file_system_t::normalize_path_absolute(const path_t& path) {
        assert(path.is_absolute());
        return path;
    }

#else

    std::string local_file_system_t::enviroment_variable(const std::string& env) {
        auto env_w = path_utils::UTF8_to_Unicode(env.c_str());
        auto res_w = _wgetenv(env_w.c_str());
        if (!res_w) {
            return std::string();
        }
        return path_utils::Unicode_to_UTF8(res_w);
    }

    static bool starts_with_single_backslash(const path_t& path) {
        if (path.size() < 2) {
            return false;
        }
        if (path[0] != '/' && path[0] != '\\') {
            return false;
        }
        if (path[1] == '/' || path[1] == '\\') {
            return false;
        }
        return true;
    }

    path_t local_file_system_t::normalize_path_absolute(const path_t& path) {
        assert(path.is_absolute());
        auto result = path;
        result.make_prefered();
        std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
        if (starts_with_single_backslash(result)) {
            return working_directory().substr(0, 2) + result;
        }
        return result;
    }

    bool local_file_system_t::set_working_directory(const path_t& path) {
        auto unicode_path = path_utils::UTF8_to_Unicode(path.c_str());
        if (!SetCurrentDirectoryW(unicode_path.c_str())) {
            return false;
            ;
        }
        return true;
    }

    uint64_t local_file_system_t::available_memory() {
        ULONGLONG available_memory_kb;
        if (GetPhysicallyInstalledSystemMemory(&available_memory_kb)) {
            return std::min<uint64_t>(available_memory_kb * 1000, UINTPTR_MAX);
        }
        MEMORYSTATUSEX mem_state;
        mem_state.dwLength = sizeof(MEMORYSTATUSEX);

        if (GlobalMemoryStatusEx(&mem_state)) {
            return std::min<uint64_t>(mem_state.ullTotalPhys, UINTPTR_MAX);
        }
        return INVALID_INDEX;
    }

    path_t local_file_system_t::working_directory() {
        uint64_t count = GetCurrentDirectoryW(0, nullptr);
        if (count == 0) {
            return path_t();
        }
        auto buffer = std::make_unique<wchar_t[]>(count);
        uint64_t ret = GetCurrentDirectoryW(count, buffer.get());
        if (count != ret + 1) {
            return path_t();
        }
        return path_utils::Unicode_to_UTF8(buffer.get());
    }

#endif

    path_t local_file_system_t::home_directory() {
        if (!home_directory_.empty()) {
            return home_directory_;
        }

#ifdef PLATFORM_WINDOWS
        return local_file_system_t::enviroment_variable("USERPROFILE");
#else
        return local_file_system_t::enviroment_variable("HOME");
#endif
    }

    bool local_file_system_t::set_home_directory(path_t path) {
        if (path.is_absolute()) {
            home_directory_ = path;
            return true;
        }
        return false;
    }

    path_t local_file_system_t::expand_path(const path_t& path) {
        if (path.empty()) {
            return path;
        }
        path_t result = home_directory_;
        return result /= path;
    }

    bool local_file_system_t::has_glob(const std::string& str) {
        for (uint64_t i = 0; i < str.size(); i++) {
            switch (str[i]) {
                case '*':
                case '?':
                case '[':
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    void local_file_system_t::reset(file_handle_t& handle) { handle.seek(0); }

#ifndef PLATFORM_WINDOWS
    bool file_exists(local_file_system_t&, const path_t& filename) {
        if (!filename.empty()) {
            if (access(filename.c_str(), 0) == 0) {
                struct stat status;
                stat(filename.c_str(), &status);
                if (S_ISREG(status.st_mode)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool is_pipe(local_file_system_t&, const path_t& filename) {
        if (!filename.empty()) {
            if (access(filename.c_str(), 0) == 0) {
                struct stat status;
                stat(filename.c_str(), &status);
                if (S_ISFIFO(status.st_mode)) {
                    return true;
                }
            }
        }
        return false;
    }

#else
    bool file_exists(local_file_system_t&, const path_t& filename) {
        auto unicode_path = path_utils::UTF8_to_Unicode(filename.c_str());
        const wchar_t* wpath = unicode_path.c_str();
        if (_waccess(wpath, 0) == 0) {
            struct _stati64 status;
            _wstati64(wpath, &status);
            if (status.st_mode & S_IFREG) {
                return true;
            }
        }
        return false;
    }
    bool is_pipe(local_file_system_t&, const path_t& filename) {
        auto unicode_path = path_utils::UTF8_to_Unicode(filename.c_str());
        const wchar_t* wpath = unicode_path.c_str();
        if (_waccess(wpath, 0) == 0) {
            struct _stati64 status;
            _wstati64(wpath, &status);
            if (status.st_mode & _S_IFCHR) {
                return true;
            }
        }
        return false;
    }
#endif

#ifndef _WIN32

    struct unix_file_handle_t : public file_handle_t {
    public:
        unix_file_handle_t(local_file_system_t& file_system, path_t path, int fd)
            : file_handle_t(file_system, std::move(path))
            , fd(fd) {}
        ~unix_file_handle_t() override { unix_file_handle_t::close(); }

        int fd;

    public:
        void close() override {
            if (fd != -1) {
                ::close(fd);
                fd = -1;
            }
        };
    };

    static file_type_t file_type_internal(int fd) {
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return file_type_t::INVALID;
        }
        switch (s.st_mode & S_IFMT) {
            case S_IFBLK:
                return file_type_t::BLOCKDEV;
            case S_IFCHR:
                return file_type_t::CHARDEV;
            case S_IFIFO:
                return file_type_t::FIFO;
            case S_IFDIR:
                return file_type_t::DIR;
            case S_IFLNK:
                return file_type_t::LINK;
            case S_IFREG:
                return file_type_t::REGULAR;
            case S_IFSOCK:
                return file_type_t::SOCKET;
            default:
                return file_type_t::INVALID;
        }
    }

    bool local_file_system_t::set_file_pointer(file_handle_t& handle, uint64_t location) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        off_t offset = lseek(fd, static_cast<off_t>(location), SEEK_SET);
        if (offset == static_cast<off_t>(-1)) {
            return false;
        }
        return true;
    }

    uint64_t local_file_system_t::file_pointer(file_handle_t& handle) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        off_t position = lseek(fd, 0, SEEK_CUR);
        if (position == static_cast<off_t>(-1)) {
            return INVALID_INDEX;
        }
        return static_cast<uint64_t>(position);
    }

    std::unique_ptr<file_handle_t>
    open_file(local_file_system_t& lfs, const path_t& path_p, file_flags flags, file_lock_type lock_type) {
        auto path = lfs.expand_path(path_p);

        int open_flags = 0;
        int rc;
        bool open_read = (flags & file_flags::READ) != file_flags::EMPTY;
        bool open_write = (flags & file_flags::WRITE) != file_flags::EMPTY;
        if (open_read && open_write) {
            open_flags = O_RDWR;
        } else if (open_read) {
            open_flags = O_RDONLY;
        } else if (open_write) {
            open_flags = O_WRONLY;
        } else {
            return nullptr;
        }
        if (open_write) {
            assert((flags & file_flags::WRITE) != file_flags::EMPTY);
            open_flags |= O_CLOEXEC;
            if ((flags & file_flags::FILE_CREATE) != file_flags::EMPTY) {
                open_flags |= O_CREAT;
            } else if ((flags & file_flags::FILE_CREATE_NEW) != file_flags::EMPTY) {
                open_flags |= O_CREAT | O_TRUNC;
            }
            if ((flags & file_flags::APPEND) != file_flags::EMPTY) {
                open_flags |= O_APPEND;
            }
        }
        if ((flags & file_flags::DIRECT_IO) != file_flags::EMPTY) {
#if defined(__sun) && defined(__SVR4)
            throw std::logic_error("DIRECT_IO not supported on Solaris");
#endif
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
            open_flags |= O_SYNC;
#else
            open_flags |= O_DIRECT | O_SYNC;
#endif
        }
        int fd = open(path.c_str(), open_flags, 0666);
        if (fd == -1) {
            return nullptr;
        }
        if (lock_type != file_lock_type::NO_LOCK) {
            auto file_type_t = file_type_internal(fd);
            if (file_type_t != file_type_t::FIFO && file_type_t != file_type_t::SOCKET) {
                struct flock fl;
                memset(&fl, 0, sizeof fl);
                fl.l_type = lock_type == file_lock_type::READ_LOCK ? F_RDLCK : F_WRLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0;
                rc = fcntl(fd, F_SETLK, &fl);
                if (rc == -1) {
                    return nullptr;
                }
            }
        }
        return std::make_unique<unix_file_handle_t>(lfs, path, fd);
    }

    bool read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        auto read_buffer = reinterpret_cast<char*>(buffer);
        while (nr_bytes > 0) {
            int64_t bytes_read = pread(fd, read_buffer, static_cast<size_t>(nr_bytes), static_cast<off_t>(location));
            if (bytes_read == -1 || bytes_read == 0) {
                return false;
            }
            read_buffer += bytes_read;
            nr_bytes -= bytes_read;
        }
        return true;
    }

    int64_t read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        int64_t bytes_read = ::read(fd, buffer, static_cast<size_t>(nr_bytes));
        return bytes_read;
    }

    bool write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        auto write_buffer = reinterpret_cast<char*>(buffer);
        while (nr_bytes > 0) {
            int64_t bytes_written =
                pwrite(fd, write_buffer, static_cast<size_t>(nr_bytes), static_cast<off_t>(location));
            if (bytes_written <= 0) {
                return false;
            }
            write_buffer += bytes_written;
            nr_bytes -= bytes_written;
        }
        return true;
    }

    int64_t write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        int64_t bytes_written = 0;
        while (nr_bytes > 0) {
            auto bytes_to_write = std::min<uint64_t>(uint64_t(std::numeric_limits<int32_t>::max()), uint64_t(nr_bytes));
            int64_t current_bytes_written = ::write(fd, buffer, bytes_to_write);
            if (current_bytes_written <= 0) {
                return current_bytes_written;
            }
            bytes_written += current_bytes_written;
            buffer = static_cast<void*>(static_cast<uint8_t*>(buffer) + current_bytes_written);
            nr_bytes -= current_bytes_written;
        }
        return bytes_written;
    }

    int64_t file_size(local_file_system_t&, file_handle_t& handle) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_size;
    }

    time_t last_modified_time(local_file_system_t&, file_handle_t& handle) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_mtime;
    }

    file_type_t file_type(local_file_system_t&, file_handle_t& handle) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        return file_type_internal(fd);
    }

    bool truncate(local_file_system_t&, file_handle_t& handle, int64_t new_size) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        if (ftruncate(fd, new_size) != 0) {
            return false;
        }
        return true;
    }

    bool trim(local_file_system_t&, [[maybe_unused]] file_handle_t& handle, [[maybe_unused]] uint64_t offset_bytes, [[maybe_unused]] uint64_t length_bytes) {
#if defined(__linux__)
        // FALLOC_FL_PUNCH_HOLE requires glibc 2.18 or up
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 18)
        return false;
#else
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        int res = fallocate(fd,
                            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                            static_cast<int64_t>(offset_bytes),
                            static_cast<int64_t>(length_bytes));
        return res == 0;
#endif
#else
        return false;
#endif
    }

    bool directory_exists(local_file_system_t&, const path_t& directory) {
        if (!directory.empty()) {
            if (access(directory.c_str(), 0) == 0) {
                struct stat status;
                stat(directory.c_str(), &status);
                if (status.st_mode & S_IFDIR) {
                    return true;
                }
            }
        }
        return false;
    }

    bool create_directory(local_file_system_t&, const path_t& directory) {
        struct stat st;

        if (stat(directory.c_str(), &st) != 0) {
            if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
        } else if (!S_ISDIR(st.st_mode)) {
            return false;
        }
        return true;
    }

    int remove_directory_recursive(const char* path) {
        DIR* d = opendir(path);
        uint64_t path_len = static_cast<uint64_t>(strlen(path));
        int r = -1;

        if (d) {
            struct dirent* p;
            r = 0;
            while (!r && (p = readdir(d))) {
                int r2 = -1;
                char* buf;
                uint64_t len;
                if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
                    continue;
                }
                len = path_len + static_cast<uint64_t>(strlen(p->d_name) + 2);
                buf = new (std::nothrow) char[len];
                if (buf) {
                    struct stat statbuf;
                    snprintf(buf, len, "%s/%s", path, p->d_name);
                    if (!stat(buf, &statbuf)) {
                        if (S_ISDIR(statbuf.st_mode)) {
                            r2 = remove_directory_recursive(buf);
                        } else {
                            r2 = unlink(buf);
                        }
                    }
                    delete[] buf;
                }
                r = r2;
            }
            ::closedir(d);
        }
        if (!r) {
            r = rmdir(path);
        }
        return r;
    }

    bool remove_directory(local_file_system_t&, const path_t& directory) {
        return remove_directory_recursive(directory.c_str()) != -1;
    }

    bool remove_file(local_file_system_t&, const path_t& filename) {
        if (std::remove(filename.c_str()) != 0) {
            return false;
        }
        return true;
    }

    bool
    list_files(local_file_system_t& lfs, path_t directory, const std::function<void(const path_t&, bool)>& callback) {
        if (!directory_exists(lfs, directory)) {
            return false;
        }
        DIR* dir = opendir(directory.c_str());
        if (!dir) {
            return false;
        }
        struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            path_t name = path_t(ent->d_name);
            if (name.empty() || name == "." || name == "..") {
                continue;
            }
            path_t full_path = directory;
            full_path /= name;
            if (access(full_path.c_str(), 0) != 0) {
                continue;
            }
            struct stat status;
            stat(full_path.c_str(), &status);
            if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
                continue;
            }
            callback(name, status.st_mode & S_IFDIR);
        }
        ::closedir(dir);
        return true;
    }

    bool file_sync(local_file_system_t&, file_handle_t& handle) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        if (fsync(fd) != 0) {
            return false;
        }
        return true;
    }

    bool move_files(local_file_system_t&, const path_t& source, const path_t& target) {
        if (rename(source.c_str(), target.c_str()) != 0) {
            return false;
        }
        return true;
    }

#else

    constexpr char PIPE_PREFIX[] = "\\\\.\\pipe\\";

    std::string last_error_as_string(local_file_system_t&) {
        DWORD errorMessageID = GetLastError();
        if (errorMessageID == 0)
            return std::string();

        LPSTR messageBuffer = nullptr;
        uint64_t size =
            FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                           NULL,
                           errorMessageID,
                           MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                           (LPSTR) &messageBuffer,
                           0,
                           NULL);

        std::string message(messageBuffer, size);

        LocalFree(messageBuffer);

        return message;
    }

    struct windows_file_handle_t : public file_handle_t {
    public:
        windows_file_handle_t(local_file_system_t& file_system, path_t path, HANDLE fd)
            : file_handle_t(file_system, path)
            , position_(0)
            , fd_(fd) {}
        ~windows_file_handle_t() override { close(); }

        uint64_t position_;
        HANDLE fd_;

    public:
        void close() override {
            if (!fd_) {
                return;
            }
            CloseHandle(fd_);
            fd_ = nullptr;
        };
    };

    std::unique_ptr<file_handle_t>
    open_file(local_file_system_t& lfs, const path_t& path_p, file_flags flags, file_lock_type) {
        auto path = lfs.expand_path(path_p);
        uint16_t flags = static_cast<uint16_t>(flags);

        DWORD desired_access;
        DWORD share_mode;
        DWORD creation_disposition = OPEN_EXISTING;
        DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
        bool open_read = (flags & file_flags::READ) != file_flags::EMPTY;
        bool open_write = (flags & file_flags::WRITE != file_flags::EMPTY);
        if (open_read && open_write) {
            desired_access = GENERIC_READ | GENERIC_WRITE;
            share_mode = 0;
        } else if (open_read) {
            desired_access = GENERIC_READ;
            share_mode = FILE_SHARE_READ;
        } else if (open_write) {
            desired_access = GENERIC_WRITE;
            share_mode = 0;
        } else {
            return nullptr;
        }
        if (open_write) {
            if ((flags & file_flags::FILE_CREATE) != file_flags::EMPTY) {
                creation_disposition = OPEN_ALWAYS;
            } else if ((flags & file_flags::FILE_CREATE_NEW) != file_flags::EMPTY) {
                creation_disposition = CREATE_ALWAYS;
            }
        }
        if ((flags & file_flags::DIRECT_IO) != file_flags::EMPTY) {
            flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
        }
        auto unicode_path = path_utils::UTF8_to_Unicode(path.c_str());
        HANDLE hFile = CreateFileW(unicode_path.c_str(),
                                   desired_access,
                                   share_mode,
                                   NULL,
                                   creation_disposition,
                                   flags_and_attributes,
                                   NULL);
        if (hFile == INVALID_HANDLE_VALUE) {
            auto error = last_error_as_string(lfs);

            return nullptr;
        }
        auto handle = std::make_unique<windows_file_handle_t>(lfs, path.c_str(), hFile);
        if ((flags & file_flags::APPEND) != file_flags::EMPTY) {
            set_file_pointer(*handle, file_size(*handle));
        }
        return std::move(handle);
    }

    bool set_file_pointer(local_file_system_t&, file_handle_t& handle, uint64_t location) {
        auto& whandle = reinterpret_cast<windows_file_handle_t&>(handle);
        whandle.position_ = location;
        LARGE_INTEGER wlocation;
        wlocation.QuadPart = location;
        SetFilePointerEx(whandle.fd_, wlocation, NULL, FILE_BEGIN);
    }

    uint64_t file_pointer(local_file_system_t&, file_handle_t& handle) {
        return reinterpret_cast<windows_file_handle_t&>(handle).position_;
    }

    static DWORD
    FSInternalRead(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        DWORD bytes_read = 0;
        OVERLAPPED ov = {};
        ov.Internal = 0;
        ov.InternalHigh = 0;
        ov.Offset = location & 0xFFFFFFFF;
        ov.OffsetHigh = location >> 32;
        ov.hEvent = 0;
        auto rc = ReadFile(hFile, buffer, (DWORD) nr_bytes, &bytes_read, &ov);
        if (!rc) {
            return DWORD();
        }
        return bytes_read;
    }

    bool read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        HANDLE hFile = ((windows_file_handle_t&) handle).fd_;
        auto bytes_read = FSInternalRead(handle, hFile, buffer, nr_bytes, location);
        if (bytes_read != nr_bytes) {
            return false;
        }
        return true;
    }

    int64_t read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        auto& pos = reinterpret_cast<windows_file_handle_t&>(handle).position_;
        auto n = std::min<uint64_t>(std::max<uint64_t>(file_size(handle), pos) - pos, nr_bytes);
        auto bytes_read = FSInternalRead(handle, hFile, buffer, n, pos);
        pos += bytes_read;
        return bytes_read;
    }

    static DWORD
    FSInternalWrite(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        DWORD bytes_written = 0;
        OVERLAPPED ov = {};
        ov.Internal = 0;
        ov.InternalHigh = 0;
        ov.Offset = location & 0xFFFFFFFF;
        ov.OffsetHigh = location >> 32;
        ov.hEvent = 0;
        auto rc = WriteFile(hFile, buffer, (DWORD) nr_bytes, &bytes_written, &ov);
        if (!rc) {
            return DWORD();
        }
        return bytes_written;
    }

    static int64_t FSWrite(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        int64_t bytes_written = 0;
        while (nr_bytes > 0) {
            auto bytes_to_write = std::min<uint64_t>(uint64_t(std::numeric_limits<int32_t>::max()), uint64_t(nr_bytes));
            DWORD current_bytes_written = FSInternalWrite(handle, hFile, buffer, bytes_to_write, location);
            if (current_bytes_written <= 0) {
                return current_bytes_written;
            }
            bytes_written += current_bytes_written;
            buffer = buffer + current_bytes_written;
            location += current_bytes_written;
            nr_bytes -= current_bytes_written;
        }
        return bytes_written;
    }

    bool write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, location);
        if (bytes_written != nr_bytes) {
            return false;
        }
        return true;
    }

    int64_t write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        auto& pos = reinterpret_cast<windows_file_handle_t&>(handle).position_;
        auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, pos);
        pos += bytes_written;
        return bytes_written;
    }

    int64_t file_size(local_file_system_t&, file_handle_t& handle) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        LARGE_INTEGER result;
        if (!GetFileSizeEx(hFile, &result)) {
            return -1;
        }
        return result.QuadPart;
    }

    time_t llast_modified_time(local_file_system_t&, file_handle_t& handle) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;

        FILETIME last_write;
        if (GetFileTime(hFile, nullptr, nullptr, &last_write) == 0) {
            return -1;
        }

        ULARGE_INTEGER ul;
        ul.LowPart = last_write.dwLowDateTime;
        ul.HighPart = last_write.dwHighDateTime;
        int64_t fileTime64 = ul.QuadPart;

        const auto WINDOWS_TICK = 10000000;
        const auto SEC_TO_UNIX_EPOCH = 11644473600LL;
        time_t result = (fileTime64 / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
        return result;
    }

    bool truncate(local_file_system_t&, file_handle_t& handle, int64_t new_size) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        set_file_pointer(handle, new_size);
        if (!SetEndOfFile(hFile)) {
            return false;
        }
        return true;
    }

    static DWORD WindowsGetFileAttributes(const path_t& filename) {
        auto unicode_path = path_utils::UTF8_to_Unicode(filename.c_str());
        return GetFileAttributesW(unicode_path.c_str());
    }

    bool directory_exists(local_file_system_t&, const path_t& directory) {
        DWORD attrs = WindowsGetFileAttributes(directory);
        return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
    }

    bool create_directory(local_file_system_t&, const path_t& directory) {
        if (directory_exists(directory)) {
            return true;
        }
        auto unicode_path = path_utils::UTF8_to_Unicode(directory.c_str());
        if (directory.empty() || !CreateDirectoryW(unicode_path.c_str(), NULL) || !directory_exists(directory)) {
            return false;
        }
        return true;
    }

    static bool delete_directory_recursive(local_file_system_t& fs, path_t directory) {
        list_files(fs, directory, [directory](const path_t& fname, bool is_directory) {
            if (is_directory) {
                delete_directory_recursive(fs, directory /= fname);
            } else {
                fs.remove_file(directory /= fname);
            }
        });
        auto unicode_path = path_utils::UTF8_to_Unicode(directory.c_str());
        if (!RemoveDirectoryW(unicode_path.c_str())) {
            return false;
        }
        return true;
    }

    bool remove_directory(local_file_system_t&, const path_t& directory) {
        if (file_exists(directory)) {
            return false;
        }
        if (!directory_exists(directory)) {
            return true;
        }
        return delete_directory_recursive(*this, directory.c_str());
    }

    bool remove_file(local_file_system_t& lfs, const path_t& filename) {
        auto unicode_path = path_utils::UTF8_to_Unicode(filename.c_str());
        if (!DeleteFileW(unicode_path.c_str())) {
            auto error = last_error_as_string(lfs);
            return false;
        }
        return true;
    }

    bool
    list_files(local_file_system_t& lfs, path_t directory, const std::function<void(const path_t&, bool)>& callback) {
        directory /= "*";

        auto unicode_path = path_utils::UTF8_to_Unicode(directory.c_str());

        WIN32_FIND_DATAW ffd;
        HANDLE hFind = FindFirstFileW(unicode_path.c_str(), &ffd);
        if (hFind == INVALID_HANDLE_VALUE) {
            return false;
        }
        do {
            path_t cFileName = path_utils::Unicode_to_UTF8(ffd.cFileName);
            if (cFileName == "." || cFileName == "..") {
                continue;
            }
            callback(cFileName, ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
        } while (FindNextFileW(hFind, &ffd) != 0);

        DWORD dwError = GetLastError();
        if (dwError != ERROR_NO_MORE_FILES) {
            FindClose(hFind);
            return false;
        }

        FindClose(hFind);
        return true;
    }

    bool file_sync(local_file_system_t&, file_handle_t& handle) {
        HANDLE hFile = reinterpret_cast<windows_file_handle_t&>(handle).fd_;
        if (FlushFileBuffers(hFile) == 0) {
            return false;
        }
        return true;
    }

    bool move_files(local_file_system_t&, const path_t& source, const path_t& target) {
        auto source_unicode = path_utils::UTF8_to_Unicode(source.c_str());
        auto target_unicode = path_utils::UTF8_to_Unicode(target.c_str());
        if (!MoveFileW(source_unicode.c_str(), target_unicode.c_str())) {
            return false;
        }
        return true;
    }

    file_type_t file_type(local_file_system_t&, file_handle_t& handle) {
        auto path = reinterpret_cast<windows_file_handle_t&>(handle).path_;
        if (strncmp(path.c_str(), PIPE_PREFIX, strlen(PIPE_PREFIX)) == 0) {
            return file_type_t::FIFO;
        }
        DWORD attrs = WindowsGetFileAttributes(path.c_str());
        if (attrs != INVALID_FILE_ATTRIBUTES) {
            if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
                return file_type_t::DIR;
            } else {
                return file_type_t::REGULAR;
            }
        }
        return file_type_t::INVALID;
    }
#endif

    bool seek(local_file_system_t& lfs, file_handle_t& handle, uint64_t location) {
        lfs.set_file_pointer(handle, location);
        return true;
    }

    uint64_t seek_position(local_file_system_t& lfs, file_handle_t& handle) { return lfs.file_pointer(handle); }

    static bool is_crawl(const path_t& glob) { return glob == "**"; }
    static bool is_symbolic_link(const path_t& path) {
#ifndef PLATFORM_WINDOWS
        struct stat status;
        return (lstat(path.c_str(), &status) != -1 && S_ISLNK(status.st_mode));
#else
        auto attributes = WindowsGetFileAttributes(path);
        if (attributes == INVALID_FILE_ATTRIBUTES)
            return false;
        return attributes & FILE_ATTRIBUTE_REPARSE_POINT;
#endif
    }

    static void recursive_glob_directories(local_file_system_t& lfs,
                                           const path_t& path,
                                           std::vector<path_t>& result,
                                           bool match_directory,
                                           bool join_path) {
        list_files(lfs, path, [&](const path_t& fname, bool is_directory) {
            path_t concat;
            if (join_path) {
                concat = path;
                concat /= fname;
            } else {
                concat = fname;
            }
            if (is_symbolic_link(concat)) {
                return;
            }
            if (is_directory == match_directory) {
                result.push_back(concat);
            }
            if (is_directory) {
                recursive_glob_directories(lfs, concat, result, match_directory, true);
            }
        });
    }

    static void glob_files_internal(local_file_system_t& lfs,
                                    const path_t& path,
                                    const path_t& glob,
                                    bool match_directory,
                                    std::vector<path_t>& result,
                                    bool join_path) {
        list_files(lfs, path, [&](const path_t& fname, bool is_directory) {
            if (is_directory != match_directory) {
                return;
            }
            if (path_utils::glob(fname.c_str(), fname.string().size(), glob.c_str(), glob.string().size(), true)) {
                if (join_path) {
                    path_t p = path;
                    result.push_back(p /= fname);
                } else {
                    result.push_back(fname);
                }
            }
        });
    }

    std::vector<path_t> fetch_file_without_glob(local_file_system_t& lfs, const path_t& path, bool absolute_path) {
        std::vector<path_t> result;
        if (file_exists(lfs, path) || is_pipe(lfs, path)) {
            result.push_back(path);
        } else if (!absolute_path) {
            std::vector<std::string> search_paths = path_utils::split(lfs.file_search_path(), ',');
            for (const auto& search_path : search_paths) {
                path_t joined_path(search_path);
                joined_path /= path;
                if (file_exists(lfs, joined_path) || is_pipe(lfs, joined_path)) {
                    result.push_back(joined_path);
                }
            }
        }
        return result;
    }

    std::vector<path_t> glob_files(local_file_system_t& lfs, const std::string& path) {
        if (path.empty()) {
            return std::vector<path_t>();
        }
        std::vector<std::string> splits;
        uint64_t last_pos = 0;
        for (uint64_t i = 0; i < path.size(); i++) {
            if (path[i] == '\\' || path[i] == '/') {
                if (i == last_pos) {
                    last_pos = i + 1;
                    continue;
                }
                if (splits.empty()) {
                    splits.push_back(path.substr(0, i));
                } else {
                    splits.push_back(path.substr(last_pos, i - last_pos));
                }
                last_pos = i + 1;
            }
        }
        splits.push_back(path.substr(last_pos, path.size() - last_pos));
        bool absolute_path = false;
        if (path[0] == '/') {
            absolute_path = true;
        } else if (splits[0].find(':') != splits[0].npos) {
            absolute_path = true;
        } else if (splits[0] == "~") {
            if (!lfs.home_directory().empty()) {
                absolute_path = true;
                splits[0] = lfs.home_directory().string();
                assert(path[0] == '~');
                if (!lfs.has_glob(path)) {
                    return glob_files(lfs, lfs.home_directory().string() + path.substr(1));
                }
            }
        }
        if (!lfs.has_glob(path)) {
            return fetch_file_without_glob(lfs, path, absolute_path);
        }
        std::vector<path_t> previous_directories;
        if (absolute_path) {
            previous_directories.push_back(splits[0]);
        } else {
            std::vector<std::string> search_paths = path_utils::split(lfs.file_search_path(), ',');
            for (const auto& search_path : search_paths) {
                previous_directories.push_back(path_t(search_path));
            }
        }

        if (std::count(splits.begin(), splits.end(), "**") > 1) {
            return {};
        }

        for (uint64_t i = absolute_path ? 1 : 0; i < splits.size(); i++) {
            bool is_last_chunk = i + 1 == splits.size();
            std::vector<path_t> result;
            if (!lfs.has_glob(splits[i])) {
                if (previous_directories.empty()) {
                    result.push_back(splits[i]);
                } else {
                    if (is_last_chunk) {
                        for (auto& prev_directory : previous_directories) {
                            path_t filename = prev_directory;
                            filename /= splits[i];
                            if (file_exists(lfs, filename) || directory_exists(lfs, filename)) {
                                result.push_back(filename);
                            }
                        }
                    } else {
                        for (auto& prev_directory : previous_directories) {
                            path_t filename = prev_directory;
                            filename /= splits[i];
                            result.push_back(filename);
                        }
                    }
                }
            } else {
                if (is_crawl(splits[i])) {
                    if (!is_last_chunk) {
                        result = previous_directories;
                    }
                    if (previous_directories.empty()) {
                        recursive_glob_directories(lfs, ".", result, !is_last_chunk, false);
                    } else {
                        for (auto& prev_dir : previous_directories) {
                            recursive_glob_directories(lfs, prev_dir, result, !is_last_chunk, true);
                        }
                    }
                } else {
                    if (previous_directories.empty()) {
                        glob_files_internal(lfs, ".", splits[i], !is_last_chunk, result, false);
                    } else {
                        for (auto& prev_directory : previous_directories) {
                            glob_files_internal(lfs, prev_directory, splits[i], !is_last_chunk, result, true);
                        }
                    }
                }
            }
            if (result.empty()) {
                return fetch_file_without_glob(lfs, path, absolute_path);
            }
            if (is_last_chunk) {
                return result;
            }
            previous_directories = std::move(result);
        }
        return std::vector<path_t>();
    }

} // namespace core::filesystem