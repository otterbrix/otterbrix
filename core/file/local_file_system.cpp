#include "local_file_system.hpp"

// NO WINDOWS IMPLEMENTATION LIVES IN THIS FILE, AND SAYING SO OUT LOUD IS THE POINT.
//
// What stood under `#ifdef PLATFORM_WINDOWS` here was not platform support. No configuration
// this tree builds ever compiled it -- CI is macOS and Linux -- so it rotted freely for as long
// as it existed, and every change to the core/file interface was checked by the POSIX half
// alone. Measured before removal, with the Win32 SDK types supplied so that every diagnostic
// came from the arm itself and not from a missing header: 24 hard errors and one silently
// value-less `bool`.
//   - the handle/IO arm, 16 errors: a parameter redefined as a different type (was line 717),
//     `&` parsed after `!=` (724), calls to file_size / set_file_pointer / directory_exists /
//     file_exists with the wrong arity (762 twice, 807, 900, 918, 922, 944, 947), a lambda
//     using `fs` without capturing it (931, 933), `this` inside a free function (950);
//   - the path/environment arm, 8 errors: path_t IS std::filesystem::path, which has no
//     size(), no operator[], no substr() and no make_prefered() (110, 113, 116, 125, 126, 128);
//   - a `bool` function whose body simply ends (767-773).
// Repairing all of that would still not have linked: `trim` and `last_modified_time` had no
// Windows definition at all -- the second was spelled `llast_modified_time` -- and
// set_file_pointer / file_pointer were written as free functions, leaving the members of
// local_file_system_t undefined.
//
// So the honest answer is a refusal at the earliest point, not an implementation that only
// pretends to exist (rule 6: loud, and never silent). A refusal cannot rot; that code could,
// and did.
#ifdef PLATFORM_WINDOWS
#error "core/file/local_file_system.cpp has no Windows implementation. The POSIX arm is the only one this tree builds or tests; a Windows port has to be written and given a CI job, not resurrected from the dead arm that used to sit here."
#endif

#include "path_utils.hpp"
#include <algorithm>
#include <cassert>
#include <limits>

#include <cstdint>
#include <cstdio>
#include <sys/stat.h>

#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

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
#endif

namespace core::filesystem {

    static constexpr uint64_t INVALID_INDEX = uint64_t(-1);

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

    path_t local_file_system_t::home_directory() {
        if (!home_directory_.empty()) {
            return home_directory_;
        }

        return local_file_system_t::enviroment_variable("HOME");
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

    struct unix_file_handle_t : public file_handle_t {
    public:
        unix_file_handle_t(local_file_system_t& file_system, path_t path, int fd)
            : file_handle_t(file_system, std::move(path))
            , fd(fd) {}
        // THE ONE CALLER THAT HAS NOWHERE TO SEND A REFUSAL. close() below returns
        // core::error_t now, and this call site cannot hand it upward: a destructor's only
        // channel above itself is a throw, and a throw crossing a destructor is
        // std::terminate -- trading a report about ONE lost write for the loss of the whole
        // process, including every other handle that was still going to be flushed. Rule 6
        // asks refusals to be LOUD, not FATAL, so this prints and drops.
        //
        // The value is bound to a NAMED local and then read, so that the drop is a decision
        // and not an oversight -- error_t is [[nodiscard]] and rule 14 forbids the (void)
        // cast that would otherwise silence it. close() has already put the path and errno on
        // stderr by the time control gets here; the line below adds the one fact close()
        // cannot know, which is that nobody is going to act on it.
        ~unix_file_handle_t() override {
            const core::error_t closed = unix_file_handle_t::close();
            if (closed.contains_error()) {
                std::fprintf(stderr,
                             "core::filesystem::unix_file_handle_t::~unix_file_handle_t: the refused close of "
                             "'%s' above is dropped here -- a destructor has no caller to report it to\n",
                             path_.c_str());
            }
        }

        int fd;

    public:
        core::error_t close() override {
            if (fd == -1) {
                // Already closed. Idempotent AND honest: there is no refusal to re-report, and
                // answering io_error here would make every second close a lie.
                return core::error_t::no_error();
            }
            // ::close CAN FAIL, and its failure is not cosmetic: on a write-back filesystem
            // this is where a deferred write error (EIO) is finally reported, so a discarded
            // return is a lost write reported as a clean close. That is why this returns
            // core::error_t and no longer void -- see the note at the declaration in
            // core/file/file_handle.hpp.
            const int rc = ::close(fd);
            const int err = errno; // read BEFORE anything else can overwrite it

            // THE DESCRIPTOR IS RELEASED EITHER WAY, including on EINTR, and that is the one
            // place this file deliberately clears state before knowing the call succeeded: on
            // this platform (and on Linux) close() consumes the descriptor before it can
            // report, so keeping fd set would invite a second close of a number the kernel may
            // have already handed to another opener.
            fd = -1;

            if (rc == 0) {
                return core::error_t::no_error();
            }

            // BOTH HALVES OF THE REFUSAL, split by what can be carried where. The variable half
            // -- which file, which errno -- goes to stderr, because the value cannot hold it:
            // core::error_t's message is a std::pmr::string and this layer owns no arena to
            // build one on (rule 14 leaves no process-global to borrow, and an arena owned by
            // the handle would die before a caller could read the string). The value therefore
            // carries the CODE with an empty message anchored on null_memory_resource, which
            // allocates nothing by construction -- exactly what error_t::no_error() already
            // does -- and the caller learns THAT the close was refused.
            std::fprintf(stderr,
                         "core::filesystem::unix_file_handle_t::close: closing '%s' failed (errno %d: %s); "
                         "data written to it may not have reached the device\n",
                         path_.c_str(),
                         err,
                         std::strerror(err));
            return core::error_t{core::error_code_t::io_error, std::pmr::string{std::pmr::null_memory_resource()}};
        }
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

    write_result_t write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        int fd = reinterpret_cast<unix_file_handle_t&>(handle).fd;
        uint64_t bytes_written = 0;
        while (nr_bytes > 0) {
            auto bytes_to_write = std::min<uint64_t>(uint64_t(std::numeric_limits<int32_t>::max()), uint64_t(nr_bytes));
            int64_t current_bytes_written = ::write(fd, buffer, bytes_to_write);
            if (current_bytes_written <= 0) {
                // THE COUNT SURVIVES THE REFUSAL. write(2) short-counts before it refuses --
                // a file-size rlimit, a filling volume and a signal all produce that shape --
                // so answering with the refusing call's -1 would drop every byte the earlier
                // iterations already put on the device and moved the descriptor past. The
                // caller would be told "nothing", could not tell a stump from an untouched
                // file, and so could neither truncate it nor rewind to it.
                return write_result_t::refused(bytes_written);
            }
            bytes_written += static_cast<uint64_t>(current_bytes_written);
            buffer = static_cast<void*>(static_cast<uint8_t*>(buffer) + current_bytes_written);
            nr_bytes -= current_bytes_written;
        }
        return write_result_t::done(bytes_written);
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

    bool trim(local_file_system_t&,
              [[maybe_unused]] file_handle_t& handle,
              [[maybe_unused]] uint64_t offset_bytes,
              [[maybe_unused]] uint64_t length_bytes) {
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
    list_files(local_file_system_t& lfs, path_t directory, const list_files_callback_t& callback) {
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
#if defined(__APPLE__)
        // On Darwin fsync(2) only pushes dirty pages to the DRIVE, not through
        // the drive's own write cache — after a crash the data is there, after a
        // POWER LOSS it may not be. F_FULLFSYNC is the barrier that reaches the
        // platter; every sync in this engine is a durability barrier (WAL,
        // checkpoint root, bitcask segment), so anything less is a silent lie.
        // No fsync fallback when it fails (rule 6): a filesystem that cannot
        // give the barrier reports false and the caller decides, loudly.
        if (::fcntl(fd, F_FULLFSYNC) == -1) {
            return false;
        }
        return true;
#else
        if (fsync(fd) != 0) {
            return false;
        }
        return true;
#endif
    }

    bool move_files(local_file_system_t&, const path_t& source, const path_t& target) {
        if (rename(source.c_str(), target.c_str()) != 0) {
            return false;
        }
        return true;
    }

    bool seek(local_file_system_t& lfs, file_handle_t& handle, uint64_t location) {
        lfs.set_file_pointer(handle, location);
        return true;
    }

    uint64_t seek_position(local_file_system_t& lfs, file_handle_t& handle) { return lfs.file_pointer(handle); }

    static bool is_crawl(const path_t& glob) { return glob == "**"; }
    static bool is_symbolic_link(const path_t& path) {
        struct stat status;
        return (lstat(path.c_str(), &status) != -1 && S_ISLNK(status.st_mode));
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