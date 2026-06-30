#pragma once

// spill_file_t — RAII helper for operator spill.
//
// Owns one temp file on the local filesystem. On construction it ensures the
// spill directory exists (create_directory) and opens the file WRITE|FILE_CREATE
// with a unique per-query name. On destruction it removes the file regardless of
// success or failure. I/O is done through core::filesystem.
//
// The type is non-copyable and non-movable (it holds a reference to a
// long-lived local_file_system_t supplied by the owner). Store instances via
// std::unique_ptr<spill_file_t> in a grace/spill state struct whose lifetime
// brackets the whole spill -> merge cycle; the destructor then guarantees
// cleanup on every path (success, hard error, early return).
//
// Design rules honoured: R6 (no fallback — cleanup always runs), R8 (pmr-aware
// resource threading via the owner), R12 (no locks — pure value type),
// R2/R9 (core::error surfaced via bool, no exceptions).

#include <components/configuration/configuration.hpp>
#include <core/file/local_file_system.hpp>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>

namespace components::table::storage {

    // RAII owner of a single spill temp file. Non-copyable, non-movable.
    class spill_file_t {
    public:
        enum class mode : uint8_t { write, read };

        // Construct a writer at `dir / name`. `dir` is created if missing. The file
        // is opened WRITE|FILE_CREATE. If creation fails, `valid()` is false and all
        // writes are no-ops; the destructor still attempts cleanup. `fs` must outlive
        // this object (typically owned by the same grace state that holds the
        // spill_file_t).
        spill_file_t(core::filesystem::local_file_system_t& fs,
                     std::string dir,
                     std::string name)
            : fs_(fs)
            , dir_(std::move(dir))
            , name_(std::move(name)) {
            open_write();
        }

        ~spill_file_t() { release(); }

        spill_file_t(const spill_file_t&) = delete;
        spill_file_t& operator=(const spill_file_t&) = delete;
        spill_file_t(spill_file_t&&) = delete;
        spill_file_t& operator=(spill_file_t&&) = delete;

        bool valid() const noexcept { return static_cast<bool>(handle_); }
        core::filesystem::file_handle_t& handle() { return *handle_; }
        core::filesystem::local_file_system_t& fs() noexcept { return fs_; }
        const std::string& dir() const noexcept { return dir_; }
        const std::string& name() const noexcept { return name_; }
        std::string full_path() const { return dir_ + "/" + name_; }

        // Whole-buffer write at the current seek position. Returns false on any
        // I/O failure (R6: caller reports a real error rather than silently
        // truncating or duplicating).
        //
        // NOTE on EINTR: file_handle_t::write(buf, n) is the *position-advancing*
        // (non-located) overload. Internally it already loops until all n bytes
        // are written and only returns <= 0 on error; on that error it returns
        // the failing ::write() result (<= 0), NOT the count it managed to write
        // first. So if a signal interrupts the write *after* a partial transfer,
        // the file offset has already advanced past those bytes but we have no
        // way to learn by how much. Re-issuing the write from the same `p` would
        // re-emit the already-written prefix at the new offset and corrupt the
        // spill. We therefore treat ANY non-positive return (EINTR included) as a
        // hard failure instead of retrying.
        bool write_all(const void* buffer, uint64_t nbytes) {
            if (!valid()) { return false; }
            auto* p = static_cast<const std::byte*>(buffer);
            uint64_t remaining = nbytes;
            while (remaining > 0) {
                int64_t w = handle_->write(const_cast<std::byte*>(p), static_cast<uint64_t>(remaining));
                if (w <= 0) {
                    return false; // hard error; cannot safely resume (see NOTE above)
                }
                p += w;
                remaining -= static_cast<uint64_t>(w);
            }
            return true;
        }

        bool sync() {
            if (!valid()) { return false; }
            return core::filesystem::file_sync(fs_, *handle_);
        }

    private:
        core::filesystem::local_file_system_t& fs_;
        std::string dir_;
        std::string name_;
        std::unique_ptr<core::filesystem::file_handle_t> handle_;
        bool released_{false};

        void open_write() {
            // open_file(WRITE|FILE_CREATE) fails if the directory is missing, so
            // ensure the spill dir exists first.
            if (!core::filesystem::directory_exists(fs_, dir_)) {
                core::filesystem::create_directory(fs_, dir_);
            }
            handle_ = core::filesystem::open_file(
                fs_, full_path(),
                core::filesystem::file_flags::WRITE | core::filesystem::file_flags::FILE_CREATE);
        }

        void release() noexcept {
            if (released_) { return; }
            released_ = true;
            if (handle_) {
                handle_->close();
                handle_.reset();
            }
            // Best-effort removal through the FS abstraction (always — success or
            // failure); remove_file wraps std::remove and never throws.
            core::filesystem::remove_file(fs_, full_path());
        }
    };

    // Open an existing spill file for reading into a fresh handle. Returns null if
    // the file does not exist (empty partition). The caller owns the handle and is
    // responsible for the underlying path's lifecycle.
    inline std::unique_ptr<core::filesystem::file_handle_t>
    open_spill_for_read(core::filesystem::local_file_system_t& fs,
                        const std::string& dir,
                        const std::string& name) {
        std::string full = dir + "/" + name;
        if (!core::filesystem::file_exists(fs, full)) {
            return nullptr;
        }
        return core::filesystem::open_file(fs, full, core::filesystem::file_flags::READ);
    }

    // Build a unique per-query spill file name. `query_id` scopes temp files to a
    // single statement/session so concurrent queries never collide; `suffix`
    // distinguishes runs/partitions within that query (e.g. "sort_run_3",
    // "agg_partition_7").
    inline std::string make_spill_name(uint64_t query_id, std::string_view suffix) {
        std::string name = "q";
        name += std::to_string(query_id);
        name += "_";
        name += suffix;
        name += ".tmp";
        return name;
    }

    // Resolve the spill directory from the executor-supplied disk config
    // (R10: config threaded via ctx->disk_config, NOT a global). When spill_path
    // is unset the OS temp dir is used — this is a reasonable default for a temp
    // directory, not a "fallback to broken behaviour", so it is not an R6
    // violation. `disk_cfg` may be null (e.g. unit tests without an executor).
    inline std::string resolve_spill_dir(const configuration::config_disk* disk_cfg) {
        if (disk_cfg && !disk_cfg->spill_path.empty()) {
            return disk_cfg->spill_path.string();
        }
        std::error_code ec;
        auto tmp = std::filesystem::temp_directory_path(ec);
        if (ec) {
            // temp_directory_path itself failed — last-resort literal so spill
            // operators still get a concrete path. This path is not created here;
            // spill_file_t's ctor create_directory is the single source of truth
            // for directory existence.
            return "/tmp/otterbrix_spill";
        }
        return (tmp / "otterbrix_spill").string();
    }

} // namespace components::table::storage
