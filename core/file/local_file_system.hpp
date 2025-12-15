#pragma once

#include "file_handle.hpp"

namespace core::filesystem {

    // TODO: find a better name for it
    class local_file_system_t {
    public:
        local_file_system_t() = default;
        virtual ~local_file_system_t() = default;

        static bool set_working_directory(const path_t& path);
        static path_t working_directory();
        static uint64_t available_memory();
        static std::string enviroment_variable(const std::string& name);
        static bool has_glob(const std::string& str);

        virtual void reset(file_handle_t& handle);

        virtual path_t expand_path(const path_t& path);

        virtual std::string name() const { return "local_file_system_t"; }
        virtual bool can_handle_files(const path_t&) { return true; }
        virtual bool can_seek() const { return true; }

        path_t home_directory();
        bool set_home_directory(path_t path);
        path_t normalize_path_absolute(const path_t& path);
        path_t extract_base_name(const path_t& path);
        path_t extract_name(const path_t& path);

        bool set_file_pointer(file_handle_t& handle, uint64_t location);
        uint64_t file_pointer(file_handle_t& handle);

        std::vector<path_t> fetch_file_without_glob(const path_t& path, bool absolute_path);
        const path_t& file_search_path() const { return file_search_path_; }

    protected:
        path_t home_directory_;
        path_t file_search_path_;
    };

    std::unique_ptr<file_handle_t> open_file(local_file_system_t&,
                                             const path_t& path,
                                             file_flags flags,
                                             file_lock_type lock = file_lock_type::NO_LOCK);
    bool read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location);
    int64_t read(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes);
    bool write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location);
    int64_t write(local_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes);

    int64_t file_size(local_file_system_t&, file_handle_t& handle);
    time_t last_modified_time(local_file_system_t&, file_handle_t& handle);
    file_type_t file_type(local_file_system_t&, file_handle_t& handle);
    bool truncate(local_file_system_t&, file_handle_t& handle, int64_t new_size);
    bool trim(local_file_system_t&, file_handle_t& handle, uint64_t offset_bytes, uint64_t length_bytes);

    bool directory_exists(local_file_system_t&, const path_t& directory);
    bool create_directory(local_file_system_t&, const path_t& directory);
    bool remove_directory(local_file_system_t&, const path_t& directory);
    bool list_files(local_file_system_t&, path_t directory, const std::function<void(const path_t&, bool)>& callback);
    bool move_files(local_file_system_t&, const path_t& source, const path_t& target);
    bool file_exists(local_file_system_t&, const path_t& filename);

    bool is_pipe(local_file_system_t&, const path_t& filename);
    bool remove_file(local_file_system_t&, const path_t& filename);
    bool file_sync(local_file_system_t&, file_handle_t& handle);

    std::vector<path_t> glob_files(local_file_system_t&, const std::string& path);

    bool seek(local_file_system_t&, file_handle_t& handle, uint64_t location);
    uint64_t seek_position(local_file_system_t&, file_handle_t& handle);

#ifdef PLATFORM_WINDOWS
    std::string last_error_as_string(local_file_system_t&);
#endif

} // namespace core::filesystem
