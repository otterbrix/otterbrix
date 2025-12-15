#pragma once

#include "local_file_system.hpp"
#include <map>
#include <unordered_set>

namespace core::filesystem {

    // Does not have much uses for now
    // TODO: extend usage beyond just local_file_system_t
    class virtual_file_system_t : public local_file_system_t {
    public:
        virtual_file_system_t();

        bool can_handle_files(const path_t&) override { return false; }
        bool can_seek() const override { return true; }
        std::string name() const override;

        void register_sub_system(std::unique_ptr<local_file_system_t> fs);
        void unregister_sub_system(const std::string& name);
        void register_sub_system(file_compression_type compression_type, std::unique_ptr<local_file_system_t> fs);
        void set_disabled_file_systems(const std::vector<std::string>& names);
        std::vector<std::string> list_sub_system();
        local_file_system_t& find_file_system(const path_t& path);
        local_file_system_t& default_file_system() { return *(default_fs_.get()); }
        std::string path_separator(const path_t& path);

    private:
        local_file_system_t& find_file_system_(const path_t& path);

        std::vector<std::unique_ptr<local_file_system_t>> sub_systems_;
        std::map<file_compression_type, std::unique_ptr<local_file_system_t>> compressed_fs_;
        const std::unique_ptr<local_file_system_t> default_fs_;
        std::unordered_set<std::string> disabled_file_systems_;
    };

    std::unique_ptr<file_handle_t> open_file(virtual_file_system_t&, const path_t& path, file_flags flags);
    bool read(virtual_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location);
    int64_t read(virtual_file_system_t&, file_handle_t& handle, void* buffer, int64_t nr_bytes);
    int64_t file_size(virtual_file_system_t&, file_handle_t& handle);
    time_t last_modified_time(virtual_file_system_t&, file_handle_t& handle);
    file_type_t file_type(virtual_file_system_t&, file_handle_t& handle);
    bool truncate(virtual_file_system_t&, file_handle_t& handle, int64_t new_size);
    bool trim(virtual_file_system_t&, file_handle_t& handle, uint64_t offset_bytes, uint64_t length_bytes);
    bool directory_exists(virtual_file_system_t&, const path_t& directory);
    bool create_directory(virtual_file_system_t&, const path_t& directory);
    bool remove_directory(virtual_file_system_t&, const path_t& directory);
    bool list_files(virtual_file_system_t&,
                    const path_t& directory,
                    const std::function<void(const path_t&, bool)>& callback);
    bool move_files(virtual_file_system_t&, const path_t& source, const path_t& target);
    bool file_exists(virtual_file_system_t&, const path_t& filename);
    bool is_pipe(virtual_file_system_t&, const path_t& filename);
    bool remove_file(virtual_file_system_t&, const path_t& filename);
    bool file_sync(virtual_file_system_t&, file_handle_t& handle);
    std::vector<path_t> glob_files(virtual_file_system_t&, const std::string& path);

} // namespace core::filesystem