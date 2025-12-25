#include "virtual_file_system.hpp"
#include "file_system.hpp"
#include "path_utils.hpp"
#include <cassert>

namespace core::filesystem {

    virtual_file_system_t::virtual_file_system_t()
        : default_fs_(std::make_unique<local_file_system_t>()) {}

    void virtual_file_system_t::register_sub_system(std::unique_ptr<local_file_system_t> fs) {
        sub_systems_.push_back(std::move(fs));
    }

    void virtual_file_system_t::unregister_sub_system(const std::string& name) {
        for (auto sub_system = sub_systems_.begin(); sub_system != sub_systems_.end(); sub_system++) {
            if (sub_system->get()->name() == name) {
                sub_systems_.erase(sub_system);
                return;
            }
        }
        assert(false && ("Could not find file_handle with name " + name).data());
    }

    void virtual_file_system_t::register_sub_system(file_compression_type compression_type,
                                                    std::unique_ptr<local_file_system_t> fs) {
        compressed_fs_[compression_type] = std::move(fs);
    }

    std::vector<std::string> virtual_file_system_t::list_sub_system() {
        std::vector<std::string> names(sub_systems_.size());
        for (uint64_t i = 0; i < sub_systems_.size(); i++) {
            names[i] = sub_systems_[i]->name();
        }
        return names;
    }

    std::string virtual_file_system_t::name() const { return "virtual_file_system"; }

    void virtual_file_system_t::set_disabled_file_systems(const std::vector<std::string>& names) {
        std::unordered_set<std::string> new_disabled_file_systems;
        for (auto& name : names) {
            if (name.empty()) {
                continue;
            }
            if (new_disabled_file_systems.find(name) != new_disabled_file_systems.end()) {
                assert(false && ("Duplicate disabled file system: " + name).data());
            }
            new_disabled_file_systems.insert(name);
        }
        for (auto& disabled_fs : disabled_file_systems_) {
            if (new_disabled_file_systems.find(disabled_fs) == new_disabled_file_systems.end()) {
                assert(
                    false &&
                    ("File system: " + disabled_fs + " has been disabled previously, it cannot be re-enabled").data());
            }
        }
        disabled_file_systems_ = std::move(new_disabled_file_systems);
    }

    local_file_system_t& virtual_file_system_t::find_file_system(const path_t& path) {
        auto& fs = find_file_system_(path);
        if (!disabled_file_systems_.empty() && disabled_file_systems_.find(fs.name()) != disabled_file_systems_.end()) {
            assert(false && ("File system: " + fs.name() + " has been disabled by configuration").data());
        }
        return fs;
    }

    local_file_system_t& virtual_file_system_t::find_file_system_(const path_t& path) {
        for (auto& sub_system : sub_systems_) {
            if (sub_system->can_handle_files(path)) {
                return *sub_system;
            }
        }
        return *default_fs_;
    }

    std::unique_ptr<file_handle_t> open_file(virtual_file_system_t& vfs, const path_t& path, file_flags flags) {
        auto file_handle = open_file(vfs.find_file_system(path), path, flags);
        if (!file_handle) {
            return nullptr;
        }
        return file_handle;
    }

    bool read(virtual_file_system_t& vfs, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        return read(vfs.default_file_system(), handle, buffer, nr_bytes, location);
    }

    int64_t read(virtual_file_system_t& vfs, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        return read(vfs.default_file_system(), handle, buffer, nr_bytes);
    }

    bool write(virtual_file_system_t& vfs, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        return write(vfs.default_file_system(), handle, buffer, nr_bytes, location);
    }

    int64_t write(virtual_file_system_t& vfs, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        return write(vfs.default_file_system(), handle, buffer, nr_bytes);
    }

    int64_t file_size(virtual_file_system_t&, file_handle_t& handle) {
        return static_cast<int64_t>(handle.file_size());
    }
    time_t last_modified_time(virtual_file_system_t& vfs, file_handle_t& handle) {
        return last_modified_time(vfs.default_file_system(), handle);
    }
    file_type_t file_type(virtual_file_system_t& vfs, file_handle_t& handle) {
        return file_type(vfs.default_file_system(), handle);
    }

    bool truncate(virtual_file_system_t& vfs, file_handle_t& handle, int64_t new_size) {
        return truncate(vfs.default_file_system(), handle, new_size);
    }

    bool trim(virtual_file_system_t& vfs, file_handle_t& handle, uint64_t offset_bytes, uint64_t length_bytes) {
        return trim(vfs.default_file_system(), handle, offset_bytes, length_bytes);
    }

    bool file_sync(virtual_file_system_t& vfs, file_handle_t& handle) {
        return file_sync(vfs.default_file_system(), handle);
    }

    bool directory_exists(virtual_file_system_t& vfs, const path_t& directory) {
        return directory_exists(vfs.find_file_system(directory), directory);
    }

    bool create_directory(virtual_file_system_t& vfs, const path_t& directory) {
        return create_directory(vfs.find_file_system(directory), directory);
    }

    bool remove_directory(virtual_file_system_t& vfs, const path_t& directory) {
        return remove_directory(vfs.find_file_system(directory), directory);
    }

    bool list_files(virtual_file_system_t& vfs,
                    const path_t& directory,
                    const std::function<void(const path_t&, bool)>& callback) {
        return list_files(vfs.find_file_system(directory), directory, callback);
    }

    bool move_files(virtual_file_system_t& vfs, const path_t& source, const path_t& target) {
        return move_files(vfs.find_file_system(source), source, target);
    }

    bool file_exists(virtual_file_system_t& vfs, const path_t& filename) {
        return file_exists(vfs.find_file_system(filename), filename);
    }

    bool is_pipe(virtual_file_system_t& vfs, const path_t& filename) {
        return is_pipe(vfs.find_file_system(filename), filename);
    }

    bool remove_file(virtual_file_system_t& vfs, const path_t& filename) {
        return remove_file(vfs.find_file_system(filename), filename);
    }

    std::vector<path_t> glob_files(virtual_file_system_t& vfs, const std::string& path) {
        return glob_files(vfs.find_file_system(path), path);
    }

} // namespace core::filesystem