#pragma once

#include <components/types/logical_value.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

#include <filesystem>
#include <memory_resource>

namespace services::disk {

    // TODO: add checkpoints to avoid flushing b+tree after each call
    class index_disk_t {
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;

    public:
        using result = std::pmr::vector<size_t>;

        index_disk_t(const path_t& path, std::pmr::memory_resource* resource);
        ~index_disk_t();

        void insert(const value_t& key, size_t value);
        void remove(value_t key);
        void remove(const value_t& key, size_t row_id);
        void find(const value_t& value, result& res) const;
        result find(const value_t& value) const;
        void lower_bound(const value_t& value, result& res) const;
        result lower_bound(const value_t& value) const;
        void upper_bound(const value_t& value, result& res) const;
        result upper_bound(const value_t& value) const;

        void drop();

    private:
        std::filesystem::path path_;
        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::b_plus_tree::btree_t> db_;
    };

} // namespace services::disk
