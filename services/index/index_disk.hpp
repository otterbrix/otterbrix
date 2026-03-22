#pragma once

#include <components/types/logical_value.hpp>

#include <cstddef>
#include <filesystem>
#include <memory_resource>
#include <vector>

namespace services::index {

    class index_disk_t {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        virtual ~index_disk_t() = default;

        virtual void insert(const value_t& key, size_t value) = 0;
        virtual void remove(value_t key) = 0;
        virtual void remove(const value_t& key, size_t row_id) = 0;
        virtual void find(const value_t& value, result& res) const = 0;
        virtual result find(const value_t& value) const = 0;
        virtual void lower_bound(const value_t& value, result& res) const = 0;
        virtual result lower_bound(const value_t& value) const = 0;
        virtual void upper_bound(const value_t& value, result& res) const = 0;
        virtual result upper_bound(const value_t& value) const = 0;
        virtual void drop() = 0;
        virtual void force_flush() = 0;
    };

} // namespace services::index
