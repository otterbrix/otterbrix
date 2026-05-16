#pragma once

#include <components/types/logical_value.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <vector>

namespace services::index {

    class index_disk_t {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        explicit index_disk_t(uint64_t flush_threshold = default_flush_threshold_)
            : flush_threshold_(flush_threshold) {}
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

    protected:
        static constexpr uint64_t default_flush_threshold_{1000};

        bool should_flush() const noexcept { return ops_since_flush_ >= flush_threshold_; }
        void mark_operation_dirty() noexcept {
            dirty_ = true;
            ++ops_since_flush_;
        }
        bool is_dirty() const noexcept { return dirty_; }
        void reset_flush_state() noexcept {
            dirty_ = false;
            ops_since_flush_ = 0;
        }

    private:
        uint64_t flush_threshold_;
        bool dirty_{false};
        uint64_t ops_since_flush_{0};
    };

} // namespace services::index
