#pragma once

#include "index_disk.hpp"

#include <components/types/logical_value.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

#include <filesystem>
#include <memory_resource>

namespace services::index {

    // TODO: add checkpoints to avoid flushing b+tree after each call
    class btree_index_disk_t final : public index_disk_t {
    public:
        btree_index_disk_t(const path_t& path, std::pmr::memory_resource* resource);
        ~btree_index_disk_t() override;

        void insert(const value_t& key, size_t value) override;
        void remove(value_t key) override;
        void remove(const value_t& key, size_t row_id) override;
        void find(const value_t& value, result& res) const override;
        result find(const value_t& value) const override;
        void lower_bound(const value_t& value, result& res) const override;
        result lower_bound(const value_t& value) const override;
        void upper_bound(const value_t& value, result& res) const override;
        result upper_bound(const value_t& value) const override;

        void drop() override;
        void force_flush() override;

    private:
        void flush_if_needed();

        std::filesystem::path path_;
        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::b_plus_tree::btree_t> db_;
        bool dirty_{false};
        uint64_t ops_since_flush_{0};
        static constexpr uint64_t flush_threshold_{1000};
    };

} // namespace services::index
