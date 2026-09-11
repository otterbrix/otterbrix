#pragma once

#include <core/result_wrapper.hpp>

#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <utility>
#include <vector>

namespace services::index {

    [[nodiscard]] components::types::physical_value convert(const components::types::logical_value_t& value);

    // TODO: add checkpoints to avoid flushing b+tree after each call
    class btree_index_disk_t final {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        static constexpr uint64_t default_flush_threshold_{1000};

        btree_index_disk_t(const path_t& path,
                           std::pmr::memory_resource* resource,
                           uint64_t flush_threshold = default_flush_threshold_);
        ~btree_index_disk_t();

        btree_index_disk_t(const btree_index_disk_t&) = delete;
        btree_index_disk_t& operator=(const btree_index_disk_t&) = delete;

        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        [[nodiscard]] core::error_t insert(const value_t& key, size_t value);
        [[nodiscard]] core::error_t remove(value_t key);
        [[nodiscard]] core::error_t remove(const value_t& key, size_t row_id);

        // An undecodable record surfaces as data_corruption, never a fabricated row id 0.
        [[nodiscard]] core::error_t find(const value_t& value, result& res) const;
        [[nodiscard]] core::error_t
        scan_range(components::expressions::compare_type compare, const value_t& value, result& res) const;

        // Misnomer, not STL semantics: lower_bound(k) is key < k, upper_bound(k) is key > k (see scan_range lte/gte).
        [[nodiscard]] core::error_t lower_bound(const value_t& value, result& res) const {
            return scan_range(components::expressions::compare_type::lt, value, res);
        }
        [[nodiscard]] core::error_t upper_bound(const value_t& value, result& res) const {
            return scan_range(components::expressions::compare_type::gt, value, res);
        }

        [[nodiscard]] result find(const value_t& value) const {
            result res(resource_);
            [[maybe_unused]] const auto unreported = find(value, res);
            return res;
        }
        [[nodiscard]] result lower_bound(const value_t& value) const {
            result res(resource_);
            [[maybe_unused]] const auto unreported = lower_bound(value, res);
            return res;
        }
        [[nodiscard]] result upper_bound(const value_t& value) const {
            result res(resource_);
            [[maybe_unused]] const auto unreported = upper_bound(value, res);
            return res;
        }

        void drop();
        [[nodiscard]] core::error_t clear();
        // The caller must fail the statement on io_error, or the table and its index silently disagree.
        [[nodiscard]] core::error_t force_flush();

        // Skips the per-op find() dedup; caller must feed each (key, row_id) pair at most once (not unique keys).
        void insert_bulk_unchecked(const value_t& key, size_t value);
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

    private:
        [[nodiscard]] static bool key_is_absent(const value_t& key) noexcept;

        [[nodiscard]] bool should_flush() const noexcept { return ops_since_flush_ >= flush_threshold_; }
        void mark_operation_dirty() noexcept {
            dirty_ = true;
            ++ops_since_flush_;
        }
        [[nodiscard]] bool is_dirty() const noexcept { return dirty_; }
        void reset_flush_state() noexcept {
            dirty_ = false;
            ops_since_flush_ = 0;
        }
        [[nodiscard]] core::error_t flush_if_needed();

        std::pmr::memory_resource* resource_;
        uint64_t flush_threshold_;
        bool dirty_{false};
        uint64_t ops_since_flush_{0};
        std::filesystem::path path_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::b_plus_tree::btree_t> db_;
    };

} // namespace services::index
