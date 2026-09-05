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

    // Ordered-index probe encoder: logical key -> the physical_value the b+tree compares with.
    // Carries exactly the ordered half of is_representable_index_key_type (the CREATE INDEX
    // gate); any other type is a gate/encoder drift bug and aborts.
    [[nodiscard]] components::types::physical_value convert(const components::types::logical_value_t& value);

    // No base class: one agent per family holds its store by value and concrete type instead, so
    // backend questions (txn log? bulk window? ordered probe?) resolve by type, not virtual
    // dispatch -- a base here would buy only a vtable plus three abort-only stubs. The shared bits
    // (resource, flush accounting) are duplicated in bitcask_index_disk_t instead.
    //
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

        // The resource every answer this store produces is built on (not the process default).
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Each write reports whether its threshold flush reached the device -- a void return
        // would make an entry lost to a failed flush indistinguishable from success.
        [[nodiscard]] core::error_t insert(const value_t& key, size_t value);
        [[nodiscard]] core::error_t remove(value_t key);
        [[nodiscard]] core::error_t remove(const value_t& key, size_t row_id);

        // Every answer is complete (a subset would be a wrong answer, not a fast one) and in
        // ascending key order. An undecodable record would otherwise surface as row id 0,
        // indistinguishable from a real one -- data_corruption travels instead, failing the query.
        [[nodiscard]] core::error_t find(const value_t& value, result& res) const;
        // The ordered contract in full (eq/ne/lt/lte/gt/gte), each an inclusive-bounded
        // ascending walk -- the reason supports_ordered_probe_v is true for this family.
        [[nodiscard]] core::error_t
        scan_range(components::expressions::compare_type compare, const value_t& value, result& res) const;

        // Names are HISTORICAL, not STL iterator positions: lower_bound(k) is key < k, and
        // upper_bound(k) is key > k. The inclusive halves (<=, >=) are compare_type::lte/gte,
        // asked of scan_range directly.
        [[nodiscard]] core::error_t lower_bound(const value_t& value, result& res) const {
            return scan_range(components::expressions::compare_type::lt, value, res);
        }
        [[nodiscard]] core::error_t upper_bound(const value_t& value, result& res) const {
            return scan_range(components::expressions::compare_type::gt, value, res);
        }

        // Test-facing shorthands, built on resource_ (never a default-constructed
        // std::pmr::vector). Not a swallowed error: no production caller uses them, and a
        // refusal still can't pass unnoticed -- find/scan_range stop at the unreadable record,
        // so the answer comes back short and every test asserts on its size.
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
        // Wipes index data in place (not the terminal drop -- the directory survives,
        // re-initialized empty). Reports the one refusal it can see: a directory that would
        // not remove, over which the tree is reloaded either way, keeping the old contents.
        [[nodiscard]] core::error_t clear();
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and
        // nothing downstream would ever notice.
        [[nodiscard]] core::error_t force_flush();

        // Bulk fast path: append/erase without the per-op find() dedup, persisting once via the
        // caller's force_flush() (O(rows) vs O(rows^2)). Caller guarantees each (key, row_id)
        // PAIR is fed at most once -- not unique keys, which the ordinary non-unique index breaks.
        void insert_bulk_unchecked(const value_t& key, size_t value);
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

    private:
        // A NULL key is neither stored nor looked up (index_key_is_null's rule,
        // index_agent_contract.hpp), enforced here too since backend tests reach this class
        // directly. Defined next to the reasons in the .cpp.
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
        // Flushes when the operation counter crosses the threshold, and reports the io_error
        // when that flush does not reach the disk (see the .cpp).
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
