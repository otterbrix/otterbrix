#pragma once

#include "index_disk.hpp"

#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>

namespace services::index {

    // Ordered-index probe encoder: logical key -> the physical_value the on-disk b+tree
    // compares with. Carries exactly the ordered half of
    // components::index::codec::is_representable_index_key_type (the CREATE INDEX gate);
    // any other type is a gate/encoder drift bug and aborts.
    [[nodiscard]] components::types::physical_value convert(const components::types::logical_value_t& value);

    // TODO: add checkpoints to avoid flushing b+tree after each call
    class btree_index_disk_t final : public index_disk_t {
    public:
        static constexpr uint64_t default_flush_threshold_{1000};

        btree_index_disk_t(const path_t& path,
                           std::pmr::memory_resource* resource,
                           uint64_t flush_threshold = default_flush_threshold_);
        ~btree_index_disk_t() override;

        void insert(const value_t& key, size_t value) override;
        void remove(value_t key) override;
        void remove(const value_t& key, size_t row_id) override;
        void find(const value_t& value, result& res) const override;
        // The ordered contract in full: eq / ne / lt / lte / gt / gte, every one of them
        // an inclusive-bounded ascending walk of the tree. See index_disk.hpp for what
        // the three named shorthands mean and why lte / gte / ne could not be expressed
        // through them before.
        void scan_range(components::expressions::compare_type compare,
                        const value_t& value,
                        result& res) const override;
        // The by-value shorthands are non-virtual base members; without this they would be
        // hidden by the find() override above.
        using index_disk_t::find;

        void drop() override;
        void clear() override;
        [[nodiscard]] core::error_t force_flush() override;

        // Bulk-load fast path (see index_disk_t): append/erase without the per-op
        // find() dedup, persisting once via force_flush(). Removes the O(rows^2) cost
        // of insert()/remove() calling find() per row. They never call flush_if_needed,
        // so no bulk-mode flag is needed — the caller force_flush()es once at the end.
        void insert_bulk_unchecked(const value_t& key, size_t value) override;
        void remove_bulk_unchecked(const value_t& key, size_t row_id) override;

        // The ordered b+tree owns no transaction log: a committed statement reaches it
        // through the bulk path, exactly as it did when index_agent_disk_t decided this
        // with a dynamic_cast that simply failed to match bitcask.
        [[nodiscard]] bool has_txn_log() const noexcept override { return false; }

        // Unreachable by contract -- the router calls these only when has_txn_log() is
        // true. They exist, and fail LOUDLY, because the alternative (inheriting a
        // do-nothing default) is a backend silently reporting success for writes it never
        // journalled and never made. The assert stops a debug build at the bug site; the
        // returned error is the release-build equivalent, and [[nodiscard]] plus the
        // agent's co_return carry it out to the failing statement.
        [[nodiscard]] core::error_t apply_txn_inserts(uint64_t txn_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values) override;
        [[nodiscard]] core::error_t apply_txn_deletes(uint64_t txn_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values) override;

        // No window to open: the b+tree bulk methods already skip the per-op find() and
        // never call flush_if_needed, so there is nothing to suppress and nothing to
        // restore. Stated here rather than inherited, so a reader sees the decision.
        void set_bulk_mode(bool enabled) override;

    private:
        // A NULL key is neither stored nor looked up -- the same invariant index_t owns
        // for the in-memory side, enforced again here because this class has doors index_t
        // does not guard. Defined next to the reasons in the .cpp.
        [[nodiscard]] static bool key_is_absent(const value_t& key) noexcept;

        void flush_if_needed();

        std::filesystem::path path_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::b_plus_tree::btree_t> db_;
    };

} // namespace services::index
