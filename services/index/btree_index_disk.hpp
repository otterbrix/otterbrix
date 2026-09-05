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

    // Ordered-index probe encoder: logical key -> the physical_value the on-disk b+tree
    // compares with. Carries exactly the ordered half of
    // components::index::codec::is_representable_index_key_type (the CREATE INDEX gate);
    // any other type is a gate/encoder drift bug and aborts.
    [[nodiscard]] components::types::physical_value convert(const components::types::logical_value_t& value);

    // THE ORDERED STORE. It has no base class and there deliberately is not going to be
    // one: the erased index_disk_t it used to derive from existed so a single index agent
    // could hold either family behind one pointer and ASK it at runtime which it was --
    // does it own a txn log, has it a bulk window, can it answer an ordered probe. There
    // is one agent class per family now, each holding its store BY VALUE and by this
    // concrete type, so every one of those questions is answered by the type and the
    // virtuals bought nothing but a vtable and three abort-only stubs (a btree
    // apply_txn_inserts, a btree apply_txn_deletes, an empty set_bulk_mode) that existed
    // only to satisfy the base.
    //
    // What the base really owned -- the resource, the flush accounting, the by-value read
    // shorthands -- is duplicated here and in bitcask_index_disk_t. That duplication is
    // the price of not having the coupling, and it is the cheaper half.
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

        // The resource every answer this store produces is built on. A result built
        // anywhere else is a result built on the process default resource.
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // WRITES, and each reports whether the data it wrote reached the device. A write
        // whose threshold flush failed left the tree naming entries no reader will ever find
        // again after a restart; returning void made that indistinguishable from success.
        [[nodiscard]] core::error_t insert(const value_t& key, size_t value);
        [[nodiscard]] core::error_t remove(value_t key);
        [[nodiscard]] core::error_t remove(const value_t& key, size_t row_id);

        // READS. Every answer is COMPLETE -- an index that reports a subset is a wrong
        // answer, not a fast one -- and every answer comes back in ASCENDING key order.
        void find(const value_t& value, result& res) const;
        // The ordered contract in full: eq / ne / lt / lte / gt / gte, every one of them
        // an inclusive-bounded ascending walk of the tree. It is THE reason this family
        // exists, and it is why btree_index_agent_t answers supports_ordered_probe_v with
        // true where the hashed family answers false.
        void scan_range(components::expressions::compare_type compare, const value_t& value, result& res) const;

        // Shorthands for two of scan_range's six predicates, and their names are
        // HISTORICAL: they are not the STL iterator positions. lower_bound(k) is the open
        // ray BELOW k (key < k) and upper_bound(k) the open ray ABOVE it (key > k). The
        // inclusive halves -- key <= k and key >= k, which SQL's <= and >= need -- are
        // compare_type::lte and ::gte, asked of scan_range directly.
        void lower_bound(const value_t& value, result& res) const {
            scan_range(components::expressions::compare_type::lt, value, res);
        }
        void upper_bound(const value_t& value, result& res) const {
            scan_range(components::expressions::compare_type::gt, value, res);
        }

        // By-value shorthands, built on resource_. Never on a default-constructed
        // std::pmr::vector, which is std::pmr::get_default_resource() by consequence.
        [[nodiscard]] result find(const value_t& value) const {
            result res(resource_);
            find(value, res);
            return res;
        }
        [[nodiscard]] result lower_bound(const value_t& value) const {
            result res(resource_);
            lower_bound(value, res);
            return res;
        }
        [[nodiscard]] result upper_bound(const value_t& value) const {
            result res(resource_);
            upper_bound(value, res);
            return res;
        }

        void drop();
        // Wipe all stored index data IN PLACE, keeping the backing live and writable:
        // subsequent insert/remove repopulate cleanly. NOT the terminal drop -- the file
        // survives (re-initialized empty) and the instance stays usable.
        //
        // AND IT REPORTS THE ONE REFUSAL IT CAN SEE: a tree directory that would not be
        // removed. The tree is reloaded over the survivor either way, so the instance stays
        // usable and its contents stay honest -- they are simply the contents this call
        // promised to erase, which is exactly what the error says. Without this,
        // index_agent_contract::clear told the truth about one of its two implementations.
        [[nodiscard]] core::error_t clear();
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and
        // nothing downstream would ever notice.
        [[nodiscard]] core::error_t force_flush();

        // Bulk-load fast path: append/erase without the per-op find() dedup, persisting
        // once via force_flush(). Removes the O(rows^2) cost of insert()/remove() calling
        // find() per row. They never call flush_if_needed, so there is no bulk-mode
        // WINDOW to open -- the caller force_flush()es once at the end.
        //
        // WHAT THE CALLER GUARANTEES, precisely: each (key, row_id) PAIR is fed at most
        // once and, for the remove side, is present. It does NOT guarantee unique KEYS --
        // a non-unique index is the ordinary case, and every rebuild feed replays a whole
        // table, repeated keys included.
        void insert_bulk_unchecked(const value_t& key, size_t value);
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

    private:
        // A NULL key is neither stored nor looked up. The invariant is owned by
        // index_key_is_null (services/index/index_agent_contract.hpp) and enforced by both
        // agents; this is the store's own second door on it, because the backend tests
        // reach these methods directly. Defined next to the reasons in the .cpp.
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
