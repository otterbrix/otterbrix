#pragma once

#include <core/result_wrapper.hpp>

#include <components/types/logical_value.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <utility>
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
        // Wipe all stored index data IN PLACE, keeping the backing live and
        // writable: subsequent insert/remove (incl. the direct, non-txn-log
        // txn_id==0 path) repopulate cleanly. NOT the terminal drop — the
        // files/directory survive (re-initialized empty), the instance stays
        // usable. Used by the runtime repopulate path.
        virtual void clear() = 0;
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and nothing
        // downstream would ever notice.
        [[nodiscard]] virtual core::error_t force_flush() = 0;

        // Bulk-load fast path. insert_bulk_unchecked / remove_bulk_unchecked skip the
        // per-operation dedup find() and the per-operation flush; force_flush() persists
        // once at the end, eliminating the O(rows^2) cost for backends (e.g. btree) whose
        // find() is not O(1). Pure virtual: each backend supplies a real bulk path.
        //
        // WHAT THE CALLER GUARANTEES, precisely: each (key, row_id) PAIR is fed at most
        // once and, for the remove side, is present. It does NOT guarantee unique KEYS —
        // a non-unique index is the ordinary case, and every rebuild feed
        // (repopulate_table, the txn-0 leg of index_agent_disk_t::insert_many) replays a
        // whole table, repeated keys included. An implementation that reads the weaker
        // sentence this comment used to carry as "one row per key" and overwrites the
        // key's row list drops every duplicate, which is what bitcask's did.
        virtual void insert_bulk_unchecked(const value_t& key, size_t value) = 0;
        virtual void remove_bulk_unchecked(const value_t& key, size_t row_id) = 0;

        // Backend dispatch, asked of the backend instead of guessed from its type.
        //
        // Does this backend own a durable transaction log? index_agent_disk_t routes a
        // committed statement on this answer alone: true means apply_txn_inserts /
        // apply_txn_deletes journal the whole statement under its txn_id (which is what
        // arms the crash-recover gate, replaying only WAL-committed frames); false means
        // the bulk path above. The router used to ask
        // dynamic_cast<bitcask_index_disk_t*> instead.
        //
        // Pure virtual on purpose, and so are the three hooks below: there is NO default
        // for a backend to fall into. A default `false` here is precisely the silent
        // regression the no-fallback rule forbids -- a backend that does own a log would
        // inherit it, quietly take the bulk path, and lose txn semantics with nothing
        // anywhere reporting a problem. A default no-op apply_txn_* would be the same
        // failure one level down. Every backend states its own answer and the compiler
        // enforces that it does.
        [[nodiscard]] virtual bool has_txn_log() const noexcept = 0;

        // Journal a whole statement's inserts / deletes under txn_id and apply them.
        // Called ONLY when has_txn_log() is true. A backend that answers false still
        // implements these, and implements them as a LOUD failure -- never as a quiet
        // no-op, which would report success for writes it never made.
        // Returns an error when the journal or the data did not reach disk; the caller
        // fails the statement.
        [[nodiscard]] virtual core::error_t
        apply_txn_inserts(uint64_t txn_id, const std::vector<std::pair<value_t, size_t>>& values) = 0;
        [[nodiscard]] virtual core::error_t
        apply_txn_deletes(uint64_t txn_id, const std::vector<std::pair<value_t, size_t>>& values) = 0;

        // Opens (true) and closes (false) the backend's bulk-load window around a run of
        // insert_bulk_unchecked / remove_bulk_unchecked. A backend with a window to open
        // must not inherit a do-nothing one by accident; a backend with no window says so
        // explicitly in its own override.
        virtual void set_bulk_mode(bool enabled) = 0;

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
