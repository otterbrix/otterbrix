#pragma once

#include <core/result_wrapper.hpp>

#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <type_traits>
#include <utility>
#include <vector>

namespace services::index {

    class index_disk_t {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        index_disk_t(std::pmr::memory_resource* resource, uint64_t flush_threshold)
            : resource_(resource)
            , flush_threshold_(flush_threshold) {
            assert(resource != nullptr);
        }
        virtual ~index_disk_t() = default;

        // The resource every answer this backend produces is built on. Held here rather
        // than in each backend because the by-value read shorthands below live here too,
        // and a result built anywhere else is a result built on the process default.
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        virtual void insert(const value_t& key, size_t value) = 0;
        virtual void remove(value_t key) = 0;
        virtual void remove(const value_t& key, size_t row_id) = 0;

        // READS. Every answer is COMPLETE -- an index that reports a subset is a wrong
        // answer, not a fast one -- and every answer comes back in ASCENDING key order.
        //
        // find() is equality and every backend answers it. scan_range() is the ORDERED
        // contract and only an ordered backend answers it; a hashed backend has no
        // ordering to scan and says so LOUDLY rather than returning an empty range,
        // because an empty answer from a registered engine is indistinguishable from
        // "no row carries this key".
        //
        // lower_bound / upper_bound are shorthands for two of scan_range's six
        // predicates, and their names are HISTORICAL: they are not the STL iterator
        // positions. lower_bound(k) is the open ray BELOW k (key < k, i.e. lt) and
        // upper_bound(k) the open ray ABOVE it (key > k, i.e. gt). The inclusive halves
        // -- key <= k and key >= k, which SQL's <= and >= need and which no caller could
        // ask this facade for at all before -- are compare_type::lte and ::gte.
        virtual void find(const value_t& value, result& res) const = 0;
        virtual void scan_range(components::expressions::compare_type compare, const value_t& value, result& res) const = 0;

        void lower_bound(const value_t& value, result& res) const {
            scan_range(components::expressions::compare_type::lt, value, res);
        }
        void upper_bound(const value_t& value, result& res) const {
            scan_range(components::expressions::compare_type::gt, value, res);
        }

        // By-value shorthands. NOT virtual, and that is the fix rather than a style
        // choice: each backend used to return its own default-constructed
        // std::pmr::vector here, which is std::pmr::get_default_resource() by
        // consequence -- and insert()/remove() reach these internally, so the process
        // default resource sat on the WRITE path. One implementation, on resource_.
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
        std::pmr::memory_resource* resource_;
        uint64_t flush_threshold_;
        bool dirty_{false};
        uint64_t ops_since_flush_{0};
    };

    // WHAT EVERY BACKEND MUST BE, checked AT ITS OWN DEFINITION.
    //
    // The erasure above is FORCED, not chosen: which backend an index gets is read from
    // pg_index.indtype off DISK at runtime (index_agent_disk.cpp), and index_engine_t
    // holds a heterogeneous set the catalog decides at startup — so no template can be
    // instantiated on it and the virtuals stay. What a concept buys where the runtime
    // choice cannot be removed is the DIAGNOSTIC POSITION: each implementation asserts
    // itself against this contract on the line after its own class, so a missing or
    // mistyped member fails where the class is written instead of where it is used.
    //
    // Three things `override` does NOT catch, and this does:
    //   * a member never written at all. `override` can only be attached to a member that
    //     exists; forgetting one entirely leaves the class ABSTRACT, and the first word of
    //     that arrives at the unrelated make_unique / spawn site in another translation
    //     unit. is_abstract_v below moves that failure back to the definition.
    //   * a NON-virtual member of the wrong shape. resource() and the by-value find /
    //     lower_bound / upper_bound shorthands carry no `virtual`, so `override` says
    //     nothing about them at all.
    //   * name HIDING. An override of `find(const value_t&, result&)` hides the base's
    //     by-value `find(const value_t&)` unless the implementation says
    //     `using index_disk_t::find;`. Nothing about the override is wrong, so no
    //     diagnostic fires — the call simply stops compiling at whichever caller wanted
    //     the shorthand. The find(value) requirement below fails at the class instead.
    template<typename backend_t>
    concept index_disk_impl =
        std::derived_from<backend_t, index_disk_t> && !std::is_abstract_v<backend_t> &&
        requires(backend_t& backend,
                 const backend_t& const_backend,
                 const index_disk_t::value_t& key,
                 index_disk_t::result& res,
                 const std::vector<std::pair<index_disk_t::value_t, size_t>>& values,
                 uint64_t txn_id,
                 size_t row_id,
                 bool flag) {
        { backend.insert(key, row_id) } -> std::same_as<void>;
        { backend.remove(key) } -> std::same_as<void>;
        { backend.remove(key, row_id) } -> std::same_as<void>;
        { const_backend.find(key, res) } -> std::same_as<void>;
        { const_backend.scan_range(components::expressions::compare_type::eq, key, res) } -> std::same_as<void>;
        // The by-value shorthands: non-virtual base members, and hidden by an override of
        // the same name unless the implementation un-hides them.
        { const_backend.find(key) } -> std::same_as<index_disk_t::result>;
        { const_backend.lower_bound(key) } -> std::same_as<index_disk_t::result>;
        { const_backend.upper_bound(key) } -> std::same_as<index_disk_t::result>;
        { backend.drop() } -> std::same_as<void>;
        { backend.clear() } -> std::same_as<void>;
        { backend.force_flush() } -> std::same_as<core::error_t>;
        { backend.insert_bulk_unchecked(key, row_id) } -> std::same_as<void>;
        { backend.remove_bulk_unchecked(key, row_id) } -> std::same_as<void>;
        { backend.set_bulk_mode(flag) } -> std::same_as<void>;
        { const_backend.has_txn_log() } noexcept -> std::same_as<bool>;
        { backend.apply_txn_inserts(txn_id, values) } -> std::same_as<core::error_t>;
        { backend.apply_txn_deletes(txn_id, values) } -> std::same_as<core::error_t>;
        { const_backend.resource() } noexcept -> std::same_as<std::pmr::memory_resource*>;
    };

} // namespace services::index
