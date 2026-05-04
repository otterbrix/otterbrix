#pragma once

#include "resolved_objects.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/session/session.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace services::dispatcher {

    // M5 / Decision 4 (catalog-migration-to-postgresql-style.md §3 lines 163–169, §6).
    // Versioned plan cache: keyed by (plan_hash, catalog_version). On a DDL the disk side
    // bumps catalog_version_; queries that started under a prior version still see their
    // pinned snapshot — they do not see partially-applied schema changes.
    //
    // Pinning lifecycle:
    //   begin_transaction(session) -> pin_version(session, current_version)
    //   commit/abort                -> unpin_version(session)
    //
    // GC removes entries whose ref_count == 0 AND version < min(active_txns_). Memory and
    // version-count soft limits trigger eviction (LRU-by-version) when crossed.

    using session_id_t = ::components::session::session_id_t;

    // Opaque payload — stored verbatim. The plan cache doesn't interpret resolved data,
    // it only manages key/version/refcount lifecycle. M6 will plug the real
    // catalog_resolve_response_t in here.
    struct resolved_data_t {
        // Per-entry size estimate for the memory_bytes budget. Producer is responsible for
        // computing this when calling store().
        std::size_t memory_bytes{0};
    };

    enum class gc_strategy_t : std::uint8_t {
        // Run GC only when the soft memory ratio is crossed. Hard caps are always
        // enforced afterwards. Cheaper steady state, allows memory creep up to the cap.
        lazy = 0,
        // Run GC on every commit/abort. Bounds memory more tightly, costs more per txn.
        every_commit = 1,
        // Run GC every `periodic_gc_ops_interval` cache operations (store + unpin).
        // Useful when memory thresholds rarely fire but entries accumulate.
        periodic = 2,
    };

    struct plan_cache_config_t {
        std::size_t max_data_versions{100};            // hard cap on distinct versions held
        std::size_t max_memory_bytes{256ull << 20};    // 256 MiB hard cap
        gc_strategy_t gc_strategy{gc_strategy_t::lazy};
        // Lazy strategy fires gc() when total_memory_bytes_ > max_memory_bytes * ratio.
        // The hard cap (max_memory_bytes) is enforced after the soft GC, regardless of ratio.
        // Range: (0.0, 1.0]. Default 0.75 — start reclaiming early, leave headroom.
        float gc_soft_limit_ratio{0.75f};
        // Periodic strategy: run gc() every N store/unpin operations. 0 disables.
        std::size_t periodic_gc_ops_interval{100};
    };

    class versioned_plan_cache_t {
    public:
        explicit versioned_plan_cache_t(std::pmr::memory_resource* /*resource*/,
                                         plan_cache_config_t config = {})
            : config_(config) {}

        // Pin the given session's view to `version`. A session can be re-pinned (replaces
        // the previous binding); double-pin without unpin in between is a no-op vs the new.
        void pin_version(session_id_t session, std::uint64_t version);

        // Drop the session's pin (if any). Triggers GC under every_commit strategy.
        void unpin_version(session_id_t session);

        // Probe by (plan_hash, version). Returns nullptr on miss. On hit, the data lives
        // until either (a) GC removes it (only possible when no active txn pins this
        // version), or (b) the cache is destroyed.
        resolved_data_t* probe(std::uint64_t plan_hash, std::uint64_t version);

        // Insert a freshly resolved entry. If (plan_hash, version) already exists, the
        // call is a no-op (alias dedup — multiple miss-paths racing for the same key).
        void store(std::uint64_t plan_hash, std::uint64_t version, resolved_data_t data);

        // V4 per-name lookups. Same alias-dedup, ref_count, memory_bytes, GC machinery as
        // the plan_hash-keyed path above — just keyed by object name (+ namespace where
        // applicable) so validate_types/validate_schema can probe by table/function/type
        // name instead of plan hash.
        //
        // probe_*: returns nullptr on miss. Pointer stable until next store/clear at same
        // version. store_*: alias-dedup — second store at same key is a no-op.

        resolved_table_t* probe_table(components::catalog::oid_t namespace_oid,
                                       std::string_view name,
                                       std::uint64_t version);
        void store_table(components::catalog::oid_t namespace_oid,
                         std::string name,
                         std::uint64_t version,
                         resolved_table_t data);

        resolved_function_t* probe_function(std::string_view name,
                                              std::span<const components::catalog::oid_t> arg_type_oids,
                                              std::uint64_t version);
        void store_function(std::string name,
                            std::vector<components::catalog::oid_t> arg_type_oids,
                            std::uint64_t version,
                            resolved_function_t data);

        resolved_type_t* probe_type(components::catalog::oid_t namespace_oid,
                                     std::string_view name,
                                     std::uint64_t version);
        void store_type(components::catalog::oid_t namespace_oid,
                        std::string name,
                        std::uint64_t version,
                        resolved_type_t data);

        resolved_namespace_t* probe_namespace(std::string_view name, std::uint64_t version);
        void store_namespace(std::string name, std::uint64_t version, resolved_namespace_t data);

        // Drop everything tied to versions strictly older than min(active_txns_) AND not
        // currently referenced. Always safe to call. Public so tests can observe.
        void gc();

        // Drop all entries — used on invalidation_ring overflow (consumer fell too far
        // behind, must reset wholesale).
        void clear();

        // Return the catalog version pinned for a given session, or nullopt if not pinned.
        std::optional<std::uint64_t> pinned_version_for(session_id_t session) const noexcept {
            auto it = active_txns_.find(session);
            if (it == active_txns_.end()) return std::nullopt;
            return it->second;
        }

        // Inspection (mostly for tests):
        std::size_t version_count() const noexcept { return version_to_entries_.size(); }
        std::size_t total_memory_bytes() const noexcept { return total_memory_bytes_; }
        std::size_t entry_count() const noexcept;
        std::size_t active_txn_count() const noexcept { return active_txns_.size(); }
        bool has_entry(std::uint64_t plan_hash, std::uint64_t version) const noexcept;

    private:
        struct entry_t {
            resolved_data_t data;
            std::size_t ref_count{0}; // # of active sessions whose pinned version == this entry's version
        };

        // (plan_hash, version) → entry. Stored as nested map: version → plan_hash → entry,
        // so gc can scan candidates by version efficiently.
        using plan_map_t = std::unordered_map<std::uint64_t, entry_t>;

        // Per-name entries share the same ref_count/memory accounting as plan-keyed entries.
        // Payloads are object-specific (resolved_table_t / resolved_function_t / etc.) so
        // probe returns a typed pointer; the entry wrappers are templates over payload.
        template<typename Payload>
        struct named_entry_t {
            Payload data;
            std::size_t ref_count{0};
        };

        template<typename Payload>
        using named_map_t = std::unordered_map<std::string, named_entry_t<Payload>>;

        plan_cache_config_t config_;
        // Versions in order; insertion adds, eviction removes oldest unreferenced first.
        std::map<std::uint64_t, plan_map_t> version_to_entries_;
        // Per-object stores. Keyed string format: see encode_*_key helpers in .cpp.
        std::map<std::uint64_t, named_map_t<resolved_table_t>> tables_by_version_;
        std::map<std::uint64_t, named_map_t<resolved_function_t>> functions_by_version_;
        std::map<std::uint64_t, named_map_t<resolved_type_t>> types_by_version_;
        std::map<std::uint64_t, named_map_t<resolved_namespace_t>> namespaces_by_version_;
        std::unordered_map<session_id_t, std::uint64_t> active_txns_;
        std::size_t total_memory_bytes_{0};
        std::size_t ops_since_periodic_gc_{0};

        std::uint64_t min_active_version() const noexcept;
        void incr_ref(std::uint64_t version) noexcept;
        void decr_ref(std::uint64_t version) noexcept;
        void enforce_hard_limits() noexcept;
        bool over_soft_memory_limit() const noexcept;
        void maybe_run_periodic_gc() noexcept;
    };

} // namespace services::dispatcher
