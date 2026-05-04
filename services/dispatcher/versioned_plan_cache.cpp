#include "versioned_plan_cache.hpp"

#include <limits>
#include <string>

namespace services::dispatcher {

    namespace {
        // Encode (namespace_oid, name) into a unique key for hash-map lookup. Format:
        // "<oid>:<name>". OIDs are bounded uint32_t so the digit count is bounded too.
        std::string encode_table_key(components::catalog::oid_t namespace_oid, std::string_view name) {
            std::string s = std::to_string(namespace_oid);
            s += ':';
            s.append(name.data(), name.size());
            return s;
        }

        std::string encode_type_key(components::catalog::oid_t namespace_oid, std::string_view name) {
            return encode_table_key(namespace_oid, name); // same shape
        }

        // Functions key on (name, ordered arg type oids). Format: "<name>(<oid>,<oid>,...)".
        std::string encode_function_key(std::string_view name,
                                         std::span<const components::catalog::oid_t> arg_type_oids) {
            std::string s;
            s.reserve(name.size() + 2 + arg_type_oids.size() * 5);
            s.append(name.data(), name.size());
            s += '(';
            bool first = true;
            for (auto oid : arg_type_oids) {
                if (!first) s += ',';
                s += std::to_string(oid);
                first = false;
            }
            s += ')';
            return s;
        }
    } // namespace

    void versioned_plan_cache_t::pin_version(session_id_t session, std::uint64_t version) {
        auto [it, inserted] = active_txns_.try_emplace(session, version);
        if (!inserted) {
            // Already pinned — re-bind. Decrement refcount of old version's entries first,
            // then bind to the new one.
            decr_ref(it->second);
            it->second = version;
        }
        incr_ref(version);
    }

    void versioned_plan_cache_t::unpin_version(session_id_t session) {
        auto it = active_txns_.find(session);
        if (it == active_txns_.end()) {
            return;
        }
        decr_ref(it->second);
        active_txns_.erase(it);
        switch (config_.gc_strategy) {
            case gc_strategy_t::every_commit:
                gc();
                break;
            case gc_strategy_t::lazy:
                if (over_soft_memory_limit()) {
                    gc();
                }
                break;
            case gc_strategy_t::periodic:
                // counter handled below
                break;
        }
        maybe_run_periodic_gc();
    }

    resolved_data_t* versioned_plan_cache_t::probe(std::uint64_t plan_hash, std::uint64_t version) {
        auto v_it = version_to_entries_.find(version);
        if (v_it == version_to_entries_.end()) {
            return nullptr;
        }
        auto p_it = v_it->second.find(plan_hash);
        if (p_it == v_it->second.end()) {
            return nullptr;
        }
        return &p_it->second.data;
    }

    void versioned_plan_cache_t::store(std::uint64_t plan_hash,
                                        std::uint64_t version,
                                        resolved_data_t data) {
        auto& bucket = version_to_entries_[version];
        auto [it, inserted] = bucket.try_emplace(plan_hash);
        if (!inserted) {
            // Alias dedup: same key already cached. Drop the new payload; existing wins.
            return;
        }
        it->second.data = std::move(data);
        // Match the entry's ref_count to the number of active sessions pinned at `version`.
        std::size_t pin_count = 0;
        for (const auto& [sid, ver] : active_txns_) {
            if (ver == version) {
                ++pin_count;
            }
        }
        it->second.ref_count = pin_count;
        total_memory_bytes_ += it->second.data.memory_bytes;

        if (config_.gc_strategy == gc_strategy_t::lazy) {
            if (over_soft_memory_limit()) {
                gc(); // proactive cleanup of unreferenced entries
            }
            enforce_hard_limits(); // force eviction if still over hard cap
        }
        maybe_run_periodic_gc();
    }

    namespace {
        // Drop ref_count==0 entries, decrement total_memory_bytes by payload bytes.
        // Memory accessor extracts the per-entry byte size from the payload.
        // Different shapes between resolved_data_t (field memory_bytes) and named
        // payloads (memory_bytes() method) are unified by the lambda.
        template<typename Map, typename MemFn>
        void gc_pass(Map& by_version, std::size_t& total_bytes, MemFn mem) {
            for (auto v_it = by_version.begin(); v_it != by_version.end();) {
                for (auto e_it = v_it->second.begin(); e_it != v_it->second.end();) {
                    if (e_it->second.ref_count == 0) {
                        total_bytes -= mem(e_it->second.data);
                        e_it = v_it->second.erase(e_it);
                    } else {
                        ++e_it;
                    }
                }
                if (v_it->second.empty()) {
                    v_it = by_version.erase(v_it);
                } else {
                    ++v_it;
                }
            }
        }
    } // namespace

    void versioned_plan_cache_t::gc() {
        // Drop any entry with ref_count==0: no active session pins its version, so it's
        // unreachable. version is monotonic (catalog_version_++ on DDL), and pin_version
        // only takes the current version, so a session can never re-pin a version that's
        // already been abandoned. This covers two cases: (1) versions below floor
        // (the original semantic — old snapshots whose holders have committed/aborted);
        // (2) versions above floor whose holders have already unpinned (e.g. a short txn
        // ended while a long txn at older version is still alive).
        gc_pass(version_to_entries_, total_memory_bytes_,
                 [](const resolved_data_t& d) { return d.memory_bytes; });
        gc_pass(tables_by_version_, total_memory_bytes_,
                 [](const resolved_table_t& d) { return d.memory_bytes(); });
        gc_pass(functions_by_version_, total_memory_bytes_,
                 [](const resolved_function_t& d) { return d.memory_bytes(); });
        gc_pass(types_by_version_, total_memory_bytes_,
                 [](const resolved_type_t& d) { return d.memory_bytes(); });
        gc_pass(namespaces_by_version_, total_memory_bytes_,
                 [](const resolved_namespace_t& d) { return d.memory_bytes(); });
    }

    void versioned_plan_cache_t::clear() {
        version_to_entries_.clear();
        tables_by_version_.clear();
        functions_by_version_.clear();
        types_by_version_.clear();
        namespaces_by_version_.clear();
        total_memory_bytes_ = 0;
        // active_txns_ kept — sessions still hold their pins; next store() under their
        // version will repopulate. (Resetting them would force-abort live transactions.)
    }

    // ----- Per-name probe/store implementations -----
    // Pattern is identical for each object kind (table/function/type/namespace): probe
    // looks up version → key → entry and returns a pointer; store inserts under
    // alias-dedup, sets ref_count to match active_txns_ pinned at version, and runs
    // soft/hard memory checks. Helper templates avoid copy-paste.

    namespace {
        template<typename Map, typename Payload>
        Payload* probe_named(Map& by_version,
                             const std::string& key,
                             std::uint64_t version) {
            auto v_it = by_version.find(version);
            if (v_it == by_version.end()) return nullptr;
            auto e_it = v_it->second.find(key);
            if (e_it == v_it->second.end()) return nullptr;
            return &e_it->second.data;
        }
    } // namespace

    resolved_table_t* versioned_plan_cache_t::probe_table(components::catalog::oid_t namespace_oid,
                                                            std::string_view name,
                                                            std::uint64_t version) {
        return probe_named<decltype(tables_by_version_), resolved_table_t>(
            tables_by_version_, encode_table_key(namespace_oid, name), version);
    }

    void versioned_plan_cache_t::store_table(components::catalog::oid_t namespace_oid,
                                              std::string name,
                                              std::uint64_t version,
                                              resolved_table_t data) {
        auto key = encode_table_key(namespace_oid, name);
        auto& bucket = tables_by_version_[version];
        auto [it, inserted] = bucket.try_emplace(std::move(key));
        if (!inserted) return; // alias dedup
        const std::size_t bytes = data.memory_bytes();
        it->second.data = std::move(data);
        std::size_t pin_count = 0;
        for (const auto& [sid, ver] : active_txns_) {
            if (ver == version) ++pin_count;
        }
        it->second.ref_count = pin_count;
        total_memory_bytes_ += bytes;
        if (config_.gc_strategy == gc_strategy_t::lazy) {
            if (over_soft_memory_limit()) gc();
            enforce_hard_limits();
        }
        maybe_run_periodic_gc();
    }

    resolved_function_t* versioned_plan_cache_t::probe_function(
        std::string_view name,
        std::span<const components::catalog::oid_t> arg_type_oids,
        std::uint64_t version) {
        return probe_named<decltype(functions_by_version_), resolved_function_t>(
            functions_by_version_, encode_function_key(name, arg_type_oids), version);
    }

    void versioned_plan_cache_t::store_function(std::string name,
                                                  std::vector<components::catalog::oid_t> arg_type_oids,
                                                  std::uint64_t version,
                                                  resolved_function_t data) {
        auto key = encode_function_key(name, std::span<const components::catalog::oid_t>(arg_type_oids));
        auto& bucket = functions_by_version_[version];
        auto [it, inserted] = bucket.try_emplace(std::move(key));
        if (!inserted) return;
        const std::size_t bytes = data.memory_bytes();
        it->second.data = std::move(data);
        std::size_t pin_count = 0;
        for (const auto& [sid, ver] : active_txns_) {
            if (ver == version) ++pin_count;
        }
        it->second.ref_count = pin_count;
        total_memory_bytes_ += bytes;
        if (config_.gc_strategy == gc_strategy_t::lazy) {
            if (over_soft_memory_limit()) gc();
            enforce_hard_limits();
        }
        maybe_run_periodic_gc();
    }

    resolved_type_t* versioned_plan_cache_t::probe_type(components::catalog::oid_t namespace_oid,
                                                          std::string_view name,
                                                          std::uint64_t version) {
        return probe_named<decltype(types_by_version_), resolved_type_t>(
            types_by_version_, encode_type_key(namespace_oid, name), version);
    }

    void versioned_plan_cache_t::store_type(components::catalog::oid_t namespace_oid,
                                              std::string name,
                                              std::uint64_t version,
                                              resolved_type_t data) {
        auto key = encode_type_key(namespace_oid, name);
        auto& bucket = types_by_version_[version];
        auto [it, inserted] = bucket.try_emplace(std::move(key));
        if (!inserted) return;
        const std::size_t bytes = data.memory_bytes();
        it->second.data = std::move(data);
        std::size_t pin_count = 0;
        for (const auto& [sid, ver] : active_txns_) {
            if (ver == version) ++pin_count;
        }
        it->second.ref_count = pin_count;
        total_memory_bytes_ += bytes;
        if (config_.gc_strategy == gc_strategy_t::lazy) {
            if (over_soft_memory_limit()) gc();
            enforce_hard_limits();
        }
        maybe_run_periodic_gc();
    }

    resolved_namespace_t* versioned_plan_cache_t::probe_namespace(std::string_view name,
                                                                     std::uint64_t version) {
        return probe_named<decltype(namespaces_by_version_), resolved_namespace_t>(
            namespaces_by_version_, std::string(name), version);
    }

    void versioned_plan_cache_t::store_namespace(std::string name,
                                                   std::uint64_t version,
                                                   resolved_namespace_t data) {
        auto& bucket = namespaces_by_version_[version];
        auto [it, inserted] = bucket.try_emplace(std::move(name));
        if (!inserted) return;
        const std::size_t bytes = data.memory_bytes();
        it->second.data = std::move(data);
        std::size_t pin_count = 0;
        for (const auto& [sid, ver] : active_txns_) {
            if (ver == version) ++pin_count;
        }
        it->second.ref_count = pin_count;
        total_memory_bytes_ += bytes;
        if (config_.gc_strategy == gc_strategy_t::lazy) {
            if (over_soft_memory_limit()) gc();
            enforce_hard_limits();
        }
        maybe_run_periodic_gc();
    }

    std::size_t versioned_plan_cache_t::entry_count() const noexcept {
        std::size_t total = 0;
        for (const auto& [v, bucket] : version_to_entries_) {
            total += bucket.size();
        }
        return total;
    }

    bool versioned_plan_cache_t::has_entry(std::uint64_t plan_hash, std::uint64_t version) const noexcept {
        auto v_it = version_to_entries_.find(version);
        if (v_it == version_to_entries_.end())
            return false;
        return v_it->second.find(plan_hash) != v_it->second.end();
    }

    std::uint64_t versioned_plan_cache_t::min_active_version() const noexcept {
        if (active_txns_.empty()) {
            return std::numeric_limits<std::uint64_t>::max();
        }
        std::uint64_t m = std::numeric_limits<std::uint64_t>::max();
        for (const auto& [s, v] : active_txns_) {
            if (v < m)
                m = v;
        }
        return m;
    }

    namespace {
        template<typename Map>
        void incr_pass(Map& by_version, std::uint64_t version) noexcept {
            auto v_it = by_version.find(version);
            if (v_it == by_version.end()) return;
            for (auto& [k, entry] : v_it->second) {
                ++entry.ref_count;
            }
        }

        template<typename Map>
        void decr_pass(Map& by_version, std::uint64_t version) noexcept {
            auto v_it = by_version.find(version);
            if (v_it == by_version.end()) return;
            for (auto& [k, entry] : v_it->second) {
                if (entry.ref_count > 0) --entry.ref_count;
            }
        }
    } // namespace

    void versioned_plan_cache_t::incr_ref(std::uint64_t version) noexcept {
        incr_pass(version_to_entries_, version);
        incr_pass(tables_by_version_, version);
        incr_pass(functions_by_version_, version);
        incr_pass(types_by_version_, version);
        incr_pass(namespaces_by_version_, version);
    }

    void versioned_plan_cache_t::decr_ref(std::uint64_t version) noexcept {
        decr_pass(version_to_entries_, version);
        decr_pass(tables_by_version_, version);
        decr_pass(functions_by_version_, version);
        decr_pass(types_by_version_, version);
        decr_pass(namespaces_by_version_, version);
    }

    bool versioned_plan_cache_t::over_soft_memory_limit() const noexcept {
        if (config_.max_memory_bytes == 0) {
            return false;
        }
        const float ratio = (config_.gc_soft_limit_ratio <= 0.0f) ? 0.75f
                          : (config_.gc_soft_limit_ratio > 1.0f)  ? 1.0f
                                                                  : config_.gc_soft_limit_ratio;
        const auto threshold = static_cast<std::size_t>(
            static_cast<double>(config_.max_memory_bytes) * static_cast<double>(ratio));
        return total_memory_bytes_ > threshold;
    }

    void versioned_plan_cache_t::maybe_run_periodic_gc() noexcept {
        if (config_.gc_strategy != gc_strategy_t::periodic) {
            return;
        }
        if (config_.periodic_gc_ops_interval == 0) {
            return;
        }
        ++ops_since_periodic_gc_;
        if (ops_since_periodic_gc_ >= config_.periodic_gc_ops_interval) {
            ops_since_periodic_gc_ = 0;
            gc();
        }
    }

    namespace {
        // Generic eviction pass over a versioned bucket. Walks oldest version first,
        // evicts ref_count==0 entries until memory_check returns false. Stops when the
        // floor (min active version) is reached — pinned versions are protected.
        template<typename Map, typename MemFn, typename MemCheck>
        void evict_pass(Map& by_version,
                        std::uint64_t floor,
                        std::size_t& total_bytes,
                        MemFn mem,
                        MemCheck still_over_limit) {
            for (auto v_it = by_version.begin(); v_it != by_version.end();) {
                if (!still_over_limit()) break;
                if (v_it->first >= floor) break; // protect pinned versions
                for (auto e_it = v_it->second.begin(); e_it != v_it->second.end();) {
                    if (e_it->second.ref_count == 0) {
                        total_bytes -= mem(e_it->second.data);
                        e_it = v_it->second.erase(e_it);
                    } else {
                        ++e_it;
                    }
                }
                if (v_it->second.empty()) {
                    v_it = by_version.erase(v_it);
                } else {
                    ++v_it;
                }
            }
        }
    } // namespace

    void versioned_plan_cache_t::enforce_hard_limits() noexcept {
        // Evict from the oldest version forward across all bucket types until both the
        // version count and memory caps are satisfied. Order: plan-hash bucket first
        // (oldest semantic), then named buckets which back V4 lookups.
        const auto floor = min_active_version();
        auto over_limit = [this]() {
            const bool over_versions = version_to_entries_.size() > config_.max_data_versions;
            const bool over_memory = total_memory_bytes_ > config_.max_memory_bytes;
            return over_versions || over_memory;
        };
        evict_pass(version_to_entries_, floor, total_memory_bytes_,
                    [](const resolved_data_t& d) { return d.memory_bytes; }, over_limit);
        evict_pass(tables_by_version_, floor, total_memory_bytes_,
                    [](const resolved_table_t& d) { return d.memory_bytes(); }, over_limit);
        evict_pass(functions_by_version_, floor, total_memory_bytes_,
                    [](const resolved_function_t& d) { return d.memory_bytes(); }, over_limit);
        evict_pass(types_by_version_, floor, total_memory_bytes_,
                    [](const resolved_type_t& d) { return d.memory_bytes(); }, over_limit);
        evict_pass(namespaces_by_version_, floor, total_memory_bytes_,
                    [](const resolved_namespace_t& d) { return d.memory_bytes(); }, over_limit);
    }

} // namespace services::dispatcher
