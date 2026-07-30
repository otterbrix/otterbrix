#include "single_field_index.hpp"

#include <algorithm>
#include <cassert>
#include <optional>

namespace components::index {

    single_field_index_t::single_field_index_t(std::pmr::memory_resource* resource,
                                               std::string name,
                                               const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::single, std::move(name), keys)
        , storage_(resource) {}

    single_field_index_t::~single_field_index_t() = default;

    index_t::iterator::reference single_field_index_t::impl_t::value_ref() const { return iterator_->second; }
    index_t::iterator_t::iterator_impl_t* single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }

    // A btree position is only ever equal to another btree position: the tag check rejects a null
    // or foreign body BEFORE the downcast.
    bool single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        const auto* rhs = same_kind_as<impl_t>(other);
        return rhs != nullptr && iterator_ == rhs->iterator_;
    }

    bool single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        const auto* rhs = same_kind_as<impl_t>(other);
        return rhs == nullptr || iterator_ != rhs->iterator_;
    }

    index_t::iterator::iterator_impl_t* single_field_index_t::impl_t::copy() const { return new impl_t(*this); }

    single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_impl_t(iterator_kind)
        , iterator_(iterator) {}

    // The btree holds keys of exactly ONE type: stored_type_ locks to the first inserted key's
    // type, and the comparator (std::less<logical_value_t>) requires both operands to carry that
    // type — a mixed tree is not a strict weak ordering. Rebuilding the index when a
    // dynamic-schema column's type evolves is the caller's responsibility (the CREATE INDEX
    // relkind='g' validation contract). So every key that enters or probes the tree is first
    // normalised into stored_type_, and a key that can not be is OUT-OF-DOMAIN.
    //
    // "Can not be" is NOT "cast_as reported an error". cast_as is a SQL CAST, not a domain check:
    // STRING -> BIGINT runs std::atoll, which maps every non-numeric string to 0 and reports
    // success — trusting it indexes 'hello' and 'world' both under the INVENTED key 0, and an
    // equality probe (which collapses the same way) then matches rows that do not satisfy the
    // predicate; index_scan carries no operator_match above it to catch the invention.
    //
    // in_domain() therefore accepts a conversion only when it is LOSSLESS: casting the result back
    // to the source type must reproduce the source value. Widening (INTEGER 5 -> BIGINT 5) passes;
    // 'hello' -> 0 does not, because 0 -> '0' is not 'hello'. The check costs nothing on the hot
    // path — a key already of stored_type_ short-circuits before any cast.
    //
    // The index_t maintenance API is void, so the defined no-exception semantics are:
    //   * writes: an out-of-domain key stays un-indexed (insert/insert_txn); remove / mark_delete
    //     on one is an exact no-op (it was never stored);
    //   * probes: an out-of-domain probe orders AFTER every in-domain key (type bracketing), so
    //     its equal-range and both bounds sit at cend() — eq/gt/gte are empty, lt/lte cover the
    //     in-domain keys, and in-domain lookups stay exact.
    //
    // A rejected key leaves the index INCOMPLETE, never wrong. Completeness is the planner's
    // side of the contract: context_storage_t::has_index_on only offers this index for a predicate
    // whose key is the one it was built on, so a '::?'-selected type-variant of an evolved field
    // is answered by a full scan instead of by an index that cannot represent it.
    //
    // Never assert-then-value(): a failed cast in Release would dereference an empty optional
    // (UB, garbage key, silent index corruption).

    std::optional<value_t> single_field_index_t::in_domain(const value_t& key,
                                                           core::date::timezone_offset_t local_timezone) const {
        // The shape question: the index stores VALUES, and a value carries no column name.
        if (key.type() == stored_type_) {
            return key; // already the key domain's type — nothing to convert, nothing to verify
        }
        auto casted = key.cast_as(stored_type_, local_timezone);
        if (casted.has_error()) {
            return std::nullopt;
        }
        // Round-trip: only a conversion that survives coming back is faithful enough to index by.
        auto restored = casted.value().cast_as(key.type(), local_timezone);
        if (restored.has_error() || !(restored.value() == key)) {
            return std::nullopt;
        }
        return std::move(casted.value());
    }

    auto single_field_index_t::insert_impl(value_t key,
                                           index_value_t value,
                                           core::date::timezone_offset_t local_timezone) -> void {
        if (stored_type_.type() == types::logical_type::NA) {
            stored_type_ = key.type();
        }
        auto casted = in_domain(key, local_timezone);
        if (!casted) {
            return; // out-of-domain key: not representable in this index (see note above)
        }
        storage_.insert({std::move(*casted), std::move(value)});
    }

    auto single_field_index_t::remove_impl(components::index::value_t key, core::date::timezone_offset_t local_timezone)
        -> void {
        auto casted = in_domain(key, local_timezone);
        if (!casted) {
            return; // out-of-domain key: never stored, nothing to erase
        }
        auto it = storage_.find(*casted);
        if (it != storage_.end()) {
            storage_.erase(it);
        }
    }

    index_t::range single_field_index_t::find_impl(const value_t& value,
                                                   core::date::timezone_offset_t local_timezone) const {
        auto casted = in_domain(value, local_timezone);
        if (!casted) {
            // out-of-domain probe orders after every in-domain key: empty range at cend()
            return std::make_pair(iterator(new impl_t(storage_.cend())), iterator(new impl_t(storage_.cend())));
        }
        auto range = storage_.equal_range(*casted);
        return std::make_pair(iterator(new impl_t(range.first)), iterator(new impl_t(range.second)));
    }

    index_t::range single_field_index_t::lower_bound_impl(const value_t& value,
                                                          core::date::timezone_offset_t local_timezone) const {
        auto casted = in_domain(value, local_timezone);
        // out-of-domain probe orders after every in-domain key -> its bound is cend()
        auto it = casted ? storage_.lower_bound(*casted) : storage_.cend();
        return std::make_pair(cbegin(), index_t::iterator(new impl_t(it)));
    }

    index_t::range single_field_index_t::upper_bound_impl(const value_t& value,
                                                          core::date::timezone_offset_t local_timezone) const {
        auto casted = in_domain(value, local_timezone);
        // out-of-domain probe orders after every in-domain key -> its bound is cend()
        auto it = casted ? storage_.upper_bound(*casted) : storage_.cend();
        return std::make_pair(index_t::iterator(new impl_t(it)), cend());
    }

    index_t::iterator single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(storage_.cbegin()));
    }

    index_t::iterator single_field_index_t::cend_impl() const { return index_t::iterator(new impl_t(storage_.cend())); }

    void single_field_index_t::insert_txn_impl(value_t key,
                                               int64_t row_index,
                                               uint64_t txn_id,
                                               core::date::timezone_offset_t local_timezone) {
        index_value_t val(row_index, txn_id, table::NOT_DELETED_ID);
        if (stored_type_.type() == types::logical_type::NA) {
            stored_type_ = key.type();
        }
        auto casted = in_domain(key, local_timezone);
        if (!casted) {
            return; // out-of-domain key: not representable; pending/storage stay in lockstep
        }
        auto casted_key = std::move(*casted);
        pending_inserts_[txn_id].emplace_back(casted_key, row_index);
        storage_.insert({std::move(casted_key), std::move(val)});
    }

    void single_field_index_t::mark_delete_impl(value_t key,
                                                int64_t row_index,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t local_timezone) {
        auto casted = in_domain(key, local_timezone);
        if (!casted) {
            return; // out-of-domain key: never stored, nothing to mark deleted
        }
        auto casted_key = std::move(*casted);
        auto range = storage_.equal_range(casted_key);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second.row_index == row_index && it->second.delete_id == table::NOT_DELETED_ID) {
                it->second.delete_id = txn_id;
                pending_deletes_[txn_id].emplace_back(casted_key, row_index);
                return;
            }
        }
    }

    void single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t commit_id) {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        // Group by key: sort this txn's pending entries (erased right after) so equal_range is
        // scanned ONCE per distinct key, flipping every entry still tagged insert_id==txn_id —
        // exactly this txn's inserts for that key (insert_txn_impl records pending and storage_
        // in lockstep). Avoids the per-entry rescans that made a low-cardinality bulk commit
        // O(rows^2); now O(rows log rows).
        auto& entries = it->second;
        comparator_t less;
        std::sort(entries.begin(), entries.end(), [&less](const pending_entry& a, const pending_entry& b) {
            return less(a.first, b.first);
        });
        for (std::size_t i = 0; i < entries.size(); ++i) {
            if (i > 0 && !less(entries[i - 1].first, entries[i].first)) {
                continue; // same key as the previous entry (sorted ascending)
            }
            auto range = storage_.equal_range(entries[i].first);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.insert_id == txn_id) {
                    sit->second.insert_id = commit_id;
                }
            }
        }
        pending_inserts_.erase(it);
    }

    void single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t commit_id) {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        // Group by key (see commit_insert_impl): scan equal_range ONCE per distinct key,
        // flipping every entry still tagged delete_id==txn_id. O(rows^2) -> O(rows log rows).
        auto& entries = it->second;
        comparator_t less;
        std::sort(entries.begin(), entries.end(), [&less](const pending_entry& a, const pending_entry& b) {
            return less(a.first, b.first);
        });
        for (std::size_t i = 0; i < entries.size(); ++i) {
            if (i > 0 && !less(entries[i - 1].first, entries[i].first)) {
                continue; // same key as the previous entry (sorted ascending)
            }
            auto range = storage_.equal_range(entries[i].first);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.delete_id == txn_id) {
                    sit->second.delete_id = commit_id;
                }
            }
        }
        pending_deletes_.erase(it);
    }

    void single_field_index_t::revert_insert_impl(uint64_t txn_id) {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.insert_id == txn_id) {
                    storage_.erase(sit);
                    break;
                }
            }
        }
        pending_inserts_.erase(it);
    }

    void single_field_index_t::revert_delete_impl(uint64_t txn_id) {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.delete_id == txn_id) {
                    sit->second.delete_id = table::NOT_DELETED_ID;
                    break;
                }
            }
        }
        pending_deletes_.erase(it);
    }

    void single_field_index_t::cleanup_versions_impl(uint64_t lowest_active) {
        for (auto it = storage_.begin(); it != storage_.end();) {
            if (it->second.delete_id < lowest_active && it->second.delete_id < table::TRANSACTION_ID_START) {
                it = storage_.erase(it);
            } else {
                ++it;
            }
        }
        // Also clean up any stale pending entries for committed txns
        for (auto it = pending_deletes_.begin(); it != pending_deletes_.end();) {
            if (it->first < lowest_active && it->first < table::TRANSACTION_ID_START) {
                it = pending_deletes_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void
    single_field_index_t::for_each_pending_insert_impl(uint64_t txn_id,
                                                       const std::function<void(const value_t&, int64_t)>& fn) const {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            fn(key, row_index);
        }
    }

    void
    single_field_index_t::for_each_pending_delete_impl(uint64_t txn_id,
                                                       const std::function<void(const value_t&, int64_t)>& fn) const {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            fn(key, row_index);
        }
    }

    void single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        storage_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
