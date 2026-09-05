#include "disk_hash_single_field_index.hpp"
#include "logical_value_binary_codec.hpp"

#include <components/table/row_version_manager.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace components::index {

    disk_hash_single_field_index_t::disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                                   catalog::oid_t oid,
                                                                   const keys_base_storage_t& keys,
                                                                   disk_hash_storage_ptr storage)
        : index_t(resource, logical_plan::index_type::hashed, oid, keys)
        , disk_table_(std::move(storage))
        , scratch_results_(resource)
        , pending_inserts_(resource)
        , pending_deletes_(resource) {}

    disk_hash_single_field_index_t::~disk_hash_single_field_index_t() = default;

    disk_hash_storage_t& disk_hash_single_field_index_t::storage_ref() const {
        assert(disk_table_ && "disk_hash_single_field_index requires disk storage");
        return *disk_table_;
    }

    value_t disk_hash_single_field_index_t::normalize_key(const value_t& key,
                                                          core::date::timezone_offset_t local_timezone) const {
        using namespace components::types;
        switch (key.type().type()) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT: {
                // Signed-integer widening can not fail for the types this switch admits;
                // still, never assert-then-value() (a failed cast in Release would deref an
                // empty optional). A non-widenable key keeps its native representation —
                // identical to the default branch, and self-consistent between insert and
                // find (both normalize the same way).
                auto casted = key.cast_as(complex_logical_type(logical_type::BIGINT), local_timezone);
                if (casted.has_error()) {
                    return key;
                }
                return std::move(casted.value());
            }
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT: {
                auto casted = key.cast_as(complex_logical_type(logical_type::UBIGINT), local_timezone);
                if (casted.has_error()) {
                    return key;
                }
                return std::move(casted.value());
            }
            default:
                return key;
        }
    }

    std::string disk_hash_single_field_index_t::encode_key(const value_t& key,
                                                           core::date::timezone_offset_t local_timezone) const {
        auto normalized = normalize_key(key, local_timezone);
        return codec::encode_disk_hash_key(normalized);
    }

    namespace {
        components::types::logical_value_t decode_key(std::pmr::memory_resource* resource, std::string_view encoded) {
            std::pmr::string payload(encoded.data(), encoded.size(), resource);
            size_t pos = 0;
            return codec::read_logical_value(resource, payload, pos);
        }
    } // namespace

    auto disk_hash_single_field_index_t::insert_impl(value_t, index_value_t, core::date::timezone_offset_t) -> void {}

    auto disk_hash_single_field_index_t::remove_impl(value_t, core::date::timezone_offset_t) -> void {}

    // NO LONGER THE PRODUCTION READ PATH. A SELECT reaches this index's committed rows
    // through its disk agent (index_agent_disk_t::find_rows, routed by
    // manager_index_t::search) and folds the txn-local half back in through
    // merge_uncommitted_rows_impl below. This override still exists because index_t
    // requires it and the component's own unit tests exercise it directly; it reads the
    // KEYDIR, which keeps one entry per key and therefore cannot see duplicates — the
    // very reason the read moved to the agent.
    index_t::range disk_hash_single_field_index_t::find_impl(const value_t& value,
                                                             core::date::timezone_offset_t local_timezone) const {
        scratch_results_.clear();
        const auto encoded = encode_key(value, local_timezone);
        auto values = storage_ref().get_all(encoded);
        for (const auto& v : values) {
            scratch_results_.emplace_back(v.value, 0, table::NOT_DELETED_ID);
        }
        for (const auto& [txn_id, rows] : pending_inserts_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (std::string_view(pending_key) == std::string_view(encoded)) {
                    scratch_results_.emplace_back(row_id, txn_id, table::NOT_DELETED_ID);
                }
            }
        }
        for (const auto& [txn_id, rows] : pending_deletes_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (std::string_view(pending_key) != std::string_view(encoded)) {
                    continue;
                }
                for (auto& entry : scratch_results_) {
                    if (entry.row_index == row_id && entry.delete_id == table::NOT_DELETED_ID) {
                        entry.delete_id = txn_id;
                        break;
                    }
                }
            }
        }
        return {iterator(new impl_t(scratch_results_.cbegin())), iterator(new impl_t(scratch_results_.cend()))};
    }

    // Unreachable by contract, and terminal rather than quiet. A hash bucket has no
    // ordering, so nothing but eq can be asked of this index: supports_ordered_probe()
    // answers false, manager_index_t refuses a range predicate on it before any read is
    // dispatched, and the planner refuses one earlier still (can_use_index picks a hashed
    // index only for an equality).
    //
    // What stood here raised the STRING LITERAL "not supported" as an exception, so a
    // caller could only have caught it as `catch (const char*)` -- and it was raised from
    // inside an actor coroutine whose unhandled_exception() is empty, so the exception was
    // swallowed and the statement reported success over zero rows. An empty range would be
    // the same wrong answer with the crash removed, which is why it is not the
    // replacement. Same shape as bitcask_index_disk_t::scan_range one level down.
    index_t::range disk_hash_single_field_index_t::lower_bound_impl(const value_t&,
                                                                    core::date::timezone_offset_t) const {
        assert(false && "disk_hash_single_field_index_t::lower_bound_impl: a hash index has no ordering");
        std::abort();
    }

    index_t::range disk_hash_single_field_index_t::upper_bound_impl(const value_t&,
                                                                    core::date::timezone_offset_t) const {
        assert(false && "disk_hash_single_field_index_t::upper_bound_impl: a hash index has no ordering");
        std::abort();
    }

    index_t::iterator disk_hash_single_field_index_t::cbegin_impl() const {
        return iterator(new impl_t(scratch_results_.cbegin()));
    }

    index_t::iterator disk_hash_single_field_index_t::cend_impl() const {
        return iterator(new impl_t(scratch_results_.cend()));
    }

    void disk_hash_single_field_index_t::insert_txn_impl(value_t key,
                                                         int64_t row_index,
                                                         uint64_t txn_id,
                                                         core::date::timezone_offset_t local_timezone) {
        auto encoded = encode_key(key, local_timezone);
        pending_inserts_[txn_id].emplace_back(std::pmr::string(encoded.data(), encoded.size(), resource()), row_index);
    }

    void disk_hash_single_field_index_t::mark_delete_impl(value_t key,
                                                          int64_t row_index,
                                                          uint64_t txn_id,
                                                          core::date::timezone_offset_t local_timezone) {
        auto encoded = encode_key(key, local_timezone);
        pending_deletes_[txn_id].emplace_back(std::pmr::string(encoded.data(), encoded.size(), resource()), row_index);
    }

    void disk_hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t) {
        pending_inserts_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t) {
        pending_deletes_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) { pending_inserts_.erase(txn_id); }

    // mark_delete_impl only records the pending bucket here (storage entries are
    // never stamped on the disk-hash variant), so reverting is a pure bucket
    // erase — symmetric with revert_insert_impl.
    void disk_hash_single_field_index_t::revert_delete_impl(uint64_t txn_id) { pending_deletes_.erase(txn_id); }

    void disk_hash_single_field_index_t::cleanup_versions_impl(uint64_t) {}

    index_t::pending_entries_t disk_hash_single_field_index_t::pending_inserts_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [encoded, row_id] : it->second) {
            out.push_back(pending_entry_t{decode_key(resource(), encoded), row_id});
        }
        return out;
    }

    index_t::pending_entries_t disk_hash_single_field_index_t::pending_deletes_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [encoded, row_id] : it->second) {
            out.push_back(pending_entry_t{decode_key(resource(), encoded), row_id});
        }
        return out;
    }

    void disk_hash_single_field_index_t::merge_uncommitted_rows_impl(const value_t& key,
                                                                     uint64_t txn_id,
                                                                     core::date::timezone_offset_t local_timezone,
                                                                     std::pmr::vector<int64_t>& rows) const {
        // `rows` already holds the committed half, read out of this index's disk agent.
        // Add what has not reached disk yet, and only what the ASKING transaction is
        // entitled to see.
        //
        // Two buckets, two map lookups -- not a walk of every pending transaction:
        //   bucket 0    committed for everyone but not yet mirrored to disk.
        //               repopulate_table refills it between its clear() fan-out and its
        //               closing commit_insert(0, 0), and each of those steps co_awaits,
        //               so a lookup CAN land in that window: the disk has been wiped and
        //               these entries are the only copy of the rebuilt index.
        //   bucket txn  this transaction's own uncommitted inserts and deletes.
        // Every other bucket belongs to a transaction that has not committed. It is
        // skipped because it is not looked up at all -- there is no stamp to compare and
        // no predicate to get wrong.
        //
        // Keys are compared ENCODED. The bucket holds the key exactly as encode_key
        // produced it on the way in, so encoding the probe the same way makes the
        // comparison byte-for-byte and, more importantly, applies the SAME
        // normalization (narrow ints widened to BIGINT/UBIGINT) to both sides. Decoding
        // the stored key back to a logical_value_t and comparing values would compare a
        // normalized key against an un-normalized probe.
        const auto encoded = encode_key(key, local_timezone);
        const std::string_view probe(encoded);

        const auto add_bucket = [&](uint64_t bucket) {
            auto it = pending_inserts_.find(bucket);
            if (it == pending_inserts_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (std::string_view(pending_key) == probe) {
                    rows.push_back(row_id);
                }
            }
        };
        const auto drop_bucket = [&](uint64_t bucket) {
            auto it = pending_deletes_.find(bucket);
            if (it == pending_deletes_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (std::string_view(pending_key) != probe) {
                    continue;
                }
                rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            }
        };

        // Inserts first, then deletes: a row this transaction inserted AND deleted must
        // end up absent, which only holds if the removal runs over the merged list.
        add_bucket(0);
        if (txn_id != 0) {
            add_bucket(txn_id);
        }
        drop_bucket(0);
        if (txn_id != 0) {
            drop_bucket(txn_id);
        }
    }

    void disk_hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        scratch_results_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
