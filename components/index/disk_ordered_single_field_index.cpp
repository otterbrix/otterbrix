#include "disk_ordered_single_field_index.hpp"
#include "logical_value_binary_codec.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace components::index {

    namespace {

        // A pending key, decoded into the value the on-disk b+tree orders by.
        //
        // This is not "a" decoder, it is THE one: services::index::item_key_getter hands
        // the tree its own stored keys through this exact call, so a bucket key and a
        // committed key become the same kind of value, compared by the same operators.
        // The b+tree's probe encoder services::index::convert() produces the identical
        // physical_value for a given logical key, which is why encoding the probe here
        // and decoding it straight back (see merge_uncommitted_rows_impl) is faithful
        // rather than a round trip for its own sake.
        //
        // A STRING physical_value is a VIEW into `encoded`; every use below keeps the
        // owning buffer alive for the whole comparison.
        components::types::physical_value decode_as_tree_key(std::string_view encoded) {
            size_t pos = 0;
            return codec::read_logical_value_as_view(encoded.data(), encoded.size(), pos);
        }

        // Does `stored <compare> probe` hold? The six value comparisons an index can be
        // asked, answered on the same operators btree_index_disk_t::scan_range walks the
        // tree with, so the txn-local half of an answer and its committed half agree by
        // construction instead of by coincidence.
        //
        // ALL SIX, which is what separates this family from the hashed one: an ordered
        // index may be asked lt/lte/gt/gte/ne, and a pending row with key 3 belongs in
        // the answer to `x < 5`.
        bool predicate_holds(expressions::compare_type compare,
                             const components::types::physical_value& stored,
                             const components::types::physical_value& probe) {
            switch (compare) {
                case expressions::compare_type::eq:
                    return stored == probe;
                case expressions::compare_type::ne:
                    return stored != probe;
                case expressions::compare_type::lt:
                    return stored < probe;
                case expressions::compare_type::lte:
                    return stored <= probe;
                case expressions::compare_type::gt:
                    return stored > probe;
                case expressions::compare_type::gte:
                    return stored >= probe;
                default:
                    // Only the six value comparisons reach an index: the planner routes
                    // nothing else (can_use_index), and manager_index_t refuses a range on
                    // an unordered backend before the read is dispatched. Anything else is
                    // a routing bug, and answering "does not match" would hide it as a
                    // silently short result. Unconditional — std::abort() runs under NDEBUG.
                    assert(false && "disk_ordered_single_field_index_t: predicate is not a value comparison");
                    std::abort();
            }
        }

    } // namespace

    disk_ordered_single_field_index_t::disk_ordered_single_field_index_t(std::pmr::memory_resource* resource,
                                                                         catalog::oid_t oid,
                                                                         const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::single, oid, keys)
        , pending_inserts_(resource)
        , pending_deletes_(resource) {}

    disk_ordered_single_field_index_t::~disk_ordered_single_field_index_t() = default;

    std::pmr::string disk_ordered_single_field_index_t::encode_key(const value_t& key) const {
        std::pmr::string out(resource());
        codec::append_logical_value(out, key);
        return out;
    }

    // --- The doors this facade does not own -----------------------------------------
    //
    // Unreachable by contract, and terminal rather than quiet. This index keeps no
    // committed rows in memory at all: they are in the b+tree its disk agent owns, and
    // the ONLY way to them is a message (manager_index_t::search_with_preferred_type ->
    // index_agent_disk_t::read_rows). A no-op write would drop the row with nothing
    // reporting it; an empty range would read as "no row carries this key". Both are the
    // silent wrong answer rule 6 forbids, so both are a crash at the bug site instead.
    //
    // insert/remove (the non-txn pair) had ONE production caller, and removing it is part
    // of the same change: manager_index_t used to re-open the agent's b+tree from its own
    // thread and replay every entry into an in-memory twin. There is no twin now.

    auto disk_ordered_single_field_index_t::insert_impl(value_t, index_value_t, core::date::timezone_offset_t) -> void {
        assert(false && "disk_ordered_single_field_index_t::insert_impl: committed rows live in the disk agent");
        std::abort();
    }

    auto disk_ordered_single_field_index_t::remove_impl(value_t, core::date::timezone_offset_t) -> void {
        assert(false && "disk_ordered_single_field_index_t::remove_impl: committed rows live in the disk agent");
        std::abort();
    }

    index_t::range disk_ordered_single_field_index_t::find_impl(const value_t&, core::date::timezone_offset_t) const {
        assert(false && "disk_ordered_single_field_index_t::find_impl: reads travel the agent's mailbox");
        std::abort();
    }

    index_t::range disk_ordered_single_field_index_t::lower_bound_impl(const value_t&,
                                                                       core::date::timezone_offset_t) const {
        assert(false && "disk_ordered_single_field_index_t::lower_bound_impl: reads travel the agent's mailbox");
        std::abort();
    }

    index_t::range disk_ordered_single_field_index_t::upper_bound_impl(const value_t&,
                                                                       core::date::timezone_offset_t) const {
        assert(false && "disk_ordered_single_field_index_t::upper_bound_impl: reads travel the agent's mailbox");
        std::abort();
    }

    index_t::iterator disk_ordered_single_field_index_t::cbegin_impl() const {
        assert(false && "disk_ordered_single_field_index_t::cbegin_impl: there is no in-memory sequence to walk");
        std::abort();
    }

    index_t::iterator disk_ordered_single_field_index_t::cend_impl() const {
        assert(false && "disk_ordered_single_field_index_t::cend_impl: there is no in-memory sequence to walk");
        std::abort();
    }

    // --- The half that never reaches disk --------------------------------------------

    void disk_ordered_single_field_index_t::insert_txn_impl(value_t key,
                                                            int64_t row_index,
                                                            uint64_t txn_id,
                                                            core::date::timezone_offset_t) {
        pending_inserts_[txn_id].emplace_back(encode_key(key), row_index);
    }

    void disk_ordered_single_field_index_t::mark_delete_impl(value_t key,
                                                             int64_t row_index,
                                                             uint64_t txn_id,
                                                             core::date::timezone_offset_t) {
        pending_deletes_[txn_id].emplace_back(encode_key(key), row_index);
    }

    // Committing drops the bucket: the entries have just been mirrored into the b+tree by
    // manager_index_t::commit_inserts / commit_deletes (it reads pending_*_impl below
    // BEFORE calling these), so keeping them would double every committed row.
    void disk_ordered_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t) {
        pending_inserts_.erase(txn_id);
    }

    void disk_ordered_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t) {
        pending_deletes_.erase(txn_id);
    }

    void disk_ordered_single_field_index_t::revert_insert_impl(uint64_t txn_id) { pending_inserts_.erase(txn_id); }

    // mark_delete_impl only records the bucket — nothing on disk was touched, because an
    // uncommitted delete is never mirrored — so reverting is a pure bucket erase,
    // symmetric with revert_insert_impl.
    void disk_ordered_single_field_index_t::revert_delete_impl(uint64_t txn_id) { pending_deletes_.erase(txn_id); }

    // Nothing to do: version stamps (insert_id / delete_id) are an in-memory-index
    // concept. Here a committed row is simply in the tree and an uncommitted one is
    // simply in a bucket, so there is no old version to reclaim.
    void disk_ordered_single_field_index_t::cleanup_versions_impl(uint64_t) {}

    index_t::pending_entries_t disk_ordered_single_field_index_t::pending_inserts_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [encoded, row_id] : it->second) {
            size_t pos = 0;
            out.push_back(pending_entry_t{codec::read_logical_value(resource(), encoded, pos), row_id});
        }
        return out;
    }

    index_t::pending_entries_t disk_ordered_single_field_index_t::pending_deletes_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [encoded, row_id] : it->second) {
            size_t pos = 0;
            out.push_back(pending_entry_t{codec::read_logical_value(resource(), encoded, pos), row_id});
        }
        return out;
    }

    void disk_ordered_single_field_index_t::merge_uncommitted_rows_impl(expressions::compare_type compare,
                                                                        const value_t& key,
                                                                        uint64_t txn_id,
                                                                        core::date::timezone_offset_t,
                                                                        std::pmr::vector<int64_t>& rows) const {
        // `rows` already holds the committed half — btree_index_disk_t::scan_range's answer
        // for this same predicate, read out of this index's disk agent. Add what has not
        // reached disk yet, and only what the ASKING transaction is entitled to see.
        //
        // Two buckets, two map lookups — not a walk of every pending transaction:
        //   bucket 0    committed for everyone but not yet mirrored to disk.
        //               repopulate_table refills it between its clear() fan-out and its
        //               closing commit_insert(0, 0), and each of those steps co_awaits, so
        //               a lookup CAN land in that window: the tree has just been wiped and
        //               these entries are the only copy of the rebuilt index.
        //   bucket txn  this transaction's own uncommitted inserts and deletes.
        // Every other bucket belongs to a transaction that has not committed. It is skipped
        // because it is not looked up at all — there is no stamp to compare and no
        // visibility predicate to get wrong.
        //
        // WHAT MAKES THIS DIFFERENT FROM THE HASHED FACADE'S MERGE: the predicate, and the
        // comparison domain. A hash bucket can only ever be asked `= k`, and answers it on
        // the encoded BYTES. Here the same call may carry lt/lte/gt/gte/ne, so the probe is
        // encoded and decoded back into the tree's own key value and each bucket key is
        // decoded the same way — the comparison then runs on the operators the tree itself
        // uses. No normalization on either side: the tree stores and compares the column's
        // own type.
        const auto encoded_probe = encode_key(key);
        const auto probe = decode_as_tree_key(encoded_probe);

        const auto add_bucket = [&](uint64_t bucket) {
            auto it = pending_inserts_.find(bucket);
            if (it == pending_inserts_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (predicate_holds(compare, decode_as_tree_key(pending_key), probe)) {
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
                // The key test is not redundant with the row-id erase: a row whose key does
                // NOT satisfy the predicate was never in the committed half, so testing
                // first keeps the erase from scanning `rows` for an id that cannot be there.
                if (!predicate_holds(compare, decode_as_tree_key(pending_key), probe)) {
                    continue;
                }
                rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            }
        };

        // Inserts first, then deletes: a row this transaction inserted AND deleted must end
        // up absent, which only holds if the removal runs over the merged list.
        add_bucket(0);
        if (txn_id != 0) {
            add_bucket(txn_id);
        }
        drop_bucket(0);
        if (txn_id != 0) {
            drop_bucket(txn_id);
        }
    }

    void disk_ordered_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
