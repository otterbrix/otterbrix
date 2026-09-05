#include "disk_hash_single_field_index.hpp"
#include "logical_value_binary_codec.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <string_view>

namespace components::index {

    namespace {

        // Narrow signed / unsigned integers widened to their 64-bit form, exactly as
        // bitcask_index_disk_t::key_bytes_for_hash does before hashing. Without it a
        // SMALLINT probe and the BIGINT-encoded key it should match hash to different
        // buckets, and the txn-local half of an answer would key differently from the
        // committed half.
        //
        // This is the HASHED family's own step and has no counterpart on the ordered
        // side, where the b+tree stores and compares the column's own type.
        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key,
                                                              core::date::timezone_offset_t local_timezone) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT: {
                    // Signed-integer widening can not fail for the types this switch
                    // admits; still, never assert-then-value() (a failed cast in Release
                    // would deref an empty optional). A non-widenable key keeps its native
                    // representation — identical to the default branch, and self-consistent
                    // between insert and probe (both normalize the same way).
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

        // Does the stored bucket key satisfy `compare` against the encoded probe?
        //
        // BYTE equality rather than value equality, and that is what makes the two halves
        // of one answer agree: the committed half comes out of a store that HASHES and
        // memcmps these exact bytes, so a probe that compares equal by value but differs
        // by byte (-0.0 against +0.0) would be found in the pending half and missed in the
        // committed one.
        //
        // It is also the only comparison this family can express at all. A hashed key may
        // be DECIMAL — is_representable_index_key_type admits it for hashed and refuses it
        // for ordered — and physical_value carries no DECIMAL tag, so the ordered facade's
        // decoder aborts on one. Comparing bytes never has to decode.
        //
        // The non-eq arm is this comparator's DOMAIN, not a re-check of the predicate. The
        // guard that keeps a range predicate away from an unordered index lives in
        // manager_index_t and nowhere else, deliberately: it asks supports_ordered_probe()
        // of EVERY index, so it also covers the in-memory hashed index, which had the same
        // hole and which a guard sitting here could not see (C2a). Duplicating it here
        // would invite deleting it there.
        bool key_satisfies(expressions::compare_type compare, std::string_view stored, std::string_view probe) {
            switch (compare) {
                case expressions::compare_type::eq:
                    return stored == probe;
                default:
                    // An unordered key family has no ordering to compare by, so there is
                    // no answer to give and a byte comparison would answer a range
                    // predicate with something that merely looks like one. Terminal at the
                    // bug site instead. Unconditional — std::abort() runs under NDEBUG.
                    assert(false && "disk_hash_single_field_index_t: a hashed key family compares only for equality");
                    std::abort();
            }
        }

    } // namespace

    disk_hash_single_field_index_t::disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                                   catalog::oid_t oid,
                                                                   const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::hashed, oid, keys)
        , pending_inserts_(resource)
        , pending_deletes_(resource) {}

    disk_hash_single_field_index_t::~disk_hash_single_field_index_t() = default;

    std::pmr::string disk_hash_single_field_index_t::encode_key(const value_t& key,
                                                                core::date::timezone_offset_t local_timezone) const {
        std::pmr::string out(resource());
        codec::append_logical_value(out, normalize_hash_key(key, local_timezone));
        return out;
    }

    // --- The doors this facade does not own -----------------------------------------
    //
    // Unreachable by contract, and terminal rather than quiet. This index keeps no
    // committed rows in memory at all: they are in the bitcask store its disk agent owns,
    // and the ONLY way to them is a message
    // (manager_index_t::search_with_preferred_type -> index_agent_disk_t::read_rows). A
    // no-op write would drop the row with nothing reporting it; an empty range would read
    // as "no row carries this key". Both are the silent wrong answer rule 6 forbids, so
    // both are a crash at the bug site instead.
    //
    // insert/remove (the non-txn pair) had ONE production caller, and removing it was part
    // of C2a/C2b: manager_index_t used to re-open the agent's backing from its own thread
    // and replay every entry into an in-memory twin. There is no twin now.
    //
    // find_impl is terminal HERE TOO, and that is C2c's one behavioural change on this
    // class. It used to read the KEYDIR through the handle C2c removed; its own comment
    // already conceded it was not the production read path, and with the handle gone the
    // only thing left for it to answer from would be the txn-local half — a SUBSET of the
    // rows the key carries, indistinguishable by the caller from the whole answer.
    //
    // lower_bound / upper_bound were terminal before that: what stood there raised the
    // STRING LITERAL "not supported" as an exception, catchable only as
    // `catch (const char*)`, from inside an actor coroutine whose unhandled_exception() is
    // empty — so it was swallowed and the statement reported success over zero rows.

    auto disk_hash_single_field_index_t::insert_impl(value_t, index_value_t, core::date::timezone_offset_t) -> void {
        assert(false && "disk_hash_single_field_index_t::insert_impl: committed rows live in the disk agent");
        std::abort();
    }

    auto disk_hash_single_field_index_t::remove_impl(value_t, core::date::timezone_offset_t) -> void {
        assert(false && "disk_hash_single_field_index_t::remove_impl: committed rows live in the disk agent");
        std::abort();
    }

    index_t::range disk_hash_single_field_index_t::find_impl(const value_t&, core::date::timezone_offset_t) const {
        assert(false && "disk_hash_single_field_index_t::find_impl: reads travel the agent's mailbox");
        std::abort();
    }

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
        assert(false && "disk_hash_single_field_index_t::cbegin_impl: there is no in-memory sequence to walk");
        std::abort();
    }

    index_t::iterator disk_hash_single_field_index_t::cend_impl() const {
        assert(false && "disk_hash_single_field_index_t::cend_impl: there is no in-memory sequence to walk");
        std::abort();
    }

    // --- The half that never reaches disk --------------------------------------------

    void disk_hash_single_field_index_t::insert_txn_impl(value_t key,
                                                         int64_t row_index,
                                                         uint64_t txn_id,
                                                         core::date::timezone_offset_t local_timezone) {
        pending_inserts_[txn_id].emplace_back(encode_key(key, local_timezone), row_index);
    }

    void disk_hash_single_field_index_t::mark_delete_impl(value_t key,
                                                          int64_t row_index,
                                                          uint64_t txn_id,
                                                          core::date::timezone_offset_t local_timezone) {
        pending_deletes_[txn_id].emplace_back(encode_key(key, local_timezone), row_index);
    }

    // Committing drops the bucket: the entries have just been mirrored into the bitcask
    // store by manager_index_t::commit_inserts / commit_deletes (it reads pending_*_impl
    // below BEFORE calling these), so keeping them would double every committed row.
    void disk_hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t) {
        pending_inserts_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t) {
        pending_deletes_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) { pending_inserts_.erase(txn_id); }

    // mark_delete_impl only records the bucket — nothing on disk was touched, because an
    // uncommitted delete is never mirrored — so reverting is a pure bucket erase,
    // symmetric with revert_insert_impl.
    void disk_hash_single_field_index_t::revert_delete_impl(uint64_t txn_id) { pending_deletes_.erase(txn_id); }

    // Nothing to do: version stamps (insert_id / delete_id) are an in-memory-index
    // concept. Here a committed row is simply in the bitcask store and an uncommitted one
    // is simply in a bucket, so there is no old version to reclaim.
    void disk_hash_single_field_index_t::cleanup_versions_impl(uint64_t) {}

    index_t::pending_entries_t disk_hash_single_field_index_t::pending_inserts_impl(uint64_t txn_id) const {
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

    index_t::pending_entries_t disk_hash_single_field_index_t::pending_deletes_impl(uint64_t txn_id) const {
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

    void disk_hash_single_field_index_t::merge_uncommitted_rows_impl(expressions::compare_type compare,
                                                                     const value_t& key,
                                                                     uint64_t txn_id,
                                                                     core::date::timezone_offset_t local_timezone,
                                                                     std::pmr::vector<int64_t>& rows) const {
        // `rows` already holds the committed half — the answer this index's disk agent
        // gave for this same predicate. Add what has not reached disk yet, and only what
        // the ASKING transaction is entitled to see.
        //
        // Two buckets, two map lookups — not a walk of every pending transaction:
        //   bucket 0    committed for everyone but not yet mirrored to disk.
        //               repopulate_table refills it between its clear() fan-out and its
        //               closing commit_insert(0, 0), and each of those steps co_awaits, so
        //               a lookup CAN land in that window: the store has just been wiped
        //               and these entries are the only copy of the rebuilt index.
        //   bucket txn  this transaction's own uncommitted inserts and deletes.
        // Every other bucket belongs to a transaction that has not committed. It is skipped
        // because it is not looked up at all — there is no stamp to compare and no
        // visibility predicate to get wrong.
        //
        // Keys are compared ENCODED. The bucket holds the key exactly as encode_key
        // produced it on the way in, so encoding the probe the same way makes the
        // comparison byte-for-byte and, more importantly, applies the SAME normalization
        // (narrow ints widened to BIGINT/UBIGINT) to both sides. Decoding the stored key
        // back to a logical_value_t and comparing values would compare a normalized key
        // against an un-normalized probe.
        const auto encoded_probe = encode_key(key, local_timezone);
        const std::string_view probe(encoded_probe);

        const auto add_bucket = [&](uint64_t bucket) {
            auto it = pending_inserts_.find(bucket);
            if (it == pending_inserts_.end()) {
                return;
            }
            for (const auto& [pending_key, row_id] : it->second) {
                if (key_satisfies(compare, pending_key, probe)) {
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
                if (!key_satisfies(compare, pending_key, probe)) {
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

    void disk_hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
