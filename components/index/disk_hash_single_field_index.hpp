#pragma once

#include "forward.hpp"
#include "index.hpp"

#include <cstdint>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::index {

    // The facade for a DISK-BACKED HASHED index — the engine behind `CREATE INDEX ...
    // USING hash`, which is the one spelling SQL has for it (everything else, including
    // no USING clause at all, is index_type::single and builds
    // disk_ordered_single_field_index_t instead).
    //
    // A SEPARATE CLASS from the ordered facade, and deliberately so. The two carry the
    // same method SET and share no code, because what they do inside those methods is
    // two different algorithms: a hashed key is NORMALIZED (narrow ints widened to
    // BIGINT / UBIGINT) before it is encoded and is compared BYTEWISE, an ordered key is
    // neither. One class holding both would have to ask what it is on every key
    // operation, and a class that branches on its own type is two classes glued
    // together. The duplication between the two files is the price of not having that
    // coupling, and it is the cheaper half.
    //
    // It holds NEITHER a handle NOR a path, and that is the point rather than an
    // omission. Its committed rows live in the bitcask store owned by this index's
    // index_agent_disk_t and are reachable only by sending that agent a message
    // (index_agent_disk_t::read_rows). Nothing here opens a file, so there is no
    // cross-actor state to share and no second owner of those files (rule 10) — the shape
    // this facade departed from until C2c, holding a keydir handle the manager created
    // for it and the agent then derived a second time from the same oids.
    //
    // What it DOES hold is the half of an answer that never reaches disk: this
    // transaction's own uncommitted inserts and deletes, in per-txn buckets
    // (owner decision 16: no write-through before commit). merge_uncommitted_rows_impl
    // folds that half back into the committed half the agent returned.
    //
    // Every read door below — find/lower_bound/upper_bound/cbegin/cend — and both
    // non-txn write doors are unreachable by contract and TERMINAL rather than quiet.
    // There is no local structure for them to answer from, so a no-op insert would
    // silently drop a row and an empty range would be indistinguishable from "no row
    // carries this key" (rule 6, and C6a's "a registered engine answering empty is
    // forbidden").
    class disk_hash_single_field_index_t final : public index_t {
    public:
        // A pending entry keeps its key ENCODED, in the record format the bitcask store
        // uses (codec::append_logical_value over the NORMALIZED key, byte-for-byte what
        // codec::encode_disk_hash_key produces for the same value). Two reasons, both
        // load-bearing:
        //   * the comparison that decides whether a pending key satisfies the probe must
        //     be the SAME comparison the committed half was answered by, and that one
        //     hashes and memcmps these exact bytes
        //     (bitcask_index_disk_t::key_bytes_for_hash);
        //   * a bucket key is stored ENCODED, so a merge performed anywhere but inside
        //     this facade would compare an encoded key against an un-encoded probe — the
        //     landmine C1 recorded, kept confined to one class.
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;

        disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                       catalog::oid_t oid,
                                       const keys_base_storage_t& keys);
        ~disk_hash_single_field_index_t() override;

    private:
        // A hash bucket has no ordering, so nothing but equality can be asked of this
        // index. This is NOT the guard that keeps a range off it: that guard lives in
        // manager_index_t, which asks THIS question of every index before dispatching a
        // read, so it also covers the in-memory hashed index — which had the same hole,
        // and which a guard sitting down here could never see (C2a).
        [[nodiscard]] bool supports_ordered_probe_impl() const noexcept final { return false; }

        // The committed half comes back from this index's disk agent; only the txn-local
        // half is here. Answered by the class rather than guessed by the caller from
        // is_disk() or from type(), for the reason index_disk_t::has_txn_log() is asked
        // the same way: a guess is wrong the day a new implementation appears.
        [[nodiscard]] bool reads_through_disk_agent_impl() const noexcept final { return true; }

        auto insert_impl(value_t, index_value_t, core::date::timezone_offset_t local_timezone) -> void final;
        auto remove_impl(value_t, core::date::timezone_offset_t local_timezone) -> void final;
        range find_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
        range lower_bound_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
        range upper_bound_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
        iterator cbegin_impl() const final;
        iterator cend_impl() const final;

        void insert_txn_impl(value_t key,
                             int64_t row_index,
                             uint64_t txn_id,
                             core::date::timezone_offset_t local_timezone) final;
        void mark_delete_impl(value_t key,
                              int64_t row_index,
                              uint64_t txn_id,
                              core::date::timezone_offset_t local_timezone) final;
        void commit_insert_impl(uint64_t txn_id, uint64_t commit_id) final;
        void commit_delete_impl(uint64_t txn_id, uint64_t commit_id) final;
        void revert_insert_impl(uint64_t txn_id) final;
        void revert_delete_impl(uint64_t txn_id) final;
        void cleanup_versions_impl(uint64_t lowest_active) final;
        index_t::pending_entries_t pending_inserts_impl(uint64_t txn_id) const final;
        index_t::pending_entries_t pending_deletes_impl(uint64_t txn_id) const final;
        void merge_uncommitted_rows_impl(expressions::compare_type compare,
                                         const value_t& key,
                                         uint64_t txn_id,
                                         core::date::timezone_offset_t local_timezone,
                                         std::pmr::vector<int64_t>& rows) const final;
        void clean_memory_to_new_elements_impl(std::size_t count) final;

        // key -> the bitcask store's key bytes, NORMALIZED first. The row id is NOT
        // appended: a bucket carries it beside the key, and the merge needs the key alone.
        std::pmr::string encode_key(const value_t& key, core::date::timezone_offset_t local_timezone) const;

        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;
    };

} // namespace components::index
