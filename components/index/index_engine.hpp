#pragma once

#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <scoped_allocator>
#include <string>
#include <utility>
#include <vector>

#include "forward.hpp"
#include "index.hpp"
#include <core/pmr.hpp>

namespace components::vector {
    class data_chunk_t;
}
namespace components::index {

#ifdef DEV_MODE
    // Test-observable count of CHUNK COLUMN INSPECTIONS performed while matching an index key to
    // a column. Resolved once per chunk this is O(chunks); a regression back to matching per row
    // shows up as O(rows).
    uint64_t index_key_column_probes() noexcept;
    void reset_index_key_column_probes() noexcept;
#endif

    constexpr uint32_t INDEX_ID_UNDEFINED = std::numeric_limits<uint32_t>::max();

    struct index_engine_t final {
    public:
        explicit index_engine_t(std::pmr::memory_resource* resource);
        auto matching(id_index id) -> index_t::pointer;
        auto matching(const keys_base_storage_t& query) -> index_t::pointer;
        auto matching(const keys_base_storage_t& query, index_type type) -> index_t::pointer;
        auto matching(const actor_zeta::address_t& address) -> index_t::pointer;
        // Lookup by pg_index.indexrelid — the index's only identity below the
        // planner (rule 16). Named (not an overload of matching) because
        // catalog::oid_t and id_index are both uint32_t.
        auto matching_relid(catalog::oid_t relid) -> index_t::pointer;
        auto has_index(catalog::oid_t relid) -> bool;
        auto add_index(const keys_base_storage_t&, index_ptr) -> uint32_t;
        auto add_disk_agent(id_index id, actor_zeta::address_t address) -> void;
        auto drop_index(index_t::pointer index) -> void;
        auto size() const -> std::size_t;
        std::pmr::memory_resource* resource() noexcept;

        // Which chunk column carries each index's key. Entry j corresponds to the j-th index in
        // iteration order; key_column_absent means this chunk does not carry that index's key and
        // the index is skipped.
        //
        // Resolved ONCE per chunk: the answer is a property of the chunk's column layout, not of
        // a row.
        using chunk_key_binding_t = std::pmr::vector<std::size_t>;
        static constexpr std::size_t key_column_absent = std::numeric_limits<std::size_t>::max();

        [[nodiscard]] chunk_key_binding_t bind_chunk(const vector::data_chunk_t& chunk) const;

        void insert_row(const chunk_key_binding_t& binding,
                        const vector::data_chunk_t& chunk,
                        size_t chunk_row,
                        int64_t storage_row,
                        uint64_t txn_id,
                        core::date::timezone_offset_t local_timezone);
        void mark_delete_row(const chunk_key_binding_t& binding,
                             const vector::data_chunk_t& chunk,
                             size_t chunk_row,
                             int64_t storage_row,
                             uint64_t txn_id,
                             core::date::timezone_offset_t local_timezone);
        void commit_insert(uint64_t txn_id, uint64_t commit_id);
        void commit_delete(uint64_t txn_id, uint64_t commit_id);
        void revert_insert(uint64_t txn_id);
        void revert_delete(uint64_t txn_id);
        void cleanup_versions(uint64_t lowest_active);

        // indexrelids of every index in engine iteration order.
        auto indexes() -> std::pmr::vector<catalog::oid_t>;
        auto all_indexed_keys() const -> std::pmr::vector<keys_base_storage_t>;
        auto all_indexed_descriptions() const -> std::pmr::vector<index_description_t>;

        // Mirror pending txn entries to disk agents (call BEFORE commit clears pending state).
        template<typename Fn>
        void for_each_pending_disk_insert(uint64_t txn_id, Fn&& fn) const {
            for (const auto& index : storage_) {
                if (!index->is_disk()) {
                    continue;
                }
                for (const auto& entry : index->pending_inserts(txn_id)) {
                    fn(index->disk_agent(), entry.key, entry.row_index);
                }
            }
        }

        template<typename Fn>
        void for_each_pending_disk_delete(uint64_t txn_id, Fn&& fn) const {
            for (const auto& index : storage_) {
                if (!index->is_disk()) {
                    continue;
                }
                for (const auto& entry : index->pending_deletes(txn_id)) {
                    fn(index->disk_agent(), entry.key, entry.row_index);
                }
            }
        }

    private:
        using comparator_t = std::less<keys_base_storage_t>;
        using base_storage = std::pmr::list<index_ptr>;

        using keys_to_doc_t = std::pmr::map<keys_base_storage_t, index_t::pointer, comparator_t>;
        using index_to_doc_t = std::pmr::unordered_map<id_index, index_t::pointer>;
        using index_to_address_t = std::pmr::map<actor_zeta::address_t, index_t::pointer>;
        using relid_to_index_t = std::pmr::unordered_map<catalog::oid_t, index_t::pointer>;

        std::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        index_to_address_t index_to_address_;
        relid_to_index_t relid_to_index_;
        base_storage storage_;
    };

    using index_engine_ptr = core::pmr::unique_ptr<index_engine_t>;

    auto make_index_engine(std::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t& query) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const actor_zeta::address_t& address) -> index_t::pointer;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, catalog::oid_t relid, const keys_base_storage_t& keys, Args&&... args)
        -> uint32_t {
        return ptr->add_index(keys,
                              core::pmr::make_polymorphic_unique<Target>(ptr->resource(),
                                                                         relid,
                                                                         keys,
                                                                         std::forward<Args>(args)...));
    }

    void drop_index(const index_engine_ptr& ptr, index_t::pointer index);

    void find(const index_engine_ptr& index, id_index id, result_set_t*);
    void find(const index_engine_ptr& index, query_t query, result_set_t*);

    void set_disk_agent(const index_engine_ptr& ptr,
                        id_index id,
                        actor_zeta::address_t agent,
                        actor_zeta::address_t manager);

} // namespace components::index
