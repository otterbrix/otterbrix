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
    // Test-observable count of CHUNK COLUMN INSPECTIONS performed while matching an index key
    // to a column. Each inspection compared a column alias against a freshly built std::string,
    // and it ran per index PER ROW — twice, once to check the key is present and once to read
    // it. Resolved once per chunk instead, this drops from O(rows) to O(chunks).
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
        auto matching(const std::string& name) -> index_t::pointer;
        auto has_index(const std::string& name)
            -> bool; // TODO figure out how to make it faster (not using matching inside)
        auto add_index(const keys_base_storage_t&, index_ptr) -> uint32_t;
        auto add_disk_agent(id_index id, actor_zeta::address_t address) -> void;
        auto drop_index(index_t::pointer index) -> void;
        auto size() const -> std::size_t;
        std::pmr::memory_resource* resource() noexcept;

        // Which chunk column carries each index's key. Entry j corresponds to the j-th index in
        // iteration order; key_column_absent means this chunk does not carry that index's key
        // (the index is then skipped, exactly as the old per-row presence check did).
        //
        // Resolved ONCE per chunk: the answer is a property of the chunk's column layout, not of
        // a row, and computing it per row meant walking the columns and building a std::string
        // per comparison for every row.
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

        auto indexes() -> std::vector<std::string>;
        auto all_indexed_keys() const -> std::pmr::vector<keys_base_storage_t>;
        auto all_indexed_descriptions() const -> std::pmr::vector<index_description_t>;

        // Call fn(disk_agent_address, key_value) for each disk-backed index matching chunk columns.
        //
        // TEMPLATE, not std::function: these are ordinary (non-virtual) methods, so the callable
        // can stay a type parameter — no erasure, no heap allocation for a capturing lambda, and
        // the call is inlinable. std::function is forbidden by the project rules; a function
        // pointer would erase just as much.
        template<typename Fn>
        void for_each_disk_op(const chunk_key_binding_t& binding,
                              const vector::data_chunk_t& chunk,
                              size_t row,
                              Fn&& fn) const {
            std::size_t slot = 0;
            for (const auto& index : storage_) {
                const auto column = slot < binding.size() ? binding[slot] : key_column_absent;
                ++slot;
                if (!index->is_disk() || column == key_column_absent) {
                    continue;
                }
                fn(index->disk_agent(), chunk.data[column].value(row));
            }
        }

        // Mirror pending txn entries to disk agents (call BEFORE commit clears pending maps)
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
        using index_to_name_t = std::pmr::unordered_map<std::string, index_t::pointer>;

        std::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        index_to_address_t index_to_address_;
        index_to_name_t index_to_name_;
        base_storage storage_;
    };

    using index_engine_ptr = core::pmr::unique_ptr<index_engine_t>;

    auto make_index_engine(std::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t& query) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const actor_zeta::address_t& address) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const std::string& name) -> index_t::pointer;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, std::string name, const keys_base_storage_t& keys, Args&&... args)
        -> uint32_t {
        return ptr->add_index(keys,
                              core::pmr::make_polymorphic_unique<Target>(ptr->resource(),
                                                                         std::move(name),
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
