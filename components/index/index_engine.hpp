#pragma once

#include <limits>
#include <map>
#include <memory>
#include <scoped_allocator>
#include <string>
#include <utility>
#include <vector>

#include "forward.hpp"
#include "index.hpp"
#include <components/context/context.hpp>
#include <core/btree/btree.hpp>
#include <core/pmr.hpp>

namespace components::vector {
    class data_chunk_t;
}
namespace components::index {

    constexpr uint32_t INDEX_ID_UNDEFINED = std::numeric_limits<uint32_t>::max();

    struct index_engine_t final {
    public:
        explicit index_engine_t(std::pmr::memory_resource* resource);
        auto matching(id_index id) -> index_t::pointer;
        auto matching(const keys_base_storage_t& query) -> index_t::pointer;
        auto matching(const actor_zeta::address_t& address) -> index_t::pointer;
        auto matching(const std::string& name) -> index_t::pointer;
        auto has_index(const std::string& name)
            -> bool; // TODO figure out how to make it faster (not using matching inside)
        auto add_index(const keys_base_storage_t&, index_ptr) -> uint32_t;
        auto add_disk_agent(id_index id, actor_zeta::address_t address) -> void;
        auto drop_index(index_t::pointer index) -> void;
        auto size() const -> std::size_t;
        std::pmr::memory_resource* resource() noexcept;

        void insert_document(const document_ptr& document, pipeline::context_t* pipeline_context);
        void delete_document(const document_ptr& document, pipeline::context_t* pipeline_context);
        void insert_row(const vector::data_chunk_t& chunk, size_t row, pipeline::context_t* pipeline_context);
        void delete_row(const vector::data_chunk_t& chunk, size_t row, pipeline::context_t* pipeline_context);

        auto indexes() -> std::vector<std::string>;

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
        return ptr->add_index(
            keys,
            core::pmr::make_unique<Target>(
                ptr->resource(),
                std::move(name),
                keys,
                std::forward<Args>(args).../*,
                core::pmr::deleter_t(ptr->resource())*/));
    }

    void drop_index(const index_engine_ptr& ptr, index_t::pointer index);

    void insert(const index_engine_ptr& ptr, id_index id, std::pmr::vector<document_ptr>& docs);
    void insert(const index_engine_ptr& ptr,
                id_index id,
                core::pmr::btree::btree_t<document::document_id_t, document_ptr>& docs);
    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr docs);
    void find(const index_engine_ptr& index, id_index id, result_set_t*);
    void find(const index_engine_ptr& index, query_t query, result_set_t*);

    void set_disk_agent(const index_engine_ptr& ptr, id_index id,
                        actor_zeta::address_t agent, actor_zeta::address_t manager);
    void sync_index_from_disk(const index_engine_ptr& ptr,
                              const actor_zeta::address_t& index_address,
                              const std::pmr::vector<document::document_id_t>& ids,
                              const core::pmr::btree::btree_t<document::document_id_t, document_ptr>& storage);

} // namespace components::index