#include "index_engine.hpp"

#include <iostream>
#include <utility>

#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

#include <services/disk/index_agent_disk.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::index {

    void find(const index_engine_ptr&, query_t, result_set_t*) {
        /// auto* index  = search_index(ptr, query);
        /// index->find(std::move(query),set);
    }

    void find(const index_engine_ptr&, id_index, result_set_t*) {
        /// auto* index  = search_index(ptr, id);
        /// index->find(id,set);
    }

    void drop_index(const index_engine_ptr& ptr, index_t::pointer index) { ptr->drop_index(index); }

    void insert(const index_engine_ptr& ptr, id_index id, std::pmr::vector<document_ptr>& docs) {
        auto* index = search_index(ptr, id);
        for (const auto& i : docs) {
            auto range = index->keys();
            for (auto j = range.first; j != range.second; ++j) {
                const auto& key_tmp = *j;
                const std::string& key = key_tmp.as_string(); // hack
                if (!(i->is_null(key))) {
                    auto data = i->get_value(key).as_logical_value();
                    index->insert(data, i);
                }
            }
        }
    }

    void insert(const index_engine_ptr& ptr,
                id_index id,
                core::pmr::btree::btree_t<document::document_id_t, document_ptr>& docs) {
        auto* index = search_index(ptr, id);
        for (auto& doc : docs) {
            auto range = index->keys();
            for (auto j = range.first; j != range.second; ++j) {
                const auto& key_tmp = *j;
                const std::string& key = key_tmp.as_string(); // hack
                if (!(doc.second->is_null(key))) {
                    auto data = doc.second->get_value(key).as_logical_value();
                    index->insert(data, {doc.first, doc.second, -1});
                }
            }
        }
    }

    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr doc) {
        auto* index = search_index(ptr, id);
        auto range = index->keys();
        for (auto j = range.first; j != range.second; ++j) {
            const auto& key_tmp = *j;
            const std::string& key = key_tmp.as_string(); // hack
            if (!(doc->is_null(key))) {
                auto data = doc->get_value(key).as_logical_value();
                index->insert(data, doc);
            }
        }
    }

    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t::pointer { return ptr->matching(id); }

    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t& query) -> index_t::pointer {
        return ptr->matching(query);
    }

    auto search_index(const index_engine_ptr& ptr, const actor_zeta::address_t& address) -> index_t::pointer {
        return ptr->matching(address);
    }

    auto search_index(const index_engine_ptr& ptr, const std::string& name) -> index_t::pointer {
        return ptr->matching(name);
    }

    auto make_index_engine(std::pmr::memory_resource* resource) -> index_engine_ptr {
        auto size = sizeof(index_engine_t);
        auto align = alignof(index_engine_t);
        auto* buffer = resource->allocate(size, align);
        auto* index_engine = new (buffer) index_engine_t(resource);
        return {index_engine, core::pmr::deleter_t(resource)};
    }

    bool is_match_document(const index_ptr& index, const document_ptr& document) {
        auto keys = index->keys();
        for (auto key = keys.first; key != keys.second; ++key) {
            if (!document->is_exists(key->as_string())) {
                return false;
            }
        }
        return true;
    }

    bool is_match_column(const index_ptr& index, const components::vector::data_chunk_t& chunk) {
        auto keys = index->keys();
        for (auto key = keys.first; key != keys.second; ++key) {
            bool key_found = false;
            for (const auto& column : chunk.data) {
                if (column.type().alias() == key->as_string()) {
                    key_found = true;
                    break;
                }
            }
            if (!key_found) {
                return false;
            }
        }
        return true;
    }

    value_t get_value_by_index(const index_ptr& index, const document_ptr& document) {
        auto keys = index->keys();
        if (keys.first != keys.second) {
            return document->get_value(keys.first->as_string()).as_logical_value();
            //todo: multi values index
        }
        return value_t{};
    }

    value_t get_value_by_index(const index_ptr& index, const vector::data_chunk_t& chunk, size_t row) {
        auto keys = index->keys();
        if (keys.first != keys.second) {
            //todo: multi values index
            for (const auto& column : chunk.data) {
                if (column.type().alias() == keys.first->as_string()) {
                    return column.value(row);
                }
            }
        }
        return types::logical_value_t{};
    }

    index_engine_t::index_engine_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , mapper_(resource)
        , index_to_mapper_(resource)
        , index_to_address_(resource)
        , index_to_name_(resource)
        , storage_(resource) {}

    auto index_engine_t::add_index(const keys_base_storage_t& keys, index_ptr index) -> uint32_t {
        auto end = storage_.cend();
        auto d = storage_.insert(end, std::move(index));
        mapper_.emplace(keys, d->get());
        auto new_id = index_to_mapper_.size();
        index_to_mapper_.emplace(new_id, d->get());
        index_to_name_.emplace(d->get()->name(), d->get());
        return uint32_t(new_id);
    }

    auto index_engine_t::add_disk_agent(id_index id, actor_zeta::address_t address) -> void {
        index_to_address_.emplace(address, index_to_mapper_.find(id)->second);
    }

    auto index_engine_t::drop_index(index_t::pointer index) -> void {
        auto equal = [&index](const index_ptr& ptr) { return index == ptr.get(); };
        if (index->is_disk()) {
            index_to_address_.erase(index->disk_agent());
        }
        index_to_name_.erase(index->name());
        //index_to_mapper_.erase(index.id); //todo
        mapper_.erase(index->keys_);
        storage_.erase(std::remove_if(storage_.begin(), storage_.end(), equal), storage_.end());
    }

    std::pmr::memory_resource* index_engine_t::resource() noexcept { return resource_; }

    auto index_engine_t::matching(id_index id) -> index_t::pointer { return index_to_mapper_.find(id)->second; }

    auto index_engine_t::size() const -> std::size_t { return mapper_.size(); }

    auto index_engine_t::matching(const keys_base_storage_t& query) -> index_t::pointer {
        auto it = mapper_.find(query);
        if (it != mapper_.end()) {
            return it->second;
        }
        return nullptr;
    }

    auto index_engine_t::matching(const actor_zeta::address_t& address) -> index_t::pointer {
        auto it = index_to_address_.find(address);
        if (it != index_to_address_.end()) {
            return it->second;
        }
        return nullptr;
    }

    auto index_engine_t::matching(const std::string& name) -> index_t::pointer {
        auto it = index_to_name_.find(name);
        if (it != index_to_name_.end()) {
            return it->second;
        }
        return nullptr;
    }

    auto index_engine_t::has_index(const std::string& name) -> bool { return matching(name) == nullptr ? false : true; }

    void index_engine_t::insert_document(const document_ptr& document, pipeline::context_t* pipeline_context) {
        for (auto& index : storage_) {
            if (is_match_document(index, document)) {
                auto key = get_value_by_index(index, document);
                index->insert(key, document);
                if (index->is_disk() && pipeline_context && index->disk_manager()) {
                    auto session_copy = pipeline_context->session;
                    auto agent_copy = index->disk_agent();
                    auto key_copy = key;
                    auto [_, future] = actor_zeta::send(index->disk_manager(),
                                                   &services::disk::manager_disk_t::index_insert_by_agent,
                                                   std::move(session_copy),
                                                   std::move(agent_copy),
                                                   std::move(key_copy),
                                                   document::get_document_id(document));
                    pipeline_context->add_pending_disk_future(std::move(future));
                }
            }
        }
    }

    void index_engine_t::delete_document(const document_ptr& document, pipeline::context_t* pipeline_context) {
        for (auto& index : storage_) {
            if (is_match_document(index, document)) {
                auto key = get_value_by_index(index, document);
                index->remove(key); //todo: bug
                if (index->is_disk() && pipeline_context && index->disk_manager()) {
                    auto session_copy = pipeline_context->session;
                    auto agent_copy = index->disk_agent();
                    auto key_copy = key;
                    auto [_, future] = actor_zeta::send(index->disk_manager(),
                                                   &services::disk::manager_disk_t::index_remove_by_agent,
                                                   std::move(session_copy),
                                                   std::move(agent_copy),
                                                   std::move(key_copy),
                                                   document::get_document_id(document));
                    pipeline_context->add_pending_disk_future(std::move(future));
                }
            }
        }
    }

    void
    index_engine_t::insert_row(const vector::data_chunk_t& chunk, size_t row, pipeline::context_t* pipeline_context) {
        for (auto& index : storage_) {
            if (is_match_column(index, chunk)) {
                auto key = get_value_by_index(index, chunk, row);
                index->insert(key, static_cast<int64_t>(row));
                if (index->is_disk() && pipeline_context && index->disk_manager()) {
                    auto session_copy = pipeline_context->session;
                    auto agent_copy = index->disk_agent();
                    auto key_copy = key;
                    auto [_, future] = actor_zeta::send(index->disk_manager(),
                                                   &services::disk::manager_disk_t::index_insert_by_agent,
                                                   std::move(session_copy),
                                                   std::move(agent_copy),
                                                   std::move(key_copy),
                                                   components::document::document_id_t(row));
                    pipeline_context->add_pending_disk_future(std::move(future));
                }
            }
        }
    }

    void
    index_engine_t::delete_row(const vector::data_chunk_t& chunk, size_t row, pipeline::context_t* pipeline_context) {
        for (auto& index : storage_) {
            if (is_match_column(index, chunk)) {
                auto key = get_value_by_index(index, chunk, row);
                index->remove(key);
                if (index->is_disk() && pipeline_context && index->disk_manager()) {
                    auto session_copy = pipeline_context->session;
                    auto agent_copy = index->disk_agent();
                    auto key_copy = key;
                    auto [_, future] = actor_zeta::send(index->disk_manager(),
                                                   &services::disk::manager_disk_t::index_remove_by_agent,
                                                   std::move(session_copy),
                                                   std::move(agent_copy),
                                                   std::move(key_copy),
                                                   components::document::document_id_t(row));
                    pipeline_context->add_pending_disk_future(std::move(future));
                }
            }
        }
    }

    auto index_engine_t::indexes() -> std::vector<std::string> {
        std::vector<std::string> res;
        res.reserve(storage_.size());
        for (const auto& index : storage_) {
            res.emplace_back(index->name());
        }
        return res;
    }

    void set_disk_agent(const index_engine_ptr& ptr, id_index id,
                        actor_zeta::address_t agent, actor_zeta::address_t manager) {
        auto* index = search_index(ptr, id);
        if (index) {
            auto agent_copy = agent;
            index->set_disk_agent(std::move(agent), std::move(manager));
            ptr->add_disk_agent(id, std::move(agent_copy));
        }
    }

    void sync_index_from_disk(const index_engine_ptr& ptr,
                              const actor_zeta::address_t& index_address,
                              const std::pmr::vector<document::document_id_t>& ids,
                              const core::pmr::btree::btree_t<document::document_id_t, document_ptr>& storage) {
        auto* index = search_index(ptr, index_address);
        if (index) {
            index->clean_memory_to_new_elements(ids.size());
            for (const auto& id : ids) {
                assert(storage.find(id) != storage.end());
                index->insert(storage.find(id)->second);
            }
        }
    }

} // namespace components::index
