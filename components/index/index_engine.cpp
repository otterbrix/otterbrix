#include "index_engine.hpp"

#include <iostream>
#include <utility>

#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

// index_engine no longer sends to manager_disk_t — disk persistence handled by manager_index_t

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
        return types::logical_value_t{chunk.resource(), types::complex_logical_type{types::logical_type::NA}};
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

    void
    index_engine_t::insert_row(const vector::data_chunk_t& chunk, size_t row) {
        for (auto& index : storage_) {
            if (is_match_column(index, chunk)) {
                auto key = get_value_by_index(index, chunk, row);
                index->insert(key, static_cast<int64_t>(row));
            }
        }
    }

    void
    index_engine_t::delete_row(const vector::data_chunk_t& chunk, size_t row) {
        for (auto& index : storage_) {
            if (is_match_column(index, chunk)) {
                auto key = get_value_by_index(index, chunk, row);
                index->remove(key);
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

    void
    index_engine_t::for_each_disk_op(const vector::data_chunk_t& chunk, size_t row,
                                      const std::function<void(const actor_zeta::address_t&, const value_t&)>& fn) const {
        for (const auto& index : storage_) {
            if (index->is_disk() && is_match_column(index, chunk)) {
                auto key = get_value_by_index(index, chunk, row);
                fn(index->disk_agent(), key);
            }
        }
    }

    void set_disk_agent(const index_engine_ptr& ptr, id_index id,
                        actor_zeta::address_t agent, actor_zeta::address_t manager) {
        auto* index = search_index(ptr, id);
        if (index) {
            auto agent_copy = agent;  // copy for add_disk_agent
            index->set_disk_agent(std::move(agent), std::move(manager));
            ptr->add_disk_agent(id, std::move(agent_copy));
        }
    }

} // namespace components::index
