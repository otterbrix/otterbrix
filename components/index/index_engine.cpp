#include "index_engine.hpp"
#include <atomic>

#include <iostream>
#include <utility>

#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

// index_engine no longer sends to manager_disk_t — disk persistence handled by manager_index_t

namespace components::index {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_index_key_column_probes{0};
    } // namespace
    uint64_t index_key_column_probes() noexcept { return g_index_key_column_probes.load(std::memory_order_relaxed); }
    void reset_index_key_column_probes() noexcept { g_index_key_column_probes.store(0, std::memory_order_relaxed); }
#endif

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

    auto make_index_engine(std::pmr::memory_resource* resource) -> index_engine_ptr {
        auto size = sizeof(index_engine_t);
        auto align = alignof(index_engine_t);
        auto* buffer = resource->allocate(size, align);
        auto* index_engine = new (buffer) index_engine_t(resource);
        return {index_engine, core::pmr::deleter_t(resource)};
    }

    // Resolve, for ONE index, the chunk column that carries its key — or key_column_absent when
    // the chunk does not carry every key column. Runs once per chunk, not once per row, and
    // builds each key name once instead of once per candidate column.
    //
    // ALL key columns must be present for the index to apply, but the value read is the FIRST
    // key's column: multi-column index keys are still a todo on the index side.
    static std::size_t resolve_key_column(const index_ptr& index, const components::vector::data_chunk_t& chunk) {
        auto keys = index->keys();
        if (keys.first == keys.second) {
            return index_engine_t::key_column_absent;
        }
        std::size_t first_key_column = index_engine_t::key_column_absent;
        for (auto key = keys.first; key != keys.second; ++key) {
            const auto key_name = key->as_string();
            std::size_t found = index_engine_t::key_column_absent;
            for (std::size_t column = 0; column < chunk.data.size(); ++column) {
#ifdef DEV_MODE
                g_index_key_column_probes.fetch_add(1, std::memory_order_relaxed);
#endif
                if (chunk.data[column].type().alias() == key_name) {
                    found = column;
                    break;
                }
            }
            if (found == index_engine_t::key_column_absent) {
                return index_engine_t::key_column_absent;
            }
            if (first_key_column == index_engine_t::key_column_absent) {
                first_key_column = found;
            }
        }
        return first_key_column;
    }

    index_engine_t::index_engine_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , index_to_mapper_(resource)
        , index_to_address_(resource)
        , relid_to_index_(resource)
        , storage_(resource) {}

    auto index_engine_t::add_index(index_ptr index) -> uint32_t {
        auto end = storage_.cend();
        auto d = storage_.insert(end, std::move(index));
        auto new_id = index_to_mapper_.size();
        index_to_mapper_.emplace(new_id, d->get());
        relid_to_index_.emplace(d->get()->oid(), d->get());
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
        relid_to_index_.erase(index->oid());
        //index_to_mapper_.erase(index.id); //todo
        // Removed BY IDENTITY, never by key set: two indexes may share one, and an
        // erase-by-key here used to take the surviving sibling's registration with it.
        storage_.erase(std::remove_if(storage_.begin(), storage_.end(), equal), storage_.end());
    }

    std::pmr::memory_resource* index_engine_t::resource() noexcept { return resource_; }

    auto index_engine_t::matching(id_index id) -> index_t::pointer { return index_to_mapper_.find(id)->second; }

    auto index_engine_t::size() const -> std::size_t { return storage_.size(); }

    // ORDERED FIRST, and that is a decision, not an artefact of iteration order.
    //
    // The one production caller is manager_index_t::search_with_preferred_type, reached
    // when the plan named no backend (index_type::no_valid) or when the backend it named
    // is not registered. An ORDERED index answers all six comparison predicates; an
    // unordered one answers eq and nothing else -- manager_index_t refuses a range on an
    // index whose supports_ordered_probe() is false, and answers the statement with an
    // error. So handing back the hashed twin of an ordered index over the same column
    // would fail a range probe with an index that could have answered it sitting right
    // beside it in storage_.
    //
    // Asked OF the index rather than read off its type enumerator, for the same reason
    // manager_index_t asks: the answer is a property of the implementation, and a caller
    // that guesses from the enumerator gets it wrong the day a new backend appears.
    auto index_engine_t::matching(const keys_base_storage_t& query) -> index_t::pointer {
        index_t::pointer unordered_match = nullptr;
        for (const auto& idx : storage_) {
            if (!idx || idx->keys_ != query) {
                continue;
            }
            if (idx->supports_ordered_probe()) {
                return idx.get();
            }
            if (unordered_match == nullptr) {
                unordered_match = idx.get();
            }
        }
        return unordered_match;
    }

    auto index_engine_t::matching(const keys_base_storage_t& query, logical_plan::index_type type) -> index_t::pointer {
        for (const auto& idx : storage_) {
            if (idx && idx->type() == type && idx->keys_ == query) {
                return idx.get();
            }
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

    auto index_engine_t::matching_relid(catalog::oid_t relid) -> index_t::pointer {
        auto it = relid_to_index_.find(relid);
        if (it != relid_to_index_.end()) {
            return it->second;
        }
        return nullptr;
    }

    auto index_engine_t::has_index(catalog::oid_t relid) -> bool { return matching_relid(relid) != nullptr; }

    index_engine_t::chunk_key_binding_t index_engine_t::bind_chunk(const vector::data_chunk_t& chunk) const {
        chunk_key_binding_t binding(resource_);
        binding.reserve(storage_.size());
        for (const auto& index : storage_) {
            binding.push_back(resolve_key_column(index, chunk));
        }
        return binding;
    }

    void index_engine_t::insert_row(const chunk_key_binding_t& binding,
                                    const vector::data_chunk_t& chunk,
                                    size_t chunk_row,
                                    int64_t storage_row,
                                    uint64_t txn_id,
                                    core::date::timezone_offset_t local_timezone) {
        std::size_t slot = 0;
        for (auto& index : storage_) {
            const auto column = slot < binding.size() ? binding[slot] : key_column_absent;
            ++slot;
            if (column == key_column_absent) {
                continue;
            }
            index->insert(chunk.data[column].value(chunk_row), storage_row, txn_id, local_timezone);
        }
    }

    void index_engine_t::mark_delete_row(const chunk_key_binding_t& binding,
                                         const vector::data_chunk_t& chunk,
                                         size_t chunk_row,
                                         int64_t storage_row,
                                         uint64_t txn_id,
                                         core::date::timezone_offset_t local_timezone) {
        std::size_t slot = 0;
        for (auto& index : storage_) {
            const auto column = slot < binding.size() ? binding[slot] : key_column_absent;
            ++slot;
            if (column == key_column_absent) {
                continue;
            }
            index->mark_delete(chunk.data[column].value(chunk_row), storage_row, txn_id, local_timezone);
        }
    }

    void index_engine_t::commit_insert(uint64_t txn_id, uint64_t commit_id) {
        for (auto& index : storage_) {
            index->commit_insert(txn_id, commit_id);
        }
    }

    void index_engine_t::commit_delete(uint64_t txn_id, uint64_t commit_id) {
        for (auto& index : storage_) {
            index->commit_delete(txn_id, commit_id);
        }
    }

    void index_engine_t::revert_insert(uint64_t txn_id) {
        for (auto& index : storage_) {
            index->revert_insert(txn_id);
        }
    }

    void index_engine_t::revert_delete(uint64_t txn_id) {
        for (auto& index : storage_) {
            index->revert_delete(txn_id);
        }
    }

    void index_engine_t::cleanup_versions(uint64_t lowest_active) {
        for (auto& index : storage_) {
            index->cleanup_versions(lowest_active);
        }
    }

    // DEDUPLICATED, deliberately.
    //
    // Both consumers ask an EXISTENCE question of this vector and nothing else: the
    // planner's context_storage_t::has_index_on ("is this column indexed at all?") and
    // enrich_logical_plan's stamp_table_has_indexes ("does this table have any index, so
    // must DML mirror into it?"). Neither counts, so a repeat is not wrong for them --
    // but the vector is published as "which key sets are indexed", and a table carrying
    // an ordered AND a hashed index over one column has ONE indexed key set, not two. A
    // bag would invite the next caller to read the repeat as two different columns.
    // Multiplicity is available, exactly, from all_indexed_descriptions(), which pairs
    // each key set with the type that distinguishes the duplicates -- so nothing is lost
    // by folding them here.
    //
    // Linear scan over the result: an index count per table is 1-3, and the alternative
    // is the key-keyed map this whole change removed.
    auto index_engine_t::all_indexed_keys() const -> std::pmr::vector<keys_base_storage_t> {
        std::pmr::vector<keys_base_storage_t> result(resource_);
        result.reserve(storage_.size());
        for (const auto& idx : storage_) {
            if (!idx) {
                continue;
            }
            bool already_listed = false;
            for (const auto& listed : result) {
                if (listed == idx->keys_) {
                    already_listed = true;
                    break;
                }
            }
            if (!already_listed) {
                result.push_back(idx->keys_);
            }
        }
        return result;
    }

    auto index_engine_t::all_indexed_descriptions() const -> std::pmr::vector<index_description_t> {
        std::pmr::vector<index_description_t> result(resource_);
        result.reserve(storage_.size());
        for (const auto& idx : storage_) {
            index_description_t desc{keys_base_storage_t(resource_), idx->type()};
            auto [it, end] = idx->keys();
            for (; it != end; ++it) {
                desc.keys.push_back(*it);
            }
            result.push_back(std::move(desc));
        }
        return result;
    }

    auto index_engine_t::indexes() -> std::pmr::vector<catalog::oid_t> {
        std::pmr::vector<catalog::oid_t> res(resource_);
        res.reserve(storage_.size());
        for (const auto& index : storage_) {
            res.emplace_back(index->oid());
        }
        return res;
    }

    void set_disk_agent(const index_engine_ptr& ptr,
                        id_index id,
                        actor_zeta::address_t agent,
                        actor_zeta::address_t manager) {
        auto* index = search_index(ptr, id);
        if (index) {
            auto agent_copy = agent; // copy for add_disk_agent
            index->set_disk_agent(std::move(agent), std::move(manager));
            ptr->add_disk_agent(id, std::move(agent_copy));
        }
    }

} // namespace components::index
