#include "disk_hash_single_field_index.hpp"
#include "logical_value_binary_codec.hpp"

#include <components/table/row_version_manager.hpp>

namespace components::index {

    disk_hash_single_field_index_t::disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                                   std::string name,
                                                                   const keys_base_storage_t& keys,
                                                                   std::unique_ptr<disk_hash_storage_t> storage)
        : index_t(resource, logical_plan::index_type::hashed, std::move(name), keys)
        , disk_table_(std::move(storage))
        , scratch_results_(resource) {}

    disk_hash_single_field_index_t::~disk_hash_single_field_index_t() = default;

    disk_hash_storage_t& disk_hash_single_field_index_t::storage_ref() const {
        assert(disk_table_ && "disk_hash_single_field_index requires disk storage");
        return *disk_table_;
    }

    std::string disk_hash_single_field_index_t::encode_key(const value_t& key) const {
        return codec::encode_disk_hash_key(key);
    }

    auto disk_hash_single_field_index_t::insert_impl(value_t key, index_value_t value) -> void {
        if (value.insert_id < table::TRANSACTION_ID_START &&
            (value.delete_id == table::NOT_DELETED_ID || value.delete_id >= table::TRANSACTION_ID_START)) {
            storage_ref().put(encode_key(key), value.row_index, 0, 0);
        }
    }

    auto disk_hash_single_field_index_t::remove_impl(value_t key) -> void {
        storage_ref().erase(encode_key(key));
    }

    index_t::range disk_hash_single_field_index_t::find_impl(const value_t& value) const {
        scratch_results_.clear();
        const auto encoded = encode_key(value);
        auto values = storage_ref().get_all(encoded);
        for (const auto& v : values) {
            scratch_results_.emplace_back(v.value, 0, table::NOT_DELETED_ID);
        }
        for (const auto& [txn_id, rows] : pending_inserts_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (pending_key == encoded) {
                    scratch_results_.emplace_back(row_id, txn_id, table::NOT_DELETED_ID);
                }
            }
        }
        for (const auto& [txn_id, rows] : pending_deletes_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (pending_key != encoded) {
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

    index_t::range disk_hash_single_field_index_t::lower_bound_impl(const value_t&) const {
        throw "not supported"; // hash index has no ordering
    }

    index_t::range disk_hash_single_field_index_t::upper_bound_impl(const value_t&) const {
        throw "not supported"; // hash index has no ordering
    }

    index_t::iterator disk_hash_single_field_index_t::cbegin_impl() const {
        return iterator(new impl_t(scratch_results_.cbegin()));
    }

    index_t::iterator disk_hash_single_field_index_t::cend_impl() const {
        return iterator(new impl_t(scratch_results_.cend()));
    }

    void disk_hash_single_field_index_t::insert_txn_impl(value_t key, int64_t row_index, uint64_t txn_id) {
        auto encoded = encode_key(key);
        pending_inserts_[txn_id].emplace_back(encoded, row_index);
        storage_ref().append_pending_insert(txn_id, encoded, row_index);
    }

    void disk_hash_single_field_index_t::mark_delete_impl(value_t key, int64_t row_index, uint64_t txn_id) {
        auto encoded = encode_key(key);
        pending_deletes_[txn_id].emplace_back(encoded, row_index);
        storage_ref().append_pending_delete(txn_id, encoded, row_index);
    }

    void disk_hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t commit_id) {
        (void) commit_id;
        auto it = pending_inserts_.find(txn_id);
        if (it != pending_inserts_.end()) {
            for (const auto& [encoded, row_id] : it->second) {
                storage_ref().put(encoded, row_id, 0, 0);
            }
            pending_inserts_.erase(it);
        }
        storage_ref().finalize_txn(txn_id, true, false);
    }

    void disk_hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t commit_id) {
        (void) commit_id;
        auto it = pending_deletes_.find(txn_id);
        if (it != pending_deletes_.end()) {
            for (const auto& [encoded, row_id] : it->second) {
                storage_ref().erase(encoded, row_id);
            }
            pending_deletes_.erase(it);
        }
        storage_ref().finalize_txn(txn_id, false, true);
    }

    void disk_hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) {
        pending_inserts_.erase(txn_id);
        storage_ref().finalize_txn(txn_id, false, false);
    }

    void disk_hash_single_field_index_t::cleanup_versions_impl(uint64_t lowest_active) {
        (void) lowest_active;
    }

    void disk_hash_single_field_index_t::for_each_pending_insert_impl(
        uint64_t txn_id,
        const std::function<void(const value_t&, int64_t)>& fn) const {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return;
        }
        for (const auto& [encoded, row_id] : it->second) {
            fn(value_t(resource(), encoded), row_id);
        }
    }

    void disk_hash_single_field_index_t::for_each_pending_delete_impl(
        uint64_t txn_id,
        const std::function<void(const value_t&, int64_t)>& fn) const {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end()) {
            return;
        }
        for (const auto& [encoded, row_id] : it->second) {
            fn(value_t(resource(), encoded), row_id);
        }
    }

    void disk_hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        scratch_results_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
