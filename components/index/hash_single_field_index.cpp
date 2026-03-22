#include "hash_single_field_index.hpp"

namespace components::index {

    hash_single_field_index_t::hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                         std::string name,
                                                         const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::hashed, std::move(name), keys)
        , storage_(resource) {}

    hash_single_field_index_t::~hash_single_field_index_t() = default;

    index_t::iterator::reference hash_single_field_index_t::impl_t::value_ref() const { return iterator_->second; }
    index_t::iterator_t::iterator_impl_t* hash_single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }

    bool hash_single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        return iterator_ == dynamic_cast<const impl_t*>(other)->iterator_; //todo
    }

    bool hash_single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        return iterator_ != dynamic_cast<const impl_t*>(other)->iterator_; //todo
    }

    index_t::iterator::iterator_impl_t* hash_single_field_index_t::impl_t::copy() const { return new impl_t(*this); }

    hash_single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {}

    auto hash_single_field_index_t::insert_impl(value_t key, index_value_t value) -> void {
        storage_.emplace(std::move(key), std::move(value));
    }

    auto hash_single_field_index_t::remove_impl(components::index::value_t key) -> void {
        auto range = storage_.equal_range(key);
        if (range.first != range.second) {
            storage_.erase(range.first);
        }
    }

    index_t::range hash_single_field_index_t::find_impl(const value_t& value) const {
        auto range = storage_.equal_range(value);
        return std::make_pair(iterator(new impl_t(range.first)), iterator(new impl_t(range.second)));
    }

    index_t::range hash_single_field_index_t::lower_bound_impl(const value_t&) const {
        throw "not supported";
    }

    index_t::range hash_single_field_index_t::upper_bound_impl(const value_t&) const {
        throw "not supported";
    }

    index_t::iterator hash_single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(storage_.cbegin()));
    }

    index_t::iterator hash_single_field_index_t::cend_impl() const {
        return index_t::iterator(new impl_t(storage_.cend()));
    }

    void hash_single_field_index_t::insert_txn_impl(value_t key, int64_t row_index, uint64_t txn_id) {
        index_value_t val(row_index, txn_id, table::NOT_DELETED_ID);
        pending_inserts_[txn_id].emplace_back(key, row_index);
        storage_.emplace(std::move(key), std::move(val));
    }

    void hash_single_field_index_t::mark_delete_impl(value_t key, int64_t row_index, uint64_t txn_id) {
        auto range = storage_.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            if (it->second.row_index == row_index && it->second.delete_id == table::NOT_DELETED_ID) {
                it->second.delete_id = txn_id;
                pending_deletes_[txn_id].emplace_back(key, row_index);
                return;
            }
        }
    }

    void hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t commit_id) {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.insert_id == txn_id) {
                    sit->second.insert_id = commit_id;
                    break;
                }
            }
        }
        pending_inserts_.erase(it);
    }

    void hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t commit_id) {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.delete_id == txn_id) {
                    sit->second.delete_id = commit_id;
                    break;
                }
            }
        }
        pending_deletes_.erase(it);
    }

    void hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.insert_id == txn_id) {
                    storage_.erase(sit);
                    break;
                }
            }
        }
        pending_inserts_.erase(it);
    }

    void hash_single_field_index_t::cleanup_versions_impl(uint64_t lowest_active) {
        for (auto it = storage_.begin(); it != storage_.end();) {
            if (it->second.delete_id < lowest_active && it->second.delete_id < table::TRANSACTION_ID_START) {
                it = storage_.erase(it);
            } else {
                ++it;
            }
        }
        // Also clean up any stale pending entries for committed txns
        for (auto it = pending_deletes_.begin(); it != pending_deletes_.end();) {
            if (it->first < lowest_active && it->first < table::TRANSACTION_ID_START) {
                it = pending_deletes_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void
    hash_single_field_index_t::for_each_pending_insert_impl(uint64_t txn_id,
                                                            const std::function<void(const value_t&, int64_t)>& fn)
        const {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            fn(key, row_index);
        }
    }

    void
    hash_single_field_index_t::for_each_pending_delete_impl(uint64_t txn_id,
                                                            const std::function<void(const value_t&, int64_t)>& fn)
        const {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            fn(key, row_index);
        }
    }

    void hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        storage_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
