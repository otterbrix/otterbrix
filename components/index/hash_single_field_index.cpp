#include "hash_single_field_index.hpp"

#include <cassert>
#include <cstdlib>

namespace components::index {

    hash_single_field_index_t::hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                         catalog::oid_t oid,
                                                         const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::hashed, oid, keys)
        , storage_(resource) {}

    hash_single_field_index_t::~hash_single_field_index_t() = default;

    index_t::iterator::reference hash_single_field_index_t::impl_t::value_ref() const { return iterator_->second; }
    index_t::iterator_t::iterator_impl_t* hash_single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }

    bool hash_single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        abort_unless_same_kind(other);
        return iterator_ == static_cast<const impl_t*>(other)->iterator_;
    }

    bool hash_single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        abort_unless_same_kind(other);
        return iterator_ != static_cast<const impl_t*>(other)->iterator_;
    }

    index_t::iterator::iterator_impl_t* hash_single_field_index_t::impl_t::copy() const { return new impl_t(*this); }

    hash_single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {}

    auto hash_single_field_index_t::insert_impl(value_t key, index_value_t value, core::date::timezone_offset_t)
        -> void {
        storage_.emplace(std::move(key), std::move(value));
    }

    auto hash_single_field_index_t::remove_impl(components::index::value_t key, core::date::timezone_offset_t) -> void {
        auto range = storage_.equal_range(key);
        if (range.first != range.second) {
            storage_.erase(range.first);
        }
    }

    index_t::range hash_single_field_index_t::find_impl(const value_t& value, core::date::timezone_offset_t) const {
        auto range = storage_.equal_range(value);
        return std::make_pair(iterator(new impl_t(range.first)), iterator(new impl_t(range.second)));
    }

    // Unreachable by contract, and terminal rather than quiet. A hash bucket has no
    // ordering, so nothing but eq can be asked of this index: supports_ordered_probe()
    // answers false, manager_index_t refuses a range predicate on it before any read is
    // dispatched, and the planner refuses one earlier still (can_use_index picks a hashed
    // index only for an equality).
    //
    // What stood here raised the STRING LITERAL "not supported" as an exception, so a
    // caller could only have caught it as `catch (const char*)` -- and it was raised from
    // inside an actor coroutine whose unhandled_exception() is empty, so the exception was
    // swallowed and the statement reported success over zero rows. An empty range would be
    // the same wrong answer with the crash removed, which is why it is not the
    // replacement. Same shape as bitcask_index_disk_t::scan_range one level down.
    index_t::range hash_single_field_index_t::lower_bound_impl(const value_t&,
                                                               core::date::timezone_offset_t /*local_timezone*/) const {
        assert(false && "hash_single_field_index_t::lower_bound_impl: a hash index has no ordering");
        std::abort();
    }

    index_t::range hash_single_field_index_t::upper_bound_impl(const value_t&,
                                                               core::date::timezone_offset_t /*local_timezone*/) const {
        assert(false && "hash_single_field_index_t::upper_bound_impl: a hash index has no ordering");
        std::abort();
    }

    index_t::iterator hash_single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(storage_.cbegin()));
    }

    index_t::iterator hash_single_field_index_t::cend_impl() const {
        return index_t::iterator(new impl_t(storage_.cend()));
    }

    void hash_single_field_index_t::insert_txn_impl(value_t key,
                                                    int64_t row_index,
                                                    uint64_t txn_id,
                                                    core::date::timezone_offset_t) {
        index_value_t val(row_index, txn_id, table::NOT_DELETED_ID);
        pending_inserts_[txn_id].emplace_back(key, row_index);
        storage_.emplace(std::move(key), std::move(val));
    }

    void hash_single_field_index_t::mark_delete_impl(value_t key,
                                                     int64_t row_index,
                                                     uint64_t txn_id,
                                                     core::date::timezone_offset_t) {
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

    void hash_single_field_index_t::revert_delete_impl(uint64_t txn_id) {
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end())
            return;
        for (const auto& [key, row_index] : it->second) {
            auto range = storage_.equal_range(key);
            for (auto sit = range.first; sit != range.second; ++sit) {
                if (sit->second.row_index == row_index && sit->second.delete_id == txn_id) {
                    sit->second.delete_id = table::NOT_DELETED_ID;
                    break;
                }
            }
        }
        pending_deletes_.erase(it);
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

    index_t::pending_entries_t hash_single_field_index_t::pending_inserts_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [key, row_index] : it->second) {
            out.push_back(pending_entry_t{key, row_index});
        }
        return out;
    }

    index_t::pending_entries_t hash_single_field_index_t::pending_deletes_impl(uint64_t txn_id) const {
        pending_entries_t out{resource()};
        auto it = pending_deletes_.find(txn_id);
        if (it == pending_deletes_.end()) {
            return out;
        }
        out.reserve(it->second.size());
        for (const auto& [key, row_index] : it->second) {
            out.push_back(pending_entry_t{key, row_index});
        }
        return out;
    }

    void hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        storage_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

    void hash_single_field_index_t::merge_uncommitted_rows_impl(const value_t&,
                                                                uint64_t,
                                                                core::date::timezone_offset_t,
                                                                std::pmr::vector<int64_t>&) const {
        // Unreachable by contract: this hook only means something for an index whose
        // COMMITTED rows are read out of a disk agent, leaving the txn-local half to be
        // folded back in by the caller. Everything this index holds -- committed and
        // pending alike -- is in storage_ and comes back from find_impl, so reaching
        // here means a caller routed an in-memory index down the disk read path.
        //
        // A quiet no-op would be worse than a crash: the caller would keep whatever
        // "disk half" it thinks it read (nothing, for an index with no agent) and serve
        // a SELECT that silently lost every row. Die where the bug is. Unconditional --
        // std::abort() runs under NDEBUG too.
        assert(false && "merge_uncommitted_rows: an in-memory index reached the disk read path");
        std::abort();
    }

} // namespace components::index
