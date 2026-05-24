#include "disk_hash_single_field_index.hpp"

#include <sstream>

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
        std::ostringstream os;
        os << static_cast<int>(key.type().type()) << ":";
        if (key.type().type() == components::types::logical_type::STRING_LITERAL) {
            auto sv = key.value<std::string_view>();
            os << sv.size() << ":";
            os.write(sv.data(), static_cast<std::streamsize>(sv.size()));
            return os.str();
        }
        os << key.hash();
        return os.str();
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
        auto v = storage_ref().get(encode_key(value));
        if (v.has_value()) {
            scratch_results_.emplace_back(v->value);
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
        storage_ref().append_pending_insert(txn_id, encode_key(key), row_index);
    }

    void disk_hash_single_field_index_t::mark_delete_impl(value_t key, int64_t row_index, uint64_t txn_id) {
        storage_ref().append_pending_delete(txn_id, encode_key(key), row_index);
    }

    void disk_hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t commit_id) {
        (void) commit_id;
        storage_ref().finalize_txn(txn_id, true, false);
    }

    void disk_hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t commit_id) {
        (void) commit_id;
        storage_ref().finalize_txn(txn_id, false, true);
    }

    void disk_hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) {
        storage_ref().finalize_txn(txn_id, false, false);
    }

    void disk_hash_single_field_index_t::cleanup_versions_impl(uint64_t lowest_active) {
        (void) lowest_active;
    }

    void disk_hash_single_field_index_t::for_each_pending_insert_impl(
        uint64_t txn_id,
        const std::function<void(const value_t&, int64_t)>& fn) const {
        (void) txn_id;
        (void) fn;
        // TODO: expose pending entries from disk_hash_storage_t when needed by caller.
    }

    void disk_hash_single_field_index_t::for_each_pending_delete_impl(
        uint64_t txn_id,
        const std::function<void(const value_t&, int64_t)>& fn) const {
        (void) txn_id;
        (void) fn;
        // TODO: expose pending entries from disk_hash_storage_t when needed by caller.
    }

    void disk_hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        scratch_results_.clear();
    }

} // namespace components::index
