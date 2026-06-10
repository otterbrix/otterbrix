#include "disk_hash_single_field_index.hpp"
#include "logical_value_binary_codec.hpp"

#include <components/table/row_version_manager.hpp>

namespace components::index {

    disk_hash_single_field_index_t::disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                                                   std::string name,
                                                                   const keys_base_storage_t& keys,
                                                                   disk_hash_storage_ptr storage)
        : index_t(resource, logical_plan::index_type::hashed, std::move(name), keys)
        , disk_table_(std::move(storage))
        , scratch_results_(resource)
        , pending_inserts_(resource)
        , pending_deletes_(resource) {}

    disk_hash_single_field_index_t::~disk_hash_single_field_index_t() = default;

    disk_hash_storage_t& disk_hash_single_field_index_t::storage_ref() const {
        assert(disk_table_ && "disk_hash_single_field_index requires disk storage");
        return *disk_table_;
    }

    value_t disk_hash_single_field_index_t::normalize_key(const value_t& key,
                                                          core::date::timezone_offset_t local_timezone) const {
        using namespace components::types;
        switch (key.type().type()) {
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT:
                return key.cast_as(complex_logical_type(logical_type::BIGINT), local_timezone);
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
                return key.cast_as(complex_logical_type(logical_type::UBIGINT), local_timezone);
            default:
                return key;
        }
    }

    std::string disk_hash_single_field_index_t::encode_key(const value_t& key,
                                                           core::date::timezone_offset_t local_timezone) const {
        auto normalized = normalize_key(key, local_timezone);
        return codec::encode_disk_hash_key(normalized);
    }

    namespace {
        components::types::logical_value_t decode_key(std::pmr::memory_resource* resource, std::string_view encoded) {
            std::pmr::string payload(encoded.data(), encoded.size(), resource);
            size_t pos = 0;
            return codec::read_logical_value(resource, payload, pos);
        }
    } // namespace

    auto disk_hash_single_field_index_t::insert_impl(value_t, index_value_t, core::date::timezone_offset_t) -> void {}

    auto disk_hash_single_field_index_t::remove_impl(value_t, core::date::timezone_offset_t) -> void {}

    index_t::range disk_hash_single_field_index_t::find_impl(const value_t& value,
                                                             core::date::timezone_offset_t local_timezone) const {
        scratch_results_.clear();
        const auto encoded = encode_key(value, local_timezone);
        auto values = storage_ref().get_all(encoded);
        for (const auto& v : values) {
            scratch_results_.emplace_back(v.value, 0, table::NOT_DELETED_ID);
        }
        for (const auto& [txn_id, rows] : pending_inserts_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (std::string_view(pending_key) == std::string_view(encoded)) {
                    scratch_results_.emplace_back(row_id, txn_id, table::NOT_DELETED_ID);
                }
            }
        }
        for (const auto& [txn_id, rows] : pending_deletes_) {
            for (const auto& [pending_key, row_id] : rows) {
                if (std::string_view(pending_key) != std::string_view(encoded)) {
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

    index_t::range disk_hash_single_field_index_t::lower_bound_impl(const value_t&,
                                                                    core::date::timezone_offset_t) const {
        throw "not supported"; // hash index has no ordering
    }

    index_t::range disk_hash_single_field_index_t::upper_bound_impl(const value_t&,
                                                                    core::date::timezone_offset_t) const {
        throw "not supported"; // hash index has no ordering
    }

    index_t::iterator disk_hash_single_field_index_t::cbegin_impl() const {
        return iterator(new impl_t(scratch_results_.cbegin()));
    }

    index_t::iterator disk_hash_single_field_index_t::cend_impl() const {
        return iterator(new impl_t(scratch_results_.cend()));
    }

    void disk_hash_single_field_index_t::insert_txn_impl(value_t key,
                                                         int64_t row_index,
                                                         uint64_t txn_id,
                                                         core::date::timezone_offset_t local_timezone) {
        auto encoded = encode_key(key, local_timezone);
        pending_inserts_[txn_id].emplace_back(std::pmr::string(encoded.data(), encoded.size(), resource()), row_index);
    }

    void disk_hash_single_field_index_t::mark_delete_impl(value_t key,
                                                          int64_t row_index,
                                                          uint64_t txn_id,
                                                          core::date::timezone_offset_t local_timezone) {
        auto encoded = encode_key(key, local_timezone);
        pending_deletes_[txn_id].emplace_back(std::pmr::string(encoded.data(), encoded.size(), resource()), row_index);
    }

    void disk_hash_single_field_index_t::commit_insert_impl(uint64_t txn_id, uint64_t) {
        pending_inserts_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::commit_delete_impl(uint64_t txn_id, uint64_t) {
        pending_deletes_.erase(txn_id);
    }

    void disk_hash_single_field_index_t::revert_insert_impl(uint64_t txn_id) { pending_inserts_.erase(txn_id); }

    // mark_delete_impl only records the pending bucket here (storage entries are
    // never stamped on the disk-hash variant), so reverting is a pure bucket
    // erase — symmetric with revert_insert_impl.
    void disk_hash_single_field_index_t::revert_delete_impl(uint64_t txn_id) { pending_deletes_.erase(txn_id); }

    void disk_hash_single_field_index_t::cleanup_versions_impl(uint64_t) {}

    void disk_hash_single_field_index_t::for_each_pending_insert_impl(
        uint64_t txn_id,
        const std::function<void(const value_t&, int64_t)>& fn) const {
        auto it = pending_inserts_.find(txn_id);
        if (it == pending_inserts_.end()) {
            return;
        }
        for (const auto& [encoded, row_id] : it->second) {
            fn(decode_key(resource(), encoded), row_id);
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
            fn(decode_key(resource(), encoded), row_id);
        }
    }

    void disk_hash_single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        scratch_results_.clear();
        pending_inserts_.clear();
        pending_deletes_.clear();
    }

} // namespace components::index
