#include "btree_index_disk.hpp"

#include "btree_record_codec.hpp"

#include <components/index/logical_value_binary_codec.hpp>

namespace services::index {

    using namespace core::b_plus_tree;
    using components::types::logical_type;

    components::types::physical_value convert(const components::types::logical_value_t& value) {
        switch (value.type().type()) {
            case logical_type::BOOLEAN:
                return components::types::physical_value(value.value<bool>());
            case logical_type::UTINYINT:
                return components::types::physical_value(value.value<uint8_t>());
            case logical_type::TINYINT:
                return components::types::physical_value(value.value<int8_t>());
            case logical_type::USMALLINT:
                return components::types::physical_value(value.value<uint16_t>());
            case logical_type::SMALLINT:
                return components::types::physical_value(value.value<int16_t>());
            case logical_type::UINTEGER:
                return components::types::physical_value(value.value<uint32_t>());
            case logical_type::INTEGER:
                return components::types::physical_value(value.value<int32_t>());
            case logical_type::UBIGINT:
                return components::types::physical_value(value.value<uint64_t>());
            case logical_type::BIGINT:
                return components::types::physical_value(value.value<int64_t>());
            // TODO: physical_value does not support 128 bit integers for now
            // case logical_type::UHUGEINT:
            //     return components::types::physical_value(value.value<components::types::uint128_t>());
            // case logical_type::HUGEINT:
            //     return components::types::physical_value(value.value<components::types::int128_t>());
            case logical_type::FLOAT:
                return components::types::physical_value(value.value<float>());
            case logical_type::DOUBLE:
                return components::types::physical_value(value.value<double>());
            case logical_type::STRING_LITERAL:
                return components::types::physical_value(*value.value<std::string*>());
            // The temporal types are raw counters physically: DATE an INT32 day count, the
            // other three INT64 microsecond counts. Encoding the counter gives physical_value
            // exactly the column's ordering, and read_logical_value_as_view decodes the stored
            // keys to the same INT32/INT64, so tree comparison, probes and bounds all agree.
            case logical_type::DATE:
                return components::types::physical_value(value.value<int32_t>());
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                return components::types::physical_value(value.value<int64_t>());
            case logical_type::NA:
                return components::types::physical_value();
            default:
                // Unreachable from user data: CREATE INDEX refuses every key type this switch
                // does not carry (is_representable_index_key_type in
                // components/index/logical_value_binary_codec.hpp — the ONE authoritative
                // list) with index_create_fail before any row reaches an encoder. A key
                // arriving here is a gate/encoder drift bug, and the old `return NA` answer
                // was worse than a crash: under NDEBUG it collapsed every key of the column
                // to one NA value and served wrong rows. Die loudly instead.
                // NDEBUG coverage gap, stated plainly: the main suite builds Debug+DEV_MODE,
                // where the assert aborts first — the std::abort() below (and the silent-NA
                // behaviour it replaces) is NOT exercised by any test.
                assert(false && "services::index::convert: key type not representable in physical_value");
                std::abort();
        }
    }

    btree_index_disk_t::btree_index_disk_t(const path_t& path,
                                           std::pmr::memory_resource* resource,
                                           uint64_t flush_threshold)
        : index_disk_t(flush_threshold)
        , path_(path)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , db_(std::make_unique<btree_t>(resource_, fs_, path, item_key_getter)) {
        db_->load();
    }

    btree_index_disk_t::~btree_index_disk_t() = default;

    void btree_index_disk_t::insert(const value_t& key, size_t value) {
        auto values = find(key);
        if (std::find(values.begin(), values.end(), value) == values.end()) {
            values.push_back(value);
            std::pmr::string out(resource_);
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(value));
            db_->append(out.data(), static_cast<uint32_t>(out.size()));
            mark_operation_dirty();
            flush_if_needed();
        }
    }

    void btree_index_disk_t::remove(value_t key) {
        db_->remove_index(convert(key));
        mark_operation_dirty();
        flush_if_needed();
    }

    void btree_index_disk_t::remove(const value_t& key, size_t row_id) {
        auto values = find(key);
        if (!values.empty()) {
            values.erase(std::remove(values.begin(), values.end(), row_id), values.end());
            std::pmr::string out(resource_);
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row_id));
            db_->remove(out.data(), static_cast<uint32_t>(out.size()));
            mark_operation_dirty();
            flush_if_needed();
        }
    }

    void btree_index_disk_t::flush_if_needed() {
        if (should_flush()) {
            auto flush_error = force_flush();
            if (flush_error.type != core::error_code_t::none) {
                return;
            }
        }
    }

    void btree_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        // Bulk fast path: append (key,value) WITHOUT the per-insert find() dedup
        // (insert()'s O(items-per-key) scan + binary decode) and WITHOUT a per-insert
        // flush. The caller (bulk load / repopulate) guarantees uniqueness, so the
        // dedup is unnecessary; force_flush() persists once at the end. This turns a
        // bulk load from O(rows^2) into O(rows).
        std::pmr::string out(resource_);
        components::index::codec::append_logical_value(out, key);
        components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(value));
        db_->append(out.data(), static_cast<uint32_t>(out.size()));
        mark_operation_dirty();
    }

    void btree_index_disk_t::remove_bulk_unchecked(const value_t& key, size_t row_id) {
        // Bulk fast path: erase the (key,row_id) entry directly WITHOUT the per-remove
        // find() guard. The caller guarantees the entry is present; force_flush() once.
        std::pmr::string out(resource_);
        components::index::codec::append_logical_value(out, key);
        components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row_id));
        db_->remove(out.data(), static_cast<uint32_t>(out.size()));
        mark_operation_dirty();
    }

    core::error_t btree_index_disk_t::force_flush() {
        if (is_dirty() && db_) {
            if (!db_->flush()) {
                // The tree keeps the failed leaves dirty, so a later flush can still succeed —
                // but this attempt did not persist, and the caller must not be told otherwise.
                return core::error_t{core::error_code_t::io_error,
                                     std::pmr::string{"btree index flush failed to reach the disk", resource_}};
            }
            reset_flush_state();
        }
        return core::error_t::no_error();
    }

    void btree_index_disk_t::find(const value_t& value, result& res) const {
        auto index = convert(value);
        size_t count = db_->item_count(index);
        res.reserve(count);
        for (size_t i = 0; i < count; i++) {
            res.emplace_back(id_getter(db_->get_item(index, i)).value<components::types::physical_type::UINT64>());
        }
    }

    btree_index_disk_t::result btree_index_disk_t::find(const value_t& value) const {
        btree_index_disk_t::result res;
        find(value, res);
        return res;
    }

    void btree_index_disk_t::lower_bound(const value_t& value, result& res) const {
        auto max_index = convert(value);
        db_->scan_ascending(
            std::numeric_limits<btree_t::index_t>::min(),
            max_index,
            size_t(-1),
            &res,
            [](void* data, size_t size) -> size_t {
                return id_getter(btree_t::item_data{static_cast<data_ptr_t>(data), static_cast<uint32_t>(size)})
                    .value<components::types::physical_type::UINT64>();
            },
            [&max_index](const auto& index, const auto&) { return index != max_index; });
    }

    btree_index_disk_t::result btree_index_disk_t::lower_bound(const value_t& value) const {
        btree_index_disk_t::result res;
        lower_bound(value, res);
        return res;
    }

    void btree_index_disk_t::upper_bound(const value_t& value, result& res) const {
        auto min_index = convert(value);
        db_->scan_decending(
            convert(value),
            std::numeric_limits<btree_t::index_t>::max(),
            size_t(-1),
            &res,
            [](void* data, size_t size) -> size_t {
                return id_getter(btree_t::item_data{static_cast<data_ptr_t>(data), static_cast<uint32_t>(size)})
                    .value<components::types::physical_type::UINT64>();
            },
            [&min_index](const auto& index, const auto&) { return index != min_index; });
    }

    btree_index_disk_t::result btree_index_disk_t::upper_bound(const value_t& value) const {
        btree_index_disk_t::result res;
        upper_bound(value, res);
        return res;
    }

    void btree_index_disk_t::drop() {
        db_.reset();
        core::filesystem::remove_directory(fs_, path_);
    }

    void btree_index_disk_t::clear() {
        // Wipe tree contents in place but keep the index writable: drop the
        // on-disk tree directory, then re-create an empty btree at the same
        // path. load() on a freshly created directory yields an empty tree,
        // so subsequent inserts repopulate cleanly. Unlike drop(), the
        // instance stays alive and usable.
        db_.reset();
        core::filesystem::remove_directory(fs_, path_);
        db_ = std::make_unique<btree_t>(resource_, fs_, path_, item_key_getter);
        db_->load();
        reset_flush_state();
    }

} // namespace services::index
