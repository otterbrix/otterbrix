#include "btree_index_disk.hpp"

#include "btree_record_codec.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <cassert>
#include <cstdlib>

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
            // Temporal types are raw counters physically (DATE = INT32 days, others INT64 microseconds).
            case logical_type::DATE:
                return components::types::physical_value(value.value<int32_t>());
            case logical_type::TIME:
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                return components::types::physical_value(value.value<int64_t>());
            case logical_type::NA:
                return components::types::physical_value();
            default:
                // Unreachable from user data; untested since the suite runs Debug+DEV_MODE.
                assert(false && "services::index::convert: key type not representable in physical_value");
                std::abort();
        }
    }

    btree_index_disk_t::btree_index_disk_t(const path_t& path,
                                           std::pmr::memory_resource* resource,
                                           uint64_t flush_threshold)
        : resource_(resource)
        , flush_threshold_(flush_threshold)
        , path_(path)
        , fs_(core::filesystem::local_file_system_t())
        , db_(std::make_unique<btree_t>(resource, fs_, path, item_key_getter)) {
        db_->load();
    }

    // A NULL key is never stored/looked up, same rule as index_agent_contract.hpp's index_key_is_null.
    bool btree_index_disk_t::key_is_absent(const value_t& key) noexcept { return key.is_null(); }

    btree_index_disk_t::~btree_index_disk_t() = default;

    namespace {
        // Unread, a walk over an unreadable block comes back SHORT with no_error() (an accepted duplicate for
        // UNIQUE, a lost parent for an FK); sticky by design, so a damaged tree refuses every later question.
        core::error_t tree_load_refusal(core::b_plus_tree::load_failure_t failure,
                                        std::pmr::memory_resource* resource) {
            using core::b_plus_tree::load_failure_t;
            const auto code = failure == load_failure_t::data_corruption ? core::error_code_t::data_corruption
                              : failure == load_failure_t::out_of_memory ? core::error_code_t::out_of_memory
                                                                         : core::error_code_t::io_error;
            std::pmr::string message{"btree index: the tree reports an unresolved load failure: ", resource};
            message += core::b_plus_tree::to_string(failure);
            return core::error_t{code, std::move(message)};
        }

        [[nodiscard]] core::error_t consult_failure_channel(const core::b_plus_tree::btree_t& db,
                                                            std::pmr::memory_resource* resource) {
            const auto failure = db.load_failure();
            if (failure == core::b_plus_tree::load_failure_t::none) {
                return core::error_t::no_error();
            }
            return tree_load_refusal(failure, resource);
        }
    } // namespace

    core::error_t btree_index_disk_t::insert(const value_t& key, size_t value) {
        if (key_is_absent(key)) {
            return core::error_t::no_error();
        }
        result values(resource());
        if (auto probe_error = find(key, values); probe_error.contains_error()) {
            return probe_error;
        }
        if (std::find(values.begin(), values.end(), value) == values.end()) {
            std::pmr::string out(resource());
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(value));
            db_->append(out.data(), static_cast<uint32_t>(out.size()));
            mark_operation_dirty();
            RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
            return flush_if_needed();
        }
        return core::error_t::no_error();
    }

    core::error_t btree_index_disk_t::remove(value_t key) {
        if (key_is_absent(key)) {
            return core::error_t::no_error();
        }
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        db_->remove_index(convert(key));
        mark_operation_dirty();
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        return flush_if_needed();
    }

    core::error_t btree_index_disk_t::remove(const value_t& key, size_t row_id) {
        if (key_is_absent(key)) {
            return core::error_t::no_error();
        }
        result values(resource());
        if (auto probe_error = find(key, values); probe_error.contains_error()) {
            return probe_error;
        }
        if (!values.empty()) {
            std::pmr::string out(resource());
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row_id));
            db_->remove(out.data(), static_cast<uint32_t>(out.size()));
            mark_operation_dirty();
            RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
            return flush_if_needed();
        }
        return core::error_t::no_error();
    }

    core::error_t btree_index_disk_t::flush_if_needed() {
        if (should_flush()) {
            return force_flush();
        }
        return core::error_t::no_error();
    }

    void btree_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        if (key_is_absent(key)) {
            return;
        }
        std::pmr::string out(resource());
        components::index::codec::append_logical_value(out, key);
        components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(value));
        db_->append(out.data(), static_cast<uint32_t>(out.size()));
        mark_operation_dirty();
    }

    void btree_index_disk_t::remove_bulk_unchecked(const value_t& key, size_t row_id) {
        if (key_is_absent(key)) {
            return;
        }
        std::pmr::string out(resource());
        components::index::codec::append_logical_value(out, key);
        components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row_id));
        db_->remove(out.data(), static_cast<uint32_t>(out.size()));
        mark_operation_dirty();
    }

    core::error_t btree_index_disk_t::force_flush() {
        if (db_) {
            RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        }
        if (is_dirty() && db_) {
            if (!db_->flush()) {
                return core::error_t{core::error_code_t::io_error,
                                     std::pmr::string{"btree index flush failed to reach the disk", resource()}};
            }
            reset_flush_state();
        }
        return core::error_t::no_error();
    }

    namespace {
        struct record_row_reader_t {
            bool last_ok{true};
            bool all_ok{true};

            size_t operator()(void* data, size_t size) {
                bool ok = true;
                const auto id =
                    id_of(btree_t::item_data{static_cast<data_ptr_t>(data), static_cast<uint32_t>(size)}, ok);
                last_ok = ok;
                all_ok = all_ok && ok;
                return ok ? id.value<components::types::physical_type::UINT64>() : 0;
            }
        };

        core::error_t unreadable_record(std::pmr::memory_resource* resource) {
            return core::error_t{core::error_code_t::data_corruption,
                                 std::pmr::string{"btree index: a stored record's key could not be decoded", resource}};
        }
    } // namespace

    core::error_t btree_index_disk_t::find(const value_t& value, result& res) const {
        if (key_is_absent(value)) {
            return core::error_t::no_error();
        }
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        auto index = convert(value);
        size_t count = db_->item_count(index);
        res.reserve(res.size() + count);
        for (size_t i = 0; i < count; i++) {
            bool ok = true;
            const auto id = id_of(db_->get_item(index, i), ok);
            if (!ok) {
                return unreadable_record(resource());
            }
            res.emplace_back(id.value<components::types::physical_type::UINT64>());
        }
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        return core::error_t::no_error();
    }

    core::error_t btree_index_disk_t::scan_range(components::expressions::compare_type compare,
                                                 const value_t& value,
                                                 result& res) const {
        using components::expressions::compare_type;

        if (key_is_absent(value)) {
            return core::error_t::no_error();
        }
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));

        // Both scan_ascending bounds are inclusive: lt/gt exclude the probe's own key via their predicate.
        const auto probe = convert(value);
        record_row_reader_t reader;
        const auto read_row = [&reader](void* data, size_t size) { return reader(data, size); };
        const auto readable = [&reader](auto keep) {
            return [&reader, keep](const auto& index, const auto& row) { return reader.last_ok && keep(index, row); };
        };
        const auto ascending = [&](const auto& lo, const auto& hi, auto keep) {
            db_->scan_ascending(lo, hi, size_t(-1), &res, read_row, readable(keep));
        };
        const auto keep_all = [](const auto&, const auto&) { return true; };

        switch (compare) {
            case compare_type::eq:
                return find(value, res);
            case compare_type::lt:
                ascending(std::numeric_limits<btree_t::index_t>::min(),
                          probe,
                          [&probe](const auto& index, const auto&) { return index < probe; });
                break;
            case compare_type::lte:
                ascending(std::numeric_limits<btree_t::index_t>::min(), probe, keep_all);
                break;
            case compare_type::gt:
                ascending(probe,
                          std::numeric_limits<btree_t::index_t>::max(),
                          [&probe](const auto& index, const auto&) { return index > probe; });
                break;
            case compare_type::gte:
                ascending(probe, std::numeric_limits<btree_t::index_t>::max(), keep_all);
                break;
            case compare_type::ne:
                db_->full_scan(&res, read_row, readable([&probe](const auto& index, const auto&) {
                    return index != probe;
                }));
                break;
            default:
                assert(false && "btree_index_disk_t::scan_range: predicate is not a value comparison");
                std::abort();
        }
        if (!reader.all_ok) {
            return unreadable_record(resource());
        }
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        return core::error_t::no_error();
    }

    void btree_index_disk_t::drop() {
        db_.reset();
        core::filesystem::remove_directory(fs_, path_);
    }

    core::error_t btree_index_disk_t::clear() {
        // Wipes tree contents in place (unlike drop(), the instance stays usable): re-creates an empty btree.
        db_.reset();
        const bool directory_removed = core::filesystem::remove_directory(fs_, path_);
        db_ = std::make_unique<btree_t>(resource(), fs_, path_, item_key_getter);
        db_->load();
        reset_flush_state();
        if (!directory_removed) {
            return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"btree: the index directory " + path_.string() + " could not be removed for a clear",
                                 resource()}};
        }
        return core::error_t::no_error();
    }

    // apply_txn_inserts, apply_txn_deletes and set_bulk_mode are absent on purpose: this store
    // owns no txn log and no bulk window, so they would be unreachable stubs.

} // namespace services::index
