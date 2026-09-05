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
            // Temporal types are raw counters physically (DATE = INT32 day count, the other
            // three INT64 microseconds); encoding the counter keeps tree comparison, probes,
            // and bounds consistent with what read_logical_value_as_view decodes back.
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
                // doesn't carry (is_representable_index_key_type) before any row reaches an
                // encoder. A `return NA` here would silently collapse every key to one value
                // under NDEBUG. Untested: the main suite builds Debug+DEV_MODE, where the
                // assert fires first, so this std::abort() is never exercised.
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

    // A NULL key is never stored/looked up (same rule as index_agent_contract.hpp's
    // index_key_is_null, enforced here too since backend tests reach this class directly).
    // Admitting one would map to the NA physical_value = numeric_limits<physical_value>::max(),
    // sorting after every real key and polluting every upper-bound/gte answer. Reads answer
    // empty rather than failing: `col <op> NULL` is UNKNOWN for every row in SQL.
    bool btree_index_disk_t::key_is_absent(const value_t& key) noexcept { return key.is_null(); }

    btree_index_disk_t::~btree_index_disk_t() = default;

    namespace {
        // btree_t::load_failure() is reported into by every leaf; leaving it unread means a walk
        // over an unreadable block comes back SHORT with no_error() (an accepted duplicate for a
        // UNIQUE constraint, a lost parent for an FK). Sticky by design -- peeked, never taken --
        // so a store that once served out of a damaged tree refuses every later question until
        // rebuilt (clear() or reopen). Checked BEFORE the operation too, so a damaged block isn't
        // re-read on every access.
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
        // The dedup probe is written into a result on THIS index's resource. A by-value find()
        // whose default-constructed vector carries no resource would put the process default
        // resource on the write path.
        result values(resource());
        // A read that couldn't decode a record can't answer "this pair is not there yet";
        // writing anyway risks a duplicate entry, so the probe's refusal fails the write.
        if (auto probe_error = find(key, values); probe_error.contains_error()) {
            return probe_error;
        }
        if (std::find(values.begin(), values.end(), value) == values.end()) {
            std::pmr::string out(resource());
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(value));
            db_->append(out.data(), static_cast<uint32_t>(out.size()));
            mark_operation_dirty();
            // The append itself walks and splits leaves; a block it could not read is on
            // the channel now and the write may not report success over it.
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
        // Same as insert(): an unfinished probe would read as "this key holds nothing" and
        // skip a removal that is owed.
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

    // Must propagate force_flush's io_error here -- nothing downstream re-checks, so
    // swallowing it would report the same silence for a persisted and unpersisted index.
    core::error_t btree_index_disk_t::flush_if_needed() {
        if (should_flush()) {
            return force_flush();
        }
        return core::error_t::no_error();
    }

    void btree_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        // Bulk fast path: skips insert()'s per-row find() dedup and flush. Caller guarantees
        // each (key, row_id) PAIR is fed at most once -- not unique keys. O(rows) vs O(rows^2).
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
        // Bulk fast path: erase the (key,row_id) entry directly WITHOUT the per-remove
        // find() guard. The caller guarantees the entry is present; force_flush() once.
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
            // Must refuse rather than tell a checkpoint "flushed" over a tree holding an
            // unreadable block -- also where a bulk load's refused block first surfaces,
            // since insert_bulk_unchecked/remove_bulk_unchecked have no channel of their own.
            RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        }
        if (is_dirty() && db_) {
            if (!db_->flush()) {
                // The tree keeps the failed leaves dirty, so a later flush can still succeed —
                // but this attempt did not persist, and the caller must not be told otherwise.
                return core::error_t{core::error_code_t::io_error,
                                     std::pmr::string{"btree index flush failed to reach the disk", resource()}};
            }
            reset_flush_state();
        }
        return core::error_t::no_error();
    }

    namespace {
        // A state object, not a function: btree_t's Deserializer (must return size_t) and
        // Predicate run back-to-back on the same record with no room to signal "unreadable",
        // so the verdict is carried out here instead. Without it, an undecodable key would
        // silently contribute row id 0; `all_ok` fails the whole read instead.
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
            return core::error_t{
                core::error_code_t::data_corruption,
                std::pmr::string{"btree index: a stored record's key could not be decoded", resource}};
        }
    } // namespace

    core::error_t btree_index_disk_t::find(const value_t& value, result& res) const {
        if (key_is_absent(value)) {
            return core::error_t::no_error();
        }
        // Before the walk: a tree that already failed to read a block refuses the question
        // instead of re-reading the damaged block on every probe.
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        auto index = convert(value);
        size_t count = db_->item_count(index);
        res.reserve(res.size() + count);
        for (size_t i = 0; i < count; i++) {
            bool ok = true;
            const auto id = id_of(db_->get_item(index, i), ok);
            if (!ok) {
                // STOP HERE, and do not emplace. The alternative is handing back row id 0,
                // which the reader cannot tell from a real row 0.
                return unreadable_record(resource());
            }
            res.emplace_back(id.value<components::types::physical_type::UINT64>());
        }
        // AFTER the walk: this probe's own refused block is on the channel now, and a
        // short answer with no_error() is exactly the wrong answer this closes.
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

        // Both scan_ascending bounds are inclusive, which is what makes lte/gte expressible
        // without a predicate; lt/gt are the same ray minus the probe's own key via their
        // predicate. All arms walk ascending, so no predicate returns rows in reverse order.
        const auto probe = convert(value);
        // ONE reader for the whole walk, referenced by the deserializer the tree copies.
        record_row_reader_t reader;
        const auto read_row = [&reader](void* data, size_t size) { return reader(data, size); };
        // A record the codec refused never reaches the answer, whatever the predicate says.
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
                // Not a bounded ray: every key except one, so the whole tree is walked.
                // Expensive and honest. The alternative this replaces was an ordered
                // facade that had no way to answer `ne` at all, which read as zero rows.
                db_->full_scan(&res,
                               read_row,
                               readable([&probe](const auto& index, const auto&) { return index != probe; }));
                break;
            default:
                // Unreachable: the planner routes only these six here (create_plan_match), and
                // manager_index_t refuses a range predicate on an unordered backend before
                // dispatch. An empty answer would hide a routing bug as "no rows match".
                assert(false && "btree_index_disk_t::scan_range: predicate is not a value comparison");
                std::abort();
        }
        // ONE exit for the five walking arms: the answer stands only if every record the walk
        // touched decoded. A subset would read as "these are the rows", which is the wrong
        // answer this channel exists to prevent.
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
        // Wipes tree contents in place, keeping the index writable: drops the directory and
        // re-creates an empty btree at the same path (unlike drop(), the instance stays usable).
        db_.reset();
        // The one refusal this function can observe: if the directory won't remove, load()
        // below reads the old tree straight back, so the index keeps every row this call
        // promised to erase unless the failure is reported.
        const bool directory_removed = core::filesystem::remove_directory(fs_, path_);
        // Rebuilt whether or not the directory went: every other door on this class
        // dereferences db_, so returning early and leaving it null would crash the next read.
        // Over a surviving directory, load() honestly brings the old contents back.
        db_ = std::make_unique<btree_t>(resource(), fs_, path_, item_key_getter);
        // btree_t::load() is void, so a read failure here isn't observable at this line -- but
        // it lands on the tree's own channel, and the first consult_failure_channel refuses.
        db_->load();
        reset_flush_state();
        if (!directory_removed) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"btree: the index directory " + path_.string() +
                                                      " could not be removed for a clear",
                                                  resource()}};
        }
        return core::error_t::no_error();
    }

    // apply_txn_inserts, apply_txn_deletes, and set_bulk_mode are absent on purpose: this
    // store owns no txn log and no bulk window, so they'd be unreachable stubs. The routing
    // question they'd answer is resolved by the type btree_index_agent_t holds, not at runtime.

} // namespace services::index
