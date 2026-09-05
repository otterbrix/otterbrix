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
        : resource_(resource)
        , flush_threshold_(flush_threshold)
        , path_(path)
        , fs_(core::filesystem::local_file_system_t())
        , db_(std::make_unique<btree_t>(resource, fs_, path, item_key_getter)) {
        db_->load();
    }

    // A NULL key is never stored and is never looked up. The invariant, and the reasons
    // for it, are written down once in services/index/index_agent_contract.hpp
    // (index_key_is_null, "An index stores exactly the NON-NULL keys of the live rows");
    // this is the same rule enforced where the STORE can enforce it, because the agent is
    // not the only door into this class -- the backend tests reach it directly.
    //
    // The cost of admitting one is specific here, not abstract: convert() maps a NULL to
    // the NA physical_value, and NA is exactly what numeric_limits<physical_value>::max()
    // returns. A stored NULL therefore sorts after every real key, so it joins EVERY
    // upper-bound and gte answer the tree gives -- and it does so as a row id the reader
    // takes at face value.
    //
    // Reads answer empty rather than failing: `col <op> NULL` is UNKNOWN for every row, so
    // "no rows" is the true SQL answer, not a degraded one.
    bool btree_index_disk_t::key_is_absent(const value_t& key) noexcept { return key.is_null(); }

    btree_index_disk_t::~btree_index_disk_t() = default;

    namespace {
        // THE TREE'S REFUSAL CHANNEL, ANSWERED AT LAST (wave #324, closing #331 with it).
        // btree_t::load_failure() is reported into by every leaf, and NOTHING above
        // core/b_plus_tree ever read it: a walk over a block the tree could not read came
        // back SHORT with no_error(). For a UNIQUE constraint that is an accepted
        // duplicate; for a FK a lost parent. The check is STICKY on purpose -- the cell is
        // peeked, never taken -- so a store that has once served out of a damaged tree
        // refuses every later question until the tree is rebuilt (clear() constructs a
        // fresh btree_t, and so does reopening the index): a wedged-loud index is
        // recoverable, a silently short one is not. The pre-op check is also what removes
        // the degraded state's price (#331): the walk that would re-read the damaged
        // 256 KB block on every access is refused before it starts, so the re-read
        // happens once -- on the operation that met the damage.
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
        // The dedup probe is written into a result on THIS index's resource. It used to go
        // through the by-value find() virtual, whose default-constructed vector put the
        // process default resource on the write path (C7).
        result values(resource());
        // THE DEDUP PROBE IS A READ, and a read that could not decode a record cannot answer
        // "this pair is not there yet". Writing over that would append a duplicate entry into
        // a tree that already holds one, so the refusal fails the write.
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

    // THE THRESHOLD FLUSH IS THE WRITE, and its failure used to end here: force_flush's
    // io_error was bound to a local and `return`ed away, so an index whose entries never
    // reached the device reported the same silence as one that did. Nothing downstream
    // re-checks. The answer travels to whoever asked for the write now.
    core::error_t btree_index_disk_t::flush_if_needed() {
        if (should_flush()) {
            return force_flush();
        }
        return core::error_t::no_error();
    }

    void btree_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        // Bulk fast path: append (key,value) WITHOUT the per-insert find() dedup
        // (insert()'s O(items-per-key) scan + binary decode) and WITHOUT a per-insert
        // flush. What the caller guarantees is that each (key, row_id) PAIR is fed at most
        // once — NOT that keys are unique, which a non-unique index breaks by definition
        // (see index_disk.hpp) — so the dedup has nothing to do; force_flush() persists
        // once at the end. This turns a bulk load from O(rows^2) into O(rows).
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
            // A checkpoint must not be told "flushed" over a tree that has served (or
            // holds) a block it could not read: the WAL behind it would be trimmed. The
            // bulk_unchecked doors have no channel of their own, so this is also where a
            // bulk load's refused block surfaces.
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
        // WHY THIS IS A STATE OBJECT AND NOT A FUNCTION ANY MORE. btree_t hands a scan its
        // Deserializer BY VALUE and its Predicate right behind it, and calls the two
        // back-to-back on the SAME record (core/b_plus_tree/b_plus_tree.hpp, scan_ascending /
        // full_scan). Neither signature has room for "I could not read this one" -- the
        // deserializer must produce a size_t -- so the verdict is carried out of the callback
        // in this object, which the scan below references rather than copies.
        //
        // WHAT IT BUYS, precisely: a record whose key the codec refuses is DROPPED from the
        // answer instead of contributing read_le_raw's T{}, i.e. ROW ID 0, WHICH IS A
        // LEGITIMATE ROW ID. `all_ok` then fails the whole read, because a partial answer from
        // an index is a wrong answer, not a fast one.
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
        // BEFORE the walk: a tree that has already failed to read a block refuses the
        // question instead of walking again (and instead of re-reading the damaged block
        // on every probe -- the #331 price).
        RETURN_IF_ERROR(consult_failure_channel(*db_, resource()));
        auto index = convert(value);
        size_t count = db_->item_count(index);
        res.reserve(res.size() + count);
        for (size_t i = 0; i < count; i++) {
            bool ok = true;
            const auto id = id_of(db_->get_item(index, i), ok);
            if (!ok) {
                // STOP HERE, and do not emplace. The alternative is the row id 0 this used to
                // hand back, which the reader cannot tell from a real row 0.
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

        // Both scan_ascending bounds are INCLUSIVE, which is what makes lte and gte
        // expressible at all: the ray simply runs to the probe and stops, with no
        // predicate excluding it. lt and gt are the same ray minus the probe's own key,
        // and that exclusion is the ONLY job their predicate has.
        //
        // Every arm walks ASCENDING. gt used to be a scan_decending, so it was the one
        // predicate whose rows arrived reversed relative to the other five.
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
                // Only the six value comparisons above can reach an index: the planner
                // routes nothing else here (create_plan_match), and manager_index_t
                // refuses a range predicate on a backend with no ordering before the read
                // is ever dispatched. Anything else is a routing bug, and an empty answer
                // would hide it behind "no rows match".
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
        // Wipe tree contents in place but keep the index writable: drop the
        // on-disk tree directory, then re-create an empty btree at the same
        // path. load() on a freshly created directory yields an empty tree,
        // so subsequent inserts repopulate cleanly. Unlike drop(), the
        // instance stays alive and usable.
        db_.reset();
        // THE ONE REFUSAL THIS FUNCTION CAN OBSERVE, and it used to be dropped: a directory
        // that would not go leaves the whole tree on the device, and the load() below reads
        // it straight back -- so the index goes on answering with every row this call
        // promised to erase, and index_agent_contract::clear reported success over it.
        const bool directory_removed = core::filesystem::remove_directory(fs_, path_);
        // THE TREE IS REBUILT WHETHER OR NOT THE DIRECTORY WENT, and returning above this
        // line would be the bug rather than the fix: every other door on this class
        // dereferences db_, so a store left holding none would turn the next read into a
        // crash. Over a surviving directory load() brings the old contents back, which is
        // the honest state -- nothing was wiped, and the return value says so.
        db_ = std::make_unique<btree_t>(resource(), fs_, path_, item_key_getter);
        // btree_t::load() IS VOID (core/b_plus_tree/b_plus_tree.hpp), so a tree that could
        // not be read back after this wipe is still not observable from here. Naming it
        // rather than papering over it: the fix is an error channel inside
        // core/b_plus_tree, which is a wider surface than this change owns.
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

    // THREE MEMBERS ARE GONE FROM HERE, and the absence is the change. apply_txn_inserts,
    // apply_txn_deletes and set_bulk_mode existed only because the erased base declared
    // them: this store owns no transaction log and has no bulk window to open, so all
    // three were abort-or-nothing stubs that no caller could reach. The routing question
    // they answered (has_txn_log()) is answered by the type btree_index_agent_t holds.

} // namespace services::index
