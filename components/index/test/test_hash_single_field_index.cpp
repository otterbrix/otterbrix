#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <memory>

#include "components/index/disk_hash_single_field_index.hpp"
#include "components/index/hash_single_field_index.hpp"
#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;
using key = components::expressions::key_t;

namespace {
    enum class hash_index_mode
    {
        in_memory,
        on_disk
    };

    // Indexes are oid-identified; `name` only labels the scratch file on disk.
    constexpr components::catalog::oid_t test_index_oid = 501u;

    // The disk facade opens NOTHING (C2c, rule 10): its committed rows live in the store
    // its disk agent owns, so there is no file and no handle to build here -- the hashed
    // facade takes neither. `name` used to label the scratch file it no longer has.
    std::unique_ptr<index_t>
    make_hash_index(std::pmr::memory_resource* resource, const std::string& /*name*/, hash_index_mode mode) {
        if (mode == hash_index_mode::in_memory) {
            return std::make_unique<hash_single_field_index_t>(resource,
                                                               test_index_oid,
                                                               keys_base_storage_t{key(resource, "count")});
        }
        return std::make_unique<disk_hash_single_field_index_t>(resource,
                                                                test_index_oid,
                                                                keys_base_storage_t{key(resource, "count")});
    }

    // The rows this index answers with for `= key`, asked through the door the mode
    // actually has.
    //
    // An in-memory index keeps committed and pending entries in ONE structure and answers
    // both from find(). The disk facade keeps only the pending half and its local doors
    // are terminal, because a door that answered from that half alone would return a
    // SUBSET of the rows the key carries and no caller could tell (rule 6). Its
    // production door is merge_uncommitted_rows, which manager_index_t calls with the
    // committed half the disk agent returned; these cases have no agent, so the half
    // handed in is empty -- exactly what find() used to read out of the never-written
    // keydir before C2c removed the handle to it.
    std::vector<int64_t> search_rows(index_t& index,
                                     hash_index_mode mode,
                                     components::expressions::compare_type compare,
                                     const components::types::logical_value_t& value,
                                     uint64_t start_time,
                                     uint64_t txn_id) {
        if (mode == hash_index_mode::in_memory) {
            auto found = index.search(compare, value, start_time, txn_id, {});
            return {found.begin(), found.end()};
        }
        std::pmr::vector<int64_t> rows(index.resource());
        index.merge_uncommitted_rows(compare, value, txn_id, {}, rows);
        return {rows.begin(), rows.end()};
    }

    // The txn bucket the disk arm writes through. Bucket 0 is "committed for everyone but
    // not yet mirrored to disk", which is the state these cases used to fake by writing
    // through the non-txn door and having it do nothing.
    constexpr uint64_t committed_bucket = 0;

    void insert_row(index_t& index,
                    hash_index_mode mode,
                    const components::types::logical_value_t& value,
                    int64_t row_index) {
        if (mode == hash_index_mode::in_memory) {
            index.insert(value, row_index, {});
            return;
        }
        // The disk facade's NON-txn insert is terminal, not a no-op: a no-op would drop
        // the row with nothing reporting it (rule 6). Its write door is the txn one.
        index.insert(value, row_index, committed_bucket, {});
    }

    void run_base_contract(hash_index_mode mode) {
        auto resource = core::pmr::otterbrix_resource();
        auto index =
            make_hash_index(&resource, mode == hash_index_mode::in_memory ? "hash_count_ram" : "hash_count_disk", mode);
        std::vector<std::pair<int64_t, int64_t>> data =
            {{0, 0}, {1, 1}, {10, 2}, {5, 3}, {6, 4}, {2, 5}, {8, 6}, {13, 7}};

        for (const auto& [value, row_idx] : data) {
            components::types::logical_value_t val(&resource, value);
            insert_row(*index, mode, val, row_idx);
        }

        components::types::logical_value_t value(&resource, static_cast<int64_t>(10));
        auto rows = search_rows(*index, mode, components::expressions::compare_type::eq, value, 0, 0);
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 2);

        components::types::logical_value_t missing(&resource, static_cast<int64_t>(11));
        REQUIRE(search_rows(*index, mode, components::expressions::compare_type::eq, missing, 0, 0).empty());

        for (const auto& [data_value, row_idx] : data) {
            components::types::logical_value_t val(&resource, data_value);
            insert_row(*index, mode, val, row_idx + 100);
        }
        rows = search_rows(*index, mode, components::expressions::compare_type::eq, value, 0, 0);
        REQUIRE(rows.size() == 2);
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(2)) != rows.end());
        REQUIRE(std::find(rows.begin(), rows.end(), static_cast<int64_t>(102)) != rows.end());
    }

    void run_engine_contract(hash_index_mode mode) {
        auto resource = core::pmr::otterbrix_resource();
        auto index_engine = make_index_engine(&resource);
        uint32_t id = INDEX_ID_UNDEFINED;
        if (mode == hash_index_mode::in_memory) {
            id = make_index<hash_single_field_index_t>(index_engine, 201u, {key(&resource, "count")});
        } else {
            id = make_index<disk_hash_single_field_index_t>(index_engine, 201u, {key(&resource, "count")});
        }

        auto* idx = search_index(index_engine, id);
        REQUIRE(idx != nullptr);

        insert_row(*idx, mode, components::types::logical_value_t(&resource, 0), int64_t(0));
        for (int i = 10; i >= 1; --i) {
            insert_row(*idx, mode, components::types::logical_value_t(&resource, i), int64_t(11 - i));
        }

        components::types::logical_value_t value(&resource, 5);
        auto rows = search_rows(*idx, mode, components::expressions::compare_type::eq, value, 0, 0);
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front() == 6);
    }
} // namespace

TEST_CASE("hash_single_field_index:base") { run_base_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:base") { run_base_contract(hash_index_mode::on_disk); }

TEST_CASE("hash_single_field_index:engine") { run_engine_contract(hash_index_mode::in_memory); }

TEST_CASE("disk_single_field_index:engine") { run_engine_contract(hash_index_mode::on_disk); }

// Same subject as before -- the disk facade materializes no committed state of its own --
// pinned at the point a caller can actually consult, because the direct doors no longer
// answer at all.
//
// They used to be silent no-ops, and find() used to read the keydir through the handle
// C2c removes. With the handle gone the only thing find() could answer from would be the
// txn-local half, i.e. a SUBSET of the rows a key carries, which no caller could tell from
// the whole answer (rule 6) -- so all of them are terminal now, exactly as they already
// were on the ordered facade this one merged with. The testable contract is therefore the
// capability: reads_through_disk_agent() is what manager_index_t consults before it
// decides to send the read to the agent instead of calling find(), and it is what keeps
// callers off those doors.
TEST_CASE("disk_single_field_index:read_only_facade_direct_ops_do_not_materialize_committed_state") {
    auto resource = core::pmr::otterbrix_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_read_only", hash_index_mode::on_disk);
    components::types::logical_value_t value(&resource, int64_t(42));

    REQUIRE(index->reads_through_disk_agent());
    // The counter-example: an in-memory index over the same key holds its own committed
    // rows, so the predicate distinguishes implementations instead of answering true for
    // everything.
    auto in_memory = make_hash_index(&resource, "hash_count_ram_read_only", hash_index_mode::in_memory);
    REQUIRE_FALSE(in_memory->reads_through_disk_agent());

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 1001;
    const uint64_t txn_other = components::table::TRANSACTION_ID_START + 1002;
    index->insert(value, int64_t(2), txn_insert, {});

    auto own_before_commit = search_rows(*index,
                                         hash_index_mode::on_disk,
                                         components::expressions::compare_type::eq,
                                         value,
                                         txn_insert - 1,
                                         txn_insert);
    REQUIRE(own_before_commit.size() == 1);
    REQUIRE(own_before_commit[0] == 2);

    // Committing hands the entry to the disk agent and drops the bucket, so nothing
    // committed is left in the facade -- which is what "materializes no committed state"
    // means now.
    index->commit_insert(txn_insert, 77);
    auto other_after_commit =
        search_rows(*index, hash_index_mode::on_disk, components::expressions::compare_type::eq, value, 78, txn_other);
    REQUIRE(other_after_commit.empty());
}

TEST_CASE("disk_single_field_index:pending_insert_delete_and_txn_state") {
    auto resource = core::pmr::otterbrix_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_txn_state", hash_index_mode::on_disk);

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 201;
    const uint64_t txn_delete = components::table::TRANSACTION_ID_START + 202;
    components::types::logical_value_t key(&resource, int64_t(50));

    index->insert(key, int64_t(700), txn_insert, {});

    std::vector<int64_t> pending_rows;
    for (const auto& pending_entry : index->pending_inserts(txn_insert)) {
        ([&](const components::types::logical_value_t& pending_key, int64_t row_id) {
            REQUIRE(pending_key == key);
            pending_rows.push_back(row_id);
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE(pending_rows.size() == 1);
    REQUIRE(pending_rows.front() == 700);

    auto visible_own_txn = search_rows(*index,
                                       hash_index_mode::on_disk,
                                       components::expressions::compare_type::eq,
                                       key,
                                       txn_insert - 1,
                                       txn_insert);
    REQUIRE(visible_own_txn.size() == 1);
    REQUIRE(visible_own_txn.front() == 700);

    index->mark_delete(key, 700, txn_delete, {});
    std::vector<int64_t> pending_delete_rows;
    for (const auto& pending_entry : index->pending_deletes(txn_delete)) {
        ([&](const components::types::logical_value_t& pending_key, int64_t row_id) {
            REQUIRE(pending_key == key);
            pending_delete_rows.push_back(row_id);
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE(pending_delete_rows.size() == 1);
    REQUIRE(pending_delete_rows.front() == 700);

    // Own delete transaction should not see row anymore.
    auto hidden_for_delete_txn = search_rows(*index,
                                             hash_index_mode::on_disk,
                                             components::expressions::compare_type::eq,
                                             key,
                                             txn_delete - 1,
                                             txn_delete);
    REQUIRE(hidden_for_delete_txn.empty());

    // Commit paths clear pending maps.
    index->commit_insert(txn_insert, 1001);
    index->commit_delete(txn_delete, 1002);

    bool seen_after_commit_insert = false;
    for (const auto& pending_entry : index->pending_inserts(txn_insert)) {
        ([&](const components::types::logical_value_t&, int64_t) {
            seen_after_commit_insert = true;
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE_FALSE(seen_after_commit_insert);

    bool seen_after_commit_delete = false;
    for (const auto& pending_entry : index->pending_deletes(txn_delete)) {
        ([&](const components::types::logical_value_t&, int64_t) {
            seen_after_commit_delete = true;
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE_FALSE(seen_after_commit_delete);
}

TEST_CASE("disk_single_field_index:revert_cleanup_and_clear_memory") {
    auto resource = core::pmr::otterbrix_resource();
    auto index = make_hash_index(&resource, "hash_count_disk_cleanup", hash_index_mode::on_disk);

    const uint64_t txn_insert = components::table::TRANSACTION_ID_START + 301;
    const uint64_t txn_delete = components::table::TRANSACTION_ID_START + 302;
    components::types::logical_value_t key(&resource, int64_t(77));

    index->insert(key, int64_t(900), txn_insert, {});
    index->mark_delete(key, 900, txn_delete, {});

    index->cleanup_versions(123456); // no-op branch for disk facade

    index->revert_insert(txn_insert);
    bool seen_after_revert = false;
    for (const auto& pending_entry : index->pending_inserts(txn_insert)) {
        ([&](const components::types::logical_value_t&, int64_t) {
            seen_after_revert = true;
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE_FALSE(seen_after_revert);

    index->clean_memory_to_new_elements(1);

    bool seen_after_clean_delete = false;
    for (const auto& pending_entry : index->pending_deletes(txn_delete)) {
        ([&](const components::types::logical_value_t&, int64_t) {
            seen_after_clean_delete = true;
        })(pending_entry.key, pending_entry.row_index);
    }
    REQUIRE_FALSE(seen_after_clean_delete);
}

// Same subject -- a HASHED key is normalized (the integer family widened to BIGINT /
// UBIGINT) before it is keyed, so a SMALLINT probe matches a BIGINT-stored key -- asked
// of the door that exists.
//
// It used to write the row straight into the keydir through the facade's storage handle
// and read it back through find(). Both are gone with C2c: the facade has no handle, and
// the committed half is the disk agent's to answer. The normalization is the SAME code on
// the SAME encoder, so the pending half proves it just as well -- and it is where the
// facade does its own encoding, which is what this case is about. The bitcask store
// normalizes identically before hashing (key_bytes_for_hash), which is what makes the two
// halves of one answer key alike.
TEST_CASE("disk_single_field_index:find_reads_disk_and_normalizes_integer_keys") {
    auto resource = core::pmr::otterbrix_resource();
    auto index = std::make_unique<disk_hash_single_field_index_t>(&resource,
                                                                  502u,
                                                                  keys_base_storage_t{key(&resource, "count")});

    // Store the row under a BIGINT key.
    components::types::logical_value_t key_bigint(&resource, int64_t(42));
    index->insert(key_bigint, int64_t(4242), committed_bucket, {});

    // Query with SMALLINT; the widening must make the probe key match the stored key.
    components::types::logical_value_t key_smallint(&resource, int16_t(42));
    auto eq_rows =
        search_rows(*index, hash_index_mode::on_disk, components::expressions::compare_type::eq, key_smallint, 0, 0);
    REQUIRE(eq_rows.size() == 1);
    REQUIRE(eq_rows.front() == 4242);

    // The control: a key nothing carries stays unmatched, so the case cannot pass by
    // matching everything.
    components::types::logical_value_t other(&resource, int16_t(43));
    REQUIRE(search_rows(*index, hash_index_mode::on_disk, components::expressions::compare_type::eq, other, 0, 0)
                .empty());
}

// Same subject as before -- a hashed index cannot answer an ordered probe -- pinned at the
// point a caller can actually consult, because the answer is no longer an exception.
//
// It used to be `REQUIRE_THROWS(index->lower_bound(...))`, and what was thrown was the
// STRING LITERAL "not supported": catchable only as `catch (const char*)`, and raised from
// inside an actor coroutine whose unhandled_exception() is empty, so on the real path it
// was swallowed and the statement reported success over zero rows. Both bound impls are
// now unreachable-and-terminal, so the testable contract is the capability itself:
// supports_ordered_probe() is what manager_index_t consults before dispatching a read, and
// it is what keeps a range predicate away from those impls.
TEST_CASE("disk_single_field_index:lower_upper_bound_not_supported") {
    auto resource = core::pmr::otterbrix_resource();

    auto on_disk = make_hash_index(&resource, "hash_count_disk_bounds", hash_index_mode::on_disk);
    REQUIRE_FALSE(on_disk->supports_ordered_probe());

    auto in_memory = make_hash_index(&resource, "hash_count_ram_bounds", hash_index_mode::in_memory);
    REQUIRE_FALSE(in_memory->supports_ordered_probe());

    // The counter-example: an ordered index over the same key says yes, so the predicate
    // distinguishes implementations rather than answering false for everything.
    single_field_index_t ordered(&resource, test_index_oid, keys_base_storage_t{key(&resource, "count")});
    REQUIRE(ordered.supports_ordered_probe());
}
