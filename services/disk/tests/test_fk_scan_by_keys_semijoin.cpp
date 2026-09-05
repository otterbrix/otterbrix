// Disk single-pass hash semi-join for FK.
//
// scan_by_keys (used by operator_fk_cascade + operator_fk_check) answers a whole key batch
// via fk_hash_semijoin: ONE typed hash of the input key set + ONE streamed pass over the
// table per call — O(table_rows + nkeys), NOT one eq-AND filtered full-table scan per key.
//
// These cases pin that behaviour:
//   * SCAN COUNT: exactly ONE streaming pass per scan_by_keys call (one fetch_next_batch
//     session, ZERO scan_batched calls) regardless of key count. A counting storage
//     decorator observes it.
//   * MULTI-KEY correctness: each key's bucket collects EVERY matching row_id (incl.
//     duplicate table rows); a missing key yields an empty bucket.
//   * HETEROGENEOUS-TYPE FK: an INT32 input key against an INT64 stored key still matches
//     (physical-type normalization). A raw typed hash WITHOUT the normalizing cast would hash
//     the two widths differently and silently report a false FK violation — this asserts the
//     cast is present.
//   * NULL key: an input key-tuple with a NULL cell references nothing -> empty bucket, while
//     the other keys still match.
//   * COMPOSITE key: multi-column key-tuples match on the full tuple only.

#include <catch2/catch_test_macros.hpp>

#include <services/disk/agent_disk.hpp>   // services::disk::fk_hash_semijoin
#include <services/disk/manager_disk.hpp> // services::disk::table_storage_t

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory_resource>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::table;
using namespace components::types;
using namespace components::vector;
using components::storage::scan_position_t;
using components::storage::storage_t;
using components::storage::table_storage_adapter_t;

namespace {

    // storage_t decorator that forwards everything to an inner storage but COUNTS the two scan
    // entry points, so a test can prove scan_by_keys streams the table once per call
    // instead of scanning once per key.
    class counting_storage_t final : public storage_t {
    public:
        explicit counting_storage_t(storage_t& inner)
            : inner_(inner) {}

        std::size_t fetch_session_starts = 0; // fetch_next_batch calls that OPEN a fresh scan (pos.next_row == 0)
        std::size_t fetch_calls = 0;
        std::size_t scan_batched_calls = 0;

        // --- forwarded pure virtuals ---
        // Un-hide the base's txn-taking overloads of the same names (not overridden
        // here — the tests never reach them through this wrapper); without the
        // using-declarations the derived overrides HIDE them and gcc's
        // -Woverloaded-virtual fails the -Werror build.
        using storage_t::append;
        using storage_t::delete_rows;
        using storage_t::scan;
        using storage_t::update;

        std::pmr::vector<complex_logical_type> types() const override { return inner_.types(); }
        const std::vector<column_definition_t>& columns() const override { return inner_.columns(); }
        size_t column_count() const override { return inner_.column_count(); }
        bool has_schema() const override { return inner_.has_schema(); }
        void adopt_schema(const std::pmr::vector<complex_logical_type>& t) override { inner_.adopt_schema(t); }
        void overlay_not_null(const std::string& c) override { inner_.overlay_not_null(c); }
        uint64_t total_rows() const override { return inner_.total_rows(); }
        uint64_t calculate_size() override { return inner_.calculate_size(); }
        void scan(data_chunk_t& o, const table_filter_t* f, int64_t l) override { inner_.scan(o, f, l); }
        [[nodiscard]] core::result_wrapper_t<bool>
        fetch(data_chunk_t& o, const vector_t& ids, uint64_t c, const std::vector<size_t>& p) override {
            return inner_.fetch(o, ids, c, p);
        }
        uint64_t append(data_chunk_t& d) override { return inner_.append(d); }
        void update(vector_t& ids, data_chunk_t& d) override { inner_.update(ids, d); }
        uint64_t delete_rows(vector_t& ids, uint64_t c) override { return inner_.delete_rows(ids, c); }
        std::pmr::memory_resource* resource() const override { return inner_.resource(); }

        // --- instrumented overrides ---
        core::result_wrapper_t<bool> scan_batched(std::pmr::vector<data_chunk_t>& batches,
                                                  const table_filter_t* filter,
                                                  int64_t limit,
                                                  const std::vector<size_t>* projected_cols,
                                                  transaction_data txn) override {
            ++scan_batched_calls;
            return inner_.scan_batched(batches, filter, limit, projected_cols, txn);
        }

        core::result_wrapper_t<bool> fetch_next_batch(data_chunk_t& output,
                                                      scan_position_t& pos,
                                                      const table_filter_t* filter,
                                                      const std::vector<size_t>* projected_cols,
                                                      transaction_data txn) override {
            if (!pos.drained && pos.next_row == 0) {
                ++fetch_session_starts;
            }
            ++fetch_calls;
            return inner_.fetch_next_batch(output, pos, filter, projected_cols, txn);
        }

    private:
        storage_t& inner_;
    };

    void append_chunk(data_table_t& table, data_chunk_t& chunk) {
        table_append_state state(chunk.resource());
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    std::pmr::vector<std::uint64_t> key_indices(std::pmr::memory_resource* res,
                                                std::initializer_list<std::uint64_t> v) {
        std::pmr::vector<std::uint64_t> out{res};
        for (auto x : v) {
            out.push_back(x);
        }
        return out;
    }

    std::set<int64_t> as_set(const std::pmr::vector<int64_t>& v) { return std::set<int64_t>(v.begin(), v.end()); }

    // B4: table_storage_t is backed by a `.otbx` and nothing else — the file-less constructor
    // these cases used went with the in-memory table mode. Each case gets a fresh file; the
    // semi-join under test reads the table through the same storage adapter either way.
    std::filesystem::path semijoin_otbx() {
        static const std::filesystem::path path =
            std::filesystem::path("/tmp") /
            ("test_otterbrix_fk_semijoin_" + std::to_string(::getpid()) + ".otbx");
        return path;
    }

    // Returns the path after removing any file at it, so the caller's manager creates a new one.
    const std::filesystem::path& fresh_semijoin_otbx() {
        static const std::filesystem::path path = [] {
            std::filesystem::remove(semijoin_otbx());
            return semijoin_otbx();
        }();
        std::filesystem::remove(path);
        return path;
    }

} // namespace

TEST_CASE("services::disk::fk_hash_semijoin::multi_key_single_pass") {
    std::pmr::synchronized_pool_resource resource;

    // Parent: id BIGINT (key), tag BIGINT (non-key). Rows (row_ids 0..4): id = 10,20,30,40,20.
    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT);
    cols.emplace_back("tag", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_semijoin_otbx());
    REQUIRE_FALSE(ts.construction_failed());

    const int64_t ids[] = {10, 20, 30, 40, 20};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 5);
        chunk.set_cardinality(5);
        for (uint64_t i = 0; i < 5; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, ids[i]});
            chunk.set_value(1, i, logical_value_t{&resource, static_cast<int64_t>(i * 100)});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);
    counting_storage_t counter(adapter);

    // 4 distinct key-tuples in ONE call: {20, 30, 999(absent), 10}.
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::BIGINT);
    data_chunk_t keys(&resource, ktypes, 4);
    keys.set_cardinality(4);
    const int64_t kv[] = {20, 30, 999, 10};
    for (uint64_t i = 0; i < 4; ++i) {
        keys.set_value(0, i, logical_value_t{&resource, kv[i]});
    }

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, counter, kidx, keys, transaction_data{0, 0});
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();

    REQUIRE(res.size() == 4);
    CHECK(as_set(res[0]) == std::set<int64_t>{1, 4}); // id == 20 -> rows 1 and 4
    CHECK(as_set(res[1]) == std::set<int64_t>{2});    // id == 30 -> row 2
    CHECK(res[2].empty());                            // id == 999 absent
    CHECK(as_set(res[3]) == std::set<int64_t>{0});    // id == 10 -> row 0

    // ONE streaming pass for the whole 4-key batch — NOT one scan per key.
    CHECK(counter.scan_batched_calls == 0);
    CHECK(counter.fetch_session_starts == 1);
}

TEST_CASE("services::disk::fk_hash_semijoin::heterogeneous_type_int32_vs_int64") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT); // stored physical INT64
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_semijoin_otbx());
    REQUIRE_FALSE(ts.construction_failed());

    const int64_t ids[] = {10, 20, 30, 40, 20};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 5);
        chunk.set_cardinality(5);
        for (uint64_t i = 0; i < 5; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, ids[i]});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);
    counting_storage_t counter(adapter);

    // Input keys are INTEGER (INT32) while the stored key is BIGINT (INT64). Normalization must
    // cast the input to the stored physical type so the typed hash aligns; otherwise both keys
    // would fall through to empty buckets (a false FK violation).
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::INTEGER);
    data_chunk_t keys(&resource, ktypes, 2);
    keys.set_cardinality(2);
    keys.set_value(0, 0, logical_value_t{&resource, static_cast<int32_t>(20)});
    keys.set_value(0, 1, logical_value_t{&resource, static_cast<int32_t>(40)});

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, counter, kidx, keys, transaction_data{0, 0});
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();

    REQUIRE(res.size() == 2);
    CHECK(as_set(res[0]) == std::set<int64_t>{1, 4}); // 20 (int32) matches 20 (int64)
    CHECK(as_set(res[1]) == std::set<int64_t>{3});    // 40 matches
    CHECK(counter.scan_batched_calls == 0);
    CHECK(counter.fetch_session_starts == 1);
}

TEST_CASE("services::disk::fk_hash_semijoin::null_key_matches_nothing") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_semijoin_otbx());
    REQUIRE_FALSE(ts.construction_failed());

    const int64_t ids[] = {10, 20, 30, 40};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 4);
        chunk.set_cardinality(4);
        for (uint64_t i = 0; i < 4; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, ids[i]});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);
    counting_storage_t counter(adapter);

    // key 0 is NULL (references nothing), key 1 is 30.
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::BIGINT);
    data_chunk_t keys(&resource, ktypes, 2);
    keys.set_cardinality(2);
    keys.set_value(0, 0, logical_value_t{&resource, static_cast<int64_t>(30)}); // placeholder
    keys.data[0].validity().set_invalid(0);                                     // then NULL it
    keys.set_value(0, 1, logical_value_t{&resource, static_cast<int64_t>(30)});

    auto kidx = key_indices(&resource, {0});
    auto res_r = services::disk::fk_hash_semijoin(&resource, counter, kidx, keys, transaction_data{0, 0});
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();

    REQUIRE(res.size() == 2);
    CHECK(res[0].empty());                         // NULL key -> no match
    CHECK(as_set(res[1]) == std::set<int64_t>{2}); // 30 -> row 2
    CHECK(counter.scan_batched_calls == 0);
    CHECK(counter.fetch_session_starts == 1);
}

TEST_CASE("services::disk::fk_hash_semijoin::composite_key") {
    std::pmr::synchronized_pool_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("a", logical_type::BIGINT);
    cols.emplace_back("b", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_semijoin_otbx());
    REQUIRE_FALSE(ts.construction_failed());

    // Rows (a,b) row_ids 0..3: (1,100),(2,200),(1,200),(2,100).
    const int64_t a_vals[] = {1, 2, 1, 2};
    const int64_t b_vals[] = {100, 200, 200, 100};
    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 4);
        chunk.set_cardinality(4);
        for (uint64_t i = 0; i < 4; ++i) {
            chunk.set_value(0, i, logical_value_t{&resource, a_vals[i]});
            chunk.set_value(1, i, logical_value_t{&resource, b_vals[i]});
        }
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);
    counting_storage_t counter(adapter);

    // 3 composite key-tuples: (2,200), (1,100), (1,999 absent).
    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::BIGINT);
    ktypes.emplace_back(logical_type::BIGINT);
    data_chunk_t keys(&resource, ktypes, 3);
    keys.set_cardinality(3);
    const int64_t ka[] = {2, 1, 1};
    const int64_t kb[] = {200, 100, 999};
    for (uint64_t i = 0; i < 3; ++i) {
        keys.set_value(0, i, logical_value_t{&resource, ka[i]});
        keys.set_value(1, i, logical_value_t{&resource, kb[i]});
    }

    auto kidx = key_indices(&resource, {0, 1});
    auto res_r = services::disk::fk_hash_semijoin(&resource, counter, kidx, keys, transaction_data{0, 0});
    REQUIRE_FALSE(res_r.has_error());
    auto& res = res_r.value();

    REQUIRE(res.size() == 3);
    CHECK(as_set(res[0]) == std::set<int64_t>{1}); // (2,200) -> row 1
    CHECK(as_set(res[1]) == std::set<int64_t>{0}); // (1,100) -> row 0
    CHECK(res[2].empty());                         // (1,999) absent
    CHECK(counter.scan_batched_calls == 0);
    CHECK(counter.fetch_session_starts == 1);
}
