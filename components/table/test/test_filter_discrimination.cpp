// Characterization of storage-filter DISCRIMINATION: which concrete table_filter_t subclass a
// dispatch site decides it is looking at, and what it then does with it.
//
// Every table_filter_t subclass shares one base and the base's `filter_type` field carries the
// COMPARISON OPERATOR, not the class — so several classes collide on it (constant_filter_t,
// column_column_filter_t and expression_filter_t can all carry `gt`; set_membership_filter_t
// carries `eq`). The dispatch sites therefore have to recover the class some other way, and this
// file pins the OBSERVABLE consequence of each such decision:
//
//   * table_filter_table_indices()          — which index vector is handed back
//   * table_filter_dispatch()               — contains() vs compare() vs regex matches()
//   * equals()                              — a cross-class compare must be false, not a
//                                             coincidental match on a shared filter_type
//   * column_data_t::check_zonemap / check_segment_zonemap — only a constant comparison prunes
//   * column_segment_t::filter_indexing     — the vectorized path declines an IN-list
//   * row_group_t::check_predicate          — the per-row branch each class lands in, observed
//                                             end to end through a data_table_t scan
//
// These are behaviour pins, not new behaviour: they must hold both before and after the
// discrimination mechanism changes.

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace {

    using namespace components::table;
    using namespace components::types;
    using components::expressions::compare_type;
    namespace expr = components::expressions;

    constexpr core::date::timezone_offset_t utc{0};

    struct env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::in_memory_block_manager_t block_manager;

        env_t()
            : pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, pool)
            , block_manager(buffer_manager, uint64_t(1) << 18) {}
    };

    std::pmr::vector<uint64_t> indices(std::pmr::memory_resource* r, std::initializer_list<uint64_t> v) {
        return std::pmr::vector<uint64_t>(v, std::pmr::polymorphic_allocator<uint64_t>{r});
    }

    logical_value_t i64(std::pmr::memory_resource* r, int64_t v) { return logical_value_t{r, v}; }
    logical_value_t null_value(std::pmr::memory_resource* r) { return logical_value_t{r, logical_type::NA}; }

    std::pmr::vector<logical_value_t> value_set(std::pmr::memory_resource* r, std::vector<logical_value_t> vs) {
        std::pmr::vector<logical_value_t> out{r};
        for (auto& v : vs) {
            out.emplace_back(std::move(v));
        }
        return out;
    }

    // A bare `col OP :param` compare, enough to give an expression_filter_t an expression whose
    // type() is the requested operator (the real planner ships a function/arithmetic operand; only
    // the operator and the identity matter to the sites pinned here).
    expr::compare_expression_ptr make_compare(std::pmr::memory_resource* r, compare_type op, const char* field) {
        expr::key_t left{r, field};
        return expr::make_compare_expression(r,
                                             op,
                                             expr::param_storage{left},
                                             expr::param_storage{core::parameter_id_t{1}});
    }

    std::unique_ptr<expression_filter_t> make_expression_filter(std::pmr::memory_resource* r,
                                                                compare_type op,
                                                                const char* field,
                                                                size_t column) {
        std::pmr::vector<std::pmr::vector<size_t>> paths{r};
        paths.emplace_back(std::initializer_list<size_t>{column});
        std::pmr::unordered_map<core::parameter_id_t, logical_value_t> params{r};
        return std::make_unique<expression_filter_t>(make_compare(r, op, field),
                                                     std::move(paths),
                                                     std::move(params),
                                                     utc);
    }

    // The concrete evaluator lives in the physical_plan layer (which may depend on table, never the
    // reverse), so a table-level test supplies its own: "column `column`'s BIGINT value > bound",
    // three-valued on a NULL operand exactly as the real one is.
    class greater_than_evaluator_t final : public expression_evaluator_t {
    public:
        greater_than_evaluator_t(size_t column, int64_t bound)
            : column_(column)
            , bound_(bound) {}

        core::result_wrapper_t<tri_bool_t> evaluate(const components::vector::data_chunk_t& row,
                                                   size_t index) const override {
            auto cell = row.value(column_, index);
            if (cell.is_null()) {
                return tri_bool_t::unknown;
            }
            return tri_of(cell.value<int64_t>() > bound_);
        }

    private:
        size_t column_;
        int64_t bound_;
    };

    // ---------------------------------------------------------------------------------------
    // A 3-column table: n = i, s = "v<i>", m = ROWS-1-i. `m` mirrors `n` so a column-vs-column
    // filter has a non-trivial answer, and `s` gives the regex / string dispatch a subject.
    // ---------------------------------------------------------------------------------------
    constexpr size_t ROWS = 32;

    std::string padded(size_t i) {
        std::string n = std::to_string(i);
        return "v" + std::string(2 - std::min<size_t>(2, n.size()), '0') + n;
    }

    std::unique_ptr<data_table_t> build_table(env_t& env) {
        using namespace components::vector;
        std::vector<column_definition_t> columns;
        columns.emplace_back("n", complex_logical_type{logical_type::BIGINT});
        columns.emplace_back("s", complex_logical_type{logical_type::STRING_LITERAL});
        columns.emplace_back("m", complex_logical_type{logical_type::BIGINT});

        auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "filters");

        data_chunk_t chunk(&env.resource, table->copy_types(), ROWS);
        chunk.set_cardinality(ROWS);
        for (size_t i = 0; i < ROWS; i++) {
            chunk.set_value(0, i, i64(&env.resource, static_cast<int64_t>(i)));
            chunk.set_value(1, i, logical_value_t{&env.resource, padded(i)});
            chunk.set_value(2, i, i64(&env.resource, static_cast<int64_t>(ROWS - 1 - i)));
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
        return table;
    }

    // Scan the whole table under `filter` and return the surviving `n` values, in order.
    std::vector<int64_t> surviving(data_table_t& table, std::pmr::memory_resource* r, const table_filter_t* filter) {
        using namespace components::vector;
        std::vector<storage_index_t> column_ids;
        for (size_t i = 0; i < table.column_count(); i++) {
            column_ids.emplace_back(static_cast<uint64_t>(i));
        }
        table_scan_state state(r);
        table.initialize_scan(state, column_ids, filter);
        std::pmr::vector<data_chunk_t> batches(r);
        table.scan_batched(table.copy_types(), nullptr, batches, state, r);
        std::vector<int64_t> out;
        for (auto& batch : batches) {
            for (size_t i = 0; i < batch.size(); i++) {
                out.push_back(batch.value(0, i).value<int64_t>());
            }
        }
        return out;
    }

    std::vector<int64_t> range(int64_t from, int64_t to_exclusive) {
        std::vector<int64_t> out;
        for (int64_t v = from; v < to_exclusive; v++) {
            out.push_back(v);
        }
        return out;
    }

} // namespace

// =============================================================================================
// 1. table_filter_table_indices(): each class must yield ITS OWN index vector.
// =============================================================================================
TEST_CASE("filter discrimination: table_filter_table_indices per filter class") {
    core::pmr::otterbrix_resource r;

    constant_filter_t constant{compare_type::eq, i64(&r, 1), indices(&r, {7})};
    CHECK(table_filter_table_indices(&constant) == indices(&r, {7}));

    set_membership_filter_t membership{value_set(&r, {i64(&r, 1)}), indices(&r, {8})};
    CHECK(table_filter_table_indices(&membership) == indices(&r, {8}));

    is_null_filter_t nulls{compare_type::is_null, indices(&r, {9})};
    CHECK(table_filter_table_indices(&nulls) == indices(&r, {9}));

    regex_filter_t regex{std::pmr::string{"^v", &r}, false, indices(&r, {10})};
    CHECK(table_filter_table_indices(&regex) == indices(&r, {10}));

    // Column-vs-column reports its LEFT path: it is dispatched by its own branch, and this only
    // has to keep the site off a constant_filter_t reinterpretation.
    column_column_filter_t col_col{compare_type::lt, indices(&r, {11}), indices(&r, {12}), utc};
    CHECK(table_filter_table_indices(&col_col) == indices(&r, {11}));
}

// =============================================================================================
// 2. table_filter_dispatch(): contains() for a set, matches() for a regex, compare() otherwise.
// =============================================================================================
TEST_CASE("filter discrimination: table_filter_dispatch per filter class") {
    core::pmr::otterbrix_resource r;

    SECTION("constant_filter_t compares") {
        constant_filter_t f{compare_type::gt, i64(&r, 10), indices(&r, {0})};
        CHECK(table_filter_dispatch(&f, int64_t(20)));
        CHECK_FALSE(table_filter_dispatch(&f, int64_t(5)));
    }

    SECTION("set_membership_filter_t tests membership, never ordering") {
        // filter_type is `eq`, so a constant_filter_t reading would compare against a single
        // value; only membership gives 20 -> true and 15 -> false with 10 also in the set.
        set_membership_filter_t f{value_set(&r, {i64(&r, 10), i64(&r, 20)}), indices(&r, {0})};
        CHECK(table_filter_dispatch(&f, int64_t(10)));
        CHECK(table_filter_dispatch(&f, int64_t(20)));
        CHECK_FALSE(table_filter_dispatch(&f, int64_t(15)));
    }

    SECTION("regex_filter_t matches strings and rejects non-strings") {
        regex_filter_t f{std::pmr::string{"^ab", &r}, false, indices(&r, {0})};
        CHECK(table_filter_dispatch(&f, std::string_view{"abc"}));
        CHECK_FALSE(table_filter_dispatch(&f, std::string_view{"xbc"}));
        CHECK_FALSE(table_filter_dispatch(&f, int64_t(1)));
    }

    SECTION("regex_filter_t honours icase") {
        regex_filter_t sensitive{std::pmr::string{"^ab", &r}, false, indices(&r, {0})};
        regex_filter_t insensitive{std::pmr::string{"^ab", &r}, true, indices(&r, {0})};
        CHECK_FALSE(table_filter_dispatch(&sensitive, std::string_view{"AByz"}));
        CHECK(table_filter_dispatch(&insensitive, std::string_view{"AByz"}));
    }
}

// =============================================================================================
// 3. equals(): a shared filter_type must NOT make two different classes compare equal.
// =============================================================================================
TEST_CASE("filter discrimination: equals does not confuse classes") {
    core::pmr::otterbrix_resource r;

    SECTION("column_column_filter_t") {
        column_column_filter_t a{compare_type::lt, indices(&r, {0}), indices(&r, {2}), utc};
        column_column_filter_t same{compare_type::lt, indices(&r, {0}), indices(&r, {2}), utc};
        column_column_filter_t other_right{compare_type::lt, indices(&r, {0}), indices(&r, {3}), utc};
        column_column_filter_t other_op{compare_type::gt, indices(&r, {0}), indices(&r, {2}), utc};
        // Same operator, different CLASS: must not compare equal.
        constant_filter_t constant{compare_type::lt, i64(&r, 5), indices(&r, {0})};

        CHECK(a.equals(same));
        CHECK_FALSE(a.equals(other_right));
        CHECK_FALSE(a.equals(other_op));
        CHECK_FALSE(a.equals(constant));
    }

    SECTION("expression_filter_t") {
        auto a = make_expression_filter(&r, compare_type::gt, "n", 0);
        auto same = make_expression_filter(&r, compare_type::gt, "n", 0);
        auto other_field = make_expression_filter(&r, compare_type::gt, "m", 2);
        // Same operator, different CLASS: must not compare equal.
        constant_filter_t constant{compare_type::gt, i64(&r, 5), indices(&r, {0})};

        CHECK(a->equals(*same));
        CHECK_FALSE(a->equals(*other_field));
        CHECK_FALSE(a->equals(constant));
    }

    SECTION("regex_filter_t") {
        regex_filter_t a{std::pmr::string{"^ab", &r}, false, indices(&r, {0})};
        regex_filter_t same{std::pmr::string{"^ab", &r}, false, indices(&r, {0})};
        regex_filter_t other_pattern{std::pmr::string{"^cd", &r}, false, indices(&r, {0})};
        regex_filter_t other_icase{std::pmr::string{"^ab", &r}, true, indices(&r, {0})};

        CHECK(a.equals(same));
        CHECK_FALSE(a.equals(other_pattern));
        CHECK_FALSE(a.equals(other_icase));
    }

    SECTION("set_membership_filter_t") {
        set_membership_filter_t a{value_set(&r, {i64(&r, 1), i64(&r, 2)}), indices(&r, {0})};
        set_membership_filter_t same{value_set(&r, {i64(&r, 1), i64(&r, 2)}), indices(&r, {0})};
        set_membership_filter_t other{value_set(&r, {i64(&r, 1), i64(&r, 3)}), indices(&r, {0})};
        set_membership_filter_t shorter{value_set(&r, {i64(&r, 1)}), indices(&r, {0})};

        CHECK(a.equals(same));
        CHECK_FALSE(a.equals(other));
        CHECK_FALSE(a.equals(shorter));
    }

    SECTION("is_null_filter_t distinguishes the two null predicates") {
        is_null_filter_t is_null{compare_type::is_null, indices(&r, {0})};
        is_null_filter_t is_null_same{compare_type::is_null, indices(&r, {0})};
        is_null_filter_t is_not_null{compare_type::is_not_null, indices(&r, {0})};

        CHECK(is_null.equals(is_null_same));
        CHECK_FALSE(is_null.equals(is_not_null));
    }

    SECTION("conjunction filters differ by their fold") {
        conjunction_and_filter_t conj_and;
        conjunction_or_filter_t conj_or;
        conjunction_not_filter_t conj_not;

        CHECK_FALSE(conj_and.equals(conj_or));
        CHECK_FALSE(conj_and.equals(conj_not));
        CHECK_FALSE(conj_or.equals(conj_not));
    }
}

// =============================================================================================
// 4. check_zonemap(): only a constant comparison carries a bound that can prune.
// =============================================================================================
TEST_CASE("filter discrimination: check_zonemap prunes constant comparisons only") {
    env_t env;
    auto* r = &env.resource;

    auto col = column_data_t::create_column(r, env.block_manager, 0, 0, complex_logical_type{logical_type::BIGINT});
    col->statistics().set_min(i64(r, 1));
    col->statistics().set_max(i64(r, 100));

    column_scan_state scan_state;

    SECTION("constant_filter_t prunes") {
        constant_filter_t f{compare_type::gt, i64(r, 200), indices(r, {0})};
        CHECK(col->check_zonemap(scan_state, f) == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("set_membership_filter_t never prunes") {
        // filter_type is `eq` and every value is out of [1,100]; a constant_filter_t reading
        // would prune. Membership must decline instead.
        set_membership_filter_t f{value_set(r, {i64(r, 200), i64(r, 300)}), indices(r, {0})};
        CHECK(col->check_zonemap(scan_state, f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("column_column_filter_t never prunes") {
        column_column_filter_t f{compare_type::gt, indices(r, {0}), indices(r, {1}), utc};
        CHECK(col->check_zonemap(scan_state, f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("expression_filter_t never prunes") {
        auto f = make_expression_filter(r, compare_type::gt, "n", 0);
        CHECK(col->check_zonemap(scan_state, *f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("regex_filter_t never prunes") {
        regex_filter_t f{std::pmr::string{"^v", r}, false, indices(r, {0})};
        CHECK(col->check_zonemap(scan_state, f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }
}

// =============================================================================================
// 5. check_segment_zonemap(): same discrimination, per segment.
// =============================================================================================
TEST_CASE("filter discrimination: check_segment_zonemap prunes constant comparisons only") {
    env_t env;
    auto* r = &env.resource;

    auto col = column_data_t::create_column(r, env.block_manager, 0, 0, complex_logical_type{logical_type::BIGINT});

    auto segment_result = column_segment_t::create_segment(env.buffer_manager,
                                                           complex_logical_type{logical_type::BIGINT},
                                                           0,
                                                           262144,
                                                           262144);
    REQUIRE_FALSE(segment_result.has_error());
    auto segment = std::move(segment_result.value());
    {
        base_statistics_t stats(r, logical_type::BIGINT);
        stats.set_min(i64(r, 1));
        stats.set_max(i64(r, 50));
        segment->set_segment_statistics(std::move(stats));
    }

    column_scan_state scan_state;
    scan_state.current = segment.get();

    SECTION("constant_filter_t prunes") {
        constant_filter_t f{compare_type::gt, i64(r, 75), indices(r, {0})};
        CHECK(col->check_segment_zonemap(scan_state, f) == filter_propagate_result_t::ALWAYS_FALSE);
    }

    SECTION("set_membership_filter_t never prunes") {
        set_membership_filter_t f{value_set(r, {i64(r, 75), i64(r, 90)}), indices(r, {0})};
        CHECK(col->check_segment_zonemap(scan_state, f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("column_column_filter_t never prunes") {
        column_column_filter_t f{compare_type::gt, indices(r, {0}), indices(r, {1}), utc};
        CHECK(col->check_segment_zonemap(scan_state, f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }

    SECTION("expression_filter_t never prunes") {
        auto f = make_expression_filter(r, compare_type::gt, "n", 0);
        CHECK(col->check_segment_zonemap(scan_state, *f) == filter_propagate_result_t::NO_PRUNING_POSSIBLE);
    }
}

// =============================================================================================
// 6. column_segment_t::filter_indexing(): the vectorized path declines an IN-list untouched.
// =============================================================================================
TEST_CASE("filter discrimination: vectorized filter_indexing declines set membership") {
    env_t env;
    auto* r = &env.resource;
    using namespace components::vector;

    constexpr uint64_t COUNT = 8;
    vector_t values(r, logical_type::BIGINT, COUNT);
    for (uint64_t i = 0; i < COUNT; i++) {
        values.set_value(i, static_cast<int64_t>(i));
    }
    unified_vector_format uvf(r, COUNT);
    values.to_unified_format(COUNT, uvf);

    SECTION("constant_filter_t narrows the indexing vector") {
        indexing_vector_t indexing(r, COUNT);
        for (uint64_t i = 0; i < COUNT; i++) {
            indexing.set_index(i, i);
        }
        uint64_t approved = COUNT;
        constant_filter_t f{compare_type::gte, i64(r, 5), indices(r, {0})};
        column_segment_t::filter_indexing(indexing, values, uvf, f, COUNT, approved);
        CHECK(approved == 3); // 5, 6, 7
    }

    SECTION("set_membership_filter_t is passed through unchanged") {
        indexing_vector_t indexing(r, COUNT);
        for (uint64_t i = 0; i < COUNT; i++) {
            indexing.set_index(i, i);
        }
        uint64_t approved = COUNT;
        // filter_type is `eq`; read as a constant_filter_t the vectorized path would narrow.
        set_membership_filter_t f{value_set(r, {i64(r, 1), i64(r, 2)}), indices(r, {0})};
        column_segment_t::filter_indexing(indexing, values, uvf, f, COUNT, approved);
        CHECK(approved == COUNT);
    }
}

// =============================================================================================
// 7. row_group_t::check_predicate(), observed end to end through a table scan.
// =============================================================================================
TEST_CASE("filter discrimination: scan dispatch per filter class") {
    env_t env;
    auto* r = &env.resource;
    auto table = build_table(env);

    SECTION("no filter scans everything") { CHECK(surviving(*table, r, nullptr) == range(0, ROWS)); }

    SECTION("constant_filter_t on the BIGINT column") {
        constant_filter_t f{compare_type::gte, i64(r, 20), indices(r, {0})};
        CHECK(surviving(*table, r, &f) == range(20, ROWS));
    }

    SECTION("constant_filter_t with a NULL constant selects nothing") {
        constant_filter_t f{compare_type::gt, null_value(r), indices(r, {0})};
        CHECK(surviving(*table, r, &f).empty());
    }

    SECTION("set_membership_filter_t selects the listed values") {
        set_membership_filter_t f{value_set(r, {i64(r, 3), i64(r, 7), i64(r, 11)}), indices(r, {0})};
        CHECK(surviving(*table, r, &f) == std::vector<int64_t>{3, 7, 11});
    }

    SECTION("set_membership_filter_t with a NULL element: miss is UNKNOWN, not FALSE") {
        // `n IN (3, NULL)` selects row 3 ...
        auto in_list = std::make_unique<set_membership_filter_t>(value_set(r, {i64(r, 3), null_value(r)}),
                                                                 indices(r, {0}));
        CHECK(surviving(*table, r, in_list.get()) == std::vector<int64_t>{3});

        // ... and `NOT (n IN (3, NULL))` selects NOTHING, because every miss is UNKNOWN and NOT
        // UNKNOWN stays UNKNOWN. Collapsing the miss to FALSE would resurrect 31 rows here.
        conjunction_not_filter_t negated;
        negated.child_filters.emplace_back(std::move(in_list));
        CHECK(surviving(*table, r, &negated).empty());
    }

    SECTION("is_null_filter_t / is_not_null over a fully populated column") {
        is_null_filter_t is_null{compare_type::is_null, indices(r, {0})};
        CHECK(surviving(*table, r, &is_null).empty());

        is_null_filter_t is_not_null{compare_type::is_not_null, indices(r, {0})};
        CHECK(surviving(*table, r, &is_not_null) == range(0, ROWS));
    }

    SECTION("regex_filter_t on the STRING column") {
        // "v0X" for X in 0..9 -> rows 0..9; the zero-padded form makes this unambiguous.
        regex_filter_t f{std::pmr::string{"^v0", r}, false, indices(r, {1})};
        CHECK(surviving(*table, r, &f) == range(0, 10));
    }

    SECTION("column_column_filter_t compares two columns per row") {
        // n < m  <=>  i < ROWS-1-i  <=>  i < 15.5  -> rows 0..15
        column_column_filter_t f{compare_type::lt, indices(r, {0}), indices(r, {2}), utc};
        CHECK(surviving(*table, r, &f) == range(0, 16));
    }

    SECTION("expression_filter_t runs its own evaluator") {
        auto f = make_expression_filter(r, compare_type::gt, "n", 0);
        f->evaluator = std::make_unique<greater_than_evaluator_t>(0, 25);
        CHECK(surviving(*table, r, f.get()) == range(26, ROWS));
    }

    SECTION("expression_filter_t is never pruned away by the zonemap") {
        // Its filter_type is `gt` and its layout is NOT a constant_filter_t's: a zonemap that
        // read it as one would prune wrongly. Every row must reach the per-row evaluator.
        auto f = make_expression_filter(r, compare_type::gt, "n", 0);
        f->evaluator = std::make_unique<greater_than_evaluator_t>(0, -1); // matches every row
        CHECK(surviving(*table, r, f.get()) == range(0, ROWS));
    }

    SECTION("conjunction_and_filter_t over two classes") {
        conjunction_and_filter_t conj;
        conj.child_filters.emplace_back(
            std::make_unique<constant_filter_t>(compare_type::gte, i64(r, 10), indices(r, {0})));
        conj.child_filters.emplace_back(
            std::make_unique<column_column_filter_t>(compare_type::lt, indices(r, {0}), indices(r, {2}), utc));
        // n >= 10 AND n < m  ->  10..15
        CHECK(surviving(*table, r, &conj) == range(10, 16));
    }

    SECTION("conjunction_or_filter_t over two classes") {
        conjunction_or_filter_t conj;
        conj.child_filters.emplace_back(
            std::make_unique<set_membership_filter_t>(value_set(r, {i64(r, 1)}), indices(r, {0})));
        conj.child_filters.emplace_back(
            std::make_unique<regex_filter_t>(std::pmr::string{"^v30$", r}, false, indices(r, {1})));
        CHECK(surviving(*table, r, &conj) == std::vector<int64_t>{1, 30});
    }

    SECTION("conjunction_not_filter_t negates a constant comparison") {
        conjunction_not_filter_t conj;
        conj.child_filters.emplace_back(
            std::make_unique<constant_filter_t>(compare_type::gte, i64(r, 4), indices(r, {0})));
        CHECK(surviving(*table, r, &conj) == range(0, 4));
    }
}

// =============================================================================================
// 8. The LIST paths: a trailing subscript addresses an ELEMENT (check_array_element_predicate)
//    and a bare index addresses the WHOLE cell (column_data_t::check_predicate's LIST branch).
//    Both discriminate set membership from a constant comparison.
// =============================================================================================
TEST_CASE("filter discrimination: list element and whole-cell dispatch") {
    using namespace components::vector;
    env_t env;
    auto* r = &env.resource;

    constexpr size_t LIST_ROWS = 16;
    const auto list_type = complex_logical_type::create_list(logical_type::BIGINT);

    std::vector<column_definition_t> columns;
    columns.emplace_back("lst", list_type);
    auto table = std::make_unique<data_table_t>(r, env.block_manager, std::move(columns), "lists");

    // row i -> [i, i + 100]
    data_chunk_t chunk(r, table->copy_types(), LIST_ROWS);
    chunk.set_cardinality(LIST_ROWS);
    for (size_t i = 0; i < LIST_ROWS; i++) {
        std::vector<int64_t> cell{static_cast<int64_t>(i), static_cast<int64_t>(i) + 100};
        chunk.set_value(0, i, cell);
    }
    table_append_state append_state(r);
    REQUIRE_FALSE(table->append_lock(append_state).has_error());
    REQUIRE_FALSE(table->initialize_append(append_state).has_error());
    REQUIRE_FALSE(table->append(chunk, append_state).has_error());
    table->finalize_append(append_state, transaction_data{0, 0});

    // Scan under `filter` and report which rows survive, identified by their element 0.
    auto surviving_rows = [&](const table_filter_t* filter) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(uint64_t{0});
        table_scan_state state(r);
        table->initialize_scan(state, column_ids, filter);
        std::pmr::vector<data_chunk_t> batches(r);
        table->scan_batched(table->copy_types(), nullptr, batches, state, r);
        std::vector<int64_t> out;
        for (auto& batch : batches) {
            for (size_t i = 0; i < batch.size(); i++) {
                out.push_back(batch.value(0, i).children().front().value<int64_t>());
            }
        }
        return out;
    };

    SECTION("element subscript + constant comparison") {
        constant_filter_t f{compare_type::eq, i64(r, 7), indices(r, {0, 0})};
        CHECK(surviving_rows(&f) == std::vector<int64_t>{7});
    }

    SECTION("element subscript + set membership") {
        // filter_type is `eq`, so only the CLASS distinguishes this from the single-constant case
        // above — read as a constant_filter_t it would select at most one row.
        set_membership_filter_t f{value_set(r, {i64(r, 5), i64(r, 9)}), indices(r, {0, 0})};
        CHECK(surviving_rows(&f) == std::vector<int64_t>{5, 9});
    }

    SECTION("element subscript + second element") {
        constant_filter_t f{compare_type::gte, i64(r, 112), indices(r, {0, 1})};
        CHECK(surviving_rows(&f) == range(12, static_cast<int64_t>(LIST_ROWS)));
    }

    SECTION("whole-cell constant comparison") {
        auto cell = logical_value_t::create_list(r, list_type, {i64(r, 3), i64(r, 103)});
        constant_filter_t f{compare_type::eq, std::move(cell), indices(r, {0})};
        CHECK(surviving_rows(&f) == std::vector<int64_t>{3});
    }

    SECTION("whole-cell set membership") {
        auto values = value_set(r,
                                {logical_value_t::create_list(r, list_type, {i64(r, 2), i64(r, 102)}),
                                 logical_value_t::create_list(r, list_type, {i64(r, 6), i64(r, 106)})});
        set_membership_filter_t f{std::move(values), indices(r, {0})};
        CHECK(surviving_rows(&f) == std::vector<int64_t>{2, 6});
    }
}
