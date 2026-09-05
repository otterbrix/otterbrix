// Two planner properties, both MEASURED rather than read off the source.
//
// 1. optimizer/rules/pushdown_filter.cpp's transitive-equi-predicate derivation copies
//    key_t three times (the two ON keys of an equi-pair, and the partner key it stamps
//    into merged coordinates). key_t's plain copy constructor keeps STANDARD pmr
//    semantics on purpose -- an un-placed copy lands on the PROCESS DEFAULT resource,
//    which is immortal and therefore safe, but which no arena tracer accounts for. The
//    rule was given an arena; its copies belong on it. Pinned by counting what the
//    derivation allocates on the process default while it runs.
//
// 2. components/planner/planner.cpp's rewrite_alter_table is the named home for closing
//    the CREATE TABLE / ALTER ADD COLUMN parity gap on DEFAULT (executor.cpp's
//    alter_table_t arm points here in as many words). The obvious shortcut -- the value
//    level cast components/types already offers, which the planner can reach without the
//    executor's cast_registry_t -- is NOT usable for it, and this file says why with a
//    number rather than an opinion.

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/optimizer/rules/pushdown_filter.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/date/date_types.hpp>

#include <memory_resource>

using namespace components::logical_plan;
using namespace components::expressions;

namespace {
    core::dbname_t adb() { return core::dbname_t{std::string{"database"}}; }
    core::relname_t arel() { return core::relname_t{std::string{"collection"}}; }

    // Counts allocations and forwards them. It stands IN FRONT OF the process default
    // resource for its own lifetime and steps back out in the destructor: the property
    // under test is "how much of this ran on the arena nobody named", and the process
    // default is exactly where an un-placed pmr copy goes, so counting what lands there
    // is the only direct observation of it. Catch2 runs cases on one thread, and the
    // swap never outlives the scope.
    //
    // The upstream is whatever set_default_resource HANDS BACK, so nothing about the
    // allocation behaviour changes and neither get_default_resource() nor
    // new_delete_resource() is named here.
    class default_resource_counter_t final : public std::pmr::memory_resource {
    public:
        default_resource_counter_t() { previous_ = std::pmr::set_default_resource(this); }
        default_resource_counter_t(const default_resource_counter_t&) = delete;
        default_resource_counter_t& operator=(const default_resource_counter_t&) = delete;
        ~default_resource_counter_t() override { std::pmr::set_default_resource(previous_); }

        size_t allocations() const noexcept { return allocations_; }

    private:
        void* do_allocate(size_t bytes, size_t align) override {
            ++allocations_;
            return previous_->allocate(bytes, align);
        }
        void do_deallocate(void* p, size_t bytes, size_t align) override {
            previous_->deallocate(p, bytes, align);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* previous_ = nullptr;
        size_t allocations_ = 0;
    };

    // A key naming column `name`, path()[0] pre-stamped to `idx` (what validate_schema
    // stamps at runtime), with an explicit join side.
    components::expressions::key_t akey(std::pmr::memory_resource* r, const char* name, size_t idx, side_t side) {
        components::expressions::key_t k(r, name, side);
        std::pmr::vector<size_t> p{r};
        p.push_back(idx);
        k.set_path(std::move(p));
        return k;
    }

    node_aggregate_ptr ascan(std::pmr::memory_resource* r, std::initializer_list<const char*> cols) {
        auto agg = make_node_aggregate(r, adb(), arel());
        std::pmr::vector<components::types::complex_logical_type> out(r);
        for (const char* c : cols) {
            out.emplace_back(components::types::logical_type::BIGINT, c);
        }
        agg->set_output_types(std::move(out));
        return agg;
    }

    // t1 = {a, k}, t2 = {b, k2} joined ON t1.k = t2.k2 (side-local ON key paths, merged
    // schema [a=0, k=1, b=2, k2=3], left_width=2), with one WHERE conjunct `<col> = 5`.
    //
    // `jt`            inner -> the derivation runs; anything else -> it is gated off.
    // `where_on_key`  true  -> WHERE t1.k = 5, an equi-key column, so t2.k2 = 5 is derived;
    //                 false -> WHERE t1.a = 5, which still PUSHES but derives nothing.
    //
    // Returns how many allocations the rule made on the process default resource.
    size_t default_allocations_of_run(join_type jt, std::pmr::memory_resource* plan_arena, bool where_on_key) {
        auto params = make_parameter_node(plan_arena);
        auto p5 = params->add_parameter(int64_t(5));

        auto left = ascan(plan_arena, {"a", "k"});
        auto right = ascan(plan_arena, {"b", "k2"});
        auto join = make_node_join(plan_arena, adb(), arel(), jt);
        join->append_child(left);
        join->append_child(right);
        join->append_expression(make_compare_expression(plan_arena,
                                                        compare_type::eq,
                                                        akey(plan_arena, "k", 1, side_t::left),
                                                        akey(plan_arena, "k2", 1, side_t::right)));

        auto where = where_on_key
                         ? make_compare_expression(plan_arena,
                                                   compare_type::eq,
                                                   akey(plan_arena, "k", 1, side_t::left),
                                                   p5)
                         : make_compare_expression(plan_arena,
                                                   compare_type::eq,
                                                   akey(plan_arena, "a", 0, side_t::left),
                                                   p5);

        auto outer = make_node_aggregate(plan_arena, adb(), arel());
        outer->append_child(join);
        outer->append_child(make_node_match(plan_arena, adb(), arel(), where));

        size_t measured = 0;
        {
            default_resource_counter_t counter;
            node_ptr out = components::planner::optimizer::pushdown_filter(plan_arena, outer);
            measured = counter.allocations();
            REQUIRE(out != nullptr);
        }
        return measured;
    }
} // namespace

// ================================================================
// The rule's own key copies live on the arena the rule was handed.
//
// Every number below is measured in this test, so the assertion does not rest on a
// constant somebody has to re-derive after a stdlib change. The two controls fix the
// baseline at ZERO: a join whose derivation is gated off, and an inner join whose WHERE
// pushes but derives nothing, both leave the process default untouched -- so every
// allocation the deriving run makes there is attributable to the derivation itself.
//
// The 5 that remain are ALL accounted for, and none of them is this component's to place:
//   4  the EXPRESSION layer's own copies of the derived key -- once into the param_storage
//      argument and once into the compare node's member, 2 allocations each
//      (components/expressions/compare_expression.cpp);
//   1  relocalize_key_path building the derived key's re-localized path vector on
//      k.resource(), which for that key IS the process default precisely because of the
//      two copies above. A right-side push of an ordinary plan-arena key relocalizes for
//      0 -- measured while writing this -- so the cost is the key's arena, not the rewrite.
// Before the rule placed its own copies the same run cost 12 = 5 + SEVEN: two ON-key
// copies at 2 each, the partner copy at 2, and its path vector at 1, all un-placed.
// ================================================================
TEST_CASE("components::planner::pushdown_filter::derivation_allocates_on_the_named_arena") {
    auto resource = core::pmr::otterbrix_resource();

    // Control 1: LEFT join -- derivation is gated off (a null-padded partner would
    // wrongly drop preserved rows), and the whole WHERE stays in the residual.
    CHECK(default_allocations_of_run(join_type::left, &resource, true) == 0);

    // Control 2: INNER join, WHERE on a NON-key column -- the conjunct is bucketed and
    // pushed below t1, and collect_equi_pairs still walks the ON condition, but no
    // partner predicate is synthesized.
    CHECK(default_allocations_of_run(join_type::inner, &resource, false) == 0);

    // Calibration A: what ONE un-placed key_t copy costs (storage vector + path vector;
    // the 2-character column name is short-string-optimised in both libc++ and libstdc++).
    size_t one_plain_key_copy = 0;
    {
        auto k = akey(&resource, "k2", 3, side_t::right);
        default_resource_counter_t counter;
        components::expressions::key_t unplaced_copy = k;
        one_plain_key_copy = counter.allocations();
        CHECK(unplaced_copy.as_string() == "k2");
    }
    CHECK(one_plain_key_copy == 2);

    // Calibration B: what the EXPRESSION layer charges to build one `key OP param`
    // comparison out of an already-placed key -- two such copies, and this component
    // cannot avoid either of them.
    size_t one_compare_expression = 0;
    {
        auto params = make_parameter_node(&resource);
        auto p5 = params->add_parameter(int64_t(5));
        auto k = akey(&resource, "k2", 3, side_t::right);
        default_resource_counter_t counter;
        auto expr = make_compare_expression(&resource, compare_type::eq, k, p5);
        one_compare_expression = counter.allocations();
        REQUIRE(expr != nullptr);
    }
    CHECK(one_compare_expression == 2 * one_plain_key_copy);

    // The deriving run: one derived predicate. Everything THIS component copies is
    // placed, so what remains is the expression layer's share plus the one path vector
    // relocalize builds on the arena that layer chose.
    const size_t deriving = default_allocations_of_run(join_type::inner, &resource, true);
    CHECK(deriving == 5);
    REQUIRE(deriving <= one_compare_expression + 1);
}

// ================================================================
// Why the CREATE TABLE / ALTER ADD COLUMN DEFAULT parity is not closed IN THIS COMPONENT.
//
// The gap itself is CLOSED as of 2026-09-05, in services/collection/executor.cpp: its
// alter_table_t arm now runs the same convert_column_defaults its create_collection_t arm
// always ran, so `ADD COLUMN c integer DEFAULT 7` stores INTEGER 7 exactly as the CREATE
// TABLE spelling does. It sits there and not here because convert_column_defaults needs the
// executor's cast_registry_t and graph_execution_context, neither of which planner takes.
// planner.cpp's rewrite_alter_table still supplies the half this component owns: the mutable
// copy of the subcommand's column that makes the write-back possible.
//
// It cannot be done with the cast this component can already reach. logical_value_t is in
// components/types, which planner links; its cast_as() converts numeric to numeric with a
// bare static_cast and has no range check, so the narrowing below SUCCEEDS and hands back
// a wrapped value. The cast_registry_t kernel the CREATE TABLE leg uses refuses the same
// pair with "out of range" (components/casts/kernels/numeric_cast.hpp). Coercing DEFAULTs
// here with cast_as would therefore not close the parity gap but replace a loud refusal
// with a silently wrong persisted default -- the one outcome the ALTER path is currently
// free of.
//
// This case is a CHARACTERIZATION of cast_as, not an endorsement: the day it grows a
// range check it goes red, and that is the day the shortcut becomes worth revisiting.
// ================================================================
TEST_CASE("components::planner::alter_default_coercion::value_cast_narrows_without_saying_so") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr auto no_session_tz = core::date::timezone_offset_t{};

    // 5'000'000'000 does not fit INT32. An assignment cast must refuse it.
    const components::types::logical_value_t written{&resource, static_cast<int64_t>(5000000000)};
    CHECK(written.type().type() == components::types::logical_type::BIGINT);

    auto narrowed = written.cast_as(
        components::types::complex_logical_type{components::types::logical_type::INTEGER},
        no_session_tz);

    // No error channel used -- the conversion reports success ...
    REQUIRE_FALSE(narrowed.has_error());
    CHECK(narrowed.value().type().type() == components::types::logical_type::INTEGER);
    // ... and the value is 5'000'000'000 - 2^32, i.e. the low 32 bits, not the number
    // anybody wrote. This is the exact shape of a DEFAULT that would be persisted if this
    // component closed the parity gap with this call.
    CHECK(narrowed.value().value<int32_t>() == 705032704);
}
