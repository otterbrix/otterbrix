
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

    // The resource itself is a program-lifetime singleton, NOT the RAII object below: whatever
    // allocates while counting is installed keeps a pointer to the resource and deallocates through
    // it LATER. A run's copies land in `outer`'s subtree, which outlives the counting scope, so a
    // stack-local resource here is a use-after-scope (ASAN: stack-use-after-scope in ~key_t()).
    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        // Captured once, before the first install, so a deallocation arriving after uninstall still
        // reaches the resource that actually served it.
        void arm() noexcept {
            if (upstream_ == nullptr) {
                upstream_ = std::pmr::get_default_resource();
            }
            allocations_ = 0;
        }

        size_t allocations() const noexcept { return allocations_; }

    private:
        void* do_allocate(size_t bytes, size_t align) override {
            ++allocations_;
            return upstream_->allocate(bytes, align);
        }
        void do_deallocate(void* p, size_t bytes, size_t align) override { upstream_->deallocate(p, bytes, align); }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_ = nullptr;
        size_t allocations_ = 0;
    };

    counting_resource_t& counting_resource() {
        static counting_resource_t instance;
        return instance;
    }

    // Counts what lands on the process default resource, the only way to see un-placed pmr copies;
    // assumes Catch2 runs cases on a single thread.
    class default_resource_counter_t final {
    public:
        default_resource_counter_t() {
            counting_resource().arm();
            previous_ = std::pmr::set_default_resource(&counting_resource());
        }
        default_resource_counter_t(const default_resource_counter_t&) = delete;
        default_resource_counter_t& operator=(const default_resource_counter_t&) = delete;
        ~default_resource_counter_t() { std::pmr::set_default_resource(previous_); }

        size_t allocations() const noexcept { return counting_resource().allocations(); }

    private:
        std::pmr::memory_resource* previous_ = nullptr;
    };

    // path()[0] is pre-stamped to `idx`, matching what validate_schema stamps at runtime.
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

    // t1={a,k}, t2={b,k2} joined ON t1.k=t2.k2, WHERE <col>=5; where_on_key selects WHERE t1.k=5 (derives
    // t2.k2=5) vs WHERE t1.a=5 (pushes only, derives nothing).
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

// The deriving run costs exactly 5 (4 from the expression layer's copies of the derived key, 1 from
// relocalize_key_path); before this rule placed its own copies, the same run cost 12.
TEST_CASE("components::planner::pushdown_filter::derivation_allocates_on_the_named_arena") {
    auto resource = core::pmr::otterbrix_resource();

    // LEFT gates the derivation off: a null-padded partner would wrongly drop preserved rows.
    CHECK(default_allocations_of_run(join_type::left, &resource, true) == 0);

    CHECK(default_allocations_of_run(join_type::inner, &resource, false) == 0);

    size_t one_plain_key_copy = 0;
    {
        auto k = akey(&resource, "k2", 3, side_t::right);
        default_resource_counter_t counter;
        components::expressions::key_t unplaced_copy = k;
        one_plain_key_copy = counter.allocations();
        CHECK(unplaced_copy.as_string() == "k2");
    }
    CHECK(one_plain_key_copy == 2);

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

    const size_t deriving = default_allocations_of_run(join_type::inner, &resource, true);
    CHECK(deriving == 5);
    REQUIRE(deriving <= one_compare_expression + 1);
}

// DEFAULT parity is closed in executor.cpp (alter_table_t reuses create_collection_t's convert_column_defaults)
// via cast_registry_t, which the planner lacks; planner-side logical_value_t::cast_as() has no range check
// (unlike numeric_cast.hpp) and silently truncates instead of refusing.
TEST_CASE("components::planner::alter_default_coercion::value_cast_narrows_without_saying_so") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr auto no_session_tz = core::date::timezone_offset_t{};

    // 5'000'000'000 overflows INT32; PostgreSQL would refuse this cast.
    const components::types::logical_value_t written{&resource, static_cast<int64_t>(5000000000)};
    CHECK(written.type().type() == components::types::logical_type::BIGINT);

    auto narrowed = written.cast_as(
        components::types::complex_logical_type{components::types::logical_type::INTEGER},
        no_session_tz);

    REQUIRE_FALSE(narrowed.has_error());
    CHECK(narrowed.value().type().type() == components::types::logical_type::INTEGER);
    // 705032704 is 5'000'000'000's low 32 bits, not what was written.
    CHECK(narrowed.value().value<int32_t>() == 705032704);
}
