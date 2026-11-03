// The disk answers storage_offer as any storage of a table does: by the block's shape alone it takes the WHERE of a
// SELECT block of one table, or its GROUP BY of the reduce form, and runs the take with storage_run_block.

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/context/execution_context.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/log/log.hpp>
#include <components/log/test/test_log.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/tests/test_directory.hpp>
#include <services/engine/bootstrap_token_def.hpp>

#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

using namespace services::disk;
namespace catalog = components::catalog;
namespace ce = components::expressions;
namespace lp = components::logical_plan;
namespace types = components::types;
using session_id_t = components::session::session_id_t;

namespace {

    std::string offer_dir() {
        static std::string p = "/tmp/test_otterbrix_disk_offer_" + std::to_string(::getpid());
        return p;
    }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;
        manager_disk_t::bootstrap_t boot{*manager, services::bootstrap_token_t{}};

        explicit fixture(int agents = 2)
            : log(make_test_log())
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c{offer_dir()};
                c.path = offer_dir();
                c.agent = agents;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource,
                                                        scheduler,
                                                        scheduler,
                                                        test_directory::created(disk_config),
                                                        log,
                                                        configuration::pump_intervals_t{})) {
            std::filesystem::remove_all(offer_dir());
            std::filesystem::create_directories(offer_dir());
            REQUIRE_FALSE(boot.bootstrap_system_tables_sync().contains_error());
        }
        ~fixture() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
            std::filesystem::remove_all(offer_dir());
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            REQUIRE(test_helpers::wait_ready(future, scheduler));
            return std::move(future).take_ready();
        }
    };

    const catalog::oid_t t_oid{catalog::FIRST_USER_OID};
    const catalog::oid_t u_oid{catalog::FIRST_USER_OID + 1};

    components::table::transaction_data open_txn(uint64_t txn_id) {
        components::table::transaction_data td(txn_id, 1);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return td;
    }

    // One session: a session's takes are its own.
    components::execution_context_t ctx_of(components::table::transaction_data txn) {
        static const session_id_t session{};
        return components::execution_context_t{session, txn, {}};
    }

    // t (id, g, v): `rows` rows, id = i, g = i % 4, v = i * 10.
    void
    create(fixture& fx, catalog::oid_t oid, uint64_t rows, components::table::transaction_data txn, bool computed) {
        std::vector<components::table::column_definition_t> cols;
        for (const char* name : {"id", "g", "v"}) {
            cols.emplace_back(name, types::complex_logical_type{types::logical_type::BIGINT});
        }
        REQUIRE_FALSE(fx.invoke(&manager_disk_t::create_storage_disk,
                                ctx_of(components::table::transaction_data{0, 0}),
                                oid,
                                catalog::well_known_oid::main_database,
                                qualified_name_t{},
                                cols,
                                computed)
                          .has_error());
        if (rows == 0) {
            return;
        }
        std::pmr::vector<types::complex_logical_type> ct{&fx.resource};
        for (const char* name : {"id", "g", "v"}) {
            types::complex_logical_type type{types::logical_type::BIGINT};
            type.set_alias(name);
            ct.emplace_back(std::move(type));
        }
        std::pmr::vector<components::vector::data_chunk_t> data{&fx.resource};
        for (uint64_t first = 0; first < rows; first += 1024) {
            const uint64_t count = std::min<uint64_t>(1024, rows - first);
            components::vector::data_chunk_t chunk{&fx.resource, ct, count};
            chunk.set_cardinality(count);
            for (uint64_t i = 0; i < count; ++i) {
                const auto id = static_cast<int64_t>(first + i);
                chunk.set_value(0, i, id);
                chunk.set_value(1, i, id % 4);
                chunk.set_value(2, i, id * 10);
            }
            data.emplace_back(std::move(chunk));
        }
        auto append_ctx = ctx_of(txn);
        append_ctx.table_oid = oid;
        REQUIRE_FALSE(fx.invoke(&manager_disk_t::storage_append,
                                append_ctx,
                                oid,
                                std::pmr::vector<catalog::oid_t>(3, catalog::INVALID_OID, &fx.resource),
                                std::move(data),
                                false)
                          .has_error());
    }

    ce::key_t column(std::pmr::memory_resource* r, const char* name, size_t position) {
        ce::key_t key{r, std::string{name}};
        key.set_path(std::pmr::vector<size_t>{{position}, r});
        return key;
    }

    lp::detached_table_t table_of(std::pmr::memory_resource* r, catalog::oid_t oid) {
        lp::detached_table_t table{oid, qualified_name_t{}, std::pmr::vector<lp::detached_column_t>{r}};
        std::int32_t position = 0;
        for (const char* name : {"id", "g", "v"}) {
            table.columns.push_back(lp::detached_column_t{catalog::oid_t{0},
                                                          std::pmr::string{name, r},
                                                          types::complex_logical_type{types::logical_type::BIGINT},
                                                          position++});
        }
        return table;
    }

    lp::node_aggregate_ptr select_of(std::pmr::memory_resource* r, catalog::oid_t oid) {
        auto select = lp::make_node_aggregate(r, qualified_name_t{});
        select->set_table_oid(oid);
        return select;
    }

    // WHERE <column> >= $id.
    lp::node_match_ptr where_at_least(std::pmr::memory_resource* r, size_t position, core::parameter_id_t id) {
        auto where =
            lp::make_node_match(r,
                                qualified_name_t{},
                                ce::make_compare_expression(r, ce::compare_type::gte, column(r, "c", position), id));
        return where;
    }

    void stamp(ce::aggregate_expression_t& aggregate, std::pmr::memory_resource* r, const char* name) {
        components::compute::function_registry_t registry{r};
        components::compute::register_default_functions(registry);
        const auto uid = registry.find_functions(name).front();
        aggregate.set_pin(components::compute::function_pin_t{uid});
        aggregate.set_function(registry.get_function(uid)->get_copy(r));
        aggregate.set_mergeable(true);
        aggregate.set_result_type(types::complex_logical_type{types::logical_type::BIGINT});
    }

    // GROUP BY g: g, sum(v); no key: sum(v) alone. `argument` replaces v.
    lp::node_group_ptr
    sum_by_g(std::pmr::memory_resource* r, bool keyed, std::optional<ce::param_storage> argument = std::nullopt) {
        auto group = lp::make_node_group(r);
        std::pmr::vector<types::complex_logical_type> out{r};
        if (keyed) {
            group->append_expression(ce::make_scalar_expression(r, ce::scalar_type::group_field, column(r, "g", 1)));
            group->append_expression(ce::make_scalar_expression(r, ce::scalar_type::get_field, column(r, "g", 1)));
            out.emplace_back(types::logical_type::BIGINT);
        }
        auto sum = ce::make_aggregate_expression(r, "sum", ce::key_t{r, std::string{"s"}});
        stamp(*sum, r, "sum");
        sum->append_param(argument ? *argument : ce::param_storage{column(r, "v", 2)});
        group->append_expression(ce::expression_ptr{sum});
        out.emplace_back(types::logical_type::BIGINT);
        group->set_output_types(std::move(out));
        group->set_input_types(
            std::pmr::vector<types::complex_logical_type>{{types::complex_logical_type{types::logical_type::BIGINT},
                                                           types::complex_logical_type{types::logical_type::BIGINT},
                                                           types::complex_logical_type{types::logical_type::BIGINT}},
                                                          r});
        return group;
    }

    lp::detached_block_t block_of(std::pmr::memory_resource* r, lp::node_ptr root) {
        lp::detached_block_t block{r};
        block.root = std::move(root);
        block.tables.push_back(table_of(r, t_oid));
        block.tables.push_back(table_of(r, u_oid));
        return block;
    }

    std::pmr::vector<block_take_t>
    offer(fixture& fx, lp::node_ptr root, components::vector::storage_parameters& params) {
        auto takes = fx.invoke(&manager_disk_t::storage_offer,
                               ctx_of(open_txn(88)),
                               block_of(&fx.resource, std::move(root)),
                               params.clone(&fx.resource));
        REQUIRE_FALSE(takes.has_error());
        return std::move(takes.value());
    }

    void release(fixture& fx, const block_take_t& take) {
        fx.invoke(&manager_disk_t::storage_release_block,
                  ctx_of(open_txn(88)),
                  std::pmr::string{take.request, &fx.resource});
    }

    ce::aggregate_expression_t& the_sum(lp::node_group_t& group) {
        return static_cast<ce::aggregate_expression_t&>(*group.expressions().back());
    }

    struct read_t {
        uint64_t rows{0};
        uint64_t pages{0};
        uint64_t widest_page{0};
        uint64_t columns{0};
        std::vector<int64_t> first_column;
    };

    // Runs a take to its empty page.
    read_t run(fixture& fx,
               const block_take_t& take,
               components::vector::storage_parameters& params,
               components::table::transaction_data txn) {
        read_t read;
        auto page = fx.invoke(&manager_disk_t::storage_run_block,
                              ctx_of(txn),
                              std::pmr::string{take.request, &fx.resource},
                              uint64_t{0},
                              params.clone(&fx.resource));
        while (true) {
            INFO((page.has_error() ? page.error().what.c_str() : "<ok>"));
            REQUIRE_FALSE(page.has_error());
            const auto cursor = page.value().cursor_id;
            REQUIRE(cursor != 0);
            auto& batch = *page.value().batch;
            if (batch.size() == 0) {
                break;
            }
            ++read.pages;
            read.rows += batch.size();
            read.widest_page = std::max<uint64_t>(read.widest_page, batch.size());
            read.columns = batch.column_count();
            for (uint64_t row = 0; row < batch.size(); ++row) {
                read.first_column.push_back(batch.data[0].is_null(row) ? -1 : batch.data[0].data<int64_t>()[row]);
            }
            page = fx.invoke(&manager_disk_t::storage_run_block,
                             ctx_of(txn),
                             std::pmr::string{take.request, &fx.resource},
                             cursor,
                             params.clone(&fx.resource));
        }
        return read;
    }

} // namespace

TEST_CASE("services::disk::offer::a_where_of_a_select_block_is_taken") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    CHECK(takes[0].preorder == 1);
    CHECK(takes[0].residual.empty());
    CHECK(takes[0].explain.empty());
}

TEST_CASE("services::disk::offer::a_group_by_of_the_reduce_form_is_taken_with_its_where") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    select->append_child(sum_by_g(&fx.resource, true));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    CHECK(takes[0].preorder == 2);
    REQUIRE(takes[0].explain.size() == 1);
    CHECK(takes[0].explain[0] == "Pushed Aggregate: sum by g");
}

TEST_CASE("services::disk::offer::a_join_of_two_tables_takes_each_side_not_the_join") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto left = select_of(&fx.resource, t_oid);
    left->append_child(where_at_least(&fx.resource, 0, zero));
    auto right = select_of(&fx.resource, u_oid);
    right->append_child(where_at_least(&fx.resource, 1, zero));
    auto join = lp::make_node_join(&fx.resource, lp::join_type::inner);
    join->append_child(left);
    join->append_child(right);
    auto top = lp::make_node_aggregate(&fx.resource, qualified_name_t{});
    top->append_child(join);
    const auto takes = offer(fx, top, params);
    REQUIRE(takes.size() == 2);
    // top 0, join 1, left 2, its WHERE 3, right 4, its WHERE 5.
    CHECK(takes[0].preorder == 3);
    CHECK(takes[1].preorder == 5);
}

TEST_CASE("services::disk::offer::what_is_not_taken") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);

    INFO("a GROUP BY under a HAVING, without a WHERE");
    {
        auto select = select_of(&fx.resource, t_oid);
        select->append_child(sum_by_g(&fx.resource, true));
        select->append_child(lp::make_node_having(
            &fx.resource,
            ce::make_compare_expression(&fx.resource, ce::compare_type::gte, column(&fx.resource, "s", 1), zero)));
        CHECK(offer(fx, select, params).empty());
    }
    INFO("a GROUP BY under DISTINCT ON, without a WHERE");
    {
        auto select = select_of(&fx.resource, t_oid);
        select->append_child(sum_by_g(&fx.resource, true));
        std::pmr::vector<ce::key_t> on{&fx.resource};
        on.push_back(column(&fx.resource, "g", 0));
        select->set_distinct(true);
        select->set_distinct_on_keys(std::move(on));
        CHECK(offer(fx, select, params).empty());
    }
    INFO("a GROUP BY by a computed key");
    {
        auto select = select_of(&fx.resource, t_oid);
        auto group = sum_by_g(&fx.resource, false);
        auto key = ce::make_scalar_expression(&fx.resource, ce::scalar_type::group_field, column(&fx.resource, "k", 1));
        key->append_param(column(&fx.resource, "g", 1));
        group->append_expression(ce::expression_ptr{key});
        select->append_child(group);
        CHECK(offer(fx, select, params).empty());
    }
    INFO("a DISTINCT aggregate");
    {
        auto select = select_of(&fx.resource, t_oid);
        auto group = sum_by_g(&fx.resource, true);
        the_sum(*group).set_distinct(true);
        select->append_child(group);
        CHECK(offer(fx, select, params).empty());
    }
    INFO("an aggregate that does not merge");
    {
        auto select = select_of(&fx.resource, t_oid);
        auto group = sum_by_g(&fx.resource, true);
        the_sum(*group).set_mergeable(false);
        select->append_child(group);
        CHECK(offer(fx, select, params).empty());
    }
    INFO("an aggregate of an expression");
    {
        auto select = select_of(&fx.resource, t_oid);
        select->append_child(sum_by_g(
            &fx.resource,
            true,
            ce::param_storage{ce::expression_ptr{
                ce::make_scalar_expression(&fx.resource, ce::scalar_type::get_field, column(&fx.resource, "v", 2))}}));
        CHECK(offer(fx, select, params).empty());
    }
    INFO("a WHERE that must stay in otterbrix, and a GROUP BY over it");
    {
        auto where = where_at_least(&fx.resource, 0, zero);
        static_cast<ce::compare_expression_t&>(*where->expressions().front()).make_unfoldable();
        auto select = select_of(&fx.resource, t_oid);
        select->append_child(where);
        select->append_child(sum_by_g(&fx.resource, true));
        CHECK(offer(fx, select, params).empty());
    }
}

TEST_CASE("services::disk::offer::a_taken_where_answers_in_pages_with_the_columns_it_reads") {
    fixture fx;
    create(fx, t_oid, 3000, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto from = params.add(components::vector::make_scalar(&fx.resource, int64_t{100}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->set_projected_cols({2, 0});
    // The block reads (v, id): id is its second column.
    select->append_child(where_at_least(&fx.resource, 1, from));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    const auto read = run(fx, takes[0], params, open_txn(88));
    CHECK(read.rows == 2900);
    CHECK(read.pages == 3);
    CHECK(read.widest_page <= 1024);
    CHECK(read.columns == 2);
    CHECK(read.first_column.front() == 1000);
}

TEST_CASE("services::disk::offer::a_taken_where_stops_at_its_read_cap") {
    fixture fx;
    create(fx, t_oid, 3000, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto from = params.add(components::vector::make_scalar(&fx.resource, int64_t{10}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    auto where = where_at_least(&fx.resource, 0, from);
    where->set_read_cap(lp::row_cap_t{7});
    select->append_child(where);
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    const auto read = run(fx, takes[0], params, open_txn(88));
    CHECK(read.rows == 7);
    CHECK(read.first_column.front() == 10);
}

TEST_CASE("services::disk::offer::a_taken_group_by_answers_its_groups_and_one_row_of_none") {
    fixture fx;
    create(fx, t_oid, 12, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto from = params.add(components::vector::make_scalar(&fx.resource, int64_t{4}),
                                 components::vector::parameter_kind::literal);
    const auto beyond = params.add(components::vector::make_scalar(&fx.resource, int64_t{1000}),
                                   components::vector::parameter_kind::literal);

    auto grouped = select_of(&fx.resource, t_oid);
    grouped->append_child(where_at_least(&fx.resource, 0, from));
    grouped->append_child(sum_by_g(&fx.resource, true));
    const auto takes = offer(fx, grouped, params);
    REQUIRE(takes.size() == 1);
    const auto groups = run(fx, takes[0], params, open_txn(88));
    CHECK(groups.rows == 4);
    CHECK(groups.columns == 2);

    auto scalar = select_of(&fx.resource, t_oid);
    scalar->append_child(where_at_least(&fx.resource, 0, beyond));
    scalar->append_child(sum_by_g(&fx.resource, false));
    const auto scalar_takes = offer(fx, scalar, params);
    REQUIRE(scalar_takes.size() == 1);
    const auto none = run(fx, scalar_takes[0], params, open_txn(88));
    CHECK(none.rows == 1);
    CHECK(none.first_column == std::vector<int64_t>{-1});
}

TEST_CASE("services::disk::offer::a_take_over_a_table_without_storage_is_refused") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    auto page = fx.invoke(&manager_disk_t::storage_run_block,
                          ctx_of(open_txn(88)),
                          std::pmr::string{takes[0].request, &fx.resource},
                          uint64_t{0},
                          params.clone(&fx.resource));
    REQUIRE(page.has_error());
    INFO(page.error().what.c_str());
    CHECK(page.error().type == core::error_code_t::internal_error);
}

TEST_CASE("services::disk::offer::a_take_reads_its_own_transactions_rows") {
    fixture fx;
    create(fx, t_oid, 5, open_txn(88), false);
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    CHECK(run(fx, takes[0], params, open_txn(88)).rows == 5);
}

TEST_CASE("services::disk::offer::a_take_runs_again_with_other_parameters") {
    fixture fx;
    create(fx, t_oid, 20, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto from = params.add(components::vector::make_scalar(&fx.resource, int64_t{15}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, from));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    CHECK(run(fx, takes[0], params, open_txn(88)).rows == 5);
    components::vector::storage_parameters other{&fx.resource};
    const auto later = other.add(components::vector::make_scalar(&fx.resource, int64_t{18}),
                                 components::vector::parameter_kind::literal);
    REQUIRE(later == from);
    CHECK(run(fx, takes[0], other, open_txn(88)).rows == 2);
}

TEST_CASE("services::disk::offer::a_released_take_is_forgotten") {
    fixture fx;
    create(fx, t_oid, 3, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    CHECK(services::disk::disk_held_takes() == 1);
    CHECK(run(fx, takes[0], params, open_txn(88)).rows == 3);
    release(fx, takes[0]);
    CHECK(services::disk::disk_held_takes() == 0);
    auto page = fx.invoke(&manager_disk_t::storage_run_block,
                          ctx_of(open_txn(88)),
                          std::pmr::string{takes[0].request, &fx.resource},
                          uint64_t{0},
                          params.clone(&fx.resource));
    REQUIRE(page.has_error());
    CHECK(page.error().type == core::error_code_t::internal_error);
}

// A statement holds as many takes as it has blocks: none is dropped before it is released.
TEST_CASE("services::disk::offer::every_take_runs_until_it_is_released") {
    fixture fx;
    create(fx, t_oid, 3, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    std::vector<block_take_t> takes;
    for (int i = 0; i < 100; ++i) {
        auto select = select_of(&fx.resource, t_oid);
        select->append_child(where_at_least(&fx.resource, 0, zero));
        auto offered = offer(fx, select, params);
        REQUIRE(offered.size() == 1);
        takes.push_back(std::move(offered.front()));
    }
    CHECK(services::disk::disk_held_takes() == 100);
    for (const auto& take : takes) {
        CHECK(run(fx, take, params, open_txn(88)).rows == 3);
        release(fx, take);
    }
    CHECK(services::disk::disk_held_takes() == 0);
}

// The manager routes a cursor by its top 8 bits: a scan's cursor and a taken block's come from the agent's one
// counter, so they name the agent and never meet.
TEST_CASE("services::disk::offer::a_scan_cursor_and_a_block_cursor_come_from_one_counter") {
    fixture fx;
    create(fx, t_oid, 12, components::table::transaction_data{0, 0}, false);
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto grouped = select_of(&fx.resource, t_oid);
    grouped->append_child(where_at_least(&fx.resource, 0, zero));
    grouped->append_child(sum_by_g(&fx.resource, true));
    const auto takes = offer(fx, grouped, params);
    REQUIRE(takes.size() == 1);
    auto block = fx.invoke(&manager_disk_t::storage_run_block,
                           ctx_of(open_txn(88)),
                           std::pmr::string{takes[0].request, &fx.resource},
                           uint64_t{0},
                           params.clone(&fx.resource));
    REQUIRE_FALSE(block.has_error());
    auto scan = fx.invoke(&manager_disk_t::storage_fetch_next_batch,
                          ctx_of(open_txn(88)).session,
                          t_oid,
                          uint64_t{0},
                          int64_t{-1},
                          std::vector<size_t>{},
                          open_txn(88));
    REQUIRE_FALSE(scan.has_error());
    const auto block_cursor = block.value().cursor_id;
    const auto scan_cursor = scan.value().cursor_id;
    CHECK(scan_cursor != block_cursor);
    CHECK((scan_cursor >> 56) == (block_cursor >> 56));
}

// A take's position is the executor's preorder: a clause after another counts that clause's whole sub-tree.
TEST_CASE("services::disk::offer::a_clause_is_placed_after_the_sub_trees_before_it") {
    fixture fx;
    components::vector::storage_parameters params{&fx.resource};
    const auto zero = params.add(components::vector::make_scalar(&fx.resource, int64_t{0}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    auto limit = lp::make_node_limit(&fx.resource, lp::limit_t{5});
    limit->append_child(lp::make_node_limit(&fx.resource, lp::limit_t{5}));
    select->append_child(limit);
    select->append_child(where_at_least(&fx.resource, 0, zero));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    // select 0, limit 1, its child 2, WHERE 3.
    CHECK(takes[0].preorder == 3);
}

TEST_CASE("services::disk::offer::a_computed_table_is_read_whole") {
    fixture fx;
    create(fx, t_oid, 5, components::table::transaction_data{0, 0}, true);
    components::vector::storage_parameters params{&fx.resource};
    const auto from = params.add(components::vector::make_scalar(&fx.resource, int64_t{2}),
                                 components::vector::parameter_kind::literal);
    auto select = select_of(&fx.resource, t_oid);
    // A computed table's block reads its columns by where its rows carry them, whatever the projection says.
    select->set_projected_cols({7});
    select->append_child(where_at_least(&fx.resource, 0, from));
    const auto takes = offer(fx, select, params);
    REQUIRE(takes.size() == 1);
    const auto read = run(fx, takes[0], params, open_txn(88));
    CHECK(read.rows == 3);
    CHECK(read.columns == 3);
    CHECK(read.first_column == std::vector<int64_t>{2, 3, 4});
}
