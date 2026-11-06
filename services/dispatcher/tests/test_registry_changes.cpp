#include <catch2/catch_test_macros.hpp>

#include <services/dispatcher/registry_changes.hpp>

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <core/pmr.hpp>

#include <memory_resource>
#include <string>

using services::dispatcher::cast_change_t;
using services::dispatcher::function_change_t;
using services::dispatcher::pending_registry_changes_t;
using services::dispatcher::registry_changes_t;

namespace {

    core::error_t noop_exec(components::compute::kernel_context&,
                            const components::vector::data_chunk_t&,
                            components::vector::vector_t&) {
        return core::error_t::no_error();
    }

    components::compute::function_ptr probe(std::pmr::memory_resource* resource, const std::string& name) {
        using namespace components::compute;
        function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::unary(),
                                                                      doc,
                                                                      std::size_t{1},
                                                                      volatility_t::immutable);
        kernel_signature_t signature(function_type_t::vector,
                                     {parameter_type::exact(components::types::logical_type::BIGINT)},
                                     {output_type::fixed(components::types::logical_type::BIGINT)});
        REQUIRE_FALSE(fn->add_kernel(resource, vector_kernel{std::move(signature), noop_exec}).contains_error());
        return fn;
    }

    registry_changes_t registered(std::pmr::memory_resource* resource, components::compute::function_uid uid) {
        registry_changes_t changes;
        changes.functions.push_back(function_change_t{uid, 7, probe(resource, "f" + std::to_string(uid))});
        return changes;
    }

    registry_changes_t dropped(components::compute::function_uid uid) {
        registry_changes_t changes;
        changes.functions.push_back(function_change_t{uid, 7, components::compute::no_function()});
        return changes;
    }

    const components::types::complex_logical_type boolean{components::types::logical_type::BOOLEAN};
    const components::types::complex_logical_type date{components::types::logical_type::DATE};

} // namespace

// The registries of the executors copy the master's: a change that finds them otherwise is a broken invariant.
TEST_CASE("services::dispatcher::registry_changes::a_change_the_registry_does_not_fit_is_refused") {
    std::pmr::monotonic_buffer_resource resource;
    components::compute::function_registry_t functions{&resource};
    components::casts::cast_registry_t casts{&resource};

    SECTION("dropping a uid the registry does not hold") {
        CHECK(dropped(1000).apply_to(functions, &casts, &resource).contains_error());
    }
    SECTION("adding a uid the registry already holds") {
        REQUIRE_FALSE(registered(&resource, 1000).apply_to(functions, &casts, &resource).contains_error());
        CHECK(registered(&resource, 1000).apply_to(functions, &casts, &resource).contains_error());
    }
    SECTION("dropping a cast the registry does not hold") {
        registry_changes_t changes;
        changes.casts.push_back(cast_change_t{boolean, date, std::nullopt});
        CHECK(changes.apply_to(functions, &casts, &resource).contains_error());
    }
    SECTION("an add and its drop") {
        auto changes = registered(&resource, 1000);
        changes.append(dropped(1000));
        CHECK_FALSE(changes.apply_to(functions, &casts, &resource).contains_error());
        CHECK(functions.get_function(1000) == nullptr);
    }
}

// PostgreSQL 18 savepoints: ROLLBACK TO and RELEASE name the latest savepoint of the name; ROLLBACK TO keeps it,
// RELEASE drops it with the later ones and keeps the changes.
TEST_CASE("services::dispatcher::registry_changes::savepoints") {
    std::pmr::monotonic_buffer_resource resource;
    pending_registry_changes_t pending;

    SECTION("ROLLBACK TO drops what came after the savepoint and keeps the savepoint") {
        pending.add(registered(&resource, 1));
        pending.savepoint("a");
        pending.add(registered(&resource, 2));
        pending.rollback_to_savepoint("a");
        REQUIRE(pending.changes().functions.size() == 1);
        CHECK(pending.changes().functions.front().uid == 1);
        pending.add(registered(&resource, 3));
        pending.rollback_to_savepoint("a");
        CHECK(pending.changes().functions.size() == 1);
    }
    SECTION("RELEASE keeps the changes and forgets the savepoint") {
        pending.savepoint("a");
        pending.add(registered(&resource, 1));
        pending.release_savepoint("a");
        pending.rollback_to_savepoint("a");
        CHECK(pending.changes().functions.size() == 1);
    }
    SECTION("a name given twice names the later savepoint") {
        pending.savepoint("a");
        pending.add(registered(&resource, 1));
        pending.savepoint("a");
        pending.add(registered(&resource, 2));
        pending.rollback_to_savepoint("a");
        REQUIRE(pending.changes().functions.size() == 1);
        CHECK(pending.changes().functions.front().uid == 1);
        pending.release_savepoint("a");
        pending.rollback_to_savepoint("a");
        CHECK(pending.changes().functions.empty());
    }
    SECTION("casts follow the savepoints too") {
        pending.savepoint("a");
        registry_changes_t cast;
        cast.casts.push_back(cast_change_t{boolean, date, std::nullopt});
        pending.add(std::move(cast));
        pending.rollback_to_savepoint("a");
        CHECK(pending.changes().casts.empty());
    }
}
