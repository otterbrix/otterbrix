#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <core/pmr.hpp>
#include <type_traits>

namespace {

    namespace lp = components::logical_plan;
    using components::pipeline::context_t;
    using address_t = actor_zeta::address_t;
    using registry_t = const components::compute::function_registry_t*;
    using session_t = components::session::session_id_t;

} // namespace

// disk_address, index_address and wal_address used to default to empty_address(), so a context
// built by either constructor arrived with no mailbox at all and every send through it depended
// on the author remembering a guard. The three are constructor arguments now: the shapes that
// omit them are the ones that must NOT compile.
TEST_CASE("components::pipeline::context_t::a context cannot be built without naming its mailboxes") {
    CHECK_FALSE(std::is_constructible_v<context_t, const lp::storage_parameters&>);
    CHECK_FALSE(
        std::is_constructible_v<context_t, session_t, address_t, address_t, registry_t, const lp::storage_parameters&>);

    CHECK(std::is_constructible_v<context_t, const lp::storage_parameters&, address_t, address_t, address_t>);
    CHECK(std::is_constructible_v<context_t,
                                  session_t,
                                  address_t,
                                  address_t,
                                  registry_t,
                                  const lp::storage_parameters&,
                                  address_t,
                                  address_t,
                                  address_t>);
}

// Three arguments of one type in a row: the constructor must land each on its own member.
TEST_CASE("components::pipeline::context_t::each mailbox argument lands on its own member") {
    core::pmr::otterbrix_resource arena;

    int disk_stand_in = 0;
    int index_stand_in = 0;
    int wal_stand_in = 0;
    address_t disk{&arena, &disk_stand_in};
    address_t index{&arena, &index_stand_in};
    address_t wal{&arena, &wal_stand_in};

    lp::storage_parameters params{&arena};
    context_t ctx{params, disk, index, wal};
    CHECK(ctx.disk_address.get() == &disk_stand_in);
    CHECK(ctx.index_address.get() == &index_stand_in);
    CHECK(ctx.wal_address.get() == &wal_stand_in);

    context_t executor_ctx{session_t{}, disk, index, nullptr, params, disk, index, wal};
    CHECK(executor_ctx.disk_address.get() == &disk_stand_in);
    CHECK(executor_ctx.index_address.get() == &index_stand_in);
    CHECK(executor_ctx.wal_address.get() == &wal_stand_in);
    CHECK(executor_ctx.address().get() == &disk_stand_in);
    CHECK(executor_ctx.current_message_sender.get() == &index_stand_in);

    // A context that has none says so, and nothing silently supplies one.
    context_t unwired{params,
                      components::pipeline::no_mailbox(),
                      components::pipeline::no_mailbox(),
                      components::pipeline::no_mailbox()};
    CHECK_FALSE(static_cast<bool>(unwired.disk_address));
    CHECK_FALSE(static_cast<bool>(unwired.index_address));
    CHECK_FALSE(static_cast<bool>(unwired.wal_address));
}
