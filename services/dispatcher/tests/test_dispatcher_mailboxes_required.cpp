#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/log/log.hpp>
#include <memory_resource>
#include <services/collection/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <type_traits>

namespace {

    using services::collection::executor::executor_t;
    using services::dispatcher::manager_dispatcher_t;
    using address_t = actor_zeta::address_t;
    using resource_t = std::pmr::memory_resource*;
    using scheduler_t = actor_zeta::scheduler_raw;

} // namespace

// The dispatcher's wal/disk/index addresses used to default to empty_address() and arrive only
// through a later sync(): a dispatcher the sync never reached built every pipeline context with
// three empty mailboxes. They are constructor arguments now — the shape that omits them is the
// one that must NOT compile. An intentionally absent mailbox is still expressible, but only by
// naming it (components::pipeline::no_mailbox()).
TEST_CASE("services::dispatcher::manager_dispatcher_t::a dispatcher cannot be built without naming its mailboxes") {
    CHECK_FALSE(std::is_constructible_v<manager_dispatcher_t, resource_t, scheduler_t, log_t&>);
    CHECK(std::is_constructible_v<manager_dispatcher_t,
                                  resource_t,
                                  scheduler_t,
                                  log_t&,
                                  address_t,
                                  address_t,
                                  address_t>);
}

// The executor already demands its four addresses at construction; pin that an address-less
// constructor never comes back.
TEST_CASE("services::collection::executor_t::an executor cannot be built without naming its mailboxes") {
    CHECK_FALSE(std::is_constructible_v<executor_t, resource_t, log_t&&>);
    CHECK(std::is_constructible_v<executor_t, resource_t, address_t, address_t, address_t, address_t, log_t&&>);
}
