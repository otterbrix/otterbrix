#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <memory_resource>
#include <services/wal/manager_wal_replicate.hpp>
#include <type_traits>

namespace {

    using services::wal::manager_wal_replicate_t;
    using address_t = actor_zeta::address_t;
    using resource_t = std::pmr::memory_resource*;
    using scheduler_t = actor_zeta::scheduler_raw;

    template<typename T>
    concept has_bootstrap_sync = requires {
        &T::sync;
    };

    template<typename T>
    concept has_dispatcher_setter = requires(T& t, address_t a) {
        t.set_manager_dispatcher_sync(a);
    };

} // namespace

// Disk and index mailboxes are constructor arguments, so omitting one must fail to compile; a
// topology that deliberately runs without one names it via components::pipeline::no_mailbox().
// The dispatcher's mailbox can't be one of them — the dispatcher is born after this manager and
// takes its mailbox in its own constructor — so it alone keeps a pre-start setter.
TEST_CASE("services::wal::manager_wal_replicate_t::a WAL manager cannot be built without naming its mailboxes") {
    CHECK_FALSE(
        std::is_constructible_v<manager_wal_replicate_t, resource_t, scheduler_t, configuration::config_wal, log_t&>);
    CHECK(std::is_constructible_v<manager_wal_replicate_t,
                                  resource_t,
                                  scheduler_t,
                                  configuration::config_wal,
                                  log_t&,
                                  address_t,
                                  address_t>);
    CHECK_FALSE(has_bootstrap_sync<manager_wal_replicate_t>);
    CHECK(has_dispatcher_setter<manager_wal_replicate_t>);
}
