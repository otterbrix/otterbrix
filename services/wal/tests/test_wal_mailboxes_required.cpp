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

    // Detects a bootstrap sync member — the shape where addresses arrive by a separate
    // call after construction, leaving the manager with empty addresses in between.
    template<typename T>
    concept has_bootstrap_sync = requires { &T::sync; };

    template<typename T>
    concept has_dispatcher_setter = requires(T& t, address_t a) { t.set_manager_dispatcher_sync(a); };

} // namespace

// The WAL manager's disk/dispatcher/index addresses used to default to empty_address()
// and arrive only through a later sync() bundle: between birth and that call every
// auto-checkpoint round saw three empty mailboxes. The disk and index mailboxes are
// constructor arguments now — the shape that omits them is the one that must NOT
// compile; a topology that deliberately runs without one names it
// (components::pipeline::no_mailbox()). The dispatcher's mailbox is the single address
// that cannot be a constructor argument — the dispatcher is born last and takes THIS
// manager's mailbox in its own constructor — so it keeps the same named pre-start
// setter the disk and index managers use.
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
