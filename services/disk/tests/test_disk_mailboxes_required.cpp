#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <services/disk/manager_disk.hpp>

namespace {

    using services::disk::manager_disk_t;

    // Detects a bootstrap sync member — the shape where an address arrives by a separate
    // call after construction, leaving the manager with an empty address in between.
    template<typename T>
    concept has_bootstrap_sync = requires { &T::sync; };

    template<typename T>
    concept has_wal_setter = requires(T& t, actor_zeta::address_t a) { t.set_manager_wal_sync(a); };

} // namespace

// The disk manager's sync() used to carry the WAL address in a one-field bundle. The
// manager-level copy it stored was written once and never read; the live half — fanning
// the address into the agents — now travels through a named setter, mirroring
// set_manager_dispatcher_sync. The WAL manager is born after the disk manager (its own
// constructor takes the disk mailbox), so this one address cannot be a constructor
// argument here; the agents' empty-address guards cover the WAL-off topology either way.
TEST_CASE("services::disk::manager_disk_t::a disk manager has no bootstrap sync bundle") {
    CHECK_FALSE(has_bootstrap_sync<manager_disk_t>);
    CHECK(has_wal_setter<manager_disk_t>);
}
