#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <services/disk/manager_disk.hpp>

namespace {

    using services::disk::manager_disk_t;

    // Bootstrap sync: an address arriving via a post-construction call, not the constructor.
    template<typename T>
    concept has_bootstrap_sync = requires {
        &T::sync;
    };

    template<typename T>
    concept has_wal_setter = requires(T& t, actor_zeta::address_t a) {
        t.set_manager_wal_sync(a);
    };

} // namespace

// The WAL manager is constructed after the disk manager, so its address cannot be a
// constructor argument and must arrive via set_manager_wal_sync instead.
TEST_CASE("services::disk::manager_disk_t::a disk manager has no bootstrap sync bundle") {
    CHECK_FALSE(has_bootstrap_sync<manager_disk_t>);
    CHECK(has_wal_setter<manager_disk_t>);
}
