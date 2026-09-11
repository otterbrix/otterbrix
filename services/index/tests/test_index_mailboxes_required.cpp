#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <services/index/manager_index.hpp>
#include <type_traits>

namespace {

    using services::index::manager_index_t;

    // Detects a bootstrap sync member — the shape where an address arrives by a separate
    // call after construction, leaving the manager with an empty address in between.
    template<typename T>
    concept has_bootstrap_sync = requires {
        &T::sync;
    };

    template<typename T>
    concept has_dispatcher_setter = requires(T& t, actor_zeta::address_t a) {
        t.set_manager_dispatcher_sync(a);
    };

} // namespace

// The index manager's sync() used to deliver a disk address that nothing ever read: the
// member was written once and never used. The bundle, the call and the member are gone;
// the only late-wired address left is the dispatcher's, through its own named setter.
TEST_CASE("services::index::manager_index_t::an index manager has no bootstrap sync step to forget") {
    CHECK_FALSE(has_bootstrap_sync<manager_index_t>);
    CHECK(has_dispatcher_setter<manager_index_t>);
}
