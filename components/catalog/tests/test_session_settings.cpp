#include <catch2/catch_test_macros.hpp>
#include <components/catalog/session_settings.hpp>

#include <string>

using namespace components::catalog;

namespace {
    auto* resource = std::pmr::new_delete_resource();

    std::string zone(const session_settings_t& own, const session_catalog_t& defaults = {}) {
        return own.effective(defaults).timezone_name;
    }
} // namespace

TEST_CASE("catalog::session_settings::layers_over_the_defaults") {
    session_settings_t own(resource);
    REQUIRE(own.empty());
    session_catalog_t defaults;
    defaults.timezone_name = "+05:00";
    REQUIRE(zone(own, defaults) == "+05:00");

    own.assign(setting_scope_t::session, setting_id::timezone, "+03:00");
    REQUIRE(zone(own, defaults) == "+03:00");
    REQUIRE(own.effective(defaults).autocommit == defaults.autocommit);

    own.assign(setting_scope_t::local, setting_id::timezone, "utc");
    REQUIRE(zone(own, defaults) == "utc");
    own.assign(setting_scope_t::session, setting_id::timezone, "+01:00");
    REQUIRE(zone(own, defaults) == "+01:00");
}

TEST_CASE("catalog::session_settings::transaction_end") {
    session_settings_t own(resource);
    own.assign(setting_scope_t::session, setting_id::timezone, "+03:00");

    SECTION("COMMIT keeps SET and drops SET LOCAL") {
        own.begin_transaction();
        own.assign(setting_scope_t::session, setting_id::autocommit, "off");
        own.assign(setting_scope_t::local, setting_id::timezone, "utc");
        own.end_transaction(true);
        REQUIRE(zone(own) == "+03:00");
        REQUIRE_FALSE(own.effective({}).autocommit);
    }
    SECTION("ROLLBACK returns to the start") {
        own.begin_transaction();
        own.assign(setting_scope_t::session, setting_id::timezone, "+05:00");
        own.assign(setting_scope_t::session, setting_id::autocommit, "off");
        own.end_transaction(false);
        REQUIRE(zone(own) == "+03:00");
        REQUIRE(own.effective({}).autocommit);
    }
    SECTION("a statement outside a block drops its SET LOCAL") {
        own.assign(setting_scope_t::local, setting_id::timezone, "utc");
        own.end_transaction(true);
        REQUIRE(zone(own) == "+03:00");
    }
    SECTION("a second BEGIN keeps the first start") {
        own.begin_transaction();
        own.assign(setting_scope_t::session, setting_id::timezone, "+05:00");
        own.begin_transaction();
        own.end_transaction(false);
        REQUIRE(zone(own) == "+03:00");
    }
}

TEST_CASE("catalog::session_settings::savepoints") {
    session_settings_t own(resource);
    own.begin_transaction();
    own.assign(setting_scope_t::session, setting_id::timezone, "+01:00");
    own.savepoint("p");
    own.assign(setting_scope_t::session, setting_id::timezone, "+02:00");
    own.savepoint("q");
    own.assign(setting_scope_t::local, setting_id::timezone, "+03:00");

    SECTION("ROLLBACK TO keeps its savepoint") {
        own.rollback_to_savepoint("q");
        REQUIRE(zone(own) == "+02:00");
        own.assign(setting_scope_t::session, setting_id::timezone, "+04:00");
        own.rollback_to_savepoint("q");
        REQUIRE(zone(own) == "+02:00");
        own.rollback_to_savepoint("p");
        REQUIRE(zone(own) == "+01:00");
    }
    SECTION("RELEASE drops the savepoint and the ones after it") {
        own.release_savepoint("p");
        REQUIRE(zone(own) == "+03:00");
        own.end_transaction(false);
        REQUIRE(zone(own) == "UTC");
        REQUIRE(own.empty());
    }
    SECTION("the latest savepoint of a repeated name") {
        own.savepoint("p");
        own.assign(setting_scope_t::session, setting_id::timezone, "+06:00");
        own.rollback_to_savepoint("p");
        REQUIRE(zone(own) == "+03:00");
    }
}

TEST_CASE("catalog::session_settings::reset") {
    session_settings_t own(resource);
    session_catalog_t defaults;
    defaults.timezone_name = "+05:00";
    own.assign(setting_scope_t::session, setting_id::timezone, "+03:00");

    SECTION("RESET takes the session's value away") {
        own.reset(setting_scope_t::session, setting_id::timezone);
        REQUIRE(zone(own, defaults) == "+05:00");
        REQUIRE(own.empty());
    }
    SECTION("RESET also takes a SET LOCAL away") {
        own.begin_transaction();
        own.assign(setting_scope_t::local, setting_id::timezone, "utc");
        own.reset(setting_scope_t::session, setting_id::timezone);
        REQUIRE(zone(own, defaults) == "+05:00");
        own.end_transaction(true);
        REQUIRE(zone(own, defaults) == "+05:00");
    }
    SECTION("SET LOCAL TO DEFAULT masks until the transaction ends") {
        own.begin_transaction();
        own.reset(setting_scope_t::local, setting_id::timezone);
        REQUIRE(zone(own, defaults) == "+05:00");
        own.end_transaction(true);
        REQUIRE(zone(own, defaults) == "+03:00");
    }
    SECTION("ROLLBACK brings the value back") {
        own.begin_transaction();
        own.reset(setting_scope_t::session, setting_id::timezone);
        own.end_transaction(false);
        REQUIRE(zone(own, defaults) == "+03:00");
    }
}
