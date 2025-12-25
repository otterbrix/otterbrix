#include <catch2/catch.hpp>
#include <components/oid/oid.hpp>

using namespace oid;

struct test_id8_size {
    static constexpr uint size_timestamp = 4;
    static constexpr uint size_random = 2;
    static constexpr uint size_increment = 2;
};

struct test_id12_size {
    static constexpr uint size_timestamp = 4;
    static constexpr uint size_random = 5;
    static constexpr uint size_increment = 3;
};

TEST_CASE("components::oid::is_valid") {
    using oid8_t = oid_t<test_id8_size>;
    REQUIRE(oid8_t::is_valid("0123456789abcdef"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcde"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcdef0"));
    REQUIRE_FALSE(oid8_t::is_valid("0123456789abcdeg"));
}

TEST_CASE("components::oid::initialization") {
    using oid8_t = oid_t<test_id8_size>;
    REQUIRE(oid8_t("0123456789abcdef").to_string() == "0123456789abcdef");
    REQUIRE(oid8_t("0123456789abcde").to_string() == "0000000000000000");
    REQUIRE(oid8_t::null().to_string() == "0000000000000000");
    REQUIRE(oid8_t::max().to_string() == "ffffffffffffffff");
}

TEST_CASE("components::oid::operators") {
    using oid8_t = oid_t<test_id8_size>;
    REQUIRE(oid8_t("0000000000000000") == oid8_t::null());
    REQUIRE(oid8_t::null() < oid8_t("0000000000000001"));
    REQUIRE(oid8_t("fffffffffffffffe") < oid8_t::max());
    REQUIRE(oid8_t("ffffffffffffffff") == oid8_t::max());
}

TEST_CASE("components::oid::generate by time") {
    using oid12_t = oid_t<test_id12_size>;
    REQUIRE(oid12_t(0x1).get_timestamp() == 0x1);
    REQUIRE(oid12_t(0xffff).get_timestamp() == 0xffff);
    REQUIRE(oid12_t(0x12345678).get_timestamp() == 0x12345678);
    REQUIRE(oid12_t(0xffffffff).get_timestamp() == 0xffffffff);
}

#ifdef EXAMPLE_OID_GENERATE
#include <iostream>
TEST_CASE("components::oid::generate") {
    using oid12_t = oid_t<test_id12_size>;
    for (uint i = 0; i < 10; ++i) {
        std::cerr << "~~~ GENERATING N" << i + 1 << " ~~~" << std::endl;
        for (uint j = 0; j < 100; ++j) {
            std::cerr << oid12_t() << std::endl;
        }
    }
}
#endif

#ifdef EXAMPLE_OID_DIFFERENT_TYPE
#include <iostream>
#include <unistd.h>
namespace ns1 {
    struct test_id_size {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_random = 5;
        static constexpr uint size_increment = 3;
    };
    using oid12_t = oid_t<test_id_size>;
} // namespace ns1
namespace ns2 {
    struct test_id_size {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_random = 5;
        static constexpr uint size_increment = 3;
    };
    using oid12_t = oid_t<test_id_size>;
} // namespace ns2
namespace ns3 {
    struct test_id_size {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_random = 5;
        static constexpr uint size_increment = 3;
    };
    using oid12_t = oid_t<test_id_size>;
} // namespace ns3
namespace ns4 {
    struct test_id_size {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_random = 5;
        static constexpr uint size_increment = 3;
    };
    using oid12_t = oid_t<test_id_size>;
} // namespace ns4
TEST_CASE("components::oid::generate") {
    std::cerr << ns1::oid12_t() << std::endl;
    usleep(1000000);
    std::cerr << ns2::oid12_t() << std::endl;
    usleep(1000000);
    std::cerr << ns3::oid12_t() << std::endl;
    usleep(1000000);
    std::cerr << ns4::oid12_t() << std::endl;
    usleep(1000000);
    std::cerr << ns1::oid12_t() << std::endl;
    std::cerr << ns2::oid12_t() << std::endl;
    std::cerr << ns3::oid12_t() << std::endl;
    std::cerr << ns4::oid12_t() << std::endl;
}
#endif
