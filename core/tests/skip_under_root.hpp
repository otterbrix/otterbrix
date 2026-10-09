#pragma once

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

namespace test_helpers {

    inline void skip_under_root() {
        if (::geteuid() == 0) {
            SKIP("root ignores file and directory permissions");
        }
    }

} // namespace test_helpers
