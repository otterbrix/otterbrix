#pragma once

#include <cstdint>

#include <components/oid/oid.hpp>

namespace components::document {

    struct document_id_size {
        static constexpr std::uint32_t size_timestamp = 4;
        static constexpr std::uint32_t size_random = 5;
        static constexpr std::uint32_t size_increment = 3;
    };

    using document_id_t = oid::oid_t<document_id_size>;

} // namespace components::document