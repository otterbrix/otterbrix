#pragma once
#include <boost/core/span.hpp>
#include <cstring>
#include <memory_resource>

namespace core {

    template<class T>
    using span = boost::span<T>;

} // namespace core