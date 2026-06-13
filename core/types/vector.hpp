#pragma once

#include <vector>

namespace otterbrix {

    template<class T, class Allocator= std::allocator<T>>
    using vector = std::vector<T, Allocator>;

}