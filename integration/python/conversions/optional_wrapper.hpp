#pragma once

#include <native/python_objects.hpp> 

namespace py = pybind11;

namespace PYBIND11_NAMESPACE {
namespace detail {

template <class T>
struct type_caster<otterbrix::Optional<T>> : public type_caster_base<otterbrix::Optional<T>> {
    using base = type_caster_base<otterbrix::Optional<T>>;
    using child = type_caster_base<T>; 
    otterbrix::Optional<T> tmp;

public:
    bool load(handle src, bool convert) {
        if (base::load(src, convert)) {
            return true;
        } else if (child::load(src, convert)) {
            return true;
        }
        return false;
    }

    static handle cast(otterbrix::Optional<T> src, return_value_policy policy, handle parent) {
        return base::cast(src, policy, parent);
    }
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE

