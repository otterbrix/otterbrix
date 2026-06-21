#pragma once

#include <native/python_objects.hpp>

namespace py = pybind11;

namespace PYBIND11_NAMESPACE { namespace detail {

    template<class T>
    struct type_caster<otterbrix::py_optional_t<T>> : public type_caster_base<otterbrix::py_optional_t<T>> {
        using base = type_caster_base<otterbrix::py_optional_t<T>>;
        using child = type_caster_base<T>;
        otterbrix::py_optional_t<T> tmp;

    public:
        bool load(handle src, bool convert) {
            if (base::load(src, convert)) {
                return true;
            } else if (child::load(src, convert)) {
                return true;
            }
            return false;
        }

        static handle cast(otterbrix::py_optional_t<T> src, return_value_policy policy, handle parent) {
            return base::cast(src, policy, parent);
        }
    };

}} // namespace PYBIND11_NAMESPACE::detail
