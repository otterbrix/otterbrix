#include "python_objects.hpp"

#include <connection_environment/connection_environment.hpp>

#include <components/types/types.hpp>

#include <ctime>
#include <limits>
#include <memory_resource>
#include <sstream>
#include <string>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::vector_t;

namespace otterbrix {

    static core::error_t make_error(core::error_code_t code, std::pmr::memory_resource* r, const std::string& what) {
        return core::error_t{code, std::pmr::string{what, r}};
    }

    py_dictionary_t::py_dictionary_t(py::object dict) {
        keys = py::list(dict.attr("keys")());
        values = py::list(dict.attr("values")());
        len = py::len(keys);
        this->dict = std::move(dict);
    }

    py_decimal_t::py_decimal_t(py::handle& obj)
        : obj(obj) {
        auto as_tuple = obj.attr("as_tuple")();

        py::object exponent = as_tuple.attr("exponent");
        set_exponent(exponent);

        auto sign = py::cast<int8_t>(as_tuple.attr("sign"));
        signed_value = sign != 0;

        auto decimal_digits = as_tuple.attr("digits");
        auto width = py::len(decimal_digits);
        digits.reserve(width);
        for (auto digit : decimal_digits) {
            digits.push_back(py::cast<uint8_t>(digit));
        }
    }

    bool py_decimal_t::try_get_type(std::pmr::memory_resource* resource, complex_logical_type& type) {
        int32_t width = static_cast<int32_t>(digits.size());

        if (!exponent_recognized) {
            // Failed to convert decimal.Decimal value, exponent type is unknown
            return false;
        }

        switch (exponent_type) {
            case py_decimal_exponent_type_t::EXPONENT_SCALE: {
                case py_decimal_exponent_type_t::EXPONENT_POWER: {
                    uint8_t scale = static_cast<uint8_t>(exponent_value);
                    if (exponent_type == py_decimal_exponent_type_t::EXPONENT_POWER) {
                        width += scale;
                    }
                    if (scale > width) {
                        // The value starts with 1 or more zeros, which are optimized out of the 'digits' array
                        // 0.001; width=1, exponent=-3
                        width = scale + 1; // DECIMAL(4,3) - add 1 for the non-decimal values
                    }
                    if (width > std::numeric_limits<int64_t>::digits10) {
                        type = logical_type::DOUBLE;
                        return true;
                    }
                    auto decimal = complex_logical_type::create_decimal(resource,
                                                                        static_cast<uint8_t>(width),
                                                                        static_cast<uint8_t>(scale));
                    if (decimal.has_error()) {
                        // A python Decimal whose (width, scale) falls outside the engine's
                        // DECIMAL window has no type here — reported, not approximated.
                        return false;
                    }
                    type = std::move(decimal.value());
                    return true;
                }
                case py_decimal_exponent_type_t::EXPONENT_INFINITY: {
                    type = logical_type::FLOAT;
                    return true;
                }
                case py_decimal_exponent_type_t::EXPONENT_NAN: {
                    type = logical_type::FLOAT;
                    return true;
                }
                default: // LCOV_EXCL_START
                    return false;
            } // LCOV_EXCL_STOP
        }
        return true;
    }

    void py_decimal_t::set_exponent(py::handle& exponent) {
        if (py::isinstance<py::int_>(exponent)) {
            this->exponent_value = py::cast<int32_t>(exponent);
            if (this->exponent_value >= 0) {
                exponent_type = py_decimal_exponent_type_t::EXPONENT_POWER;
                return;
            }
            exponent_value *= -1;
            exponent_type = py_decimal_exponent_type_t::EXPONENT_SCALE;
            return;
        }
        if (py::isinstance<py::str>(exponent)) {
            std::string exponent_string = py::str(exponent);
            if (exponent_string == "n") {
                exponent_type = py_decimal_exponent_type_t::EXPONENT_NAN;
                return;
            }
            if (exponent_string == "F") {
                exponent_type = py_decimal_exponent_type_t::EXPONENT_INFINITY;
                return;
            }
        }
        // LCOV_EXCL_START
        // Failed to convert decimal.Decimal value, exponent type is unknown.
        // Recorded here without throwing; surfaced by try_get_type / to_vector.
        exponent_recognized = false;
        // LCOV_EXCL_STOP
    }

    // width >= 1: DECIMAL(0, ...) is outside the engine's window (is_valid_decimal_spec), so
    // it routes to DOUBLE instead of a type create_decimal would refuse.
    static bool width_fits_in_decimal(int32_t width) {
        return width >= 1 && width <= std::numeric_limits<int64_t>::digits10;
    }

    template<class OP>
    vector_t PyDecimalCastSwitch(std::pmr::memory_resource* r,
                                 py_decimal_t& decimal,
                                 const complex_logical_type& type,
                                 uint8_t scale) {
        return OP::template Operation<int64_t>(r, decimal.signed_value, decimal.digits, type, scale);
    }

    // Won't fit in a DECIMAL, fall back to DOUBLE
    static vector_t cast_to_double(std::pmr::memory_resource* r, py::handle& obj) {
        return components::vector::make_scalar(r, py::cast<double>(obj));
    }

    core::result_wrapper_t<vector_t> py_decimal_t::to_vector(std::pmr::memory_resource* r) {
        if (!exponent_recognized) {
            return make_error(core::error_code_t::unimplemented_yet,
                              r,
                              "Failed to convert decimal.Decimal value, exponent type is unknown");
        }
        int32_t width = static_cast<int32_t>(digits.size());
        if (!width_fits_in_decimal(width)) {
            return cast_to_double(r, obj);
        }
        switch (exponent_type) {
            case py_decimal_exponent_type_t::EXPONENT_SCALE: {
                uint8_t scale = static_cast<uint8_t>(exponent_value);
                assert(width_fits_in_decimal(width));
                if (scale > width) {
                    // Values like '0.001'
                    width = scale + 1; // leave 1 room for the non-decimal value
                }
                if (!width_fits_in_decimal(width)) {
                    return cast_to_double(r, obj);
                }
                VALUE_OR_RETURN(auto scale_type,
                                complex_logical_type::create_decimal(r, static_cast<uint8_t>(width), scale));
                return PyDecimalCastSwitch<py_decimal_scale_converter_t>(r, *this, scale_type, scale);
            }
            case py_decimal_exponent_type_t::EXPONENT_POWER: {
                uint8_t scale = static_cast<uint8_t>(exponent_value);
                width += scale;
                if (!width_fits_in_decimal(width)) {
                    return cast_to_double(r, obj);
                }
                VALUE_OR_RETURN(auto power_type,
                                complex_logical_type::create_decimal(r, static_cast<uint8_t>(width), scale));
                return PyDecimalCastSwitch<py_decimal_power_converter_t>(r, *this, power_type, scale);
            }
            case py_decimal_exponent_type_t::EXPONENT_NAN: {
                return components::vector::make_scalar(r, NAN);
            }
            case py_decimal_exponent_type_t::EXPONENT_INFINITY: {
                return components::vector::make_scalar(r, INFINITY);
            }
            // LCOV_EXCL_START
            default: {
                return make_error(core::error_code_t::unimplemented_yet,
                                  r,
                                  "case not implemented for type PyDecimalExponentType");
            } // LCOV_EXCL_STOP
        }
    }

    bool key_is_hashable(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
            case logical_type::TINYINT:
            case logical_type::SMALLINT:
            case logical_type::INTEGER:
            case logical_type::BIGINT:
            case logical_type::UTINYINT:
            case logical_type::USMALLINT:
            case logical_type::UINTEGER:
            case logical_type::UBIGINT:
            case logical_type::HUGEINT:
            case logical_type::UHUGEINT:
            case logical_type::FLOAT:
            case logical_type::DOUBLE:
            case logical_type::DECIMAL:
            case logical_type::ENUM:
            case logical_type::STRING_LITERAL:
            case logical_type::BLOB:
            case logical_type::BIT:
            case logical_type::TIMESTAMP:
            case logical_type::UUID:
                return true;
            case logical_type::LIST:
            case logical_type::ARRAY:
            case logical_type::MAP:
                return false;
            case logical_type::UNION: {
                const auto& child_types = type.child_types();
                for (idx_t i = 0; i < child_types.size(); i++) {
                    if (!key_is_hashable(child_types[i])) {
                        return false;
                    }
                }
                // Only if all the member types are hashable do we say the entire UNION is hashable
                return true;
            }
            case logical_type::STRUCT:
                return false;
            default:
                // Unsupported key types are treated as non-hashable here; the genuine "unsupported type"
                // error is surfaced when the key/value is converted.
                return false;
        }
    }

} // namespace otterbrix
