#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <pyutil.hpp>

#include <components/types/types.hpp>
#include <components/types/logical_value.hpp>

//#include "datetime.h" //from python

#include <core/typedefs.hpp>
#include <core/types/memory.hpp>
#include <core/types/vector.hpp>
#include <core/types/string.hpp>

#include <array>
#include <chrono>
#include <utility>

/* Backport for Python < 3.10 */
#if PY_VERSION_HEX < 0x030a00a1
#ifndef PyDateTime_TIME_GET_TZINFO
#define PyDateTime_TIME_GET_TZINFO(o) ((((PyDateTime_Time *)o)->hastzinfo) ? ((PyDateTime_Time *)o)->tzinfo : Py_None)
#endif
#ifndef PyDateTime_DATE_GET_TZINFO
#define PyDateTime_DATE_GET_TZINFO(o)                                                                                  \
	((((PyDateTime_DateTime *)o)->hastzinfo) ? ((PyDateTime_DateTime *)o)->tzinfo : Py_None)
#endif
#endif


namespace otterbrix {

	struct PyDictionary {
	public:
		PyDictionary(py::object dict);
		// These are cached so we don't have to create new objects all the time
		// The CPython API offers PyDict_Keys but that creates a new reference every time, same for values
		py::object keys;
		py::object values;
		idx_t len;

	public:
		py::handle operator[](const py::object &obj) const {
			return PyDict_GetItem(dict.ptr(), obj.ptr());
		}

	public:
		string ToString() const {
			return string(py::str(dict));
		}

	private:
		py::object dict;
	};

	enum class PyDecimalExponentType {
		EXPONENT_SCALE,    //! Amount of digits after the decimal point
		EXPONENT_POWER,    //! How many zeros behind the decimal point
		EXPONENT_INFINITY, //! Decimal is INFINITY
		EXPONENT_NAN       //! Decimal is NAN
	};

	struct PyDecimal {

		struct PyDecimalScaleConverter {
			template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer, T>>
			static components::types::logical_value_t Operation(
					bool signed_value, 
					vector<uint8_t> &digits, 
					uint8_t width, uint8_t scale) {
				T value = 0;
				for (auto it = digits.begin(); it != digits.end(); it++) {
					value = value * 10 + *it;
				}
				if (signed_value) {
					value = -value;
				}
				return components::types::logical_value_t::create_decimal(value, width, scale);
			}
		};

		struct PyDecimalPowerConverter {
		private:
			template <std::size_t... Ints>
			static constexpr auto make_pow10_sequence(std::index_sequence<Ints...>) {
				auto pow10 = [](std::size_t index) constexpr {
					int64_t res = 1;
					for (int i = 0; i < index; ++i) {
						res *= 10;
					}
					return res;
				};
				return std::array<int64_t, sizeof...(Ints)>{pow10(Ints)...};
			}

		public:

			template <typename T, typename = std::enable_if<std::numeric_limits<T>::is_integer, T>>
			static components::types::logical_value_t Operation(
					bool signed_value, 
					vector<uint8_t> &digits, 
					uint8_t width, uint8_t scale) {
				T value = 0;
				for (auto &digit : digits) {
					value = value * 10 + digit;
				}
				assert(scale >= 0);

				constexpr uint8_t CACHED_POWERS_OF_TEN = 19;
				constexpr auto POWERS_OF_TEN = make_pow10_sequence(std::make_index_sequence<CACHED_POWERS_OF_TEN>{});

				int64_t multiplier =
					POWERS_OF_TEN[std::min<uint8_t>(scale, CACHED_POWERS_OF_TEN - 1)];
				for (auto power = scale; power > CACHED_POWERS_OF_TEN; power--) {
					multiplier *= 10;
				}
				value *= multiplier;
				if (signed_value) {
					value = -value;
				}
				return components::types::logical_value_t::create_decimal(value, width, scale);
			}
		};

	public:
		PyDecimal(py::handle &obj);
		vector<uint8_t> digits;
		bool signed_value = false;

		PyDecimalExponentType exponent_type;
		int32_t exponent_value;

	public:
		bool TryGetType(components::types::complex_logical_type &type);
		components::types::logical_value_t to_logical_value();

	private:
		void SetExponent(py::handle &exponent);
		py::handle &obj;
	};

	struct PythonObject {
		static void Initialize();
		static py::object FromStruct(const components::types::logical_value_t &value, const components::types::complex_logical_type &id);
		static py::object FromValue(const components::types::logical_value_t &value, const components::types::complex_logical_type &id);
	};

	template <class T>
	class Optional : public py::object {
	public:
		Optional(const py::object &o) : py::object(o, borrowed_t {}) {
		}
		using py::object::object;

	public:
		static bool check_(const py::handle &object) {
			return object.is_none() || py::isinstance<T>(object);
		}
	};

	class FileLikeObject : public py::object {
	public:
		FileLikeObject(const py::object &o) : py::object(o, borrowed_t {}) {
		}
		using py::object::object;

	public:
		static bool check_(const py::handle &object) {
			return py::isinstance(object, py::module::import("io").attr("IOBase"));
		}
	};

} // namespace otterbrix

namespace pybind11 {
	namespace detail {
		template <typename T>
		struct handle_type_name<otterbrix::Optional<T>> {
			static constexpr auto name = const_name("typing.Optional[") + concat(make_caster<T>::name) + const_name("]");
		};
	} // namespace detail
}

