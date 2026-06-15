#include "pybind_wrapper.hpp"

#include <stdexcept>

namespace pybind11 {

// NOLINTNEXTLINE(readability-identifier-naming)
bool gil_check() {
	return PyGILState_Check() != 0;
}

// NOLINTNEXTLINE(readability-identifier-naming)
void gil_assert() {
	if (!gil_check()) {
		throw std::runtime_error("The GIL should be held for this operation, but it's not!");
	}
}

} // namespace pybind11
