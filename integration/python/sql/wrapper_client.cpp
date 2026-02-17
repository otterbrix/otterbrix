#include "wrapper_client.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "spaces.hpp"
#include <utility>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace otterbrix {

    wrapper_client::wrapper_client(spaces_ptr space)
        : ptr_(std::move(space))
        , log_(ptr_->get_log().clone()) {
        debug(log_, "wrapper_client::wrapper_client()");
    }

    wrapper_client::~wrapper_client() { trace(log_, "delete wrapper_client"); }

    wrapper_cursor_ptr wrapper_client::execute(const std::string& query) {
        debug(log_, "wrapper_client::execute");
        auto session = otterbrix::session_id_t();
        return wrapper_cursor_ptr(
            new wrapper_cursor{ptr_->dispatcher()->execute_sql(session, query), ptr_->dispatcher()});
    }
} // namespace otterbrix