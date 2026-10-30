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
        , arena_(ptr_->arena())
        , log_(ptr_->get_log().clone()) {
        debug(log_, "wrapper_client::wrapper_client()");
    }

    wrapper_client::~wrapper_client() {
        trace(log_, "delete wrapper_client");
        close();
    }

    wrapper_cursor_ptr wrapper_client::execute(const std::string& query) {
        debug(log_, "wrapper_client::execute");
        return wrapper_cursor_ptr(new wrapper_cursor{execute_sql(query), wrapper_client_ptr{this}});
    }

    components::cursor::cursor_t_ptr wrapper_client::execute_sql(const std::string& query) {
        if (closed_) {
            auto* resource = &arena_->resource;
            return components::cursor::make_cursor(resource,
                                                   core::error_t{core::error_code_t::connection_closed,
                                                                 std::pmr::string{"the session is closed", resource}});
        }
        return ptr_->dispatcher()->execute_sql(session_, query);
    }

    void wrapper_client::close() {
        if (closed_) {
            return;
        }
        closed_ = true;
        py::gil_scoped_release release;
        ptr_->dispatcher()->close_session(session_);
        ptr_ = nullptr;
    }
} // namespace otterbrix