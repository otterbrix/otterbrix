#include "wrapper_client.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "spaces.hpp"
#include <services/collection/collection.hpp>
#include <utility>

#include "wrapper_database.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace otterbrix {

    wrapper_client::wrapper_client(spaces_ptr space)
        : ptr_(std::move(space))
        , log_(ptr_->get_log().clone()) {
        debug(log_, "wrapper_client::wrapper_client()");
    }

    wrapper_database_ptr wrapper_client::get_or_create(const std::string& name) {
        debug(log_, "wrapper_client::get_or_create name database: {}", name);
        auto session_tmp = otterbrix::session_id_t();
        ptr_->dispatcher()->create_database(session_tmp, name);
        auto result = wrapper_database_ptr(new wrapper_database(name, ptr_->dispatcher(), log_));
        debug(log_, "wrapper_client::get_or_create return wrapper_database_ptr");
        names_.emplace(name, result);
        return result;
    }

    wrapper_client::~wrapper_client() { trace(log_, "delete wrapper_client"); }

    auto wrapper_client::database_names() -> py::list {
        py::list tmp;
        for (auto& i : names_) {
            tmp.append(i.first);
        }
        return tmp;
    }

    wrapper_cursor_ptr wrapper_client::execute(const std::string& query) {
        debug(log_, "wrapper_client::execute");
        auto session = otterbrix::session_id_t();
        return wrapper_cursor_ptr(
            new wrapper_cursor{ptr_->dispatcher()->execute_sql(session, query), ptr_->dispatcher()});
    }
} // namespace otterbrix
