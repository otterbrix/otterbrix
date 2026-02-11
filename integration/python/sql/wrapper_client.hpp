#pragma once

#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <components/log/log.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "forward.hpp"
#include "integration/cpp/wrapper_dispatcher.hpp"
#include "spaces.hpp"
#include "wrapper_cursor.hpp"

namespace py = pybind11;
namespace otterbrix {
    class PYBIND11_EXPORT wrapper_client final : public boost::intrusive_ref_counter<wrapper_client> {
    public:
        wrapper_client(spaces_ptr space);
        ~wrapper_client();
        auto execute(const std::string& query) -> wrapper_cursor_ptr;

    private:
        friend class wrapper_connection;

        const std::string name_;
        spaces_ptr ptr_;
        mutable log_t log_;
    };
} // namespace otterbrix