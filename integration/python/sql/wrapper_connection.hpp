#pragma once
#include <components/session/session.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "wrapper_client.hpp"

namespace py = pybind11;

namespace otterbrix {

    class PYBIND11_EXPORT wrapper_connection final : public boost::intrusive_ref_counter<wrapper_connection> {
    public:
        wrapper_connection(wrapper_client* client);

        wrapper_cursor_ptr execute(const std::string& query);
        wrapper_cursor_ptr cursor() const;
        void close();

    private:
        wrapper_client* client_{nullptr};
        wrapper_cursor_ptr cursor_store_{nullptr};
    };

} // namespace otterbrix