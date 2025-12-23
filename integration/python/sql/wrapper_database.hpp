#pragma once

#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/log/log.hpp>

#include "forward.hpp"
#include "integration/cpp/wrapper_dispatcher.hpp"

namespace py = pybind11;

namespace otterbrix {

    class PYBIND11_EXPORT wrapper_database final : public boost::intrusive_ref_counter<wrapper_database> {
    public:
        wrapper_database(const std::string& name, wrapper_dispatcher_t*, log_t& log);
        ~wrapper_database();
        auto collection_names() -> py::list;
        wrapper_collection_ptr create(const std::string& collection_name);
        bool drop_collection(const std::string& collection_name);

    private:
        const std::string name_;
        wrapper_dispatcher_t* ptr_;
        log_t log_;
        std::unordered_map<std::string, wrapper_collection_ptr> collections_;
    };

    using wrapper_database_ptr = boost::intrusive_ptr<wrapper_database>;
} // namespace otterbrix