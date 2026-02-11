#pragma once
#include <components/cursor/cursor.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <integration/cpp/wrapper_dispatcher.hpp>

namespace py = pybind11;

class PYBIND11_EXPORT wrapper_cursor final : public boost::intrusive_ref_counter<wrapper_cursor> {
public:
    using pointer = components::cursor::cursor_t_ptr;

    wrapper_cursor(pointer cursor, otterbrix::wrapper_dispatcher_t* dispatcher);

    void close();
    bool has_next();
    wrapper_cursor& next();
    wrapper_cursor& iter();
    std::size_t size();
    py::object get(py::object key);
    bool is_success() const noexcept;
    bool is_error() const noexcept;
    py::tuple get_error() const;
    std::string print();
    wrapper_cursor& sort(py::object sorter, py::object order);
    void execute(std::string& query);

    py::object fetchone();
    py::list fetchmany(int size);
    py::list fetchall();
    py::object description() const;
    int64_t rowcount() const;

private:
    std::atomic_bool close_;
    pointer ptr_;
    otterbrix::wrapper_dispatcher_t* dispatcher_;

    py::object get_(const std::string& key) const;
    py::object get_(std::size_t index) const;
    py::object fetch_current_row_();
};

using wrapper_cursor_ptr = boost::intrusive_ptr<wrapper_cursor>;