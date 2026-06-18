#include <catch2/catch.hpp>
#include <native/python_conversion.hpp>
#include <otterbrix_wrapper/pyexpression.hpp>
#include <pybind11/embed.h>
#include <pybind11/pybind_wrapper.hpp>
#include <pyconnection/pyconnection.hpp>
#include <string>
#include <variant>
using namespace otterbrix;

TEST_CASE("HELLO") {
    components::types::logical_value_t logical_value;
    {
        py::scoped_interpreter guard{};
        std::string db_name = "test_db";
        py::str db_str = db_name;
        py::dict config_dict;

        auto conn = otterbrix::py_connection_t::connect(db_str, true, config_dict);
        logical_value = transform_python_value(py::int_(-1));

        auto exp = conn->make_constant(logical_value);

        REQUIRE();

        //auto value = std::get<components::document::value_t>(exp);
        conn->close();
        py_connection_t::cleanup();
    }
}