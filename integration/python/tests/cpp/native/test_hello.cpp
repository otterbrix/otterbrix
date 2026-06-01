#include <catch2/catch.hpp>
#include <pyconnection/pyconnection.hpp>
#include <otterbrix_wrapper/pyexpression.hpp>
#include <pybind11/pybind_wrapper.hpp>
#include <native/python_conversion.hpp>
#include <variant>
#include <pybind11/embed.h>
using namespace otterbrix;

TEST_CASE("HELLO") {
    components::types::logical_value_t logical_value;
    {
        py::scoped_interpreter guard{}; 
        string db_name = "test_db";
        py::str db_str = db_name;
        py::dict config_dict;

        auto conn = otterbrix::PyConnection::Connect(db_str, true, config_dict);
        logical_value = TransformPythonValue(py::int_(-1));

        auto exp = conn->MakeConstant(logical_value);
        
        REQUIRE();

        //auto value = std::get<components::document::value_t>(exp);
        conn->Close();
        PyConnection::Cleanup();

    }
    
    
}