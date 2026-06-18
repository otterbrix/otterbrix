#include "pyexpression.hpp"
#include "pyrelation.hpp"
#include <pyconnection/pyconnection.hpp>

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {
    static void initialize_read_only_properties(py::class_<py_relation_t>& m) {
        m.def_property_readonly("columns",
                                &py_relation_t::columns,
                                "Return a list containing the names of the columns of the relation.")
            .def_property_readonly("types",
                                   &py_relation_t::column_types,
                                   "Return a list containing the types of the columns of the relation.")
            .def_property_readonly("dtypes",
                                   &py_relation_t::column_types,
                                   "Return a list containing the types of the columns of the relation.");
    }

    static void initialize_consumers(py::class_<py_relation_t>& m) {
        m.def("fetchone", &py_relation_t::fetch_one, "Execute and fetch a single row as a tuple")
            .def("fetchmany",
                 &py_relation_t::fetch_many,
                 "Execute and fetch the next set of rows as a list of tuples",
                 py::arg("size") = 1)
            .def("fetchall", &py_relation_t::fetch_all, "Execute and fetch all rows as a list of tuples")
            .def("df", &py_relation_t::fetch_df, "Execute and fetch all rows as a pandas DataFrame")
            .def("fetchdf", &py_relation_t::fetch_df, "Execute and fetch all rows as a pandas DataFrame")
            .def("to_df", &py_relation_t::fetch_df, "Execute and fetch all rows as a pandas DataFrame");
    }
    void py_relation_t::initialize(py::handle& m) {
        auto relation_module = py::class_<py_relation_t>(m, "OtterBrixPyRelation", py::module_local());
        initialize_read_only_properties(relation_module);
        initialize_consumers(relation_module);
        relation_module.def("project",
                            &py_relation_t::project,
                            "Project the relation object by the projection in project_expr");
        relation_module.def("select",
                            &py_relation_t::project,
                            "Project the relation object by the projection in project_expr");
        relation_module.def("filter",
                            &py_relation_t::filter,
                            "Filter the relation object by the filter in filter_expr",
                            py::arg("filter_expr"));

        relation_module.def("group", &py_relation_t::group, "Group fields with aggregation expressions")
            .def("order", &py_relation_t::order, "Reorder the relation object by order_expr", py::arg("order_expr"))
            .def("sort", &py_relation_t::sort, "Reorder the relation object by the provided expressions")
            .def("join",
                 &py_relation_t::join,
                 "Join the relation object with another relation object in other_rel using the join condition "
                 "expression "
                 "in join_condition. Types supported are 'inner' and 'left'",
                 py::arg("other_rel"),
                 py::arg("condition") = py::none(),
                 py::arg("how") = "inner")
            .def("cross",
                 &py_relation_t::cross,
                 "Create cross/cartesian product of two relational objects",
                 py::arg("other_rel"));

        relation_module.def("limit", &py_relation_t::limit, "Limit the number of rows returned", py::arg("count"));
        relation_module.def_readwrite("optimize", &py_relation_t::optimize_);
    }

} // namespace otterbrix
