#include "pyrelation.hpp"
#include "pyexpression.hpp"
#include <pyconnection/pyconnection.hpp>

#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {
    static void InitializeReadOnlyProperties(py::class_<PyRelation> &m) {
            m.def_property_readonly("columns", &PyRelation::Columns,
                               "Return a list containing the names of the columns of the relation.")
            .def_property_readonly("types", &PyRelation::ColumnTypes,
                               "Return a list containing the types of the columns of the relation.")
            .def_property_readonly("dtypes", &PyRelation::ColumnTypes,
                               "Return a list containing the types of the columns of the relation.");
    }

    static void InitializeConsumers(py::class_<PyRelation> &m) {
        m.def("fetchone", &PyRelation::FetchOne, "Execute and fetch a single row as a tuple")
            .def("fetchmany", &PyRelation::FetchMany, "Execute and fetch the next set of rows as a list of tuples", py::arg("size") = 1)
            .def("fetchall", &PyRelation::FetchAll, "Execute and fetch all rows as a list of tuples")
            .def("df", &PyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame")
            .def("fetchdf", &PyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame")
            .def("to_df", &PyRelation::FetchDF, "Execute and fetch all rows as a pandas DataFrame");
    }
    void PyRelation::Initialize(py::handle& m) {
        auto relation_module = py::class_<PyRelation>(m, "OtterBrixPyRelation", py::module_local());
        InitializeReadOnlyProperties(relation_module);
        InitializeConsumers(relation_module);
        relation_module.def("project", &PyRelation::Project, 
                "Project the relation object by the projection in project_expr");
        relation_module.def("select", &PyRelation::Project, 
                "Project the relation object by the projection in project_expr");
        relation_module.def("filter", &PyRelation::Filter, "Filter the relation object by the filter in filter_expr",
                py::arg("filter_expr"));

        relation_module
            .def("group", &PyRelation::Group, "Group fields with aggregation expressions")
            .def("order", &PyRelation::Order, "Reorder the relation object by order_expr", py::arg("order_expr")) 
            .def("sort", &PyRelation::Sort, "Reorder the relation object by the provided expressions") 
            .def("join", &PyRelation::Join, 
                    "Join the relation object with another relation object in other_rel using the join condition expression "
                    "in join_condition. Types supported are 'inner' and 'left'",
                    py::arg("other_rel"), py::arg("condition") = py::none(), py::arg("how") = "inner")
            .def("cross", &PyRelation::Cross, "Create cross/cartesian product of two relational objects", py::arg("other_rel"));

    }
    
} // namespace otterbrix
