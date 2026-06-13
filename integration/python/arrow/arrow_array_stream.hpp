#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_wrapper.hpp>

#include <core/external_dependencies.hpp>

#include <memory>

namespace otterbrix {

    enum class PyArrowObjectType
    {
        Invalid,
        Table,
        RecordBatchReader,
        Scanner,
        Dataset,
        PyCapsule,
        PyCapsuleInterface
    };

    //! Classify a python handle into one of the recognized Arrow object kinds.
    PyArrowObjectType GetArrowType(const py::handle& object);

    namespace pyarrow {

        class RecordBatchReader : public py::object {
        public:
            RecordBatchReader(const py::object& o)
                : py::object(o, borrowed_t{}) {}
            using py::object::object;

        public:
            static bool check_(const py::handle& object) { return !py::none().is(object); }
        };

        class Table : public py::object {
        public:
            Table(const py::object& o)
                : py::object(o, borrowed_t{}) {}
            using py::object::object;

        public:
            static bool check_(const py::handle& object) { return !py::none().is(object); }
        };

    } // namespace pyarrow

    //! Factory that turns a python Arrow object (capsule / table / record-batch-reader / dataset)
    //! into a core components::vector::arrow stream wrapper that the arrow table function can drain
    //! through core data_chunk_from_arrow.
    class PythonTableArrowArrayStreamFactory {
    public:
        explicit PythonTableArrowArrayStreamFactory(PyObject* arrow_table)
            : arrow_object(arrow_table) {};

        //! Produces an Arrow stream wrapper; called once when initializing scan state.
        static std::unique_ptr<components::vector::arrow::arrow_array_schema_wrapper_t> Produce(uintptr_t factory);

        //! Get the schema of the arrow object.
        static void GetSchemaInternal(py::handle arrow_object,
                                      components::vector::arrow::arrow_schema_wrapper_t& schema);
        static void GetSchema(uintptr_t factory_ptr, components::vector::arrow::arrow_schema_wrapper_t& schema);

        //! Arrow Object (i.e., Scanner, Record Batch Reader, Table, Dataset, or Arrow C stream capsule)
        PyObject* arrow_object;

    private:
        static py::object ProduceScanner(py::object& arrow_scanner, py::handle& arrow_obj_handle);
    };

    //! Owns the arrow stream factory and the underlying python arrow object for the lifetime of a
    //! replacement scan, so the void* factory pointer threaded into the arrow_scan table function
    //! (and the python object it borrows) stay alive.
    class ArrowStreamFactoryDependency : public DependencyItem {
    public:
        explicit ArrowStreamFactoryDependency(py::object arrow_object)
            : owned_object(std::move(arrow_object))
            , factory(std::make_unique<PythonTableArrowArrayStreamFactory>(owned_object.ptr())) {}
        ~ArrowStreamFactoryDependency() override = default;

        PythonTableArrowArrayStreamFactory* get() { return factory.get(); }

    private:
        py::object owned_object;
        std::unique_ptr<PythonTableArrowArrayStreamFactory> factory;
    };
} // namespace otterbrix

namespace pybind11 {
    namespace detail {
        template<>
        struct handle_type_name<otterbrix::pyarrow::RecordBatchReader> {
            static constexpr auto name = _("pyarrow.lib.RecordBatchReader");
        };
        template<>
        struct handle_type_name<otterbrix::pyarrow::Table> {
            static constexpr auto name = _("pyarrow.lib.Table");
        };
    } // namespace detail
} // namespace pybind11
