#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_wrapper.hpp>

#include <common/external_dependencies.hpp>

#include <memory>

namespace otterbrix {

    enum class py_arrow_object_type_t
    {
        Invalid,
        arrow_table_t,
        arrow_record_batch_reader_t,
        Scanner,
        Dataset,
        PyCapsule,
        PyCapsuleInterface
    };

    //! Classify a python handle into one of the recognized Arrow object kinds.
    py_arrow_object_type_t get_arrow_type(const py::handle& object);

    namespace pyarrow {

        class arrow_record_batch_reader_t : public py::object {
        public:
            arrow_record_batch_reader_t(const py::object& o)
                : py::object(o, borrowed_t{}) {}
            using py::object::object;

        public:
            static bool check_(const py::handle& object) { return !py::none().is(object); }
        };

        class arrow_table_t : public py::object {
        public:
            arrow_table_t(const py::object& o)
                : py::object(o, borrowed_t{}) {}
            using py::object::object;

        public:
            static bool check_(const py::handle& object) { return !py::none().is(object); }
        };

    } // namespace pyarrow

    //! Factory that turns a python Arrow object (capsule / table / record-batch-reader / dataset)
    //! into a core components::vector::arrow stream wrapper that the arrow table function can drain
    //! through core data_chunk_from_arrow.
    class python_table_arrow_array_stream_factory_t {
    public:
        explicit python_table_arrow_array_stream_factory_t(PyObject* arrow_table)
            : arrow_object(arrow_table){};

        //! Produces an Arrow stream wrapper; called once when initializing scan state.
        static std::unique_ptr<components::vector::arrow::arrow_array_schema_wrapper_t> produce(uintptr_t factory);

        //! get the schema of the arrow object.
        static void get_schema_internal(py::handle arrow_object,
                                        components::vector::arrow::arrow_schema_wrapper_t& schema);
        static void get_schema(uintptr_t factory_ptr, components::vector::arrow::arrow_schema_wrapper_t& schema);

        //! Arrow Object (i.e., Scanner, Record Batch Reader, arrow_table_t, Dataset, or Arrow C stream capsule)
        PyObject* arrow_object;

    private:
        static py::object produce_scanner(py::object& arrow_scanner, py::handle& arrow_obj_handle);
    };

    //! Owns the arrow stream factory and the underlying python arrow object for the lifetime of a
    //! replacement scan, so the void* factory pointer threaded into the arrow_scan table function
    //! (and the python object it borrows) stay alive.
    class arrow_stream_factory_dependency_t : public dependency_item_t {
    public:
        explicit arrow_stream_factory_dependency_t(py::object arrow_object)
            : owned_object(std::move(arrow_object))
            , factory(std::make_unique<python_table_arrow_array_stream_factory_t>(owned_object.ptr())) {}
        ~arrow_stream_factory_dependency_t() override = default;

        python_table_arrow_array_stream_factory_t* get() { return factory.get(); }

    private:
        py::object owned_object;
        std::unique_ptr<python_table_arrow_array_stream_factory_t> factory;
    };
} // namespace otterbrix

namespace pybind11 { namespace detail {
    template<>
    struct handle_type_name<otterbrix::pyarrow::arrow_record_batch_reader_t> {
        static constexpr auto name = _("pyarrow.lib.RecordBatchReader");
    };
    template<>
    struct handle_type_name<otterbrix::pyarrow::arrow_table_t> {
        static constexpr auto name = _("pyarrow.lib.Table");
    };
}} // namespace pybind11::detail
