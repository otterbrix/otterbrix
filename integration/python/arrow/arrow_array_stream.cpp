#include "arrow_array_stream.hpp"
#include <memory>

#include <connection_environment/connection_environment.hpp>

#include <cassert>
#include <stdexcept>

namespace otterbrix {

    using components::vector::arrow::arrow_array_schema_wrapper_t;
    using components::vector::arrow::arrow_schema_wrapper_t;

    namespace {

        void VerifyArrowDatasetLoaded() {
            auto& import_cache = ConnectionEnvironment::ImportCache();
            if (!ModuleIsLoaded<PyarrowDatasetCacheItem>() || !import_cache.pyarrow.dataset().ptr()) {
                throw std::runtime_error("Optional module 'pyarrow.dataset' is required to perform this action");
            }
        }

    } // namespace

    PyArrowObjectType GetArrowType(const py::handle& object) {
        if (py::isinstance<py::capsule>(object)) {
            return PyArrowObjectType::PyCapsule;
        }
        // The Arrow C-stream PyCapsule interface (polars, pyarrow-backed pandas, pyarrow tables)
        // is recognized without importing pyarrow at all.
        if (py::hasattr(object, "__arrow_c_stream__")) {
            return PyArrowObjectType::PyCapsuleInterface;
        }
        // Only probe pyarrow classes if pyarrow is already imported; never trigger a (potentially
        // failing) import for plain numpy/pandas inputs.
        if (!ModuleIsLoaded<PyarrowCacheItem>()) {
            return PyArrowObjectType::Invalid;
        }
        auto& import_cache = ConnectionEnvironment::ImportCache();
        auto table_class = import_cache.pyarrow.Table();
        if (table_class.ptr() && py::isinstance(object, table_class)) {
            return PyArrowObjectType::Table;
        }
        auto rbr_class = import_cache.pyarrow.RecordBatchReader();
        if (rbr_class.ptr() && py::isinstance(object, rbr_class)) {
            return PyArrowObjectType::RecordBatchReader;
        }
        if (ModuleIsLoaded<PyarrowDatasetCacheItem>() && import_cache.pyarrow.dataset().ptr()) {
            auto scanner_class = import_cache.pyarrow.dataset.Scanner();
            if (scanner_class.ptr() && py::isinstance(object, scanner_class)) {
                return PyArrowObjectType::Scanner;
            }
            auto dataset_class = import_cache.pyarrow.dataset.Dataset();
            if (dataset_class.ptr() && py::isinstance(object, dataset_class)) {
                return PyArrowObjectType::Dataset;
            }
        }
        return PyArrowObjectType::Invalid;
    }

    py::object PythonTableArrowArrayStreamFactory::ProduceScanner(py::object& arrow_scanner, py::handle& arrow_obj_handle) {
        return arrow_scanner(arrow_obj_handle);
    }

    std::unique_ptr<arrow_array_schema_wrapper_t> PythonTableArrowArrayStreamFactory::Produce(uintptr_t factory_ptr) {
        py::gil_scoped_acquire acquire;
        auto factory =
            static_cast<PythonTableArrowArrayStreamFactory*>(reinterpret_cast<void*>(factory_ptr)); // NOLINT
        assert(factory->arrow_object);
        py::handle arrow_obj_handle(factory->arrow_object);
        auto arrow_object_type = GetArrowType(arrow_obj_handle);

        // Arrow C-stream PyCapsule interface (polars / pyarrow-backed pandas): pull the capsule and steal the stream.
        if (arrow_object_type == PyArrowObjectType::PyCapsuleInterface) {
            py::object capsule_obj = arrow_obj_handle.attr("__arrow_c_stream__")();
            auto capsule = py::reinterpret_borrow<py::capsule>(capsule_obj);
            auto stream = capsule.get_pointer<struct ArrowArrayStream>();
            if (!stream->release) {
                throw std::runtime_error("ArrowArrayStream was released by another thread/library");
            }
            auto res = std::make_unique<arrow_array_schema_wrapper_t>();
            res->arrow_array_stream = *stream;
            stream->release = nullptr;
            return res;
        }

        if (arrow_object_type == PyArrowObjectType::PyCapsule) {
            auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
            auto stream = capsule.get_pointer<struct ArrowArrayStream>();
            if (!stream->release) {
                throw std::runtime_error("ArrowArrayStream was released by another thread/library");
            }
            auto res = std::make_unique<arrow_array_schema_wrapper_t>();
            res->arrow_array_stream = *stream;
            stream->release = nullptr;
            return res;
        }

        auto& import_cache = ConnectionEnvironment::ImportCache();
        py::object scanner;
        py::object arrow_batch_scanner = import_cache.pyarrow.dataset.Scanner().attr("from_batches");
        switch (arrow_object_type) {
            case PyArrowObjectType::Table: {
                auto arrow_dataset = import_cache.pyarrow.dataset().attr("dataset");
                auto dataset = arrow_dataset(arrow_obj_handle);
                py::object arrow_scanner = dataset.attr("__class__").attr("scanner");
                scanner = ProduceScanner(arrow_scanner, dataset);
                break;
            }
            case PyArrowObjectType::RecordBatchReader: {
                scanner = ProduceScanner(arrow_batch_scanner, arrow_obj_handle);
                break;
            }
            case PyArrowObjectType::Scanner: {
                auto record_batches = arrow_obj_handle.attr("to_reader")();
                scanner = ProduceScanner(arrow_batch_scanner, record_batches);
                break;
            }
            case PyArrowObjectType::Dataset: {
                py::object arrow_scanner = arrow_obj_handle.attr("__class__").attr("scanner");
                scanner = ProduceScanner(arrow_scanner, arrow_obj_handle);
                break;
            }
            default: {
                auto py_object_type = std::string(py::str(arrow_obj_handle.get_type().attr("__name__")));
                throw std::runtime_error("Object of type " + py_object_type + " is not a recognized Arrow object");
            }
        }

        auto record_batches = scanner.attr("to_reader")();
        auto res = std::make_unique<arrow_array_schema_wrapper_t>();
        auto export_to_c = record_batches.attr("_export_to_c");
        export_to_c(reinterpret_cast<uint64_t>(&res->arrow_array_stream));
        return res;
    }

    void PythonTableArrowArrayStreamFactory::GetSchemaInternal(py::handle arrow_obj_handle,
                                                               arrow_schema_wrapper_t& schema) {
        auto arrow_object_type = GetArrowType(arrow_obj_handle);

        // Arrow C-stream PyCapsule interface: read the schema straight off the stream capsule.
        if (arrow_object_type == PyArrowObjectType::PyCapsuleInterface) {
            py::object capsule_obj = arrow_obj_handle.attr("__arrow_c_stream__")();
            auto capsule = py::reinterpret_borrow<py::capsule>(capsule_obj);
            auto stream = capsule.get_pointer<struct ArrowArrayStream>();
            if (!stream->release) {
                throw std::runtime_error("ArrowArrayStream was released by another thread/library");
            }
            stream->get_schema(stream, &schema.arrow_schema);
            stream->release(stream);
            return;
        }

        if (py::isinstance<py::capsule>(arrow_obj_handle)) {
            auto capsule = py::reinterpret_borrow<py::capsule>(arrow_obj_handle);
            auto stream = capsule.get_pointer<struct ArrowArrayStream>();
            if (!stream->release) {
                throw std::runtime_error("ArrowArrayStream was released by another thread/library");
            }
            stream->get_schema(stream, &schema.arrow_schema);
            return;
        }

        auto& import_cache = ConnectionEnvironment::ImportCache();
        auto table_class = import_cache.pyarrow.Table();
        if (table_class.ptr() && py::isinstance(arrow_obj_handle, table_class)) {
            auto obj_schema = arrow_obj_handle.attr("schema");
            auto export_to_c = obj_schema.attr("_export_to_c");
            export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));
            return;
        }

        VerifyArrowDatasetLoaded();

        auto scanner_class = import_cache.pyarrow.dataset.Scanner();
        if (scanner_class.ptr() && py::isinstance(arrow_obj_handle, scanner_class)) {
            auto obj_schema = arrow_obj_handle.attr("projected_schema");
            auto export_to_c = obj_schema.attr("_export_to_c");
            export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));
        } else {
            auto obj_schema = arrow_obj_handle.attr("schema");
            auto export_to_c = obj_schema.attr("_export_to_c");
            export_to_c(reinterpret_cast<uint64_t>(&schema.arrow_schema));
        }
    }

    void PythonTableArrowArrayStreamFactory::GetSchema(uintptr_t factory_ptr, arrow_schema_wrapper_t& schema) {
        py::gil_scoped_acquire acquire;
        auto factory =
            static_cast<PythonTableArrowArrayStreamFactory*>(reinterpret_cast<void*>(factory_ptr)); // NOLINT
        assert(factory->arrow_object);
        py::handle arrow_obj_handle(factory->arrow_object);
        GetSchemaInternal(arrow_obj_handle, schema);
    }

} // namespace otterbrix
