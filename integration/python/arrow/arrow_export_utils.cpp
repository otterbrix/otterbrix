#include "arrow_export_utils.hpp"

#include "arrow_array_stream.hpp"

#include <components/vector/arrow/arrow.hpp>
#include <components/vector/arrow/arrow_converter.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

    void transform_otterbrix_to_arrow_chunk(ArrowSchema& arrow_schema, ArrowArray& data, py::list& batches) {
        py::gil_assert();
        auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
        auto batch_import_func = pyarrow_lib_module.attr("RecordBatch").attr("_import_from_c");
        batches.append(batch_import_func(reinterpret_cast<uint64_t>(&data), reinterpret_cast<uint64_t>(&arrow_schema)));
    }

    namespace pyarrow {

        py::object to_arrow_table(const std::vector<components::types::complex_logical_type>& types,
                                  const std::vector<std::string>& names,
                                  const py::list& batches) {
            py::gil_scoped_acquire acquire;

            auto pyarrow_lib_module = py::module::import("pyarrow").attr("lib");
            auto from_batches_func = pyarrow_lib_module.attr("Table").attr("from_batches");
            auto schema_import_func = pyarrow_lib_module.attr("Schema").attr("_import_from_c");

            // The cursor's names and its types are two lists; to_arrow_schema takes the one
            // record that carries both (M3-B5).
            auto* resource = std::pmr::get_default_resource();
            components::vector::schema_t export_schema(resource);
            export_schema.reserve(types.size());
            for (std::size_t i = 0; i < types.size(); i++) {
                components::vector::column_schema_t record{resource};
                if (i < names.size()) {
                    record.name.assign(names[i].data(), names[i].size());
                }
                record.type = types[i];
                export_schema.push_back(std::move(record));
            }

            ArrowSchema schema;
            // A column type with no Arrow format string cannot be exported. This binding has no
            // error channel of its own, so surface it as an empty table rather than a half-built
            // schema (to_arrow_schema leaves out_schema unpublished on error).
            if (auto error = components::vector::arrow::to_arrow_schema(&schema, export_schema);
                error.contains_error()) {
                return py::none();
            }
            auto schema_obj = schema_import_func(reinterpret_cast<uint64_t>(&schema));

            return py::cast<otterbrix::pyarrow::arrow_table_t>(from_batches_func(batches, schema_obj));
        }

    } // namespace pyarrow

} // namespace otterbrix
