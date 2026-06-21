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

            // Core to_arrow_schema derives column names from the type aliases; fold names into typed aliases.
            std::pmr::vector<components::types::complex_logical_type> schema_types(std::pmr::get_default_resource());
            schema_types.reserve(types.size());
            for (std::size_t i = 0; i < types.size(); i++) {
                auto t = types[i];
                if (i < names.size()) {
                    t.set_alias(names[i]);
                }
                schema_types.push_back(std::move(t));
            }

            ArrowSchema schema;
            components::vector::arrow::to_arrow_schema(&schema, schema_types);
            auto schema_obj = schema_import_func(reinterpret_cast<uint64_t>(&schema));

            return py::cast<otterbrix::pyarrow::arrow_table_t>(from_batches_func(batches, schema_obj));
        }

    } // namespace pyarrow

} // namespace otterbrix
