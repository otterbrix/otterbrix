#include "arrow_scan_function.hpp"
#include <memory>

#include <otterbrix_wrapper/python_dependency.hpp>

#include <components/vector/arrow/scaner/arrow_type.hpp>
#include <components/tableref/tableref.hpp>

#include <algorithm>
#include <cassert>
#include <memory_resource>
#include <mutex>
#include <stdexcept>

namespace otterbrix {

    using namespace components::function;
    using components::types::complex_logical_type;
    using components::vector::arrow::arrow_array_schema_wrapper_t;
    using components::vector::arrow::arrow_array_wrapper_t;
    using components::vector::arrow::arrow_schema_wrapper_t;
    using components::vector::arrow::arrow_table_schema_t;
    using components::vector::arrow::data_chunk_from_arrow;
    using components::vector::arrow::schema_from_arrow;

    namespace {

        struct ArrowScanFunctionData : public TableFunctionData {
            ArrowScanFunctionData(uintptr_t factory_ptr_p, DependencyItem* dependency_p)
                : factory_ptr(factory_ptr_p)
                , dependency(dependency_p) {}

            std::unique_ptr<FunctionData> Copy() const override {
                throw std::runtime_error("ArrowScanFunctionData::Copy not supported");
            }
            bool Equals(const FunctionData&) const override { return false; }

            //! Pointer to the PythonTableArrowArrayStreamFactory. The owning ExternalDependency
            //! (held by the TableRef) outlives this FunctionData, so `dependency` is a non-owning
            //! observer kept only to document the borrow.
            uintptr_t factory_ptr;
            DependencyItem* dependency;
        };

        struct ArrowScanGlobalState : public GlobalTableFunctionState {
            explicit ArrowScanGlobalState(std::unique_ptr<arrow_array_schema_wrapper_t> stream_p)
                : stream(std::move(stream_p)) {}

            uint64_t MaxThreads() const override { return 1; }

            std::mutex lock;
            std::unique_ptr<arrow_array_schema_wrapper_t> stream;
            //! Lazily built on the first scan with the plan resource (see ArrowScanFunc).
            std::unique_ptr<arrow_table_schema_t> schema;
            //! Currently converted record batch and our read offset into it (batches may exceed the
            //! output vector capacity, so we hand them back windowed across calls).
            std::unique_ptr<components::vector::data_chunk_t> current;
            uint64_t current_offset = 0;
            bool done = false;
        };

        struct ArrowScanLocalState : public LocalTableFunctionState {};

        //! Build a throw-away arrow_table_schema_t to extract the otterbrix return types and column names.
        //! Uses a stack-backed PMR resource (no get_default_resource); types/names are deep-copied out.
        void derive_types(arrow_schema_wrapper_t& schema_root,
                          std::vector<complex_logical_type>& return_types,
                          std::vector<std::string>& names) {
            std::pmr::monotonic_buffer_resource scratch;
            auto table_schema = schema_from_arrow(&scratch, &schema_root.arrow_schema);
            auto& types = table_schema.get_types();
            auto& column_names = table_schema.get_names();
            return_types.reserve(types.size());
            names.reserve(column_names.size());
            for (std::size_t i = 0; i < types.size(); i++) {
                return_types.emplace_back(types[i]);
                names.emplace_back(i < column_names.size() ? column_names[i] : ("v" + std::to_string(i)));
            }
        }

    } // namespace

    ArrowScanFunction::ArrowScanFunction()
        : TableFunction("arrow_scan",
                        {components::types::logical_type::POINTER},
                        ArrowScanFunc,
                        ArrowScanBind,
                        ArrowScanInitGlobal,
                        ArrowScanInitLocal) {}

    std::unique_ptr<FunctionData> ArrowScanFunction::ArrowScanBind(TableFunctionBindInput& input,
                                                              std::vector<complex_logical_type>& return_types,
                                                              std::vector<std::string>& names) {
        if (input.inputs[0].is_null()) {
            throw std::runtime_error("arrow_scan: factory pointer cannot be null");
        }
        auto factory_ptr = reinterpret_cast<uintptr_t>(input.inputs[0].value<void*>());

        DependencyItem* dependency = nullptr;
        if (input.ref.external_dependency) {
            dependency = input.ref.external_dependency->GetDependency("replacement_cache");
        }

        // Pull the schema out of the python arrow object and derive the otterbrix types.
        arrow_schema_wrapper_t schema_root;
        PythonTableArrowArrayStreamFactory::GetSchema(factory_ptr, schema_root);
        derive_types(schema_root, return_types, names);

        if (return_types.empty()) {
            throw std::runtime_error("Provided table/dataframe must have at least one column");
        }
        return std::make_unique<ArrowScanFunctionData>(factory_ptr, dependency);
    }

    std::unique_ptr<GlobalTableFunctionState>
    ArrowScanFunction::ArrowScanInitGlobal(TableFunctionInitInput& input) {
        auto& bind_data = input.bind_data->Cast<ArrowScanFunctionData>();
        auto stream = PythonTableArrowArrayStreamFactory::Produce(bind_data.factory_ptr);
        return std::make_unique<ArrowScanGlobalState>(std::move(stream));
    }

    std::unique_ptr<LocalTableFunctionState>
    ArrowScanFunction::ArrowScanInitLocal(TableFunctionInitInput&, GlobalTableFunctionState*) {
        return std::make_unique<ArrowScanLocalState>();
    }

    void ArrowScanFunction::ArrowScanFunc(TableFunctionInput& data_p, components::vector::data_chunk_t& output) {
        auto& global_state = data_p.global_state->Cast<ArrowScanGlobalState>();
        std::lock_guard<std::mutex> guard(global_state.lock);
        if (global_state.done) {
            output.set_cardinality(0);
            return;
        }

        auto* resource = output.resource();

        // Build the schema once, on the plan resource so the per-column arrow_type metadata
        // (used by data_chunk_from_arrow) lives as long as the scan.
        if (!global_state.schema) {
            arrow_schema_wrapper_t schema_wrapper;
            global_state.stream->get_schema(schema_wrapper);
            global_state.schema =
                std::make_unique<arrow_table_schema_t>(schema_from_arrow(resource, &schema_wrapper.arrow_schema));
        }

        // Make sure we have an in-flight converted batch with rows still to emit.
        while (!global_state.current || global_state.current_offset >= global_state.current->size()) {
            std::shared_ptr<arrow_array_wrapper_t> batch = global_state.stream->get_next_chunk();
            while (batch->arrow_array.length == 0 && batch->arrow_array.release) {
                batch = global_state.stream->get_next_chunk();
            }
            if (!batch->arrow_array.release) {
                global_state.done = true;
                global_state.current.reset();
                output.set_cardinality(0);
                return;
            }
            // Convert via the canonical core path. data_chunk_from_arrow takes the schema by value;
            // its per-column arrow_type metadata is shared_ptr-backed, so the copy is cheap and the
            // cached global_state.schema stays valid for the next batch.
            global_state.current = std::make_unique<components::vector::data_chunk_t>(
                data_chunk_from_arrow(resource, &batch->arrow_array, *global_state.schema));
            global_state.current_offset = 0;
        }

        // Emit a window of the current batch no larger than the output capacity.
        auto& produced = *global_state.current;
        uint64_t available = produced.size() - global_state.current_offset;
        uint64_t row_count = std::min<uint64_t>(available, output.capacity());
        output.set_cardinality(row_count);
        for (uint64_t col = 0; col < produced.column_count(); col++) {
            for (uint64_t row = 0; row < row_count; row++) {
                output.set_value(col, row, produced.value(col, global_state.current_offset + row));
            }
        }
        global_state.current_offset += row_count;
    }

} // namespace otterbrix
