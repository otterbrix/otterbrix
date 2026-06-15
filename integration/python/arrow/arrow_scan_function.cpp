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

        struct arrow_scan_function_data_t : public table_function_data_t {
            arrow_scan_function_data_t(uintptr_t factory_ptr_p, dependency_item_t* dependency_p)
                : factory_ptr(factory_ptr_p)
                , dependency(dependency_p) {}

            std::unique_ptr<function_data_t> copy() const override {
                throw std::runtime_error("ArrowScanFunctionData::Copy not supported");
            }
            bool equals(const function_data_t&) const override { return false; }

            //! Pointer to the python_table_arrow_array_stream_factory_t. The owning external_dependency_t
            //! (held by the table_ref_t) outlives this function_data_t, so `dependency` is a non-owning
            //! observer kept only to document the borrow.
            uintptr_t factory_ptr;
            dependency_item_t* dependency;
        };

        struct arrow_scan_global_state_t : public global_table_function_state_t {
            explicit arrow_scan_global_state_t(std::unique_ptr<arrow_array_schema_wrapper_t> stream_p)
                : stream(std::move(stream_p)) {}

            uint64_t max_threads() const override { return 1; }

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

        struct arrow_scan_local_state_t : public local_table_function_state_t {};

        //! Build a throw-away arrow_table_schema_t to extract the otterbrix return types and column names.
        //! Uses a stack-backed PMR resource (no get_default_resource); types/names are deep-copied out.
        void derive_types(arrow_schema_wrapper_t& schema_root,
                          std::vector<complex_logical_type>& return_types,
                          std::vector<std::string>& names) {
            std::pmr::monotonic_buffer_resource scratch;
            auto sr = schema_from_arrow(&scratch, &schema_root.arrow_schema);
            if (sr.has_error()) {
                throw std::runtime_error(std::string(sr.error().what));
            }
            auto table_schema = std::move(sr.value());
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

    arrow_scan_function_t::arrow_scan_function_t()
        : table_function_t("arrow_scan",
                        {components::types::logical_type::POINTER},
                        ArrowScanFunc,
                        ArrowScanBind,
                        ArrowScanInitGlobal,
                        ArrowScanInitLocal) {}

    std::unique_ptr<function_data_t> arrow_scan_function_t::ArrowScanBind(table_function_bind_input_t& input,
                                                              std::vector<complex_logical_type>& return_types,
                                                              std::vector<std::string>& names) {
        if (input.inputs[0].is_null()) {
            throw std::runtime_error("arrow_scan: factory pointer cannot be null");
        }
        auto factory_ptr = reinterpret_cast<uintptr_t>(input.inputs[0].value<void*>());

        dependency_item_t* dependency = nullptr;
        if (input.ref.external_dependency) {
            dependency = input.ref.external_dependency->get_dependency("replacement_cache");
        }

        // Pull the schema out of the python arrow object and derive the otterbrix types.
        arrow_schema_wrapper_t schema_root;
        python_table_arrow_array_stream_factory_t::get_schema(factory_ptr, schema_root);
        derive_types(schema_root, return_types, names);

        if (return_types.empty()) {
            throw std::runtime_error("Provided table/dataframe must have at least one column");
        }
        return std::make_unique<arrow_scan_function_data_t>(factory_ptr, dependency);
    }

    std::unique_ptr<global_table_function_state_t>
    arrow_scan_function_t::ArrowScanInitGlobal(table_function_init_input_t& input) {
        auto& bind_data = input.bind_data->cast<arrow_scan_function_data_t>();
        auto stream = python_table_arrow_array_stream_factory_t::produce(bind_data.factory_ptr);
        return std::make_unique<arrow_scan_global_state_t>(std::move(stream));
    }

    std::unique_ptr<local_table_function_state_t>
    arrow_scan_function_t::ArrowScanInitLocal(table_function_init_input_t&, global_table_function_state_t*) {
        return std::make_unique<arrow_scan_local_state_t>();
    }

    void arrow_scan_function_t::ArrowScanFunc(table_function_input_t& data_p, components::vector::data_chunk_t& output) {
        auto& global_state = data_p.global_state->cast<arrow_scan_global_state_t>();
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
            if (auto err = global_state.stream->get_schema(resource, schema_wrapper); err.contains_error()) {
                throw std::runtime_error(std::string(err.what));
            }
            auto sr = schema_from_arrow(resource, &schema_wrapper.arrow_schema);
            if (sr.has_error()) {
                throw std::runtime_error(std::string(sr.error().what));
            }
            global_state.schema = std::make_unique<arrow_table_schema_t>(std::move(sr.value()));
        }

        // Make sure we have an in-flight converted batch with rows still to emit.
        while (!global_state.current || global_state.current_offset >= global_state.current->size()) {
            auto batch_res = global_state.stream->get_next_chunk(resource);
            if (batch_res.has_error()) {
                throw std::runtime_error(std::string(batch_res.error().what));
            }
            std::shared_ptr<arrow_array_wrapper_t> batch = std::move(batch_res.value());
            while (batch->arrow_array.length == 0 && batch->arrow_array.release) {
                auto next_res = global_state.stream->get_next_chunk(resource);
                if (next_res.has_error()) {
                    throw std::runtime_error(std::string(next_res.error().what));
                }
                batch = std::move(next_res.value());
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
            auto chunk_res = data_chunk_from_arrow(resource, &batch->arrow_array, *global_state.schema);
            if (chunk_res.has_error()) {
                throw std::runtime_error(std::string(chunk_res.error().what));
            }
            global_state.current =
                std::make_unique<components::vector::data_chunk_t>(std::move(chunk_res.value()));
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
