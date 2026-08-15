#include "operator_select.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>

namespace components::operators {

    namespace {

        vector::data_chunk_t side_by_side(std::pmr::memory_resource* resource,
                                          const vector::data_chunk_t& left,
                                          const vector::data_chunk_t& right) {
            const uint64_t capacity = left.size() > 0 ? left.size() : 1;
            vector::data_chunk_t merged(resource, {}, capacity);
            merged.data.reserve(left.column_count() + right.column_count());
            for (const auto* side : {&left, &right}) {
                for (const auto& column : side->data) {
                    vector::vector_t vec(resource, column.type(), capacity);
                    vec.reference(column);
                    merged.data.push_back(std::move(vec));
                }
            }
            merged.set_cardinality(left.size());
            return merged;
        }

    } // anonymous namespace

    core::error_t build_projection_graph(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<select_column_t>& columns,
                                         const logical_plan::storage_parameters& parameters,
                                         const vector::data_chunk_t& input,
                                         size_t right_offset,
                                         std::unique_ptr<execution_dag::execution_dag_t>* graph) {
        // star_expand copies its columns straight from the input chunk, so it contributes
        // no output slot and the graph's slots line up with the projected columns only.
        std::pmr::vector<const expressions::expression_i*> projected(resource);
        projected.reserve(columns.size());
        for (const auto& column : columns) {
            if (column.type == select_column_t::kind::star_expand) {
                continue;
            }
            assert(column.expression && "a projected column reached the graph without its expression");
            projected.push_back(column.expression.get());
        }

        auto built = expressions::build_graph(resource, parameters.parameters, projected, input.types(), right_offset);
        if (built.has_error()) {
            return built.error();
        }
        *graph = std::move(built.value());
        return core::error_t::no_error();
    }

    operator_select_t::operator_select_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::select)
        , columns_(resource) {}

    void operator_select_t::add_column(select_column_t&& col) { columns_.push_back(std::move(col)); }

    void operator_select_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& types) {
        for (size_t i = 0; i < columns_.size() && i < types.size(); ++i) {
            columns_[i].result_type = types[i];
        }
    }

    core::error_t
    operator_select_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming projection: apply the per-chunk transform to the single
        // batch handed in via `input`. No accumulation, no read of left_->output().
        // A SELECT over a JOIN already receives ONE merged chunk holding both sides' columns,
        // whose ordinals a key indexes directly whatever side it resolved to — so there is no
        // second chunk to pair and no offset to apply.
        auto result =
            evaluate_projection(resource_, columns_, &input, ctx->parameters, ctx->execution_context, &graph_);
        if (result.has_error()) {
            return result.error();
        }
        out.emplace_back(std::move(result.value()));
        return core::error_t::no_error();
    }

    core::result_wrapper_t<vector::data_chunk_t>
    evaluate_projection(std::pmr::memory_resource* resource,
                        const std::pmr::vector<select_column_t>& columns,
                        vector::data_chunk_t* input,
                        const logical_plan::storage_parameters& parameters,
                        const components::graph_execution_context& context,
                        std::unique_ptr<execution_dag::execution_dag_t>* graph,
                        const vector::data_chunk_t* right_input) {
        const auto num_rows = input->size();
        const uint64_t cap = num_rows > 0 ? num_rows : 1;

        std::optional<vector::data_chunk_t> merged;
        if (right_input != nullptr) {
            merged = side_by_side(resource, *input, *right_input);
        }
        const vector::data_chunk_t& source = merged.has_value() ? *merged : *input;

        const bool computes = std::any_of(columns.begin(), columns.end(), [](const select_column_t& column) {
            return column.type != select_column_t::kind::star_expand;
        });
        std::optional<vector::data_chunk_t> computed;
        if (computes) {
            if (*graph == nullptr) {
                if (auto error = build_projection_graph(resource,
                                                        columns,
                                                        parameters,
                                                        source,
                                                        right_input != nullptr ? input->column_count() : 0,
                                                        graph);
                    error.contains_error()) {
                    return error;
                }
            }
            auto produced = expressions::run_graph(graph->get(), parameters.parameters, source, context);
            if (produced.has_error()) {
                return produced.error();
            }
            computed = std::move(produced.value());
        }

        // One column per projection entry, in projection order (a star fans out to one per input
        // column). The graph carries every other kind and emits them in that same relative order,
        // so pairing the two lists needs only a cursor into its outputs.
        vector::data_chunk_t result(resource, {}, cap);
        size_t output_index = 0;
        for (const auto& col : columns) {
            if (col.type == select_column_t::kind::star_expand) {
                // Bare '*' — hand through the columns of the projected side. Qualified 'table.*'
                // is pre-expanded to get_field columns at validation, so it never reaches here.
                for (size_t column = 0; column < input->column_count(); ++column) {
                    result.data.push_back(input->data[column]);
                }
                continue;
            }
            // Reaching here means the column is not a star, so `computes` was true and both the
            // graph and its output exist.
            const vector::vector_t& source_vec = computed->data[output_index];
            vector::vector_t vec(resource, source_vec.type(), cap);
            if ((*graph)->slot_is_bound_input((*graph)->output_slots()[output_index])) {
                // A column the query merely names IS the input column — the slot shares its
                // buffer and no node writes it, so this stays the zero-copy passthrough it was.
                vec.reference(source_vec);
            } else {
                // A computed slot is overwritten by the next chunk, so its value is copied out.
                vector::vector_ops::copy(source_vec, vec, num_rows, 0, 0);
            }
            vec.set_type_alias(std::string{col.key.name});
            result.data.push_back(std::move(vec));
            ++output_index;
        }

        result.set_cardinality(num_rows);
        return result;
    }

} // namespace components::operators
