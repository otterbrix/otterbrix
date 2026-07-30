#include "operator_func.hpp"

#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/projection_executor.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <optional>
#include <unordered_set>

namespace {
    using namespace components;
    using namespace components::operators::aggregate;
    // ONE resolved aggregate argument. Two shapes reach the kernel: a COLUMN of the input chunk
    // (referenced, so the buffer is shared rather than copied), or one scalar VALUE broadcast over
    // the chunk's rows.
    //
    // A tagged class, not std::variant (rule 14): kind_ is the O(1) dispatch tag and there is no
    // RTTI — the same shape bound_expression_t::kind() carries. Neither alternative is readable
    // from outside, because both questions a caller has (what TYPE is this argument, and put it
    // into an output column) are answered here. So the tag is tested exactly twice per argument
    // per chunk, and no other site can name — or mis-name — an alternative.
    //
    // The column is addressed POSITIONALLY and resolved against the chunk at read time, exactly as
    // bound_reference_t addresses one. An ITERATOR would not survive the resolution itself:
    // resolving a nested-expression argument APPENDS its computed column to chunk.data, which is a
    // plain std::vector and reallocates, so an argument list that mixes a column with an expression
    // (`count(v, v + 1)`) left the earlier argument pointing into the freed block.
    class resolved_arg_t {
    public:
        static resolved_arg_t from_column(size_t column_index) { return resolved_arg_t{column_index}; }
        static resolved_arg_t from_value(types::logical_value_t value) { return resolved_arg_t{std::move(value)}; }

        const types::complex_logical_type& type(const vector::data_chunk_t& chunk) const noexcept {
            return kind_ == kind::column ? chunk.data[column_index_].type() : value_->type();
        }

        // Point `target` at this argument's data. A column is REFERENCED (no copy); a scalar is
        // referenced as a constant vector and then flattened to `count` rows, which is the full
        // column an aggregate kernel reads.
        void reference_into(vector::vector_t& target,
                            const vector::data_chunk_t& chunk,
                            std::pmr::memory_resource* resource,
                            uint64_t count) const {
            if (kind_ == kind::column) {
                target.reference(chunk.data[column_index_]);
                return;
            }
            target.reference(*value_);
            target.flatten(vector::indexing_vector_t(resource, count), count);
        }

    private:
        enum class kind : uint8_t
        {
            column,
            value
        };

        explicit resolved_arg_t(size_t column_index)
            : kind_(kind::column)
            , column_index_(column_index) {}
        explicit resolved_arg_t(types::logical_value_t value)
            : kind_(kind::value)
            , value_(std::move(value)) {}

        kind kind_;
        size_t column_index_{0};
        std::optional<types::logical_value_t> value_;
    };

    // Pre-compute any arithmetic expression arguments, returns false (sets error) on failure
    bool compute_expression_args(std::pmr::memory_resource* resource,
                                 const std::pmr::vector<expressions::param_storage>& args,
                                 vector::data_chunk_t& chunk,
                                 operator_func_t& op,
                                 pipeline::context_t* pipeline_context,
                                 std::vector<vector::vector_t>& computed_vecs) {
        for (const auto& arg : args) {
            if (expressions::is_expr(arg)) {
                const auto& expr = expressions::as_expr(arg);
                if (expr->group() == expressions::expression_group::scalar) {
                    auto* scalar_expr = static_cast<const expressions::scalar_expression_t*>(expr.get());
                    auto res = operators::evaluate_scalar(resource,
                                                          scalar_expr->type(),
                                                          scalar_expr->params(),
                                                          chunk,
                                                          pipeline_context->function_registry,
                                                          pipeline_context->parameters,
                                                          pipeline_context->session_tz);
                    if (res.has_error()) {
                        op.set_error(res.error());
                        return false;
                    }
                    computed_vecs.emplace_back(std::move(res.value()));
                }
            }
        }
        return true;
    }

    void resolve_columns(const std::pmr::vector<expressions::param_storage>& args,
                         vector::data_chunk_t& chunk,
                         pipeline::context_t* pipeline_context,
                         std::pmr::vector<resolved_arg_t>& columns,
                         std::vector<vector::vector_t>& computed_vecs) {
        size_t computed_idx = 0;
        for (const auto& arg : args) {
            if (expressions::is_key(arg)) {
                const auto& key = expressions::as_key(arg);
                assert(!key.path().empty() && "aggregate key path must be resolved");
                // Empty-input path: the global-aggregate-over-empty branch
                // (operator_group_t::empty_aggregate_result / operator_batch_t's
                // defence-in-depth) hands a 0-column, 0-row chunk when no source rows
                // were ever produced, so a column-key path indexes past the (zero)
                // columns. Append a 0-row numeric placeholder column and reference it:
                // over zero rows the aggregate value is type-independent (COUNT→0,
                // SUM/MIN/MAX/AVG→NULL), so this yields the exact materialize-path empty
                // result. Only reachable when the chunk carries no rows.
                if (key.path().front() >= chunk.data.size()) {
                    assert(chunk.size() == 0 && "out-of-range aggregate key on a non-empty chunk");
                    chunk.data.emplace_back(chunk.resource(),
                                            types::complex_logical_type{types::logical_type::BIGINT},
                                            uint64_t{0});
                    columns.emplace_back(resolved_arg_t::from_column(chunk.data.size() - 1));
                } else {
                    columns.emplace_back(resolved_arg_t::from_column(key.path().front()));
                }
            } else if (expressions::is_parameter(arg)) {
                const auto& id = expressions::as_parameter(arg);
                columns.emplace_back(resolved_arg_t::from_value(pipeline_context->parameters.parameters.at(id)));
            } else if (expressions::is_expr(arg)) {
                if (computed_idx < computed_vecs.size()) {
                    chunk.data.emplace_back(std::move(computed_vecs[computed_idx]));
                    columns.emplace_back(resolved_arg_t::from_column(chunk.data.size() - 1));
                    computed_idx++;
                }
            }
        }
    }

    vector::data_chunk_t build_arg_chunk(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<resolved_arg_t>& columns,
                                         const vector::data_chunk_t& chunk) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto& arg : columns) {
            types.emplace_back(arg.type(chunk));
        }
        vector::data_chunk_t c(resource, types, chunk.size());
        c.set_cardinality(chunk.size());
        for (size_t i = 0; i < c.column_count(); i++) {
            columns.at(i).reference_into(c.data[i], chunk, resource, chunk.size());
        }
        return c;
    }

    void apply_distinct(std::pmr::memory_resource* resource,
                        vector::data_chunk_t& c,
                        const std::pmr::vector<types::complex_logical_type>& types) {
        struct lv_hash {
            size_t operator()(const types::logical_value_t& v) const noexcept { return v.hash(); }
        };
        std::unordered_set<types::logical_value_t, lv_hash, std::equal_to<>> seen;
        seen.reserve(c.size());
        std::pmr::vector<uint64_t> unique_indices(resource);
        unique_indices.reserve(c.size());
        for (uint64_t row = 0; row < c.size(); row++) {
            if (seen.insert(c.data[0].value(row)).second) {
                unique_indices.push_back(row);
            }
        }
        vector::indexing_vector_t indexing(resource, unique_indices.data());
        vector::data_chunk_t unique_c(resource, types, unique_indices.size());
        c.copy(unique_c, indexing, unique_indices.size(), 0);
        c = std::move(unique_c);
    }
} // namespace

namespace components::operators::aggregate {
    operator_func_t::operator_func_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     compute::function* func,
                                     std::pmr::vector<expressions::param_storage> args,
                                     bool distinct)
        : operator_aggregate_t(resource, std::move(log))
        , args_(std::move(args))
        , func_(func)
        , distinct_(distinct) {
        assert(func);
    }

    core::result_wrapper_t<compute::datum_t>
    operator_func_t::aggregate_batch_impl(pipeline::context_t* pipeline_context) {
        auto& batch_chunks = left_->output()->chunks();
        std::vector<vector::data_chunk_t> arg_chunks;
        arg_chunks.reserve(batch_chunks.size());

        for (auto& chunk : batch_chunks) {
            // resolve_columns appends computed expression columns to the chunk so
            // they can be referenced like regular columns. A group's batch is shared
            // across aggregators (gathered once per group), so the chunk is restored
            // to its original column set once the argument chunk is built
            // (build_arg_chunk shares the column buffers, not the chunk slots).
            const size_t base_column_count = chunk.data.size();
            std::vector<vector::vector_t> computed_vecs;
            if (!compute_expression_args(resource_, args_, chunk, *this, pipeline_context, computed_vecs)) {
                // error already set — return empty
                return compute::datum_t{std::pmr::vector<types::logical_value_t>(resource_)};
            }

            std::pmr::vector<resolved_arg_t> columns(resource_);
            columns.reserve(args_.size());
            resolve_columns(args_, chunk, pipeline_context, columns, computed_vecs);

            if (columns.size() == args_.size()) {
                auto c = build_arg_chunk(resource_, columns, chunk);
                auto types = c.types();
                if (distinct_) {
                    apply_distinct(resource_, c, types);
                }
                arg_chunks.push_back(std::move(c));
            }
            if (chunk.data.size() > base_column_count) {
                chunk.data.erase(chunk.data.begin() + static_cast<std::ptrdiff_t>(base_column_count), chunk.data.end());
            }
        }

        auto res = func_->execute(arg_chunks);
        if (res.has_error()) {
            return res;
        }

        // The function's name is NOT stamped onto the values it returns. A value is not a
        // column and has no column name (M3-B3). The only consumer of these values is
        // operator_group (take_batch_values at :605 and :729), and it names the output column
        // from the plan's output schema or from the aggregate's own `name` — never from the
        // value's type. Stamping it here allocated a name per value that nobody read.
        return std::move(res.value());
    }

} // namespace components::operators::aggregate
