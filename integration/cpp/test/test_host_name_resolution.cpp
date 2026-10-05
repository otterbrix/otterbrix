// A test host that knows remote tables only through its own tables in the engine: the name resolution hook reads
// otterstax.remote_columns for every name the catalog did not resolve and answers it with the declared columns and a
// per-statement storage over canned backend rows. otterbrix runs the SQL over the storage's operators: scans number
// the rows they give out, and the insert, update and delete sinks change the canned rows. A host optimizer rule
// replaces a simple UPDATE / DELETE of its own table with one remote statement.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/table_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <atomic>
#include <map>
#include <set>
#include <string>
#include <vector>

using namespace components;

namespace {

    using rows_t = std::vector<std::vector<int64_t>>;

    // The "remote" data behind each name; one int64 per declared column (a TEXT column shows it as "s<n>").
    std::map<std::string, rows_t>& backend() {
        static std::map<std::string, rows_t> rows;
        return rows;
    }

    // A backend that answers in several batches (an empty one included); the default is one batch of backend().
    std::map<std::string, std::vector<rows_t>>& batches() {
        static std::map<std::string, std::vector<rows_t>> script;
        return script;
    }

    // Names whose backend cannot be reached: every operator of their storage fails with this reason.
    std::map<std::string, std::string>& unreachable() {
        static std::map<std::string, std::string> reasons;
        return reasons;
    }

    // Names whose backend cannot change a row by its number (ClickHouse, say): make_update / make_delete refuse.
    std::set<std::string>& no_row_numbers() {
        static std::set<std::string> names;
        return names;
    }

    std::vector<std::string>& asked_names() {
        static std::vector<std::string> names;
        return names;
    }

    // What reached the backend, one line per remote statement.
    std::vector<std::string>& write_log() {
        static std::vector<std::string> log;
        return log;
    }

    // decide's explicit_transaction, one per call.
    std::vector<bool>& explicit_transactions() {
        static std::vector<bool> seen;
        return seen;
    }

    struct counters_t {
        std::atomic<int> need{0};
        std::atomic<int> reads{0};
        std::atomic<int> decide{0};
        void reset() {
            need = 0;
            reads = 0;
            decide = 0;
        }
    };
    counters_t& counters() {
        static counters_t c;
        return c;
    }

    std::string qualified(std::string_view db, std::string_view schema, std::string_view rel) {
        std::string out{db};
        if (!schema.empty()) {
            out += '.';
            out += schema;
        }
        out += '.';
        out += rel;
        return out;
    }

    core::error_t backend_error(std::pmr::memory_resource* resource, const std::string& reason) {
        return core::error_t{core::error_code_t::connection_closed, std::pmr::string{reason.c_str(), resource}};
    }

    // The tag the host's storages carry: its rule takes only its own tables.
    const int host_tag = 0;

    // One statement's view of one remote table. A row number is the position of the row in the backend when the
    // scan gave it out — the "ctid" the update and delete sinks get back.
    class remote_storage_t final : public logical_plan::table_storage_t {
    public:
        remote_storage_t(std::pmr::memory_resource* resource,
                         std::string name,
                         std::pmr::vector<types::complex_logical_type> columns)
            : logical_plan::table_storage_t(&host_tag)
            , name_(std::move(name))
            , columns_(std::move(columns), resource) {}

        const std::string& name() const noexcept { return name_; }
        const std::pmr::vector<types::complex_logical_type>& columns() const noexcept { return columns_; }

        int64_t number(std::size_t position) {
            positions_.push_back(position);
            return static_cast<int64_t>(positions_.size() - 1);
        }
        std::size_t position(int64_t number) const { return positions_.at(static_cast<std::size_t>(number)); }

    private:
        logical_plan::storage_operator_t make_scan_impl(const services::context_storage_t& context) override;
        logical_plan::storage_operator_t make_insert_impl(const services::context_storage_t& context) override;
        logical_plan::storage_operator_t make_update_impl(const services::context_storage_t& context) override;
        logical_plan::storage_operator_t make_delete_impl(const services::context_storage_t& context) override;

        std::string name_;
        std::pmr::vector<types::complex_logical_type> columns_;
        std::vector<std::size_t> positions_;
    };

    class remote_source_t final : public operators::read_only_operator_t {
    public:
        remote_source_t(std::pmr::memory_resource* resource,
                        log_t log,
                        remote_storage_t* storage,
                        std::vector<rows_t> batches)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , storage_(storage)
            , batches_(std::move(batches)) {}

        [[nodiscard]] operators::pipeline_role role() const noexcept override {
            return operators::pipeline_role::source;
        }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t*) override {
            actor_zeta::promise<core::result_wrapper_t<std::optional<vector::data_chunk_t>>> promise(resource());
            auto future = promise.get_future();
            if (next_ == batches_.size()) {
                promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::nullopt});
                return future;
            }
            const auto& columns = storage_->columns();
            const auto& rows = batches_[next_++];
            vector::data_chunk_t chunk(resource(), columns, std::max<std::size_t>(rows.size(), 1));
            chunk.set_cardinality(rows.size());
            for (std::size_t row = 0; row < rows.size(); ++row) {
                chunk.row_ids.data<int64_t>()[row] = storage_->number(position_++);
                for (std::size_t col = 0; col < columns.size(); ++col) {
                    if (columns[col].type() == types::logical_type::STRING_LITERAL) {
                        chunk.set_value(col,
                                        row,
                                        types::logical_value_t(resource(), "s" + std::to_string(rows[row][col])));
                    } else if (columns[col].type() == types::logical_type::INTEGER) {
                        chunk.set_value(col,
                                        row,
                                        types::logical_value_t(resource(), static_cast<int32_t>(rows[row][col])));
                    } else {
                        chunk.set_value(col, row, types::logical_value_t(resource(), rows[row][col]));
                    }
                }
            }
            promise.set_value(core::result_wrapper_t<std::optional<vector::data_chunk_t>>{std::move(chunk)});
            return future;
        }

        void reset_pipeline_state() noexcept override {
            next_ = 0;
            position_ = 0;
        }

    private:
        std::pmr::string explain_label_impl() const override {
            return std::pmr::string{"Foreign Scan on " + storage_->name(), resource()};
        }
        std::pmr::vector<std::pmr::string> explain_details_impl() const override {
            std::pmr::vector<std::pmr::string> details{resource()};
            const auto& name = storage_->name();
            details.emplace_back("Remote SQL: SELECT * FROM " + name.substr(name.find('.') + 1));
            return details;
        }

        remote_storage_t* storage_;
        std::vector<rows_t> batches_;
        std::size_t next_{0};
        std::size_t position_{0};
    };

    std::vector<int64_t> int_row(const vector::data_chunk_t& chunk, std::uint64_t row) {
        std::vector<int64_t> values;
        for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
            const auto cell = chunk.value(col, row);
            values.push_back(cell.value<int64_t>());
        }
        return values;
    }

    std::string type_names(const vector::data_chunk_t& chunk) {
        std::string out;
        for (const auto& column : chunk.data) {
            out += out.empty() ? "" : ",";
            out += std::string{column.type().alias()} + ":" +
                   (column.type().type() == types::logical_type::BIGINT ? "bigint" : "other");
        }
        return out;
    }

    // A sink of the storage: buffers in push, talks to the backend once per await_async_and_resume.
    class remote_sink_t : public operators::read_write_operator_t {
    public:
        remote_sink_t(std::pmr::memory_resource* resource, log_t log, remote_storage_t* storage)
            : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
            , storage_(storage) {}

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    protected:
        remote_storage_t* storage_;
    };

    class remote_insert_t final : public remote_sink_t {
    public:
        using remote_sink_t::remote_sink_t;

        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            types_ = type_names(input);
            for (std::uint64_t row = 0; row < input.size(); ++row) {
                std::vector<int64_t> values;
                for (std::uint64_t col = 0; col < input.column_count(); ++col) {
                    // NULL in an omitted column reaches the backend as -1.
                    const auto cell = input.value(col, row);
                    values.push_back(cell.is_null() ? -1 : cell.value<int64_t>());
                }
                rows_.push_back(std::move(values));
            }
            return core::error_t::no_error();
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
            if (auto it = unreachable().find(storage_->name()); it != unreachable().end()) {
                set_error(backend_error(resource(), it->second));
                co_return;
            }
            write_log().push_back("insert " + storage_->name() + " " + types_);
            auto& remote = backend()[storage_->name()];
            remote.insert(remote.end(), rows_.begin(), rows_.end());
            rows_.clear();
            mark_executed();
            co_return;
        }

    private:
        std::string types_;
        rows_t rows_;
    };

    // UPDATE by number: every column of each row, the new values set.
    class remote_update_t final : public remote_sink_t {
    public:
        using remote_sink_t::remote_sink_t;

        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            for (std::uint64_t row = 0; row < input.size(); ++row) {
                changed_.emplace_back(storage_->position(input.row_ids.data<int64_t>()[row]), int_row(input, row));
            }
            return core::error_t::no_error();
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
            if (auto it = unreachable().find(storage_->name()); it != unreachable().end()) {
                set_error(backend_error(resource(), it->second));
                co_return;
            }
            write_log().push_back("update " + storage_->name() + " rows " + std::to_string(changed_.size()));
            auto& remote = backend()[storage_->name()];
            for (auto& [position, values] : changed_) {
                remote.at(position) = std::move(values);
            }
            changed_.clear();
            mark_executed();
            co_return;
        }

    private:
        std::vector<std::pair<std::size_t, std::vector<int64_t>>> changed_;
    };

    // DELETE by number.
    class remote_delete_t final : public remote_sink_t {
    public:
        using remote_sink_t::remote_sink_t;

        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            for (std::uint64_t row = 0; row < input.size(); ++row) {
                positions_.push_back(storage_->position(input.row_ids.data<int64_t>()[row]));
            }
            return core::error_t::no_error();
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
            if (auto it = unreachable().find(storage_->name()); it != unreachable().end()) {
                set_error(backend_error(resource(), it->second));
                co_return;
            }
            write_log().push_back("delete " + storage_->name() + " rows " + std::to_string(positions_.size()));
            std::sort(positions_.rbegin(), positions_.rend());
            auto& remote = backend()[storage_->name()];
            for (const auto position : positions_) {
                remote.erase(remote.begin() + static_cast<std::ptrdiff_t>(position));
            }
            positions_.clear();
            mark_executed();
            co_return;
        }

    private:
        std::vector<std::size_t> positions_;
    };

    logical_plan::storage_operator_t remote_storage_t::make_scan_impl(const services::context_storage_t& context) {
        if (auto it = unreachable().find(name_); it != unreachable().end()) {
            return backend_error(context.resource, it->second);
        }
        auto scripted = batches().find(name_);
        auto answer = scripted != batches().end() ? scripted->second : std::vector<rows_t>{backend()[name_]};
        return operators::operator_ptr{
            new remote_source_t(context.resource, context.log.clone(), this, std::move(answer))};
    }

    logical_plan::storage_operator_t remote_storage_t::make_insert_impl(const services::context_storage_t& context) {
        return operators::operator_ptr{new remote_insert_t(context.resource, context.log.clone(), this)};
    }

    core::error_t no_row_numbers_error(std::pmr::memory_resource* resource, const std::string& name) {
        std::pmr::string what{"storage \"", resource};
        what += name;
        what += "\" cannot change a row by its number";
        return core::error_t{core::error_code_t::unimplemented_yet, std::move(what)};
    }

    logical_plan::storage_operator_t remote_storage_t::make_update_impl(const services::context_storage_t& context) {
        if (no_row_numbers().count(name_) != 0) {
            return no_row_numbers_error(context.resource, name_);
        }
        return operators::operator_ptr{new remote_update_t(context.resource, context.log.clone(), this)};
    }

    logical_plan::storage_operator_t remote_storage_t::make_delete_impl(const services::context_storage_t& context) {
        if (no_row_numbers().count(name_) != 0) {
            return no_row_numbers_error(context.resource, name_);
        }
        return operators::operator_ptr{new remote_delete_t(context.resource, context.log.clone(), this)};
    }

    // Phase "need": one read of otterstax.remote_columns per unresolved name.
    core::result_wrapper_t<std::pmr::vector<logical_plan::execution_plan_t>>
    need_remote_columns(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr&,
                        std::span<const qualified_name_t> unresolved) {
        counters().need.fetch_add(1);
        std::pmr::vector<logical_plan::execution_plan_t> reads{resource};
        for (const auto& name : unresolved) {
            asked_names().push_back(qualified(name.database.t, name.schema.t, name.collection.t));
            auto agg = logical_plan::make_node_aggregate(
                resource,
                qualified_name_t{core::dbname_t{"otterstax"}, core::relname_t{"remote_columns"}});
            auto expr =
                expressions::make_compare_expression(resource,
                                                     expressions::compare_type::eq,
                                                     expressions::key_t{resource, "tbl", expressions::side_t::left},
                                                     core::parameter_id_t{1});
            agg->append_child(logical_plan::make_node_match(
                resource,
                qualified_name_t{core::dbname_t{"otterstax"}, core::relname_t{"remote_columns"}},
                std::move(expr)));
            auto params = logical_plan::make_parameter_node(resource);
            params->add_parameter(
                core::parameter_id_t{1},
                types::logical_value_t(resource, qualified(name.database.t, name.schema.t, name.collection.t)));
            reads.emplace_back(resource, std::move(agg), std::move(params));
        }
        counters().reads.fetch_add(static_cast<int>(reads.size()));
        return reads;
    }

    // Phase "decide": a name with rows in otterstax.remote_columns gets a storage with those columns; one without
    // stays unresolved and is refused as "does not exist".
    core::result_wrapper_t<std::pmr::vector<planner::table_storage_answer_t>>
    decide_remote_storages(std::pmr::memory_resource* resource,
                           std::span<const qualified_name_t> unresolved,
                           std::span<const std::pmr::vector<vector::data_chunk_t>> read_results,
                           bool explicit_transaction) {
        counters().decide.fetch_add(1);
        explicit_transactions().push_back(explicit_transaction);
        std::pmr::vector<planner::table_storage_answer_t> answers{resource};
        for (std::size_t i = 0; i < unresolved.size(); ++i) {
            planner::table_storage_answer_t answer{std::pmr::vector<types::complex_logical_type>{resource}};
            std::vector<std::pair<int64_t, types::complex_logical_type>> ordered;
            bool named = false;
            for (const auto& chunk : read_results[i]) {
                // otterstax.remote_columns (tbl TEXT, col TEXT, type TEXT, ord BIGINT)
                for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                    const auto col_cell = chunk.value(1, row);
                    const auto type_cell = chunk.value(2, row);
                    const auto ord_cell = chunk.value(3, row);
                    const std::string col{col_cell.value<std::string_view>()};
                    const std::string type{type_cell.value<std::string_view>()};
                    named = true;
                    // A row of type NONE names the relation without giving it a column.
                    if (type == "NONE") {
                        continue;
                    }
                    ordered.emplace_back(ord_cell.value<int64_t>(),
                                         types::complex_logical_type{type == "TEXT"  ? types::logical_type::STRING_LITERAL
                                                                     : type == "INT" ? types::logical_type::INTEGER
                                                                                     : types::logical_type::BIGINT,
                                                                     col});
                }
            }
            if (named) {
                std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) {
                    return a.first < b.first;
                });
                for (auto& [_, type] : ordered) {
                    answer.columns.push_back(std::move(type));
                }
                answer.storage = core::pmr::make_polymorphic_unique<remote_storage_t>(
                    resource,
                    qualified(unresolved[i].database.t, unresolved[i].schema.t, unresolved[i].collection.t),
                    std::pmr::vector<types::complex_logical_type>(answer.columns, resource));
            }
            answers.push_back(std::move(answer));
        }
        return answers;
    }

    // The value of `<column> = <value>` or of a SET: a parameter, possibly under the assignment cast.
    bool parameter_in(const expressions::param_storage& operand, core::parameter_id_t& out);

    bool parameter_in(const expressions::expression_ptr& expression, core::parameter_id_t& out) {
        switch (expression->group()) {
            case expressions::expression_group::compare: {
                const auto& compare = static_cast<const expressions::compare_expression_t&>(*expression);
                return parameter_in(compare.right(), out);
            }
            case expressions::expression_group::scalar: {
                // A value, not a computation over a column: `amount + 1` has two operands.
                const auto& scalar = static_cast<const expressions::scalar_expression_t&>(*expression);
                return scalar.params().size() == 1 && parameter_in(scalar.params().front(), out);
            }
            case expressions::expression_group::cast:
                return parameter_in(static_cast<const expressions::cast_expression_t&>(*expression).child(), out);
            default:
                return false;
        }
    }

    bool parameter_in(const expressions::param_storage& operand, core::parameter_id_t& out) {
        if (std::holds_alternative<core::parameter_id_t>(operand)) {
            out = std::get<core::parameter_id_t>(operand);
            return true;
        }
        if (std::holds_alternative<expressions::expression_ptr>(operand)) {
            return parameter_in(std::get<expressions::expression_ptr>(operand), out);
        }
        return false;
    }

    // One remote UPDATE / DELETE: `WHERE <column> = <value>` (or no WHERE) and, for UPDATE, `SET <column> = <value>`.
    struct remote_modify_spec_t {
        std::string name;
        bool is_update{false};
        bool has_where{false};
        std::size_t where_column{0};
        core::parameter_id_t where_value{0};
        std::size_t set_column{0};
        core::parameter_id_t set_value{0};
    };

    struct remote_modify_payload_t final : logical_plan::extension_payload_t {
        explicit remote_modify_payload_t(remote_modify_spec_t spec)
            : spec(std::move(spec)) {}
        remote_modify_spec_t spec;
    };

    class remote_modify_t final : public operators::read_write_operator_t {
    public:
        remote_modify_t(std::pmr::memory_resource* resource, log_t log, const remote_modify_spec_t& spec)
            : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
            , spec_(spec) {}

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override {
            if (auto it = unreachable().find(spec_.name); it != unreachable().end()) {
                set_error(backend_error(resource(), it->second));
                co_return;
            }
            write_log().push_back(std::string{spec_.is_update ? "update " : "delete "} + spec_.name +
                                  (spec_.has_where ? " where column " + std::to_string(spec_.where_column) : "") +
                                  (spec_.is_update ? " set column " + std::to_string(spec_.set_column) : ""));
            const auto key = spec_.has_where ? ctx->parameters.parameters.at(spec_.where_value).value<int64_t>() : 0;
            auto& remote = backend()[spec_.name];
            for (auto row = remote.begin(); row != remote.end();) {
                if (spec_.has_where && (*row)[spec_.where_column] != key) {
                    ++row;
                    continue;
                }
                ++changed_;
                if (spec_.is_update) {
                    (*row)[spec_.set_column] = ctx->parameters.parameters.at(spec_.set_value).value<int64_t>();
                    ++row;
                } else {
                    row = remote.erase(row);
                }
            }
            mark_executed();
            co_return;
        }

    private:
        std::optional<uint64_t> affected_rows_impl() const noexcept override { return changed_; }

        remote_modify_spec_t spec_;
        uint64_t changed_{0};
    };

    logical_plan::storage_operator_t make_remote_modify(const services::context_storage_t& context,
                                                         const compute::function_registry_t&,
                                                         const logical_plan::node_extension_t& node) {
        const auto& payload = static_cast<const remote_modify_payload_t&>(*node.payload());
        return operators::operator_ptr{new remote_modify_t(context.resource, context.log.clone(), payload.spec)};
    }

    // The host's own table, or nullptr.
    const remote_storage_t* own_storage(const logical_plan::node_t& node) {
        const auto* table = node.table_metadata();
        if (table == nullptr || table->storage == nullptr || table->storage->owner() != &host_tag) {
            return nullptr;
        }
        return static_cast<const remote_storage_t*>(table->storage);
    }

    // What one remote statement can say: no FROM / USING, no RETURNING, no LIMIT, `WHERE <column> = <value>` or
    // none, and one `SET <column> = <value>`. Anything else stays with otterbrix's batch path.
    bool one_remote_statement(const logical_plan::node_t& write, remote_modify_spec_t& spec) {
        using logical_plan::node_type;
        const bool is_update = write.type() == node_type::update_t;
        spec.is_update = is_update;
        const auto& returning = is_update ? static_cast<const logical_plan::node_update_t&>(write).returning()
                                          : static_cast<const logical_plan::node_delete_t&>(write).returning();
        if (!returning.empty()) {
            return false;
        }
        for (const auto& child : write.children()) {
            if (child->type() == node_type::limit_t) {
                if (static_cast<const logical_plan::node_limit_t&>(*child).limit().limit() !=
                    logical_plan::limit_t::unlimit().limit()) {
                    return false;
                }
                continue;
            }
            if (child->type() != node_type::match_t) {
                return false;
            }
            const auto& where = child->expressions().front();
            if (where->group() != expressions::expression_group::compare) {
                return false;
            }
            const auto& compare = static_cast<const expressions::compare_expression_t&>(*where);
            if (compare.type() == expressions::compare_type::all_true) {
                continue;
            }
            if (compare.type() != expressions::compare_type::eq ||
                !std::holds_alternative<expressions::key_t>(compare.left()) ||
                !parameter_in(compare.right(), spec.where_value)) {
                return false;
            }
            spec.has_where = true;
            spec.where_column = std::get<expressions::key_t>(compare.left()).path().front();
        }
        if (!is_update) {
            return true;
        }
        const auto& updates = static_cast<const logical_plan::node_update_t&>(write).updates();
        if (updates.size() != 1 || !parameter_in(updates.front(), spec.set_value)) {
            return false;
        }
        spec.set_column = updates.front()->key().path().front();
        return true;
    }

    logical_plan::node_ptr push_whole_modify(std::pmr::memory_resource* resource,
                                             logical_plan::node_ptr node,
                                             const planner::optimizer_rule_context_t& context) {
        for (auto& child : node->children()) {
            child = push_whole_modify(resource, child, context);
        }
        if (node->type() != logical_plan::node_type::update_t && node->type() != logical_plan::node_type::delete_t) {
            return node;
        }
        const auto* storage = own_storage(*node);
        if (storage == nullptr) {
            return node;
        }
        remote_modify_spec_t spec;
        spec.name = storage->name();
        if (!one_remote_statement(*node, spec)) {
            return node;
        }
        auto remote = logical_plan::make_node_extension(
            resource,
            storage->name(),
            std::pmr::vector<types::complex_logical_type>{resource},
            &make_remote_modify,
            logical_plan::extension_payload_ptr{new remote_modify_payload_t{std::move(spec)}});
        if (remote.has_error()) {
            return node;
        }
        return remote.value();
    }

    constexpr planner::optimizer_rule_t host_rules[] = {
        {planner::optimizer_stage::after_simplify, &push_whole_modify},
    };

    services::engine::primitives_t host_primitives() {
        return services::engine::primitives_t{host_rules, {&need_remote_columns, &decide_remote_storages}};
    }

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    std::string error_of(const components::cursor::cursor_t_ptr& cursor) {
        return cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"};
    }

    void create_host_tables(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(run(dispatcher, "CREATE DATABASE otterstax;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE otterstax.remote_columns (tbl TEXT, col TEXT, type TEXT, ord BIGINT);")
                    ->is_success());
    }

    const char* declare_orders = "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                                 "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'amount', 'BIGINT', 2);";

    rows_t sorted_int_rows(const components::cursor::cursor_t_ptr& cursor) {
        rows_t rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                rows.push_back(int_row(chunk, row));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    rows_t sorted(rows_t rows) {
        std::sort(rows.begin(), rows.end());
        return rows;
    }

} // namespace

#define HOST_TEST_BOILERPLATE(DIR)                                                                                     \
    auto config = test_create_config(integration_fixture_path(DIR));                                                   \
    test_clear_directory(config);                                                                                      \
    backend().clear();                                                                                                 \
    batches().clear();                                                                                                 \
    unreachable().clear();                                                                                             \
    no_row_numbers().clear();                                                                                          \
    asked_names().clear();                                                                                             \
    write_log().clear();                                                                                               \
    explicit_transactions().clear();                                                                                   \
    backend()["m2.shop.orders"] = {{1, 100}, {2, 200}, {3, 300}};                                                      \
    counters().reset();                                                                                                \
    test_spaces space(config, host_primitives());                                                                      \
    auto* dispatcher = space.dispatcher();                                                                             \
    create_host_tables(dispatcher);

TEST_CASE("integration::cpp::host_names::host_table_row_decides_the_name") {
    HOST_TEST_BOILERPLATE("test_host_names/row_decides")

    auto missing = run(dispatcher, "SELECT * FROM m2.shop.orders;");
    REQUIRE(missing->is_error());
    CHECK(std::string{missing->get_error().what}.find("does not exist") != std::string::npos);

    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto found = run(dispatcher, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(found->is_success());
    REQUIRE(sorted_int_rows(found) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    REQUIRE(run(dispatcher, "DELETE FROM otterstax.remote_columns WHERE tbl = 'm2.shop.orders';")->is_success());
    auto gone = run(dispatcher, "SELECT * FROM m2.shop.orders;");
    REQUIRE(gone->is_error());
    CHECK(std::string{gone->get_error().what}.find("does not exist") != std::string::npos);
}

TEST_CASE("integration::cpp::host_names::reads_run_in_the_statement_snapshot") {
    HOST_TEST_BOILERPLATE("test_host_names/snapshot")

    auto writer = otterbrix::session_id_t();
    auto other = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, writer, declare_orders)->is_success());

    auto own = run(dispatcher, writer, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(own->is_success());
    REQUIRE(own->size() == 3);

    REQUIRE(run(dispatcher, other, "SELECT * FROM m2.shop.orders;")->is_error());

    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    auto after = run(dispatcher, other, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(after->is_success());
    REQUIRE(after->size() == 3);
}

TEST_CASE("integration::cpp::host_names::join_a_storage_table_with_a_local_table") {
    HOST_TEST_BOILERPLATE("test_host_names/join_local")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE shopdb;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE shopdb.customers (id BIGINT, bonus BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO shopdb.customers (id, bonus) VALUES (1, 7), (3, 9), (5, 11);")->is_success());

    auto joined = run(dispatcher,
                      "SELECT o.id, o.amount, c.bonus FROM m2.shop.orders AS o "
                      "JOIN shopdb.customers AS c ON o.id = c.id;");
    REQUIRE(joined->is_success());
    REQUIRE(sorted_int_rows(joined) == rows_t{{1, 100, 7}, {3, 300, 9}});
}

TEST_CASE("integration::cpp::host_names::declared_columns_drive_validation_and_types") {
    HOST_TEST_BOILERPLATE("test_host_names/declared")
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'label', 'TEXT', 2);")
                ->is_success());

    REQUIRE(run(dispatcher, "SELECT nope FROM m2.shop.orders;")->is_error());

    auto labels = run(dispatcher, "SELECT label FROM m2.shop.orders WHERE id = 2;");
    REQUIRE(labels->is_success());
    REQUIRE(labels->size() == 1);
    REQUIRE(labels->chunks().front().data[0].type().type() == types::logical_type::STRING_LITERAL);
    REQUIRE(labels->value(0, 0).value<std::string_view>() == "s200");

    auto sums = run(dispatcher, "SELECT id + 1 AS n FROM m2.shop.orders WHERE id = 3;");
    REQUIRE(sums->is_success());
    REQUIRE(sums->value(0, 0).value<int64_t>() == 4);
}

TEST_CASE("integration::cpp::host_names::count_star_counts_the_storage_rows") {
    HOST_TEST_BOILERPLATE("test_host_names/count_star")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.orders;");
    REQUIRE(counted->is_success());
    REQUIRE(counted->size() == 1);
    REQUIRE(counted->value(0, 0).value<int64_t>() == 3);
}

TEST_CASE("integration::cpp::host_names::local_statements_never_reach_the_host") {
    HOST_TEST_BOILERPLATE("test_host_names/local_only")
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.a (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.b (k BIGINT, w BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.a (k, v) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.b (k, w) VALUES (1, 5);")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.av AS SELECT k, v FROM loc.a;")->is_success());
    const char* statements[] = {
        "SELECT * FROM loc.a;",
        "SELECT count(*) FROM loc.a;",
        "SELECT a.v, b.w FROM loc.a AS a JOIN loc.b AS b ON a.k = b.k;",
        "SELECT a.v FROM loc.a AS a, loc.b AS b WHERE a.k = b.k;",
        "WITH x AS (SELECT k FROM loc.a) SELECT * FROM x;",
        "SELECT v FROM loc.a WHERE k IN (SELECT k FROM loc.b);",
        "SELECT * FROM loc.av;",
        "UPDATE loc.a SET v = 11 WHERE k = 1;",
        "DELETE FROM loc.b WHERE k = 9;",
        "INSERT INTO loc.b (k, w) SELECT k, v FROM loc.a;",
    };
    for (const auto* sql : statements) {
        INFO(sql);
        REQUIRE(run(dispatcher, sql)->is_success());
    }
    CHECK(counters().need.load() == 0);
    CHECK(counters().reads.load() == 0);
    CHECK(counters().decide.load() == 0);

    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "SELECT * FROM m2.shop.orders;")->is_success());
    CHECK(counters().need.load() == 1);
    CHECK(counters().reads.load() == 1);
    CHECK(counters().decide.load() == 1);
}

TEST_CASE("integration::cpp::host_names::dml_on_a_local_table_reads_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/dml_embedded")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());

    SECTION("INSERT ... SELECT copies the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) SELECT id, amount FROM m2.shop.orders;")->is_success());
        auto copied = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(copied->is_success());
        REQUIRE(sorted_int_rows(copied) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    }
    SECTION("UPDATE ... FROM reads the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 0), (5, 0);")->is_success());
        auto upd = run(dispatcher, "UPDATE loc.t SET amount = o.amount FROM m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO(error_of(upd));
        REQUIRE(upd->is_success());
        auto updated = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(updated) == rows_t{{1, 100}, {5, 0}});
    }
    SECTION("DELETE ... USING reads the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (2, 0), (7, 0);")->is_success());
        auto del = run(dispatcher, "DELETE FROM loc.t USING m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO(error_of(del));
        REQUIRE(del->is_success());
        auto left = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(left) == rows_t{{7, 0}});
    }
}

TEST_CASE("integration::cpp::host_names::a_storage_error_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/operator_error")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    unreachable()["m2.shop.orders"] = "server m2: connection refused";
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.c (id BIGINT);")->is_success());

    for (const char* sql : {"SELECT * FROM m2.shop.orders;",
                            "SELECT count(*) FROM m2.shop.orders;",
                            "SELECT o.id FROM m2.shop.orders AS o JOIN loc.c AS c ON o.id = c.id;",
                            "INSERT INTO loc.c (id) SELECT id FROM m2.shop.orders;"}) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::connection_closed);
        CHECK(std::string{cursor->get_error().what} == "server m2: connection refused");
    }
}

// A view body is a query too: at CREATE VIEW the host resolves the names the catalog does not, and the view
// depends on nothing it resolved (a storage table has no catalog oid).
TEST_CASE("integration::cpp::host_names::a_view_over_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/view_created")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());

    auto created = run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;");
    INFO("error: " << error_of(created));
    REQUIRE(created->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.ov;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    auto oid = run(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'ov';");
    REQUIRE(oid->size() == 1);
    const auto view_oid = std::to_string(oid->chunks().front().get_value<std::uint32_t>(0, 0));
    auto depends = run(dispatcher, "SELECT refclassid FROM pg_catalog.pg_depend WHERE objid = " + view_oid + ";");
    REQUIRE(depends->size() == 1);
    CHECK(depends->chunks().front().get_value<std::uint32_t>(0, 0) ==
          components::catalog::well_known_oid::pg_namespace_table);
}

// Trino 483 checkViewStaleness compares the view's output columns only: a column the storage gained is not read.
TEST_CASE("integration::cpp::host_names::a_storage_column_the_view_does_not_read_keeps_it_fresh") {
    HOST_TEST_BOILERPLATE("test_host_names/view_node_grew")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'label', 'TEXT', 3);")
                ->is_success());
    backend()["m2.shop.orders"] = {{1, 100, 7}, {2, 200, 8}, {3, 300, 9}};

    auto read = run(dispatcher, "SELECT id, amount FROM loc.ov;");
    INFO("error: " << error_of(read));
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});
}

// The type of an output column is compared exactly: no coercion is inserted for a wider storage type.
TEST_CASE("integration::cpp::host_names::a_view_whose_output_column_changed_type_is_stale") {
    HOST_TEST_BOILERPLATE("test_host_names/view_column_type")
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'amount', 'INT', 2);")
                ->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());
    REQUIRE(run(dispatcher, "SELECT id FROM loc.ov;")->is_success());
    REQUIRE(run(dispatcher, "UPDATE otterstax.remote_columns SET type = 'BIGINT' WHERE col = 'amount';")->is_success());

    auto stale = run(dispatcher, "SELECT id FROM loc.ov;");
    REQUIRE(stale->is_error());
    CHECK(std::string{stale->get_error().what} ==
          "view \"ov\" is stale: its column \"amount\" is now int8, it was created as int4; recreate the view");
}

TEST_CASE("integration::cpp::host_names::a_view_whose_storage_name_is_gone_is_stale") {
    HOST_TEST_BOILERPLATE("test_host_names/view_name_gone")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());
    REQUIRE(run(dispatcher, "DELETE FROM otterstax.remote_columns WHERE tbl = 'm2.shop.orders';")->is_success());

    auto stale = run(dispatcher, "SELECT id FROM loc.ov;");
    REQUIRE(stale->is_error());
    CHECK(std::string{stale->get_error().what}.find("view \"ov\" is stale") != std::string::npos);
}

// A matview body is a query too: CREATE resolves through the view it names down to the host, REFRESH fills it.
TEST_CASE("integration::cpp::host_names::a_matview_over_a_view_over_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/matview_over_view")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());

    auto created = run(dispatcher, "CREATE MATERIALIZED VIEW loc.mv AS SELECT id, amount FROM loc.ov WITH NO DATA;");
    INFO("error: " << error_of(created));
    REQUIRE(created->is_success());
    auto refreshed = run(dispatcher, "REFRESH MATERIALIZED VIEW loc.mv;");
    INFO("error: " << error_of(refreshed));
    REQUIRE(refreshed->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.mv;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});
}

// A table without columns still has rows: count(*) counts them, one batch or several.
TEST_CASE("integration::cpp::host_names::rows_without_columns_are_counted") {
    HOST_TEST_BOILERPLATE("test_host_names/no_columns")
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.marks', '', 'NONE', 1);")
                ->is_success());

    SECTION("one batch") {
        backend()["m2.shop.marks"] = {{}, {}, {}};
        auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.marks;");
        REQUIRE(counted->is_success());
        REQUIRE(counted->size() == 1);
        CHECK(counted->value(0, 0).value<int64_t>() == 3);
    }
    SECTION("an empty batch between two others") {
        batches()["m2.shop.marks"] = {{{}, {}}, {}, {{}}};
        auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.marks;");
        REQUIRE(counted->is_success());
        REQUIRE(counted->size() == 1);
        CHECK(counted->value(0, 0).value<int64_t>() == 3);
    }
}

// An empty batch is data like any other; only the source's explicit end stops the read.
TEST_CASE("integration::cpp::host_names::an_empty_batch_is_not_the_end") {
    HOST_TEST_BOILERPLATE("test_host_names/empty_batch")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    batches()["m2.shop.orders"] = {{}, {{1, 100}}, {}, {{2, 200}, {3, 300}}, {}};

    auto rows = run(dispatcher, "SELECT id, amount FROM m2.shop.orders;");
    REQUIRE(rows->is_success());
    CHECK(sorted_int_rows(rows) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.orders;");
    REQUIRE(counted->is_success());
    CHECK(counted->value(0, 0).value<int64_t>() == 3);
}

// The host is asked about the whole written name, the schema part included, once per statement.
TEST_CASE("integration::cpp::host_names::a_write_target_keeps_its_schema") {
    HOST_TEST_BOILERPLATE("test_host_names/write_schema")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);",
                            "UPDATE m2.shop.orders SET amount = 1 WHERE id = 1;",
                            "DELETE FROM m2.shop.orders WHERE id = 2;"}) {
        asked_names().clear();
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
        CHECK(asked_names() == std::vector<std::string>{"m2.shop.orders"});
    }
}

TEST_CASE("integration::cpp::host_names::insert_into_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/insert")
    REQUIRE(run(dispatcher, declare_orders)->is_success());

    SECTION("the declared columns, cast to their types, reach the storage; the count is the rows written") {
        auto inserted =
            run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, CAST(400 AS INTEGER)), (5, 500);");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(inserted->affected_rows() == std::optional<std::uint64_t>{2});
        CHECK(inserted->size() == 0);
        CHECK(write_log() == std::vector<std::string>{"insert m2.shop.orders id:bigint,amount:bigint"});
        auto read = run(dispatcher, "SELECT id, amount FROM m2.shop.orders;");
        REQUIRE(read->is_success());
        CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}});
    }
    SECTION("a column list in another order still arrives in the declared order") {
        REQUIRE(run(dispatcher, "INSERT INTO m2.shop.orders (amount, id) VALUES (600, 6);")->is_success());
        CHECK(backend()["m2.shop.orders"].back() == std::vector<int64_t>{6, 600});
    }
    SECTION("INSERT ... SELECT from a local table, with and without a column list") {
        REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (8, 800), (9, 900);")->is_success());
        auto copied = run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) SELECT id, amount FROM loc.t;");
        INFO(error_of(copied));
        REQUIRE(copied->is_success());
        CHECK(copied->affected_rows() == std::optional<std::uint64_t>{2});
        auto positional = run(dispatcher, "INSERT INTO m2.shop.orders SELECT id + 10, amount FROM loc.t;");
        INFO(error_of(positional));
        REQUIRE(positional->is_success());
        CHECK(backend()["m2.shop.orders"].back() == std::vector<int64_t>{19, 900});
        CHECK(backend()["m2.shop.orders"].size() == 7);
    }
    SECTION("a value no assignment cast takes to the declared type is refused before the storage sees it") {
        auto refused = run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (10, 'ten');");
        REQUIRE(refused->is_error());
        CHECK(write_log().empty());
    }
}

// A storage table declares no defaults: a column the INSERT leaves out is NULL, as for a local table without one.
TEST_CASE("integration::cpp::host_names::an_omitted_column_is_null") {
    HOST_TEST_BOILERPLATE("test_host_names/insert_null")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (8, 800);")->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.orders (id) VALUES (4);",
                            "INSERT INTO m2.shop.orders VALUES (5);",
                            "INSERT INTO m2.shop.orders (amount) SELECT amount FROM loc.t;"}) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
    }
    CHECK(sorted(backend()["m2.shop.orders"]) == rows_t{{-1, 800}, {1, 100}, {2, 200}, {3, 300}, {4, -1}, {5, -1}});
}

TEST_CASE("integration::cpp::host_names::insert_returning_reads_the_rows_written") {
    HOST_TEST_BOILERPLATE("test_host_names/insert_returning")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto returned = run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400), (5, 500) RETURNING id;");
    INFO(error_of(returned));
    REQUIRE(returned->is_success());
    CHECK(sorted_int_rows(returned) == rows_t{{4}, {5}});
    CHECK(backend()["m2.shop.orders"].size() == 5);
}

// The host's rule replaces a simple UPDATE / DELETE of its table with one remote statement (as postgres_fdw's
// direct modify does); the statement never scans the table.
TEST_CASE("integration::cpp::host_names::a_simple_update_or_delete_is_one_remote_statement") {
    HOST_TEST_BOILERPLATE("test_host_names/one_remote_statement")
    REQUIRE(run(dispatcher, declare_orders)->is_success());

    auto updated = run(dispatcher, "UPDATE m2.shop.orders SET amount = 250 WHERE id = 2;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == std::optional<std::uint64_t>{1});
    CHECK(updated->size() == 0);

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 1;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{1});

    auto untouched = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 99;");
    REQUIRE(untouched->is_success());
    CHECK(untouched->affected_rows() == std::optional<std::uint64_t>{0});

    CHECK(write_log() == std::vector<std::string>{"update m2.shop.orders where column 0 set column 1",
                                                  "delete m2.shop.orders where column 0",
                                                  "delete m2.shop.orders where column 0"});
    CHECK(backend()["m2.shop.orders"] == rows_t{{2, 250}, {3, 300}});

    auto all = run(dispatcher, "DELETE FROM m2.shop.orders;");
    REQUIRE(all->is_success());
    CHECK(all->affected_rows() == std::optional<std::uint64_t>{2});
    CHECK(write_log().back() == "delete m2.shop.orders");
    CHECK(backend()["m2.shop.orders"].empty());
}

namespace {
    std::vector<std::string> explain_lines(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
        std::vector<std::string> lines;
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            const auto cell = cursor->value(0, row);
            lines.emplace_back(cell.value<std::string_view>());
        }
        return lines;
    }
} // namespace

// PostgreSQL 18 postgres_fdw: "Foreign Scan on ..." with "Remote SQL: ..." under it. The storage's scan says both.
TEST_CASE("integration::cpp::host_names::explain_prints_the_storage_scan_label_and_details") {
    HOST_TEST_BOILERPLATE("test_host_names/explain")
    REQUIRE(run(dispatcher, declare_orders)->is_success());

    SECTION("EXPLAIN") {
        CHECK(explain_lines(dispatcher, "EXPLAIN SELECT * FROM m2.shop.orders;") ==
              std::vector<std::string>{"Foreign Scan on m2.shop.orders", "  Remote SQL: SELECT * FROM shop.orders"});
    }
    SECTION("EXPLAIN ANALYZE") {
        auto lines = explain_lines(dispatcher, "EXPLAIN ANALYZE SELECT * FROM m2.shop.orders;");
        REQUIRE(lines.size() == 2);
        CHECK(lines[0].rfind("Foreign Scan on m2.shop.orders  (actual time=", 0) == 0);
        CHECK(lines[0].find("rows=3 loops=1)") != std::string::npos);
        CHECK(lines[1] == "  Remote SQL: SELECT * FROM shop.orders");
    }
    SECTION("the details of a node below the root are indented under its label") {
        REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE loc.c (id BIGINT);")->is_success());
        auto lines = explain_lines(dispatcher,
                                   "EXPLAIN SELECT o.id FROM m2.shop.orders AS o JOIN loc.c AS c ON o.id = c.id;");
        CHECK(lines == std::vector<std::string>{"Project",
                                                "  ->  Hash Join",
                                                "    ->  Foreign Scan on m2.shop.orders",
                                                "          Remote SQL: SELECT * FROM shop.orders",
                                                "    ->  Seq Scan on c"});
    }
}

// What one remote statement cannot say runs as otterbrix's own UPDATE / DELETE: the storage's scan numbers the
// rows, otterbrix does the FROM / USING semi-join, the WHERE and RETURNING, and the numbers come back to the
// storage's update or delete sink.
TEST_CASE("integration::cpp::host_names::update_and_delete_by_row_number") {
    HOST_TEST_BOILERPLATE("test_host_names/by_row_number")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 111), (3, 333), (9, 999);")->is_success());

    SECTION("UPDATE ... FROM a local table") {
        auto updated = run(dispatcher,
                           "UPDATE m2.shop.orders SET amount = t.amount FROM loc.t AS t "
                           "WHERE m2.shop.orders.id = t.id;");
        INFO(error_of(updated));
        REQUIRE(updated->is_success());
        CHECK(updated->affected_rows() == std::optional<std::uint64_t>{2});
        CHECK(write_log() == std::vector<std::string>{"update m2.shop.orders rows 2"});
        CHECK(backend()["m2.shop.orders"] == rows_t{{1, 111}, {2, 200}, {3, 333}});
    }
    SECTION("DELETE ... USING a local table") {
        auto deleted = run(dispatcher, "DELETE FROM m2.shop.orders USING loc.t AS t WHERE m2.shop.orders.id = t.id;");
        INFO(error_of(deleted));
        REQUIRE(deleted->is_success());
        CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{2});
        CHECK(write_log() == std::vector<std::string>{"delete m2.shop.orders rows 2"});
        CHECK(backend()["m2.shop.orders"] == rows_t{{2, 200}});
    }
    SECTION("UPDATE ... RETURNING") {
        auto returned =
            run(dispatcher, "UPDATE m2.shop.orders SET amount = amount + 1 WHERE id >= 2 RETURNING id, amount;");
        INFO(error_of(returned));
        REQUIRE(returned->is_success());
        CHECK(sorted_int_rows(returned) == rows_t{{2, 201}, {3, 301}});
        CHECK(write_log() == std::vector<std::string>{"update m2.shop.orders rows 2"});
        CHECK(backend()["m2.shop.orders"] == rows_t{{1, 100}, {2, 201}, {3, 301}});
    }
    SECTION("DELETE ... RETURNING, nothing matched: the columns are still typed") {
        auto returned = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 3 RETURNING amount;");
        INFO(error_of(returned));
        REQUIRE(returned->is_success());
        CHECK(sorted_int_rows(returned) == rows_t{{300}});
        auto none = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 42 RETURNING amount;");
        INFO(error_of(none));
        REQUIRE(none->is_success());
        CHECK(none->size() == 0);
        CHECK(backend()["m2.shop.orders"] == rows_t{{1, 100}, {2, 200}});
    }
}

// A storage that cannot change a row by its number refuses make_update / make_delete: what the host's rule did not
// take as one statement is refused with the storage's own error, and nothing reaches the backend.
TEST_CASE("integration::cpp::host_names::a_storage_without_row_numbers_refuses_the_batch_path") {
    HOST_TEST_BOILERPLATE("test_host_names/no_row_numbers")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    no_row_numbers().insert("m2.shop.orders");
    for (const char* sql :
         {"UPDATE m2.shop.orders SET amount = t.amount FROM loc.t AS t WHERE m2.shop.orders.id = t.id;",
          "DELETE FROM m2.shop.orders WHERE id = 1 RETURNING id;"}) {
        INFO(sql);
        auto refused = run(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::unimplemented_yet);
        CHECK(std::string{refused->get_error().what} ==
              "storage \"m2.shop.orders\" cannot change a row by its number");
    }
    CHECK(write_log().empty());

    auto simple = run(dispatcher, "UPDATE m2.shop.orders SET amount = 1 WHERE id = 1;");
    INFO(error_of(simple));
    REQUIRE(simple->is_success());
    CHECK(write_log() == std::vector<std::string>{"update m2.shop.orders where column 0 set column 1"});
}

// No refusal inside BEGIN ... COMMIT: the storage learns the statement is in an explicit transaction and decides
// itself (a ROLLBACK does not undo what it wrote, #663).
TEST_CASE("integration::cpp::host_names::a_storage_learns_of_an_explicit_transaction") {
    HOST_TEST_BOILERPLATE("test_host_names/write_txn")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    explicit_transactions().clear();
    auto inside = run(dispatcher, session, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);");
    INFO(error_of(inside));
    REQUIRE(inside->is_success());
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(explicit_transactions() == std::vector<bool>{true});

    explicit_transactions().clear();
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.orders (id, amount) VALUES (5, 500);")->is_success());
    CHECK(explicit_transactions() == std::vector<bool>{false});
    CHECK(backend()["m2.shop.orders"].size() == 5);
}

// The backend's refusal (a NOT NULL or CHECK it enforces, an unreachable server) reaches the cursor unchanged, on
// the batch path and through the host's one-statement rule alike.
TEST_CASE("integration::cpp::host_names::a_storage_write_error_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/write_error")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    unreachable()["m2.shop.orders"] = "server m2: new row violates check constraint \"amount_positive\"";
    for (const char* sql : {"INSERT INTO m2.shop.orders (id, amount) VALUES (4, -1);",
                            "UPDATE m2.shop.orders SET amount = -1 WHERE id = 1;",
                            "DELETE FROM m2.shop.orders WHERE id = 1;"}) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::connection_closed);
        CHECK(std::string{cursor->get_error().what} ==
              "server m2: new row violates check constraint \"amount_positive\"");
    }
    CHECK(backend()["m2.shop.orders"].size() == 3);
}
