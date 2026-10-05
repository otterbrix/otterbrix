// A test host that knows remote tables only through its own tables in the engine: the name resolution hook reads
// otterstax.remote_columns for every name the catalog did not resolve and puts a host node with the declared
// columns in its place; the host operator serves canned backend rows. An INSERT / UPDATE / DELETE into such a name
// gets its target bound to the host relation; the host's write operators change the canned rows.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/host_write_target.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <atomic>
#include <map>
#include <string>
#include <vector>

using namespace components;

namespace {

    // The "remote" data behind each name; one int64 per declared column (a TEXT column shows it as "s<n>").
    std::map<std::string, std::vector<std::vector<int64_t>>>& backend() {
        static std::map<std::string, std::vector<std::vector<int64_t>>> rows;
        return rows;
    }

    std::vector<std::string>& asked_names() {
        static std::vector<std::string> names;
        return names;
    }

    std::vector<std::string>& write_log() {
        static std::vector<std::string> log;
        return log;
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

    struct remote_payload_t final : logical_plan::extension_payload_t {
        explicit remote_payload_t(std::string name)
            : name(std::move(name)) {}
        std::string name;
    };

    using batch_t = std::vector<std::vector<int64_t>>;

    // A backend that answers in several batches (an empty one included); the default is one batch of backend().
    std::map<std::string, std::vector<batch_t>>& batches() {
        static std::map<std::string, std::vector<batch_t>> script;
        return script;
    }

    class remote_source_t final : public operators::read_only_operator_t {
    public:
        remote_source_t(std::pmr::memory_resource* resource,
                        log_t log,
                        std::pmr::vector<types::complex_logical_type> columns,
                        std::vector<batch_t> batches)
            : operators::read_only_operator_t(resource, std::move(log), operators::operator_type::extension)
            , columns_(std::move(columns))
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
            const auto& rows = batches_[next_++];
            vector::data_chunk_t chunk(resource(), columns_, std::max<std::size_t>(rows.size(), 1));
            chunk.set_cardinality(rows.size());
            for (std::size_t row = 0; row < rows.size(); ++row) {
                for (std::size_t col = 0; col < columns_.size(); ++col) {
                    if (columns_[col].type() == types::logical_type::STRING_LITERAL) {
                        chunk.set_value(col,
                                        row,
                                        types::logical_value_t(resource(), "s" + std::to_string(rows[row][col])));
                    } else if (columns_[col].type() == types::logical_type::INTEGER) {
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

        void reset_pipeline_state() noexcept override { next_ = 0; }

        void set_name(std::string name) { name_ = std::move(name); }

    private:
        std::pmr::string explain_label_impl() const override {
            return std::pmr::string{"Foreign Scan on " + name_, resource()};
        }
        std::pmr::vector<std::pmr::string> explain_details_impl() const override {
            std::pmr::vector<std::pmr::string> details{resource()};
            details.emplace_back("Remote SQL: SELECT * FROM " + name_.substr(name_.find('.') + 1));
            return details;
        }

        std::string name_;
        std::pmr::vector<types::complex_logical_type> columns_;
        std::vector<batch_t> batches_;
        std::size_t next_{0};
    };

    // Names whose backend cannot be reached: the host's operator function refuses them with its own reason.
    std::map<std::string, std::string>& unreachable() {
        static std::map<std::string, std::string> reasons;
        return reasons;
    }

    services::planner::plan_result_t make_remote_source(const services::context_storage_t& context,
                                                        const compute::function_registry_t&,
                                                        const logical_plan::node_extension_t& node) {
        const auto* payload = static_cast<const remote_payload_t*>(node.payload());
        if (auto it = unreachable().find(payload->name); it != unreachable().end()) {
            return core::error_t{core::error_code_t::connection_closed,
                                 std::pmr::string{it->second.c_str(), context.resource}};
        }
        std::pmr::vector<types::complex_logical_type> columns(node.columns(), context.resource);
        auto scripted = batches().find(payload->name);
        auto answer = scripted != batches().end() ? scripted->second : std::vector<batch_t>{backend()[payload->name]};
        auto source = boost::intrusive_ptr(
            new remote_source_t(context.resource, context.log.clone(), std::move(columns), std::move(answer)));
        source->set_name(payload->name);
        return {source};
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

    // INSERT: a sink that receives the rows already cast to the declared columns and appends them remotely.
    class remote_insert_t final : public operators::read_write_operator_t {
    public:
        remote_insert_t(std::pmr::memory_resource* resource, log_t log, std::string name)
            : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
            , name_(std::move(name)) {}

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&& input, operators::chunks_vector_t&) override {
            write_log().push_back("insert " + name_ + " " + type_names(input));
            for (std::uint64_t row = 0; row < input.size(); ++row) {
                std::vector<int64_t> values;
                for (std::uint64_t col = 0; col < input.column_count(); ++col) {
                    const auto cell = input.value(col, row);
                    values.push_back(cell.value<int64_t>());
                }
                rows_.push_back(std::move(values));
            }
            return core::error_t::no_error();
        }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t*) override {
            if (auto it = unreachable().find(name_); it != unreachable().end()) {
                set_error(core::error_t{core::error_code_t::connection_closed,
                                        std::pmr::string{it->second.c_str(), resource()}});
                co_return;
            }
            auto& remote = backend()[name_];
            remote.insert(remote.end(), rows_.begin(), rows_.end());
            written_ = rows_.size();
            rows_.clear();
            mark_executed();
            co_return;
        }

    private:
        std::optional<uint64_t> affected_rows_impl() const noexcept override { return written_; }
        std::pmr::string explain_label_impl() const override {
            return std::pmr::string{"Foreign Insert on " + name_, resource()};
        }
        std::pmr::vector<std::pmr::string> explain_details_impl() const override {
            std::pmr::vector<std::pmr::string> details{resource()};
            details.emplace_back("Remote SQL: INSERT INTO " + name_.substr(name_.find('.') + 1) + " VALUES ($1, $2)");
            details.emplace_back("Batch Size: 1");
            return details;
        }

        std::string name_;
        std::vector<std::vector<int64_t>> rows_;
        uint64_t written_{0};
    };

    core::parameter_id_t parameter_of(const expressions::expression_ptr& expression) {
        if (expression->group() == expressions::expression_group::compare) {
            const auto& compare = static_cast<const expressions::compare_expression_t&>(*expression);
            REQUIRE(std::holds_alternative<core::parameter_id_t>(compare.right()));
            return std::get<core::parameter_id_t>(compare.right());
        }
        REQUIRE(expression->group() == expressions::expression_group::scalar);
        const auto& scalar = static_cast<const expressions::scalar_expression_t&>(*expression);
        for (const auto& param : scalar.params()) {
            if (std::holds_alternative<core::parameter_id_t>(param)) {
                return std::get<core::parameter_id_t>(param);
            }
        }
        FAIL("no parameter in " << expression->to_string());
        return core::parameter_id_t{0};
    }

    // UPDATE / DELETE: the host reads the validated statement — `WHERE <column> = <value>` and, for UPDATE,
    // `SET <column> = <value>` — and changes the matching remote rows itself.
    class remote_modify_t final : public operators::read_write_operator_t {
    public:
        remote_modify_t(std::pmr::memory_resource* resource,
                        log_t log,
                        std::string name,
                        const logical_plan::node_t& write)
            : operators::read_write_operator_t(resource, std::move(log), operators::operator_type::extension)
            , name_(std::move(name))
            , is_update_(write.type() == logical_plan::node_type::update_t) {
            for (const auto& child : write.children()) {
                if (child->type() == logical_plan::node_type::match_t) {
                    const auto& where = child->expressions().front();
                    const auto& compare = static_cast<const expressions::compare_expression_t&>(*where);
                    if (compare.type() == expressions::compare_type::all_true) {
                        continue;
                    }
                    const auto& key = std::get<expressions::key_t>(compare.left());
                    has_where_ = true;
                    where_column_ = key.path().front();
                    where_value_ = parameter_of(where);
                }
            }
            if (is_update_) {
                const auto& set = static_cast<const logical_plan::node_update_t&>(write).updates().front();
                set_column_ = set->key().path().front();
                set_value_ = parameter_of(set);
            }
            write_log().push_back(std::string{is_update_ ? "update " : "delete "} + name_ + " where column " +
                                  std::to_string(where_column_) +
                                  (is_update_ ? " set column " + std::to_string(set_column_) : std::string{}));
        }

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override {
            if (auto it = unreachable().find(name_); it != unreachable().end()) {
                set_error(core::error_t{core::error_code_t::connection_closed,
                                        std::pmr::string{it->second.c_str(), resource()}});
                co_return;
            }
            const auto key = has_where_ ? ctx->parameters.parameters.at(where_value_).value<int64_t>() : 0;
            auto& remote = backend()[name_];
            for (auto row = remote.begin(); row != remote.end();) {
                if (has_where_ && (*row)[where_column_] != key) {
                    ++row;
                    continue;
                }
                ++changed_;
                if (is_update_) {
                    (*row)[set_column_] = ctx->parameters.parameters.at(set_value_).value<int64_t>();
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

        std::string name_;
        bool is_update_;
        bool has_where_{false};
        std::size_t where_column_{0};
        core::parameter_id_t where_value_{0};
        std::size_t set_column_{0};
        core::parameter_id_t set_value_{0};
        uint64_t changed_{0};
    };

    // What the host reads from the validated statement node: the documented host API
    // (docs/embedding-host-api.md, "What a write function reads").
    std::vector<std::string>& seen_writes() {
        static std::vector<std::string> seen;
        return seen;
    }

    std::string key_of(const expressions::key_t& key) {
        return "column " + std::to_string(key.path().front()) +
               (key.side() == expressions::side_t::left ? " left" : " other side");
    }

    template<class Write>
    std::string target_of(const Write& write) {
        return write.dbname() + "|" + write.schema() + "|" + write.relname();
    }

    std::string describe_write(const logical_plan::node_t& write) {
        using logical_plan::node_type;
        std::string out;
        switch (write.type()) {
            case node_type::insert_t: {
                const auto& insert = static_cast<const logical_plan::node_insert_t&>(write);
                out = "insert " + target_of(insert) + " from " +
                      (insert.children().front()->type() == node_type::data_t ? "values" : "a query");
                break;
            }
            case node_type::update_t:
                out = "update " + target_of(static_cast<const logical_plan::node_update_t&>(write));
                break;
            case node_type::delete_t:
                out = "delete " + target_of(static_cast<const logical_plan::node_delete_t&>(write));
                break;
            default:
                FAIL("a write function got " << write.to_string());
        }
        for (const auto& child : write.children()) {
            if (child->type() == node_type::match_t) {
                const auto& compare =
                    static_cast<const expressions::compare_expression_t&>(*child->expressions().front());
                if (compare.type() == expressions::compare_type::all_true) {
                    out += " where all rows";
                } else {
                    REQUIRE(compare.type() == expressions::compare_type::eq);
                    out += " where " + key_of(std::get<expressions::key_t>(compare.left())) + " = " +
                           (std::holds_alternative<core::parameter_id_t>(compare.right()) ? "parameter" : "other");
                }
            } else if (child->type() == node_type::limit_t) {
                const auto& limit = static_cast<const logical_plan::node_limit_t&>(*child).limit();
                if (limit.limit() != logical_plan::limit_t::unlimit().limit()) {
                    out += " limit " + std::to_string(limit.limit());
                }
            }
        }
        if (write.type() == node_type::update_t) {
            for (const auto& set : static_cast<const logical_plan::node_update_t&>(write).updates()) {
                out += " set " + key_of(set->key());
            }
        }
        return out;
    }

    services::planner::plan_result_t make_remote_write(const services::context_storage_t& context,
                                                       const compute::function_registry_t&,
                                                       const logical_plan::node_extension_t& relation,
                                                       const logical_plan::node_t& write) {
        seen_writes().push_back(describe_write(write));
        const auto* payload = static_cast<const remote_payload_t*>(relation.payload());
        if (write.type() == logical_plan::node_type::insert_t) {
            return {new remote_insert_t(context.resource, context.log.clone(), payload->name)};
        }
        return {new remote_modify_t(context.resource, context.log.clone(), payload->name, write)};
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
            auto agg = logical_plan::make_node_aggregate(resource,
                                                         core::dbname_t{"otterstax"},
                                                         core::relname_t{"remote_columns"});
            auto expr =
                expressions::make_compare_expression(resource,
                                                     expressions::compare_type::eq,
                                                     expressions::key_t{resource, "tbl", expressions::side_t::left},
                                                     core::parameter_id_t{1});
            agg->append_child(logical_plan::make_node_match(resource,
                                                            core::dbname_t{"otterstax"},
                                                            core::relname_t{"remote_columns"},
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

    struct declared_t {
        std::string name;
        std::pmr::vector<types::complex_logical_type> columns;
    };

    // Runs on an executor thread: a refusal goes back as the hook's error, never as a Catch assertion.
    template<class Write>
    core::error_t bind_write_target(logical_plan::node_t& node,
                                    std::pmr::memory_resource* resource,
                                    const std::vector<declared_t>& declared) {
        const auto& write = static_cast<const Write&>(node);
        const auto name = qualified(write.dbname(), write.schema(), write.relname());
        auto it = std::find_if(declared.begin(), declared.end(), [&](const declared_t& d) { return d.name == name; });
        if (it == declared.end()) {
            return core::error_t::no_error();
        }
        auto relation =
            logical_plan::make_node_extension(resource,
                                              it->name,
                                              it->columns,
                                              &make_remote_source,
                                              logical_plan::extension_payload_ptr{new remote_payload_t{name}});
        if (relation.has_error()) {
            return relation.error();
        }
        return logical_plan::bind_host_write_target(resource, node, relation.value(), &make_remote_write);
    }

    core::error_t replace_names(logical_plan::node_ptr& node,
                                std::pmr::memory_resource* resource,
                                const std::vector<declared_t>& declared) {
        core::error_t bound = core::error_t::no_error();
        switch (node->type()) {
            case logical_plan::node_type::insert_t:
                bound = bind_write_target<logical_plan::node_insert_t>(*node, resource, declared);
                break;
            case logical_plan::node_type::update_t:
                bound = bind_write_target<logical_plan::node_update_t>(*node, resource, declared);
                break;
            case logical_plan::node_type::delete_t:
                bound = bind_write_target<logical_plan::node_delete_t>(*node, resource, declared);
                break;
            default:
                break;
        }
        if (bound.contains_error()) {
            return bound;
        }
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            const auto name = qualified(static_cast<const std::string&>(agg->dbname()),
                                        agg->schema(),
                                        static_cast<const std::string&>(agg->relname()));
            auto it =
                std::find_if(declared.begin(), declared.end(), [&](const declared_t& d) { return d.name == name; });
            if (it != declared.end()) {
                auto ext =
                    logical_plan::make_node_extension(resource,
                                                      it->name,
                                                      it->columns,
                                                      &make_remote_source,
                                                      logical_plan::extension_payload_ptr{new remote_payload_t{name}});
                if (ext.has_error()) {
                    return ext.error();
                }
                ext.value()->set_result_alias(agg->result_alias().empty()
                                                  ? static_cast<const std::string&>(agg->relname())
                                                  : agg->result_alias());
                if (node->children().empty()) {
                    node = ext.value();
                    return core::error_t::no_error();
                }
                auto wrapper = logical_plan::make_node_aggregate(resource, core::dbname_t{}, core::relname_t{});
                wrapper->set_result_alias(node->result_alias());
                wrapper->append_child(ext.value());
                for (auto& child : node->children()) {
                    wrapper->append_child(child);
                }
                node = wrapper;
                return core::error_t::no_error();
            }
        }
        for (auto& child : node->children()) {
            if (auto replaced = replace_names(child, resource, declared); replaced.contains_error()) {
                return replaced;
            }
        }
        return core::error_t::no_error();
    }

    // Phase "decide": a name with declared columns becomes a host node; one without stays and is refused later.
    core::result_wrapper_t<logical_plan::node_ptr>
    decide_remote_nodes(std::pmr::memory_resource* resource,
                        logical_plan::node_ptr tree,
                        std::span<const qualified_name_t> unresolved,
                        std::span<const std::pmr::vector<vector::data_chunk_t>> read_results) {
        counters().decide.fetch_add(1);
        std::vector<declared_t> declared;
        for (std::size_t i = 0; i < unresolved.size(); ++i) {
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
            if (!named) {
                continue;
            }
            std::sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
            declared_t d{qualified(unresolved[i].database.t, unresolved[i].schema.t, unresolved[i].collection.t),
                         std::pmr::vector<types::complex_logical_type>{resource}};
            for (auto& [_, type] : ordered) {
                d.columns.push_back(std::move(type));
            }
            declared.push_back(std::move(d));
        }
        if (auto replaced = replace_names(tree, resource, declared); replaced.contains_error()) {
            return replaced;
        }
        return tree;
    }

    services::engine::primitives_t host_primitives() {
        return services::engine::primitives_t{{}, {&need_remote_columns, &decide_remote_nodes}};
    }

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    void create_host_tables(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(run(dispatcher, "CREATE DATABASE otterstax;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE otterstax.remote_columns (tbl TEXT, col TEXT, type TEXT, ord BIGINT);")
                    ->is_success());
    }

    const char* declare_orders = "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                                 "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'amount', 'BIGINT', 2);";

    std::vector<std::vector<int64_t>> sorted_int_rows(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<std::vector<int64_t>> rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::vector<int64_t> values;
                for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
                    const auto cell = chunk.value(col, row);
                    values.push_back(cell.value<int64_t>());
                }
                rows.push_back(std::move(values));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

} // namespace

#define HOST_TEST_BOILERPLATE(DIR)                                                                                     \
    auto config = test_create_config(integration_fixture_path(DIR));                                                   \
    test_clear_directory(config);                                                                                      \
    backend().clear();                                                                                                 \
    asked_names().clear();                                                                                             \
    seen_writes().clear();                                                                                             \
    write_log().clear();                                                                                               \
    batches().clear();                                                                                                 \
    backend()["m2.shop.orders"] = {{1, 100}, {2, 200}, {3, 300}};                                                      \
    unreachable().clear();                                                                                             \
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
    REQUIRE(sorted_int_rows(found) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});

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

TEST_CASE("integration::cpp::host_names::join_host_node_with_local_table") {
    HOST_TEST_BOILERPLATE("test_host_names/join_local")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE shopdb;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE shopdb.customers (id BIGINT, bonus BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO shopdb.customers (id, bonus) VALUES (1, 7), (3, 9), (5, 11);")->is_success());

    auto joined = run(dispatcher,
                      "SELECT o.id, o.amount, c.bonus FROM m2.shop.orders AS o "
                      "JOIN shopdb.customers AS c ON o.id = c.id;");
    REQUIRE(joined->is_success());
    REQUIRE(sorted_int_rows(joined) == std::vector<std::vector<int64_t>>{{1, 100, 7}, {3, 300, 9}});
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

TEST_CASE("integration::cpp::host_names::count_star_counts_the_host_rows") {
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

TEST_CASE("integration::cpp::host_names::dml_with_an_embedded_query") {
    HOST_TEST_BOILERPLATE("test_host_names/dml_embedded")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());

    SECTION("INSERT ... SELECT copies the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) SELECT id, amount FROM m2.shop.orders;")->is_success());
        auto copied = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(copied->is_success());
        REQUIRE(sorted_int_rows(copied) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});
    }
    SECTION("UPDATE ... FROM reads the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 0), (5, 0);")->is_success());
        auto upd = run(dispatcher, "UPDATE loc.t SET amount = o.amount FROM m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO((upd->is_error() ? std::string{upd->get_error().what} : std::string{"ok"}));
        REQUIRE(upd->is_success());
        auto updated = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(updated) == std::vector<std::vector<int64_t>>{{1, 100}, {5, 0}});
    }
    SECTION("DELETE ... USING reads the host rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (2, 0), (7, 0);")->is_success());
        auto del = run(dispatcher, "DELETE FROM loc.t USING m2.shop.orders AS o WHERE loc.t.id = o.id;");
        INFO((del->is_error() ? std::string{del->get_error().what} : std::string{"ok"}));
        REQUIRE(del->is_success());
        auto left = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(left) == std::vector<std::vector<int64_t>>{{7, 0}});
    }
}

TEST_CASE("integration::cpp::host_names::host_operator_error_reaches_the_cursor") {
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
// depends on nothing it resolved (a host node has no catalog oid).
TEST_CASE("integration::cpp::host_names::a_view_over_a_host_name") {
    HOST_TEST_BOILERPLATE("test_host_names/view_created")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());

    auto created = run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;");
    INFO("error: " << (created->is_error() ? std::string{created->get_error().what} : std::string{}));
    REQUIRE(created->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.ov;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});

    auto oid = run(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'ov';");
    REQUIRE(oid->size() == 1);
    const auto view_oid = std::to_string(oid->chunks().front().get_value<std::uint32_t>(0, 0));
    auto depends = run(dispatcher, "SELECT refclassid FROM pg_catalog.pg_depend WHERE objid = " + view_oid + ";");
    REQUIRE(depends->size() == 1);
    CHECK(depends->chunks().front().get_value<std::uint32_t>(0, 0) ==
          components::catalog::well_known_oid::pg_namespace_table);
}

// Trino 483 checkViewStaleness compares the view's output columns only: a column the host node gained is not read.
TEST_CASE("integration::cpp::host_names::a_host_column_the_view_does_not_read_keeps_it_fresh") {
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
    INFO("error: " << (read->is_error() ? std::string{read->get_error().what} : std::string{}));
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});
}

// The type of an output column is compared exactly: no coercion is inserted for a wider host type.
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

TEST_CASE("integration::cpp::host_names::a_view_whose_host_name_is_gone_is_stale") {
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
TEST_CASE("integration::cpp::host_names::a_matview_over_a_view_over_a_host_name") {
    HOST_TEST_BOILERPLATE("test_host_names/matview_over_view")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.orders;")->is_success());

    auto created = run(dispatcher, "CREATE MATERIALIZED VIEW loc.mv AS SELECT id, amount FROM loc.ov WITH NO DATA;");
    INFO("error: " << (created->is_error() ? std::string{created->get_error().what} : std::string{}));
    REQUIRE(created->is_success());
    auto refreshed = run(dispatcher, "REFRESH MATERIALIZED VIEW loc.mv;");
    INFO("error: " << (refreshed->is_error() ? std::string{refreshed->get_error().what} : std::string{}));
    REQUIRE(refreshed->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.mv;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});
}

// A relation without columns still has rows: count(*) counts them, one batch or several.
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
    CHECK(sorted_int_rows(rows) == std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}});

    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.orders;");
    REQUIRE(counted->is_success());
    CHECK(counted->value(0, 0).value<int64_t>() == 3);
}

// B1: the host is asked about the whole written name, the schema part included.
TEST_CASE("integration::cpp::host_names::a_write_target_keeps_its_schema") {
    HOST_TEST_BOILERPLATE("test_host_names/write_schema")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);");
    CHECK(asked_names() == std::vector<std::string>{"m2.shop.orders"});
}

TEST_CASE("integration::cpp::host_names::insert_into_a_host_relation") {
    HOST_TEST_BOILERPLATE("test_host_names/insert")
    REQUIRE(run(dispatcher, declare_orders)->is_success());

    SECTION("the declared columns, cast to their types, reach the host; the host reports the count") {
        auto inserted =
            run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, CAST(400 AS INTEGER)), (5, 500);");
        INFO((inserted->is_error() ? std::string{inserted->get_error().what} : std::string{"ok"}));
        REQUIRE(inserted->is_success());
        CHECK(inserted->affected_rows() == std::optional<std::uint64_t>{2});
        CHECK(inserted->size() == 0);
        CHECK(write_log() == std::vector<std::string>{"insert m2.shop.orders id:bigint,amount:bigint"});
        auto read = run(dispatcher, "SELECT id, amount FROM m2.shop.orders;");
        REQUIRE(read->is_success());
        CHECK(sorted_int_rows(read) ==
              std::vector<std::vector<int64_t>>{{1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}});
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
        INFO((copied->is_error() ? std::string{copied->get_error().what} : std::string{"ok"}));
        REQUIRE(copied->is_success());
        CHECK(copied->affected_rows() == std::optional<std::uint64_t>{2});
        auto positional = run(dispatcher, "INSERT INTO m2.shop.orders SELECT id + 10, amount FROM loc.t;");
        INFO((positional->is_error() ? std::string{positional->get_error().what} : std::string{"ok"}));
        REQUIRE(positional->is_success());
        CHECK(backend()["m2.shop.orders"].back() == std::vector<int64_t>{19, 900});
        CHECK(backend()["m2.shop.orders"].size() == 7);
    }
    SECTION("a value no assignment cast takes to the declared type is refused before the host sees it") {
        auto refused = run(dispatcher, "INSERT INTO m2.shop.orders (id, amount) VALUES (10, 'ten');");
        REQUIRE(refused->is_error());
        CHECK(write_log().empty());
    }
}

// No defaults for a host relation: every declared column must be written.
TEST_CASE("integration::cpp::host_names::an_insert_into_a_host_relation_lists_every_column") {
    HOST_TEST_BOILERPLATE("test_host_names/insert_every_column")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.orders (id) VALUES (4);",
                            "INSERT INTO m2.shop.orders VALUES (4);",
                            "INSERT INTO m2.shop.orders (id) SELECT id FROM loc.t;",
                            "INSERT INTO m2.shop.orders SELECT id FROM loc.t;"}) {
        INFO(sql);
        auto refused = run(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(std::string{refused->get_error().what} ==
              "INSERT into host relation \"m2.shop.orders\" must list every column; missing: amount");
    }
    CHECK(write_log().empty());
    CHECK(backend()["m2.shop.orders"].size() == 3);

    auto positional = run(dispatcher, "INSERT INTO m2.shop.orders VALUES (7, 700), (8, 800);");
    INFO((positional->is_error() ? std::string{positional->get_error().what} : std::string{"ok"}));
    REQUIRE(positional->is_success());
    CHECK(positional->affected_rows() == std::optional<std::uint64_t>{2});
    CHECK(backend()["m2.shop.orders"].back() == std::vector<int64_t>{8, 800});
}

TEST_CASE("integration::cpp::host_names::update_and_delete_a_host_relation") {
    HOST_TEST_BOILERPLATE("test_host_names/update_delete")
    REQUIRE(run(dispatcher, declare_orders)->is_success());

    auto updated = run(dispatcher, "UPDATE m2.shop.orders SET amount = 250 WHERE id = 2;");
    INFO((updated->is_error() ? std::string{updated->get_error().what} : std::string{"ok"}));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == std::optional<std::uint64_t>{1});
    CHECK(updated->size() == 0);

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 1;");
    INFO((deleted->is_error() ? std::string{deleted->get_error().what} : std::string{"ok"}));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == std::optional<std::uint64_t>{1});

    auto untouched = run(dispatcher, "DELETE FROM m2.shop.orders WHERE id = 99;");
    REQUIRE(untouched->is_success());
    CHECK(untouched->affected_rows() == std::optional<std::uint64_t>{0});

    CHECK(write_log() == std::vector<std::string>{"update m2.shop.orders where column 0 set column 1",
                                                  "delete m2.shop.orders where column 0",
                                                  "delete m2.shop.orders where column 0"});
    CHECK(backend()["m2.shop.orders"] == std::vector<std::vector<int64_t>>{{2, 250}, {3, 300}});
}

TEST_CASE("integration::cpp::host_names::host_write_shapes_not_supported_yet") {
    HOST_TEST_BOILERPLATE("test_host_names/write_shapes")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    const std::pair<const char*, const char*> refused[] = {
        {"UPDATE m2.shop.orders SET amount = t.amount FROM loc.t AS t WHERE m2.shop.orders.id = t.id;",
         "UPDATE of host relation \"m2.shop.orders\" with FROM is not supported"},
        {"DELETE FROM m2.shop.orders USING loc.t AS t WHERE m2.shop.orders.id = t.id;",
         "DELETE from host relation \"m2.shop.orders\" with USING is not supported"},
        {"INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400) RETURNING id;",
         "RETURNING from a write into host relation \"m2.shop.orders\" is not supported"},
        {"UPDATE m2.shop.orders SET amount = 1 WHERE id = 1 RETURNING id;",
         "RETURNING from a write into host relation \"m2.shop.orders\" is not supported"},
        {"DELETE FROM m2.shop.orders WHERE id = 1 RETURNING id;",
         "RETURNING from a write into host relation \"m2.shop.orders\" is not supported"},
    };
    for (const auto& [sql, what] : refused) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(std::string{cursor->get_error().what} == what);
    }
    CHECK(write_log().empty());
    CHECK(backend()["m2.shop.orders"].size() == 3);
}

// Trino 483 default (Connector.isSingleStatementWritesOnly): a write to a host relation runs only in autocommit.
TEST_CASE("integration::cpp::host_names::a_host_write_inside_a_transaction_is_refused") {
    HOST_TEST_BOILERPLATE("test_host_names/write_txn")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    auto session = otterbrix::session_id_t();
    for (const char* sql : {"INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);",
                            "UPDATE m2.shop.orders SET amount = 1 WHERE id = 1;",
                            "DELETE FROM m2.shop.orders WHERE id = 1;"}) {
        INFO(sql);
        REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
        auto refused = run(dispatcher, session, sql);
        REQUIRE(refused->is_error());
        CHECK(std::string{refused->get_error().what} ==
              "writes to host relation \"m2.shop.orders\" are allowed only outside an explicit transaction (#663)");
        REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
    }
    CHECK(write_log().empty());
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);")->is_success());
    CHECK(backend()["m2.shop.orders"].size() == 4);
}

// The backend's refusal (a NOT NULL or CHECK it enforces, an unreachable server) reaches the cursor unchanged.
TEST_CASE("integration::cpp::host_names::a_host_write_error_reaches_the_cursor") {
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

namespace {
    std::vector<std::string> explain_lines(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
        REQUIRE(cursor->is_success());
        std::vector<std::string> lines;
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            const auto cell = cursor->value(0, row);
            lines.emplace_back(cell.value<std::string_view>());
        }
        return lines;
    }
} // namespace

// PostgreSQL 18 postgres_fdw: "Foreign Scan on ..." with "Remote SQL: ..." under it. The host operator says both.
TEST_CASE("integration::cpp::host_names::explain_prints_the_host_operator_label_and_details") {
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
    SECTION("a write into the host relation: the host sink's line, not a scan's") {
        CHECK(explain_lines(dispatcher, "EXPLAIN INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);") ==
              std::vector<std::string>{"Foreign Insert on m2.shop.orders",
                                       "  Remote SQL: INSERT INTO shop.orders VALUES ($1, $2)",
                                       "  Batch Size: 1",
                                       "  ->  Values Scan"});
        CHECK(backend()["m2.shop.orders"].size() == 3);
    }
}

// Pins the fields of a validated INSERT / UPDATE / DELETE that a host's write function reads
// (docs/embedding-host-api.md, "What a write function reads"): changing any of them breaks the host.
TEST_CASE("integration::cpp::host_names::a_write_function_reads_the_validated_statement") {
    HOST_TEST_BOILERPLATE("test_host_names/write_fields")
    REQUIRE(run(dispatcher, declare_orders)->is_success());
    REQUIRE(run(dispatcher, "CREATE DATABASE loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.orders (id, amount) VALUES (4, 400);",
                            "INSERT INTO m2.shop.orders (id, amount) SELECT id, amount FROM loc.t;",
                            "UPDATE m2.shop.orders SET amount = 250 WHERE id = 2;",
                            "UPDATE m2.shop.orders SET amount = 0;",
                            "DELETE FROM m2.shop.orders WHERE id = 1;",
                            "DELETE FROM m2.shop.orders WHERE id = 3 LIMIT 1;",
                            "DELETE FROM m2.shop.orders;"}) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << (cursor->is_error() ? std::string{cursor->get_error().what} : std::string{"ok"}));
        REQUIRE(cursor->is_success());
    }
    CHECK(seen_writes() ==
          std::vector<std::string>{"insert m2|shop|orders from values",
                                   "insert m2|shop|orders from a query",
                                   "update m2|shop|orders where column 0 left = parameter set column 1 left",
                                   "update m2|shop|orders where all rows set column 1 left",
                                   "delete m2|shop|orders where column 0 left = parameter",
                                   "delete m2|shop|orders where column 0 left = parameter limit 1",
                                   "delete m2|shop|orders where all rows"});
    CHECK(backend()["m2.shop.orders"].empty());
}

// Trino 483 lower-cases every connector column name (ColumnMetadata); a host maps its remote names to lower case
// itself, so a declared name otterbrix would never find by an unquoted reference is refused where it is declared.
TEST_CASE("integration::cpp::host_names::a_host_column_name_must_be_lower_case") {
    HOST_TEST_BOILERPLATE("test_host_names/column_case")
    REQUIRE(run(dispatcher,
                "INSERT INTO otterstax.remote_columns (tbl, col, type, ord) VALUES "
                "('m2.shop.orders', 'id', 'BIGINT', 1), ('m2.shop.orders', 'Amount', 'BIGINT', 2);")
                ->is_success());

    for (const char* sql : {"SELECT id FROM m2.shop.orders;", "INSERT INTO m2.shop.orders (id) VALUES (4);"}) {
        INFO(sql);
        auto refused = run(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(std::string{refused->get_error().what} == "host column \"Amount\" must be lower case");
    }
}
