#pragma once

// The connector of the connector tests. Its backend refuses DDL of a name starting with "bad" and DROP of one starting
// with "nodrop".

#include <catch2/catch_test_macros.hpp>

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_raw_data.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/planner/connector.hpp>
#include <components/planner/host_hooks.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/collection/context_storage.hpp>

#include <algorithm>
#include <atomic>
#include <map>
#include <string>
#include <vector>

namespace test_connector {

    using rows_t = std::vector<std::vector<int64_t>>;

    // A journal line, numbered across every connector: the connectors are one per executor, and a test reads the
    // lines in the order they were written.
    struct line_t {
        std::uint64_t number;
        std::string text;
    };

    inline std::uint64_t next_number() {
        static std::atomic<std::uint64_t> number{0};
        return number.fetch_add(1);
    }

    // What the test backend holds, by connection.database.schema.table.
    inline std::map<std::string, rows_t>& backend_rows() {
        static std::map<std::string, rows_t> rows;
        return rows;
    }

    // The host's parameters of a table, by its oid: the backend table it reads (connection.database.schema.table).
    inline std::map<components::catalog::oid_t, std::string>& parameters() {
        static std::map<components::catalog::oid_t, std::string> by_oid;
        return by_oid;
    }

    // connection.database.schema[.table]: a schema has no table.
    inline std::string spelled(const components::planner::remote_name_t& name) {
        std::string written =
            std::string{name.connection} + "." + std::string{name.database} + "." + std::string{name.schema};
        if (!name.table.empty()) {
            written += "." + std::string{name.table};
        }
        return written;
    }

    // The rows of the host's table whose column 0 is `oid`, with their row ids, for its DELETE.
    class oid_filter_t final : public components::operators::read_only_operator_t {
    public:
        oid_filter_t(std::pmr::memory_resource* resource, log_t log, components::catalog::oid_t oid)
            : components::operators::read_only_operator_t(resource,
                                                          std::move(log),
                                                          components::operators::operator_type::extension)
            , oid_(oid) {}

        [[nodiscard]] components::operators::pipeline_role role() const noexcept override {
            return components::operators::pipeline_role::streaming;
        }

        [[nodiscard]] core::error_t push(components::pipeline::context_t*,
                                         components::vector::data_chunk_t&& input,
                                         components::operators::chunks_vector_t& out) override {
            components::vector::indexing_vector_t kept(resource());
            kept.reset(std::max<std::uint64_t>(input.size(), 1));
            std::uint64_t count = 0;
            for (std::uint64_t row = 0; row < input.size(); ++row) {
                if (!input.is_null(0, row) &&
                    input.get_value<std::int64_t>(0, row) == static_cast<std::int64_t>(oid_)) {
                    kept.set_index(count++, row);
                }
            }
            if (count != 0) {
                components::vector::data_chunk_t chunk(resource(), input.types(), count);
                input.copy(chunk, kept, count);
                out.emplace_back(std::move(chunk));
            }
            return core::error_t::no_error();
        }

    private:
        components::catalog::oid_t oid_;
    };

    // otterbrix's INSERT of (oid, name) into the host's table `table`.
    inline components::operators::operator_ptr write_parameters(const services::context_storage_t& context,
                                                                components::catalog::oid_t table,
                                                                components::catalog::oid_t oid,
                                                                const std::string& name) {
        using components::types::complex_logical_type;
        using components::types::logical_type;
        std::pmr::vector<complex_logical_type> types{context.resource};
        types.emplace_back(logical_type::BIGINT);
        types.emplace_back(logical_type::STRING_LITERAL);
        components::vector::data_chunk_t row(context.resource, types, 1);
        row.set_value(0, 0, static_cast<std::int64_t>(oid));
        row.set_value(1, 0, std::string_view{name});
        row.set_cardinality(1);
        auto insert = boost::intrusive_ptr(new components::operators::operator_insert(
            context.resource,
            context.log.clone(),
            table,
            std::pmr::vector<components::operators::projected_column_t>{context.resource}));
        components::logical_plan::insert_column_bindings_t bindings(context.resource);
        bindings.push_back({0, std::pmr::string{"oid", context.resource}, types[0], {}});
        bindings.push_back({1, std::pmr::string{"name", context.resource}, types[1], {}});
        insert->set_column_bindings(std::move(bindings));
        insert->set_table_has_indexes(false);
        insert->set_children(boost::intrusive_ptr(new components::operators::operator_raw_data_t(std::move(row))));
        return insert;
    }

    // otterbrix's DELETE of the rows of `oid` from the host's table `table`.
    inline components::operators::operator_ptr erase_parameters(const services::context_storage_t& context,
                                                                components::catalog::oid_t table,
                                                                components::catalog::oid_t oid) {
        auto erase = boost::intrusive_ptr(new components::operators::operator_delete(
            context.resource,
            context.log.clone(),
            table,
            std::pmr::vector<components::operators::projected_column_t>{context.resource}));
        erase->set_table_has_indexes(false);
        auto filter = boost::intrusive_ptr(new oid_filter_t(context.resource, context.log.clone(), oid));
        filter->set_children(boost::intrusive_ptr(
            new components::operators::full_scan(context.resource,
                                                 context.log.clone(),
                                                 table,
                                                 nullptr,
                                                 components::logical_plan::row_cap_t::uncapped())));
        erase->set_children(std::move(filter));
        return erase;
    }

    // A storage over backend_rows()[name]: a scan only.
    class read_storage_t final : public components::logical_plan::table_storage_t {
    public:
        read_storage_t(std::pmr::memory_resource*,
                       std::string name,
                       std::vector<components::types::complex_logical_type> columns)
            : components::logical_plan::table_storage_t(&tag())
            , name_(std::move(name))
            , columns_(std::move(columns)) {}

        static const int& tag() {
            static const int owner = 0;
            return owner;
        }

    private:
        class source_t final : public components::operators::read_only_operator_t {
        public:
            source_t(std::pmr::memory_resource* resource,
                     log_t log,
                     std::pmr::vector<components::types::complex_logical_type> columns,
                     rows_t rows)
                : components::operators::read_only_operator_t(resource,
                                                              std::move(log),
                                                              components::operators::operator_type::extension)
                , columns_(std::move(columns))
                , rows_(std::move(rows)) {}

            [[nodiscard]] components::operators::pipeline_role role() const noexcept override {
                return components::operators::pipeline_role::source;
            }

            [[nodiscard]] actor_zeta::unique_future<
                core::result_wrapper_t<std::optional<components::vector::data_chunk_t>>>
            source_next(components::pipeline::context_t*) override {
                actor_zeta::promise<core::result_wrapper_t<std::optional<components::vector::data_chunk_t>>> promise(
                    resource());
                auto future = promise.get_future();
                if (done_) {
                    promise.set_value(
                        core::result_wrapper_t<std::optional<components::vector::data_chunk_t>>{std::nullopt});
                    return future;
                }
                done_ = true;
                components::vector::data_chunk_t chunk(resource(), columns_, std::max<std::size_t>(rows_.size(), 1));
                chunk.set_cardinality(rows_.size());
                for (std::size_t row = 0; row < rows_.size(); ++row) {
                    chunk.row_ids.data<int64_t>()[row] = static_cast<int64_t>(row);
                    for (std::size_t col = 0; col < columns_.size(); ++col) {
                        if (columns_[col].type() == components::types::logical_type::STRING_LITERAL) {
                            chunk.set_value(col, row, std::string_view{"s" + std::to_string(rows_[row][col])});
                        } else {
                            chunk.set_value(col, row, rows_[row][col]);
                        }
                    }
                }
                promise.set_value(
                    core::result_wrapper_t<std::optional<components::vector::data_chunk_t>>{std::move(chunk)});
                return future;
            }

            void reset_pipeline_state() noexcept override { done_ = false; }

        private:
            std::pmr::vector<components::types::complex_logical_type> columns_;
            rows_t rows_;
            bool done_{false};
        };

        components::logical_plan::storage_operator_t
        make_scan_impl(const services::context_storage_t& context) override {
            std::pmr::vector<components::types::complex_logical_type> columns{columns_.begin(),
                                                                              columns_.end(),
                                                                              context.resource};
            return components::operators::operator_ptr{
                new source_t(context.resource, context.log.clone(), std::move(columns), backend_rows()[name_])};
        }
        components::logical_plan::storage_operator_t
        make_insert_impl(const services::context_storage_t& context) override {
            return refused(context);
        }
        components::logical_plan::storage_operator_t
        make_update_impl(const services::context_storage_t& context) override {
            return refused(context);
        }
        components::logical_plan::storage_operator_t
        make_delete_impl(const services::context_storage_t& context) override {
            return refused(context);
        }
        uint64_t flush_row_threshold_impl() const noexcept override { return 0; }

        core::error_t refused(const services::context_storage_t& context) const {
            return core::error_t{core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"the test backend only reads", context.resource}};
        }

        std::string name_;
        std::vector<components::types::complex_logical_type> columns_;
    };

    // How a test host makes the storage of a connection's table; the default reads backend_rows().
    using storage_maker_fn = core::result_wrapper_t<components::logical_plan::table_storage_ptr> (*)(
        std::pmr::memory_resource*,
        const components::planner::remote_table_t&,
        const components::planner::statement_ctx_t&);

    inline core::result_wrapper_t<components::logical_plan::table_storage_ptr>
    make_read_storage(std::pmr::memory_resource* resource,
                      const components::planner::remote_table_t& table,
                      const components::planner::statement_ctx_t&) {
        const auto found = parameters().find(table.oid);
        if (found == parameters().end()) {
            return core::error_t{
                core::error_code_t::table_not_exists,
                std::pmr::string{"the test host has no parameters for table oid " + std::to_string(table.oid),
                                 resource}};
        }
        std::vector<components::types::complex_logical_type> columns;
        for (const auto& column : table.columns) {
            columns.push_back(column.type);
        }
        return components::logical_plan::table_storage_ptr{
            core::pmr::make_polymorphic_unique<read_storage_t>(resource, found->second, std::move(columns))};
    }

    struct storage_call_t {
        std::uint64_t session;
        std::uint64_t transaction_id;
        bool explicit_transaction;
        components::catalog::oid_t table_oid;
        std::string table;
        std::vector<std::string> columns;
        std::vector<components::catalog::oid_t> attoids;
        std::uint64_t number{next_number()};
    };

    // What a DDL operator was given: the table's oid and its columns' attoids (for ALTER, the changed column's).
    struct ddl_call_t {
        std::string kind;
        components::catalog::oid_t table_oid;
        std::vector<components::catalog::oid_t> attoids;
        std::uint64_t number{next_number()};
    };

    // The test backend's DDL: refused for a name starting with "bad"; otherwise a journal line, and the parameters of
    // the table written (CREATE) or taken away (DROP).
    class ddl_operator_t final : public components::operators::read_write_operator_t {
    public:
        enum class parameters_t : std::uint8_t
        {
            keep,
            write,
            erase
        };

        ddl_operator_t(std::pmr::memory_resource* resource,
                       log_t log,
                       std::vector<line_t>* journal,
                       std::string table,
                       std::string line,
                       components::catalog::oid_t table_oid,
                       std::string backend_name,
                       parameters_t parameters)
            : components::operators::read_write_operator_t(resource,
                                                           std::move(log),
                                                           components::operators::operator_type::extension)
            , journal_(journal)
            , table_(std::move(table))
            , line_(std::move(line))
            , table_oid_(table_oid)
            , backend_name_(std::move(backend_name))
            , parameters_(parameters) {}

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(components::pipeline::context_t*) override {
            if (table_.starts_with("bad") || (parameters_ == parameters_t::erase && table_.starts_with("nodrop"))) {
                set_error(core::error_t{core::error_code_t::connection_closed,
                                        std::pmr::string{"backend refused \"" + table_ + "\"", resource()}});
                co_return;
            }
            journal_->push_back({next_number(), line_});
            if (parameters_ == parameters_t::write) {
                parameters()[table_oid_] = backend_name_;
            } else if (parameters_ == parameters_t::erase) {
                parameters().erase(table_oid_);
            }
            co_return;
        }

    private:
        std::vector<line_t>* journal_;
        std::string table_;
        std::string line_;
        components::catalog::oid_t table_oid_;
        std::string backend_name_;
        parameters_t parameters_;
    };

    class connector_t final : public components::planner::connector_t {
    public:
        // `parameters_table`: the host's ordinary table its DDL operators write, read when a statement is planned.
        connector_t(std::pmr::memory_resource*,
                    storage_maker_fn maker,
                    const components::catalog::oid_t* parameters_table)
            : maker_(maker)
            , parameters_table_(parameters_table) {}

        std::vector<line_t> journal;
        std::vector<storage_call_t> storage_calls;
        std::vector<ddl_call_t> ddl_calls;

    private:
        core::result_wrapper_t<components::logical_plan::table_storage_ptr>
        storage_for_impl(std::pmr::memory_resource* resource,
                         const components::planner::remote_table_t& table,
                         const components::planner::statement_ctx_t& context) override {
            storage_call_t call{context.session.data(),
                                context.transaction_id,
                                context.explicit_transaction,
                                table.oid,
                                spelled(table.name),
                                {},
                                {}};
            for (const auto& column : table.columns) {
                call.columns.emplace_back(column.name);
                call.attoids.push_back(column.attoid);
            }
            storage_calls.push_back(std::move(call));
            journal.push_back({next_number(), "storage_for " + spelled(table.name)});
            return maker_(resource, table, context);
        }

        // One DDL of the backend on `name` (`oid`): its journal `line`, and the parameters of `backend_name`.
        components::operators::operator_ptr ddl(const services::context_storage_t& context,
                                                std::string_view name,
                                                components::catalog::oid_t oid,
                                                std::string line,
                                                std::string backend_name,
                                                ddl_operator_t::parameters_t parameters) {
            return components::operators::operator_ptr{new ddl_operator_t(context.resource,
                                                                          context.log.clone(),
                                                                          &journal,
                                                                          std::string{name},
                                                                          std::move(line),
                                                                          oid,
                                                                          std::move(backend_name),
                                                                          parameters)};
        }

        // With the host's table: otterbrix's INSERT / DELETE of the host's row; for a name the backend refuses ("bad",
        // DROP of "nodrop") the refusal comes after that write, which the statement takes back.
        bool in_table() const { return *parameters_table_ != components::catalog::INVALID_OID; }

        components::logical_plan::storage_operator_t in_table(const services::context_storage_t& context,
                                                              std::string_view name,
                                                              components::catalog::oid_t oid,
                                                              const std::string& spelled_name,
                                                              bool create) {
            auto own = create ? write_parameters(context, *parameters_table_, oid, spelled_name)
                              : erase_parameters(context, *parameters_table_, oid);
            if (!name.starts_with("bad") && (create || !name.starts_with("nodrop"))) {
                return own;
            }
            auto steps = boost::intrusive_ptr(
                new components::operators::operator_sequence_t(context.resource, context.log.clone()));
            steps->append_step(std::move(own));
            steps->append_step(ddl(context,
                                   name,
                                   oid,
                                   {},
                                   {},
                                   create ? ddl_operator_t::parameters_t::keep : ddl_operator_t::parameters_t::erase));
            return steps;
        }

        components::logical_plan::storage_operator_t
        make_create_table_impl(const services::context_storage_t& context,
                               const components::planner::remote_table_t& table) override {
            std::string line = "create " + spelled(table.name);
            ddl_call_t call{"create", table.oid, {}};
            for (const auto& column : table.columns) {
                line += " " + std::string{column.name} + (column.not_null ? ":not_null" : "");
                call.attoids.push_back(column.attoid);
            }
            ddl_calls.push_back(std::move(call));
            if (in_table()) {
                return in_table(context, table.name.table, table.oid, spelled(table.name), true);
            }
            return ddl(context,
                       table.name.table,
                       table.oid,
                       std::move(line),
                       spelled(table.name),
                       ddl_operator_t::parameters_t::write);
        }

        components::logical_plan::storage_operator_t
        make_drop_table_impl(const services::context_storage_t& context,
                             const components::planner::remote_table_t& table) override {
            ddl_call_t call{"drop", table.oid, {}};
            for (const auto& column : table.columns) {
                call.attoids.push_back(column.attoid);
            }
            ddl_calls.push_back(std::move(call));
            if (in_table()) {
                return in_table(context, table.name.table, table.oid, spelled(table.name), false);
            }
            return ddl(context,
                       table.name.table,
                       table.oid,
                       "drop " + spelled(table.name),
                       spelled(table.name),
                       ddl_operator_t::parameters_t::erase);
        }

        components::logical_plan::storage_operator_t
        make_create_schema_impl(const services::context_storage_t& context,
                                const components::planner::remote_schema_t& schema) override {
            ddl_calls.push_back({"create schema", schema.oid, {}});
            if (in_table()) {
                return in_table(context, schema.name.schema, schema.oid, spelled(schema.name), true);
            }
            return ddl(context,
                       schema.name.schema,
                       schema.oid,
                       "create schema " + spelled(schema.name),
                       {},
                       ddl_operator_t::parameters_t::keep);
        }

        components::logical_plan::storage_operator_t
        make_drop_schema_impl(const services::context_storage_t& context,
                              const components::planner::remote_schema_t& schema) override {
            ddl_calls.push_back({"drop schema", schema.oid, {}});
            if (in_table()) {
                return in_table(context, schema.name.schema, schema.oid, spelled(schema.name), false);
            }
            return ddl(context,
                       schema.name.schema,
                       schema.oid,
                       "drop schema " + spelled(schema.name),
                       {},
                       ddl_operator_t::parameters_t::keep);
        }

        components::logical_plan::storage_operator_t
        make_alter_table_impl(const services::context_storage_t& context,
                              const components::planner::remote_table_t& table,
                              const components::planner::remote_alter_t& alter) override {
            const bool add = alter.kind == components::planner::remote_alter_t::kind_t::add_column;
            ddl_calls.push_back({add ? "add" : "drop column", table.oid, {alter.column.attoid}});
            return ddl(context,
                       table.name.table,
                       table.oid,
                       std::string{add ? "alter add " : "alter drop "} + spelled(table.name) + " " +
                           std::string{alter.column.name},
                       spelled(table.name),
                       ddl_operator_t::parameters_t::keep);
        }

        storage_maker_fn maker_;
        const components::catalog::oid_t* parameters_table_;
    };

    // One per test: it keeps every connector it made, one per executor, so a test reads their journals.
    class factory_t final : public components::planner::connector_factory_t {
    public:
        explicit factory_t(storage_maker_fn maker = &make_read_storage)
            : maker_(maker) {}

        mutable std::vector<connector_t*> made;
        bool refuse{false};
        // The host's own table (oid BIGINT, name TEXT) its DDL operators write; INVALID_OID: none.
        components::catalog::oid_t parameters_table{components::catalog::INVALID_OID};

        // Every executor's entries of `field`, in the order they were written.
        template<typename Entry>
        std::vector<Entry> written(std::vector<Entry> connector_t::* field) const {
            std::vector<Entry> all;
            for (const auto* connector : made) {
                all.insert(all.end(), (connector->*field).begin(), (connector->*field).end());
            }
            std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) { return a.number < b.number; });
            return all;
        }

        std::vector<std::string> journal() const {
            std::vector<std::string> lines;
            for (auto& line : written(&connector_t::journal)) {
                lines.push_back(std::move(line.text));
            }
            return lines;
        }
        std::vector<storage_call_t> storage_calls() const { return written(&connector_t::storage_calls); }
        std::vector<ddl_call_t> ddl_calls() const { return written(&connector_t::ddl_calls); }

        void clear() const {
            for (auto* connector : made) {
                connector->journal.clear();
                connector->storage_calls.clear();
                connector->ddl_calls.clear();
            }
        }

    private:
        core::result_wrapper_t<components::planner::connector_ptr>
        make_impl(std::pmr::memory_resource* resource) const override {
            if (refuse) {
                return core::error_t{core::error_code_t::connection_closed,
                                     std::pmr::string{"the test backend cannot be reached", resource}};
            }
            auto connector = core::pmr::make_polymorphic_unique<connector_t>(resource, maker_, &parameters_table);
            made.push_back(connector.get());
            return components::planner::connector_ptr{std::move(connector)};
        }

        storage_maker_fn maker_;
    };

} // namespace test_connector
