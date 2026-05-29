#include "connection_environment.hpp"
#include <iostream>
#include <functional>
#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/planner/optimizer.hpp>
using namespace components;

namespace otterbrix {

    shared_ptr<PythonImportCache> ConnectionEnvironment::import_cache = nullptr;
    shared_ptr<case_insensitive_set_t> ConnectionEnvironment::collections = nullptr; 

    boost::intrusive_ptr<otterbrix_t> ConnectionEnvironment::MakeSpace(
        const std::filesystem::path& path) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directory(path);
        return make_otterbrix(
            configuration::config::create_config(path)
            );
    }

    ConnectionEnvironment::ConnectionEnvironment() 
        : ConnectionEnvironment(MakeSpace()) {    
    }

    ConnectionEnvironment::ConnectionEnvironment(const boost::intrusive_ptr<otterbrix_t>& space)
        : ExpressionFactory(space), RelationFactory(space), space(space) {
            auto session = otterbrix::session_id_t();
            space->dispatcher()->create_database(session, "tmp");
        }

    ConnectionEnvironment::~ConnectionEnvironment() = default;

    void ConnectionEnvironment::Cleanup() {
        import_cache.reset();
    }

    void ConnectionEnvironment::ThrowConnectionException() {
            throw std::runtime_error("Connection already closed!");
    }

    void ConnectionEnvironment::SetNullConnection() {
        space = nullptr;
        ExpressionFactory::SetNullSpace();
    }

    void ConnectionEnvironment::CreateDatabase(const std::string& name) {
        auto session = session_id_t();
        space->dispatcher()->create_database(session, name);
    }

    shared_ptr<Relation> ConnectionEnvironment::RelationFromQuery(const string& query) {
        using namespace components::sql::transform;
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (result.has_error()) {
            throw std::runtime_error(result.error().what.c_str());
        }
        auto view = std::move(result.value());
        return RelationFactory::CreateFromSelect(std::move(view.node));
    }

    Result ConnectionEnvironment::ExecuteInternal(const string& query) {
        using namespace components::sql::transform;

        auto session = session_id_t();
        std::pmr::monotonic_buffer_resource parser_arena(space->dispatcher()->resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));

        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(parse_result)).finalize();

        if (result.has_error()) {
            return components::cursor::make_cursor(space->dispatcher()->resource(), result.error());
        }
        auto view = std::move(result.value());
        auto plan = std::move(view.node);
        auto cursor = space->dispatcher()->execute_plan(session, plan, std::move(view.params));

        if (cursor->is_success()) {
            // A CREATE TABLE plan is no longer a bare create_collection node: the
            // namespace-routing transform wraps it in a sequence alongside a
            // catalog_resolve_namespace node carrying the target database name.
            // Walk the plan to find the created collection and its database so
            // listTables() reports the table under the database it was created in
            // (falling back to "tmp" for unqualified creates).
            const logical_plan::node_create_collection_t* created = nullptr;
            std::string dbname;
            std::function<void(const logical_plan::node_ptr&)> scan =
                [&](const logical_plan::node_ptr& n) {
                    if (!n) {
                        return;
                    }
                    if (n->type() == logical_plan::node_type::create_collection_t) {
                        created = static_cast<const logical_plan::node_create_collection_t*>(n.get());
                    } else if (n->type() == logical_plan::node_type::catalog_resolve_namespace_t) {
                        dbname = static_cast<const logical_plan::node_catalog_resolve_namespace_t*>(n.get())->dbname();
                    }
                    for (const auto& child : n->children()) {
                        scan(child);
                    }
                };
            scan(plan);
            if (created) {
                if (dbname.empty()) {
                    dbname = "tmp";
                }
                auto& collections = GetCollections();
                collections.insert(dbname + "." + created->relname());
            }
        }

        return cursor;
    }

    Result ConnectionEnvironment::Execute(const Relation& rel, bool optimize) {
        auto session = session_id_t();
        auto node = RelationFactory::Execute(rel);
        if (optimize) {
            node = components::planner::optimize(node->resource(), node, nullptr);
        }
        return space->dispatcher()->execute_plan(session, node, ExpressionFactory::GetParams());
    }

    cursor::cursor_t_ptr ConnectionEnvironment::QueryRelation(const components::logical_plan::node_ptr &rel) {
        auto session = otterbrix::session_id_t();
        return space->dispatcher()->execute_plan(session, rel, ExpressionFactory::GetParams());
    }

    bool ConnectionEnvironment::IsJupyter() {
        return false;
    }


    PythonImportCache& ConnectionEnvironment::ImportCache() {
        if (!import_cache) {
            import_cache = make_shared<PythonImportCache>();
        }
        return *(import_cache.get());
    }

    case_insensitive_set_t& ConnectionEnvironment::GetCollections() {
        if (!collections) {
            collections = make_shared<case_insensitive_set_t>();
        }
        return *(collections.get());
    }

    
} // namespace otterbrix
