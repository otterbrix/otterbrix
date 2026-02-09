#include "connection_environment.hpp"
#include <iostream>
#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
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
           // throw PyConnectionException("Connection already closed!");
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
        auto session = session_id_t();
        auto node = raw_parser(query.c_str())->lst.front().data;
        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto plan =
            transformer.transform(sql::transform::pg_cell_to_node_cast(node), nullptr);
        // todo split node and plan; check select statement, use node.type 
        return RelationFactory::CreateFromSelect(plan);
    }

    Result ConnectionEnvironment::ExecuteInternal(const string& query) {
        auto session = session_id_t();
        auto node = raw_parser(query.c_str())->lst.front().data;
        sql::transform::transformer transformer(space->dispatcher()->resource());
        auto plan =
            transformer.transform(sql::transform::pg_cell_to_node_cast(node), nullptr);

        auto plan_type = logical_plan::node_type::create_collection_t;
        auto cursor = space->dispatcher()->execute_plan(session, plan);

        if (cursor->is_success() && plan_type == logical_plan::node_type::create_collection_t) {
            auto full_name = plan->collection_full_name();
            auto& collections = GetCollections();
            collections.insert(full_name.to_string());
        }

        return cursor;

    }

    Result ConnectionEnvironment::Execute(const Relation& rel) {
        auto session = session_id_t();
        auto plan = RelationFactory::Execute(rel);
        auto cursor = space->dispatcher()->execute_plan(session, plan, ExpressionFactory::GetParams());
        return cursor;
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
