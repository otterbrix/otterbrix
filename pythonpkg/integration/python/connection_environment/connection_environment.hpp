#pragma once

#include "expression/expression_factory.hpp"
#include "relation/relation_factory.hpp"
#include "module_cheker.hpp"
#include "import_cache/python_import_cache.hpp"

#include <core/types/string.hpp>
#include <components/cursor/cursor.hpp>


#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <core/string_util/case_insensitive.hpp>

#include <integration/cpp/otterbrix.hpp>


#include <filesystem>
#include <string_view>

namespace otterbrix {
    using Result = components::cursor::cursor_t_ptr;


    class ConnectionEnvironment : 
        public ExpressionFactory, public RelationFactory {
    public:
        static constexpr std::string_view DEFAULT_FOLDER = "default";
        static boost::intrusive_ptr<otterbrix_t> MakeSpace(const std::filesystem::path& 
            path = std::filesystem::current_path() / DEFAULT_FOLDER);
        // create default space
        ConnectionEnvironment();
        ConnectionEnvironment(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~ConnectionEnvironment();
    public:
        static void Cleanup();
        static void ThrowConnectionException();
        void SetNullConnection();

        void CreateDatabase(const string& name);
        shared_ptr<Relation> RelationFromQuery(const string& query);
        Result ExecuteInternal(const string& query);
        Result Execute(const Relation& rel);

        components::cursor::cursor_t_ptr QueryRelation(const components::logical_plan::node_ptr &rel);
    public:
        static bool IsJupyter();

        static PythonImportCache& ImportCache();
        static case_insensitive_set_t& GetCollections();
        static case_insensitive_set_t& GetTables();
    
    private:
        boost::intrusive_ptr<otterbrix_t> space;   


    private:
        static shared_ptr<PythonImportCache> import_cache;
        static shared_ptr<case_insensitive_set_t> collections;

    };
} // namespace otterbrix
