#pragma once

#include "expressions/key.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/types/types.hpp>
#include <string>

namespace components::sql::transform {
    template<class T>
    static T& pg_cast(Node& node) {
        return reinterpret_cast<T&>(node);
    }

    template<class T>
    static T* pg_ptr_cast(void* ptr) {
        return reinterpret_cast<T*>(ptr);
    }

    template<class T>
    static T* pg_ptr_assert_cast(void* ptr, NodeTag tag) {
        assert(nodeTag(ptr));
        return pg_ptr_cast<T>(ptr);
    }

    static Node& pg_cell_to_node_cast(void* node) { return pg_cast<Node&>(*reinterpret_cast<Node*>(node)); }

    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator*/);

    inline std::string construct(const char* ptr) { return ptr ? ptr : std::string(); }

    inline std::string construct_alias(Alias* alias) { return alias ? construct(alias->aliasname) : std::string(); }

    static collection_full_name_t rangevar_to_collection(RangeVar* table) {
        return {construct(table->uid),
                construct(table->catalogname),
                construct(table->schemaname),
                construct(table->relname)};
    }

    struct name_collection_t {
        collection_full_name_t left_name;
        std::string left_alias;
        collection_full_name_t right_name;
        std::string right_alias;
    };

    expressions::side_t deduce_side(const name_collection_t& names, const std::string& target_name);

    struct column_ref_t {
        std::string table;
        expressions::key_t field;

        void deduce_side(const name_collection_t& names);
    };

    column_ref_t columnref_to_fied(ColumnRef* ref);

    static logical_plan::join_type jointype_to_ql(JoinExpr* join) {
        switch (join->jointype) {
            case JOIN_FULL:
                return logical_plan::join_type::full;
            case JOIN_INNER:
                return join->quals ? logical_plan::join_type::inner : logical_plan::join_type::cross;
            case JOIN_LEFT:
                return logical_plan::join_type::left;
            case JOIN_RIGHT:
                return logical_plan::join_type::right;
            default:
                throw parser_exception_t{"unsupported join type", ""};
        }
    }

    static expressions::compare_type get_compare_type(std::string_view str) {
        static const std::unordered_map<std::string_view, expressions::compare_type> lookup = {
            {"==", expressions::compare_type::eq},
            {"=", expressions::compare_type::eq},
            {"!=", expressions::compare_type::ne},
            {"<>", expressions::compare_type::ne},
            {"<", expressions::compare_type::lt},
            {"<=", expressions::compare_type::lte},
            {">", expressions::compare_type::gt},
            {">=", expressions::compare_type::gte},
            {"regexp", expressions::compare_type::regex},
            {"~~", expressions::compare_type::regex}};

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        throw parser_exception_t{"Unknown comparison operator: " + std::string(str), ""};
    }

    static expressions::aggregate_type get_aggregate_type(std::string_view str) {
        static const std::unordered_map<std::string_view, expressions::aggregate_type> lookup = {
            {"count", expressions::aggregate_type::count},
            {"sum", expressions::aggregate_type::sum},
            {"min", expressions::aggregate_type::min},
            {"max", expressions::aggregate_type::max},
            {"avg", expressions::aggregate_type::avg},
        };

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        return expressions::aggregate_type::udf;
    }

    static types::logical_type get_logical_type(std::string_view str) {
        static const std::unordered_map<std::string_view, types::logical_type> lookup = {
            // postgres built-ins
            {"int2", types::logical_type::SMALLINT},
            {"int4", types::logical_type::INTEGER},
            {"int8_t", types::logical_type::BIGINT},
            {"bool", types::logical_type::BOOLEAN},
            {"float4", types::logical_type::FLOAT},
            {"float8", types::logical_type::DOUBLE},
            {"bit", types::logical_type::BIT},
            {"numeric", types::logical_type::DECIMAL},

            {"double", types::logical_type::DOUBLE},
            {"tinyint", types::logical_type::TINYINT},
            {"hugeint", types::logical_type::HUGEINT},
            {"timestamp_sec", types::logical_type::TIMESTAMP_SEC},
            {"timestamp_ms", types::logical_type::TIMESTAMP_MS},
            {"timestamp_us", types::logical_type::TIMESTAMP_US},
            {"timestamp_ns", types::logical_type::TIMESTAMP_NS},
            {"blob", types::logical_type::BLOB},
            {"utinyint", types::logical_type::UTINYINT},
            {"usmallint", types::logical_type::USMALLINT},
            {"uinteger", types::logical_type::UINTEGER},
            {"uint", types::logical_type::UINTEGER},
            {"ubigint", types::logical_type::UBIGINT},
            {"uhugeint", types::logical_type::UHUGEINT},
            {"pointer", types::logical_type::POINTER},
            {"uuid", types::logical_type::UUID},
            {"string", types::logical_type::STRING_LITERAL},
        };

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        return types::logical_type::UNKNOWN;
    }

    std::string node_tag_to_string(NodeTag type);
    std::string expr_kind_to_string(A_Expr_Kind type);

    types::complex_logical_type get_type(TypeName* type);
    std::vector<types::complex_logical_type> get_types(PGList& list);
    std::pmr::vector<types::complex_logical_type> get_types(std::pmr::memory_resource* resource, PGList& list);

} // namespace components::sql::transform