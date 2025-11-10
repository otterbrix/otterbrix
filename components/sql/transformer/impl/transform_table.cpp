#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::types;

namespace {
    using namespace components::sql::transform;

    complex_logical_type get_type(TypeName* type) {
        complex_logical_type column;
        if (auto linint_name = strVal(linitial(type->names)); !std::strcmp(linint_name, "pg_catalog")) {
            if (auto col = get_logical_type(strVal(lsecond(type->names))); col != logical_type::DECIMAL) {
                column = col;
            } else {
                if (list_length(type->typmods) != 2) {
                    throw parser_exception_t{"Incorrect modifiers for DECIMAL, width and scale required", ""};
                }

                auto width = pg_ptr_assert_cast<A_Const>(linitial(type->typmods), T_A_Const);
                auto scale = pg_ptr_assert_cast<A_Const>(lsecond(type->typmods), T_A_Const);

                if (width->val.type != scale->val.type || width->val.type != T_Integer) {
                    throw parser_exception_t{"Incorrect width or scale for DECIMAL, must be integer", ""};
                }
                column = complex_logical_type::create_decimal(static_cast<uint8_t>(intVal(&width->val)),
                                                              static_cast<uint8_t>(intVal(&scale->val)));
            }
        } else {
            column = get_logical_type(linint_name);
        }

        if (list_length(type->arrayBounds)) {
            auto size = pg_ptr_assert_cast<Value>(linitial(type->arrayBounds), T_Value);
            assert(size->type == T_Integer);
            column = complex_logical_type::create_array(column, intVal(size));
        }
        return column;
    }
} // namespace

namespace components::sql::transform {
    // It is guaranteed to be a table ref, but in form of a list of strings
    enum table_name
    {
        table = 1,
        database_table = 2,
        database_schema_table = 3,
        uuid_database_schema_table = 4
    };

    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        std::pmr::vector<complex_logical_type> columns(resource_);
        columns.reserve(list_length(coldefs));
        for (auto data : coldefs->lst) {
            auto coldef = pg_ptr_assert_cast<ColumnDef>(data.data, T_ColumnDef);
            complex_logical_type col = get_type(coldef->typeName);

            if (col.type() == logical_type::NA) {
                throw parser_exception_t{"Unknown type for column: " + std::string(coldef->colname), ""};
            }

            col.set_alias(coldef->colname);
            columns.push_back(std::move(col));
        }

        if (columns.empty()) {
            return logical_plan::make_node_create_collection(resource_, rangevar_to_collection(node.relation));
        }

        return logical_plan::make_node_create_collection(resource_,
                                                         rangevar_to_collection(node.relation),
                                                         std::move(columns));
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_collection(
                            resource_,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {database, collection});
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {database, schema, collection});
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource_, {uuid, database, schema, collection});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_collection(resource_, {});
                }
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }

                //when casting to enum -1 is used to account for obligated index name
                switch (static_cast<table_name>(drop_name.size() - 1)) {
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_, {database, collection}, name);
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_, {database, schema, collection}, name);
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource_,
                                                                  {uuid, database, schema, collection},
                                                                  name);
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_index(resource_, {}, "");
                }
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform
