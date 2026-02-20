#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::types;

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
        auto columns = get_types(resource_, *coldefs);

        if (columns.empty()) {
            return logical_plan::make_node_create_collection(resource_, rangevar_to_collection(node.relation));
        }

        std::vector<components::table::column_definition_t> col_defs;
        fill_column_definitions(col_defs, resource_, *coldefs);
        auto constraints = extract_table_constraints(*coldefs);

        // Parse WITH (storage = 'disk') clause
        bool disk_storage = false;
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname) continue;
                std::string opt_name(def->defname);
                if (opt_name == "storage" && def->arg) {
                    std::string val(strVal(def->arg));
                    if (val == "disk") {
                        disk_storage = true;
                    }
                }
            }
        }

        return logical_plan::make_node_create_collection(resource_,
                                                         rangevar_to_collection(node.relation),
                                                         std::move(columns),
                                                         std::move(col_defs),
                                                         std::move(constraints),
                                                         disk_storage);
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
            case OBJECT_TYPE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
                return logical_plan::make_node_drop_type(resource_, strVal(drop_name.back().data));
            }
            case OBJECT_SEQUENCE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_sequence(
                            resource_,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto seq_name = strVal(it->data);
                        return logical_plan::make_node_drop_sequence(resource_, {database, seq_name});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            case OBJECT_VIEW: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_view(
                            resource_,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto view_name = strVal(it->data);
                        return logical_plan::make_node_drop_view(resource_, {database, view_name});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            case OBJECT_FUNCTION: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_macro(
                            resource_,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto macro_name = strVal(it->data);
                        return logical_plan::make_node_drop_macro(resource_, {database, macro_name});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                }
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform
