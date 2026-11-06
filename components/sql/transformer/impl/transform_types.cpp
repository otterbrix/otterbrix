#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/types/user_type_walk.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace components::sql::transform {

    namespace {
        // Every UDT the struct fields reference; the new type's own name is registered in its schema by the
        // executor, which completes the target with the session's place.
        std::vector<qualified_name_t> field_type_names_of(const types::complex_logical_type& type) {
            std::vector<qualified_name_t> names;
            if (type.type() == types::logical_type::STRUCT) {
                for (const auto& field : type.child_types()) {
                    types::walk_user_type_refs(field, [&](const types::complex_logical_type& ref) {
                        auto name = type_reference_of(ref);
                        if (std::find(names.begin(), names.end(), name) == names.end()) {
                            names.push_back(std::move(name));
                        }
                    });
                }
            }
            return names;
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_type(CompositeTypeStmt& node) {
        VALUE_OR_RETURN(auto fields, get_types(resource_, *node.coldeflist));
        auto written = rangevar_to_qualified_name(node.typevar);
        auto type = types::complex_logical_type::create_struct(written.collection.t, fields);
        auto type_copy = type;
        auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
        register_namespace(set_target(*created, written, target_slots::schema));
        register_types(field_type_names_of(type));
        return created;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_enum_type(CreateEnumStmt& node) {
        std::vector<types::enum_entry_t> values;
        if (!node.vals || node.vals->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"Can not create enum without values", resource_});
        }
        values.reserve(node.vals->lst.size());
        int counter = 0;
        for (const auto& cell : node.vals->lst) {
            values.push_back(types::enum_entry_t{strVal(cell.data), counter++});
        }
        VALUE_OR_RETURN(auto written, qualified_name_of(resource_, *node.typeName));
        auto type = types::complex_logical_type::create_enum(written.collection.t, std::move(values));
        auto type_copy = type;
        auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
        register_namespace(set_target(*created, written, target_slots::schema));
        register_types(field_type_names_of(type));
        return created;
    }

} // namespace components::sql::transform