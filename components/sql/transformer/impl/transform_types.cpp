#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/types/user_type_walk.hpp>

#include <set>
#include <string>

namespace components::sql::transform {

    namespace {
        // Register the new type's own name (for collision detection — the resolve
        // stamps a result iff pg_type already has the name) plus every nested UDT
        // referenced by struct fields. check_type_exists / probe_type_in_path read
        // those stamps back.
        void register_create_type_resolves(std::pmr::memory_resource* resource,
                                           logical_plan::catalog_resolves_t* resolves,
                                           const types::complex_logical_type& type) {
            std::set<std::string> names;
            names.emplace(type.type_name());
            if (type.type() == types::logical_type::STRUCT) {
                for (const auto& field : type.child_types()) {
                    types::walk_user_type_refs(field, [&](std::string_view nm) { names.emplace(nm); });
                }
            }
            register_catalog_resolve_namespace(resource, resolves, "public");
            register_catalog_resolve_types(resource, resolves, std::vector<std::string>(names.begin(), names.end()));
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_type(CompositeTypeStmt& node) {
        VALUE_OR_RETURN(auto fields, get_types(resource_, *node.coldeflist));
        auto written = rangevar_to_qualified_name(node.typevar);
        auto type = types::complex_logical_type::create_struct(written.collection, fields);
        auto type_copy = type;
        auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
        // A type always lands in "public"; a name that spells uid or schema goes to enrich to refuse.
        set_target(*created, written);
        register_create_type_resolves(resource_, &catalog_resolves_, type);
        return created;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_enum_type(CreateEnumStmt& node) {
        std::vector<types::logical_value_t> values;
        if (!node.vals || node.vals->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"Can not create enum without values", resource_});
        }
        values.reserve(node.vals->lst.size());
        int counter = 0;
        for (const auto& cell : node.vals->lst) {
            values.emplace_back(resource_, counter++);
            values.back().set_alias(strVal(cell.data));
        }
        VALUE_OR_RETURN(auto written, qualified_name_of(resource_, *node.typeName));
        auto type = types::complex_logical_type::create_enum(written.collection, std::move(values));
        auto type_copy = type;
        auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
        set_target(*created, written);
        register_create_type_resolves(resource_, &catalog_resolves_, type);
        return created;
    }

} // namespace components::sql::transform