#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/types/user_type_walk.hpp>

#include <set>
#include <string>

namespace components::sql::transform {

    namespace {
        // Build a sequence_t that resolves the new type name (for collision
        // detection) plus every nested UDT referenced by struct fields. Pass 1
        // stamps type_md_by_qname for each emitted resolve_type; dispatcher's
        // check_type_exists / probe_type_in_path then read from idx.
        logical_plan::node_ptr wrap_create_type(std::pmr::memory_resource* resource,
                                                const types::complex_logical_type& type,
                                                logical_plan::node_ptr main_node) {
            std::set<std::string> nested_names;
            if (type.type() == types::logical_type::STRUCT) {
                for (const auto& field : type.child_types()) {
                    types::walk_user_type_refs(field, [&](std::string_view nm) { nested_names.emplace(nm); });
                }
            }
            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource));
            seq->append_child(
                logical_plan::make_node_catalog_resolve_namespace(resource, core::dbname_t{std::string{"public"}}));
            // Resolve the new type's own name → collision detection (Pass 1
            // returns a stamp iff pg_type already has the name).
            seq->append_child(
                logical_plan::make_node_catalog_resolve_type(resource,
                                                             core::dbname_t{std::string{"public"}},
                                                             core::typename_t{std::string(type.type_name())}));
            // Nested STRUCT field UDTs (only those referenced by name).
            for (const auto& nm : nested_names) {
                if (nm == type.type_name())
                    continue; // self-ref already emitted
                seq->append_child(logical_plan::make_node_catalog_resolve_type(resource,
                                                                               core::dbname_t{std::string{"public"}},
                                                                               core::typename_t{nm}));
            }
            seq->append_child(std::move(main_node));
            return seq;
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_create_type(CompositeTypeStmt& node) {
        if (auto field_res = get_types(resource_, *node.coldeflist); field_res.has_error()) {
            error_ = field_res.error();
            return nullptr;
        } else {
            auto type = types::complex_logical_type::create_struct(construct(node.typevar->relname), field_res.value());
            auto type_copy = type;
            auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
            return wrap_create_type(resource_, type, std::move(created));
        }
    }

    logical_plan::node_ptr transformer::transform_create_enum_type(CreateEnumStmt& node) {
        std::vector<types::logical_value_t> values;
        if (!node.vals || node.vals->lst.empty()) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"Can not create enum without values", resource_});
            return nullptr;
        }
        values.reserve(node.vals->lst.size());
        int counter = 0;
        for (const auto& cell : node.vals->lst) {
            values.emplace_back(resource_, counter++);
            values.back().set_alias(strVal(cell.data));
        }
        auto type = types::complex_logical_type::create_enum(strVal(node.typeName->lst.back().data), std::move(values));
        auto type_copy = type;
        auto created = logical_plan::make_node_create_type(resource_, std::move(type_copy));
        return wrap_create_type(resource_, type, std::move(created));
    }

} // namespace components::sql::transform