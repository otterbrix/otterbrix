#include <components/logical_plan/node_create_type.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_type(CompositeTypeStmt& node) {
        if (auto res = get_types(resource_, *node.coldeflist); res.has_error()) {
            error_ = res.error();
            return nullptr;
        } else {
            auto type = types::complex_logical_type::create_struct(construct(node.typevar->relname), res.value());
            return logical_plan::make_node_create_type(resource_, std::move(type));
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
        return logical_plan::make_node_create_type(resource_, std::move(type));
    }

} // namespace components::sql::transform
