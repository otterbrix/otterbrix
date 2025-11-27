#include <components/logical_plan/node_create_type.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_type(CompositeTypeStmt& node) {
        auto type =
            types::complex_logical_type::create_struct(get_types(*node.coldeflist), construct(node.typevar->relname));
        return logical_plan::make_node_create_type(resource_, std::move(type));
    }

    logical_plan::node_ptr transformer::transform_create_enum_type(CreateEnumStmt& node) {
        std::vector<types::logical_value_t> values;
        if (!node.vals || node.vals->lst.empty()) {
            throw parser_exception_t("Can not create enum without values", {});
        }
        values.reserve(node.vals->lst.size());
        int counter = 0;
        for (const auto& cell : node.vals->lst) {
            values.emplace_back(counter++);
            values.back().set_alias(strVal(cell.data));
        }
        auto type = types::complex_logical_type::create_enum(std::move(values), strVal(node.typeName->lst.back().data));
        return logical_plan::make_node_create_type(resource_, std::move(type));
    }

} // namespace components::sql::transform
