#include "from_substrait.hpp"
#include "substrait_adapter_types.hpp"
#include "substrait_extension_contracts.hpp"
#include "substrait_function_mapping.hpp"
#include "substrait_expression_importer.hpp"
#include "substrait_type_mapping.hpp"

#include "node_aggregate.hpp"
#include "node_create_collection.hpp"
#include "node_create_database.hpp"
#include "node_create_index.hpp"
#include "node_create_type.hpp"
#include "node_data.hpp"
#include "node_delete.hpp"
#include "node_drop_collection.hpp"
#include "node_drop_database.hpp"
#include "node_drop_index.hpp"
#include "node_drop_type.hpp"
#include "node_function.hpp"
#include "node_group.hpp"
#include "node_insert.hpp"
#include "node_join.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include "node_sort.hpp"
#include "node_update.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/types/logical_value.hpp>

#include <google/protobuf/util/json_util.h>

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"

#include <algorithm>
#include <unordered_map>

namespace components::logical_plan::substrait_adapter {

    namespace {
        struct rel_parse_result_t {
            node_ptr plan;
            field_mapping_t mapping;
        };

        rel_parse_result_t rel_to_plan(std::pmr::memory_resource* resource,
                                       const substrait::Rel& rel,
                                       from_substrait_context_t& ctx,
                                       const std::vector<std::string>* output_names = nullptr);

        rel_parse_result_t invalid_rel(std::pmr::memory_resource* resource) {
            return {make_node_aggregate(resource, {}), {}};
        }

        collection_full_name_t collection_from_names(const google::protobuf::RepeatedPtrField<std::string>& names) {
            collection_full_name_t collection;
            if (names.size() == 1) {
                collection.collection = names.at(0);
            } else if (names.size() == 2) {
                collection.database = names.at(0);
                collection.collection = names.at(1);
            } else if (names.size() == 3) {
                collection.database = names.at(0);
                collection.schema = names.at(1);
                collection.collection = names.at(2);
            } else if (names.size() >= 4) {
                collection.unique_identifier = names.at(0);
                collection.database = names.at(1);
                collection.schema = names.at(2);
                collection.collection = names.at(3);
            }
            return collection;
        }

        collection_full_name_t collection_from_ddl_names(const google::protobuf::RepeatedPtrField<std::string>& names,
                                                         int prefix_size) {
            collection_full_name_t collection;
            if (prefix_size <= 0) {
                return collection;
            }
            if (prefix_size == 1) {
                collection.database = names.at(0);
            } else if (prefix_size == 2) {
                collection.database = names.at(0);
                collection.collection = names.at(1);
            } else if (prefix_size == 3) {
                collection.database = names.at(0);
                collection.schema = names.at(1);
                collection.collection = names.at(2);
            } else {
                collection.unique_identifier = names.at(0);
                collection.database = names.at(1);
                collection.schema = names.at(2);
                collection.collection = names.at(3);
            }
            return collection;
        }

        components::types::complex_logical_type type_from_literal(const substrait::Expression_Literal& literal,
                                                                  std::string alias = "") {
            using components::types::complex_logical_type;
            using components::types::logical_type;
            switch (literal.literal_type_case()) {
                case substrait::Expression_Literal::kBoolean:
                    return complex_logical_type(logical_type::BOOLEAN, std::move(alias));
                case substrait::Expression_Literal::kI8:
                    return complex_logical_type(logical_type::TINYINT, std::move(alias));
                case substrait::Expression_Literal::kI16:
                    return complex_logical_type(logical_type::SMALLINT, std::move(alias));
                case substrait::Expression_Literal::kI32:
                    return complex_logical_type(logical_type::INTEGER, std::move(alias));
                case substrait::Expression_Literal::kI64:
                    return complex_logical_type(logical_type::BIGINT, std::move(alias));
                case substrait::Expression_Literal::kFp32:
                    return complex_logical_type(logical_type::FLOAT, std::move(alias));
                case substrait::Expression_Literal::kFp64:
                    return complex_logical_type(logical_type::DOUBLE, std::move(alias));
                case substrait::Expression_Literal::kString:
                case substrait::Expression_Literal::kVarChar:
                    return complex_logical_type(logical_type::STRING_LITERAL, std::move(alias));
                case substrait::Expression_Literal::kBinary:
                    return complex_logical_type(logical_type::BLOB, std::move(alias));
                case substrait::Expression_Literal::kList: {
                    auto child_type = literal.list().values_size() > 0
                                          ? type_from_literal(literal.list().values(0))
                                          : complex_logical_type(logical_type::NA);
                    return complex_logical_type::create_list(child_type, std::move(alias));
                }
                case substrait::Expression_Literal::kStruct: {
                    std::vector<complex_logical_type> fields;
                    fields.reserve(static_cast<size_t>(literal.struct_().fields_size()));
                    for (const auto& field : literal.struct_().fields()) {
                        fields.emplace_back(type_from_literal(field));
                    }
                    return complex_logical_type::create_struct("struct", fields, std::move(alias));
                }
                case substrait::Expression_Literal::kMap: {
                    auto key_type = literal.map().key_values_size() > 0 && literal.map().key_values(0).has_key()
                                        ? type_from_literal(literal.map().key_values(0).key())
                                        : complex_logical_type(logical_type::NA);
                    auto value_type = literal.map().key_values_size() > 0 && literal.map().key_values(0).has_value()
                                          ? type_from_literal(literal.map().key_values(0).value())
                                          : complex_logical_type(logical_type::NA);
                    return complex_logical_type::create_map(key_type, value_type, std::move(alias));
                }
                case substrait::Expression_Literal::kEmptyList:
                    return complex_logical_type::create_list(complex_logical_type(logical_type::NA), std::move(alias));
                case substrait::Expression_Literal::kEmptyMap:
                    return complex_logical_type::create_map(complex_logical_type(logical_type::NA),
                                                            complex_logical_type(logical_type::NA),
                                                            std::move(alias));
                case substrait::Expression_Literal::kNull:
                default:
                    return complex_logical_type(logical_type::NA, std::move(alias));
            }
        }

        rel_parse_result_t read_virtual_table_to_plan(std::pmr::memory_resource* resource,
                                                      const substrait::ReadRel& read) {
            const auto& virtual_table = read.virtual_table();
            const auto row_count = virtual_table.expressions_size();

            size_t column_count = 0;
            if (read.has_base_schema()) {
                column_count = std::max(static_cast<size_t>(read.base_schema().names_size()),
                                        static_cast<size_t>(read.base_schema().struct_().types_size()));
            }
            for (const auto& row : virtual_table.expressions()) {
                column_count = std::max(column_count, static_cast<size_t>(row.fields_size()));
            }

            field_mapping_t mapping;
            std::pmr::vector<components::types::complex_logical_type> chunk_types(resource);
            chunk_types.reserve(column_count);
            for (size_t col_idx = 0; col_idx < column_count; ++col_idx) {
                auto name = read.has_base_schema() && col_idx < static_cast<size_t>(read.base_schema().names_size())
                                ? read.base_schema().names(static_cast<int>(col_idx))
                                : ("col_" + std::to_string(col_idx));
                substrait::Type substrait_type;
                components::types::complex_logical_type chunk_type;
                if (read.has_base_schema() && col_idx < static_cast<size_t>(read.base_schema().struct_().types_size())) {
                    substrait_type = read.base_schema().struct_().types(static_cast<int>(col_idx));
                    chunk_type = from_substrait_type(substrait_type, name);
                } else if (virtual_table.expressions_size() > 0 &&
                           col_idx < static_cast<size_t>(virtual_table.expressions(0).fields_size()) &&
                           virtual_table.expressions(0).fields(static_cast<int>(col_idx)).has_literal()) {
                    chunk_type = type_from_literal(virtual_table.expressions(0).fields(static_cast<int>(col_idx)).literal(), name);
                    substrait_type = to_substrait_type(chunk_type);
                } else {
                    chunk_type = components::types::complex_logical_type(components::types::logical_type::NA, name);
                    substrait_type = to_substrait_type(chunk_type);
                }
                mapping.get_or_add(name, substrait_type);
                chunk_types.emplace_back(std::move(chunk_type));
            }

            components::vector::data_chunk_t chunk(resource, chunk_types, static_cast<uint64_t>(row_count));
            chunk.set_cardinality(static_cast<uint64_t>(row_count));
            for (int row_idx = 0; row_idx < virtual_table.expressions_size(); ++row_idx) {
                const auto& row = virtual_table.expressions(row_idx);
                const auto fields = std::min(row.fields_size(), static_cast<int>(column_count));
                for (int col_idx = 0; col_idx < fields; ++col_idx) {
                    if (!row.fields(col_idx).has_literal()) {
                        continue;
                    }
                    chunk.set_value(static_cast<uint64_t>(col_idx),
                                    static_cast<uint64_t>(row_idx),
                                    logical_value_from_literal(resource, row.fields(col_idx).literal()));
                }
            }
            return {make_node_raw_data(resource, std::move(chunk)), mapping};
        }

        rel_parse_result_t
        read_to_plan(std::pmr::memory_resource* resource, const substrait::ReadRel& read, const from_substrait_context_t& ctx) {
            (void) ctx;
            if (read.has_virtual_table()) {
                return read_virtual_table_to_plan(resource, read);
            }
            field_mapping_t mapping;
            if (read.has_base_schema()) {
                for (const auto& name : read.base_schema().names()) {
                    mapping.get_or_add(name);
                }
            }
            collection_full_name_t collection;
            if (read.has_named_table()) {
                collection = collection_from_names(read.named_table().names());
            }
            auto agg = make_node_aggregate(resource, collection);
            return {agg, mapping};
        }

        std::string restored_expression_output_name(const expressions::expression_ptr& expr, size_t index) {
            using namespace components::expressions;
            if (expr) {
                if (expr->group() == expression_group::scalar) {
                    auto* scalar = static_cast<const scalar_expression_t*>(expr.get());
                    if (!scalar->key().is_null()) {
                        return scalar->key().as_string();
                    }
                    if (!scalar->params().empty() &&
                        std::holds_alternative<expressions::key_t>(scalar->params().front())) {
                        return std::get<expressions::key_t>(scalar->params().front()).as_string();
                    }
                } else if (expr->group() == expression_group::aggregate) {
                    auto* aggregate = static_cast<const aggregate_expression_t*>(expr.get());
                    if (!aggregate->key().is_null()) {
                        return aggregate->key().as_string();
                    }
                    if (!aggregate->function_name().empty()) {
                        return aggregate->function_name();
                    }
                }
            }
            return "expr_" + std::to_string(index);
        }

        field_mapping_t output_mapping_from_group(const node_group_ptr& group) {
            field_mapping_t mapping;
            size_t index = 0;
            for (const auto& expr : group->expressions()) {
                mapping.get_or_add(restored_expression_output_name(expr, index));
                ++index;
            }
            return mapping;
        }

        rel_parse_result_t join_to_plan(std::pmr::memory_resource* resource,
                                        const substrait::JoinRel& join,
                                        from_substrait_context_t& ctx) {
            if (!join.has_left() || !join.has_right()) {
                return invalid_rel(resource);
            }
            auto left = rel_to_plan(resource, join.left(), ctx);
            auto right = rel_to_plan(resource, join.right(), ctx);

            field_mapping_t combined;
            combined.names = left.mapping.names;
            combined.names.insert(combined.names.end(), right.mapping.names.begin(), right.mapping.names.end());
            combined.left_size = static_cast<int32_t>(left.mapping.names.size());

            join_type jt = join_type::inner;
            switch (join.type()) {
                case substrait::JoinRel_JoinType_JOIN_TYPE_LEFT:
                    jt = join_type::left;
                    break;
                case substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT:
                    jt = join_type::right;
                    break;
                case substrait::JoinRel_JoinType_JOIN_TYPE_OUTER:
                    jt = join_type::full;
                    break;
                case substrait::JoinRel_JoinType_JOIN_TYPE_INNER:
                default:
                    jt = join_type::inner;
                    break;
            }

            auto join_node = make_node_join(resource, {}, jt);
            join_node->append_child(left.plan);
            join_node->append_child(right.plan);
            if (join.has_expression()) {
                join_node->append_expression(
                    expression_from_substrait(resource, join.expression(), combined, ctx));
            } else {
                join_node->append_expression(make_compare_expression(resource, expressions::compare_type::all_true));
            }

            auto root = make_node_aggregate(resource, {});
            root->append_child(join_node);
            return {root, combined};
        }

        rel_parse_result_t ddl_to_plan(std::pmr::memory_resource* resource, const substrait::DdlRel& ddl) {
            google::protobuf::RepeatedPtrField<std::string> empty_names;
            const auto& names = ddl.has_named_object() ? ddl.named_object().names() : empty_names;
            int marker_pos = -1;
            for (int i = 0; i < names.size(); ++i) {
                if (names.at(i) == "__otterbrix_index__") {
                    marker_pos = i;
                    break;
                }
            }
            const auto collection = collection_from_ddl_names(names, marker_pos >= 0 ? marker_pos : names.size());
            const bool has_index_marker = marker_pos >= 0;
            if (has_index_marker) {
                if (names.size() > marker_pos + 2 && names.at(marker_pos + 1) == "create") {
                    index_type type = index_type::single;
                    if (names.size() > marker_pos + 3) {
                        try {
                            const auto parsed_type = std::stoi(names.at(marker_pos + 3));
                            type = static_cast<index_type>(parsed_type);
                        } catch (const std::exception&) {
                            type = index_type::single;
                        }
                    }
                    auto node = make_node_create_index(resource, collection, names.at(marker_pos + 2), type);
                    for (int i = marker_pos + 4; i < names.size(); ++i) {
                        node->keys().emplace_back(resource, names.at(i));
                    }
                    return {node, {}};
                }
                if (names.size() > marker_pos + 2 && names.at(marker_pos + 1) == "drop") {
                    return {make_node_drop_index(resource, collection, names.at(marker_pos + 2)), {}};
                }
            }
            auto type_ext = parse_ddl_type_extension(resource, ddl);
            if (type_ext.valid) {
                if (type_ext.create) {
                    return {make_node_create_type(resource, types::complex_logical_type(std::move(type_ext.type))), {}};
                }
                return {make_node_drop_type(resource, std::string(type_ext.name)), {}};
            }
            const auto object_ext = parse_ddl_object_extension(ddl);
            if (object_ext.valid) {
                if (object_ext.object_kind == kDatabaseObjectKind) {
                    if (object_ext.create) {
                        return {make_node_create_database(resource, object_ext.collection), {}};
                    }
                    return {make_node_drop_database(resource, object_ext.collection), {}};
                }
                if (object_ext.object_kind == kCollectionObjectKind) {
                    if (object_ext.create) {
                        return {make_node_create_collection(resource, object_ext.collection), {}};
                    }
                    return {make_node_drop_collection(resource, object_ext.collection), {}};
                }
            }
            const auto index_ext = parse_ddl_index_extension(ddl);
            if (index_ext.valid) {
                const auto index_collection = index_ext.collection.empty() ? collection : index_ext.collection;
                if (index_ext.create) {
                    auto node =
                        make_node_create_index(resource, index_collection, index_ext.name, static_cast<index_type>(index_ext.type));
                    for (const auto& key : index_ext.keys) {
                        node->keys().emplace_back(resource, key);
                    }
                    return {node, {}};
                }
                return {make_node_drop_index(resource, index_collection, index_ext.name), {}};
            }
            if (ddl.op() == substrait::DdlRel_DdlOp_DDL_OP_CREATE) {
                if (collection.collection.empty()) {
                    return {make_node_create_database(resource, collection), {}};
                }
                return {make_node_create_collection(resource, collection), {}};
            }
            if (ddl.op() == substrait::DdlRel_DdlOp_DDL_OP_DROP ||
                ddl.op() == substrait::DdlRel_DdlOp_DDL_OP_DROP_IF_EXIST) {
                if (collection.collection.empty()) {
                    return {make_node_drop_database(resource, collection), {}};
                }
                return {make_node_drop_collection(resource, collection), {}};
            }
            return {make_node_aggregate(resource, {}), {}};
        }

        rel_parse_result_t write_to_plan(std::pmr::memory_resource* resource,
                                         const substrait::WriteRel& write,
                                         from_substrait_context_t& ctx) {
            collection_full_name_t collection;
            if (write.has_named_table()) {
                collection = collection_from_names(write.named_table().names());
            }
            switch (write.op()) {
                case substrait::WriteRel_WriteOp_WRITE_OP_INSERT: {
                    if (write.has_input()) {
                        auto parsed = rel_to_plan(resource, write.input(), ctx);
                        if (parsed.plan && parsed.plan->type() == components::logical_plan::node_type::data_t) {
                            const auto data = reinterpret_cast<const node_data_ptr&>(parsed.plan);
                            return {make_node_insert(resource, collection, data->data_chunk()), parsed.mapping};
                        }
                    }
                    components::vector::data_chunk_t chunk(resource, {}, 0);
                    chunk.set_cardinality(0);
                    auto node = make_node_insert(resource, collection, std::move(chunk));
                    return {node, {}};
                }
                case substrait::WriteRel_WriteOp_WRITE_OP_DELETE: {
                    auto match = make_node_match(resource,
                                                 collection,
                                                 make_compare_expression(resource, expressions::compare_type::all_true));
                    auto limit = make_node_limit(resource, collection, limit_t::unlimit());
                    auto collection_from = parse_delete_collection_from_extension(write);
                    if (write.has_input()) {
                        auto parsed = rel_to_plan(resource, write.input(), ctx);
                        auto root = parsed.plan;
                        if (root && root->type() == components::logical_plan::node_type::aggregate_t) {
                            for (const auto& child : root->children()) {
                                if (child->type() == components::logical_plan::node_type::match_t) {
                                    match = reinterpret_cast<const node_match_ptr&>(child);
                                } else if (child->type() == components::logical_plan::node_type::limit_t) {
                                    limit = reinterpret_cast<const node_limit_ptr&>(child);
                                }
                            }
                        }
                    }
                    if (!collection_from.empty()) {
                        return {make_node_delete(resource, collection, collection_from, match, limit), {}};
                    }
                    return {make_node_delete(resource, collection, match, limit), {}};
                }
                default:
                    break;
            }
            return {make_node_aggregate(resource, {}), {}};
        }

        expressions::update_expr_type update_expr_type_from_function(const std::string& name) {
            using expressions::update_expr_type;
            if (name == "add") return update_expr_type::add;
            if (name == "subtract") return update_expr_type::sub;
            if (name == "multiply") return update_expr_type::mult;
            if (name == "divide") return update_expr_type::div;
            if (name == "mod") return update_expr_type::mod;
            if (name == "pow") return update_expr_type::exp;
            if (name == "sqrt") return update_expr_type::sqr_root;
            if (name == "cuberoot") return update_expr_type::cube_root;
            if (name == "factorial") return update_expr_type::factorial;
            if (name == "abs") return update_expr_type::abs;
            if (name == "bit_and") return update_expr_type::AND;
            if (name == "bit_or") return update_expr_type::OR;
            if (name == "bit_xor") return update_expr_type::XOR;
            if (name == "bit_not") return update_expr_type::NOT;
            if (name == "bit_shift_left") return update_expr_type::shift_left;
            if (name == "bit_shift_right") return update_expr_type::shift_right;
            return update_expr_type::get_value;
        }

        expressions::update_expr_ptr update_expr_from_substrait(std::pmr::memory_resource* resource,
                                                                const substrait::Expression& expr,
                                                                const field_mapping_t& mapping,
                                                                from_substrait_context_t& ctx) {
            using namespace components::expressions;
            if (expr.has_selection() && expr.selection().has_direct_reference() &&
                expr.selection().direct_reference().has_struct_field()) {
                auto idx = expr.selection().direct_reference().struct_field().field();
                return update_expr_ptr(new update_expr_get_value_t(key_t(resource, mapping.name_or_empty(idx))));
            }
            if (expr.has_dynamic_parameter()) {
                return update_expr_ptr(
                    new update_expr_get_const_value_t(core::parameter_id_t{
                        static_cast<uint16_t>(expr.dynamic_parameter().parameter_reference())}));
            }
            if (expr.has_literal()) {
                auto id = ctx.params->next_id();
                ctx.params->add_parameter(id, logical_value_from_literal(resource, expr.literal()));
                return update_expr_ptr(new update_expr_get_const_value_t(id));
            }
            if (expr.has_scalar_function()) {
                auto func_name = get_function_name(ctx, expr.scalar_function().function_reference());
                auto type = update_expr_type_from_function(func_name);
                auto out = update_expr_ptr(new update_expr_calculate_t(type));
                if (expr.scalar_function().arguments_size() > 0) {
                    out->left() = update_expr_from_substrait(resource, expr.scalar_function().arguments(0).value(), mapping, ctx);
                }
                if (expr.scalar_function().arguments_size() > 1) {
                    out->right() = update_expr_from_substrait(resource, expr.scalar_function().arguments(1).value(), mapping, ctx);
                }
                return out;
            }
            return update_expr_ptr(new update_expr_get_const_value_t(core::parameter_id_t(0)));
        }

        bool is_supported_scalar_function(const std::string& name) {
            return name == "and" || name == "or" || name == "not" || name == "eq" || name == "ne" ||
                   name == "gt" || name == "lt" || name == "gte" || name == "lte" || name == "regex" ||
                   expressions::get_scalar_type(name) != expressions::scalar_type::invalid ||
                   is_known_aggregate_function(name);
        }

        bool is_valid_expression(const substrait::Expression& expr,
                                 const field_mapping_t& mapping,
                                 const from_substrait_context_t& ctx);

        bool is_valid_param_expression(const substrait::Expression& expr,
                                       const field_mapping_t& mapping,
                                       const from_substrait_context_t& ctx) {
            switch (expr.rex_type_case()) {
                case substrait::Expression::kSelection:
                    return expr.selection().has_direct_reference() &&
                           expr.selection().direct_reference().has_struct_field() &&
                           (mapping.names.empty() || mapping.left_size >= 0 ||
                            mapping.contains(expr.selection().direct_reference().struct_field().field()));
                case substrait::Expression::kDynamicParameter:
                case substrait::Expression::kLiteral:
                    return true;
                case substrait::Expression::kScalarFunction:
                    return is_valid_expression(expr, mapping, ctx);
                default:
                    return false;
            }
        }

        bool is_valid_expression(const substrait::Expression& expr,
                                 const field_mapping_t& mapping,
                                 const from_substrait_context_t& ctx) {
            switch (expr.rex_type_case()) {
                case substrait::Expression::kSelection:
                    return expr.selection().has_direct_reference() &&
                           expr.selection().direct_reference().has_struct_field() &&
                           (mapping.names.empty() || mapping.left_size >= 0 ||
                            mapping.contains(expr.selection().direct_reference().struct_field().field()));
                case substrait::Expression::kDynamicParameter:
                case substrait::Expression::kLiteral:
                    return true;
                case substrait::Expression::kScalarFunction: {
                    const auto function_name = get_function_name(ctx, expr.scalar_function().function_reference());
                    if (function_name.empty() || !is_supported_scalar_function(function_name)) {
                        return false;
                    }
                    const bool logical_function =
                        function_name == "and" || function_name == "or" || function_name == "not";
                    for (const auto& arg : expr.scalar_function().arguments()) {
                        if (!arg.has_value()) {
                            return false;
                        }
                        if (logical_function) {
                            if (!is_valid_expression(arg.value(), mapping, ctx)) {
                                return false;
                            }
                        } else if (!is_valid_param_expression(arg.value(), mapping, ctx)) {
                            return false;
                        }
                    }
                    return true;
                }
                default:
                    return false;
            }
        }

        bool is_valid_measure(const substrait::AggregateRel_Measure& measure,
                              const field_mapping_t& mapping,
                              const from_substrait_context_t& ctx) {
            if (!measure.has_measure()) {
                return false;
            }
            const auto function_name = get_function_name(ctx, measure.measure().function_reference());
            if (!is_known_aggregate_function(function_name)) {
                return false;
            }
            for (const auto& arg : measure.measure().arguments()) {
                if (!arg.has_value() || !is_valid_param_expression(arg.value(), mapping, ctx)) {
                    return false;
                }
            }
            if (measure.has_filter() && !is_valid_expression(measure.filter(), mapping, ctx)) {
                return false;
            }
            return true;
        }

        int fetch_count_value(const substrait::FetchRel& fetch) {
            if (fetch.has_count_expr() && fetch.count_expr().has_literal()) {
                const auto& literal = fetch.count_expr().literal();
                switch (literal.literal_type_case()) {
                    case substrait::Expression_Literal::kI8:
                        return literal.i8();
                    case substrait::Expression_Literal::kI16:
                        return literal.i16();
                    case substrait::Expression_Literal::kI32:
                        return literal.i32();
                    case substrait::Expression_Literal::kI64:
                        return static_cast<int>(literal.i64());
                    default:
                        return -1;
                }
            }
            const auto* descriptor = fetch.GetDescriptor();
            const auto* reflection = fetch.GetReflection();
            const auto* count_field = descriptor ? descriptor->FindFieldByName("count") : nullptr;
            if (count_field && reflection->HasField(fetch, count_field)) {
                return static_cast<int>(reflection->GetInt64(fetch, count_field));
            }
            return -1;
        }

        void append_grouping_expression(std::pmr::memory_resource* resource,
                                        const substrait::Expression& expr,
                                        const field_mapping_t& mapping,
                                        from_substrait_context_t& ctx,
                                        const std::vector<std::string>* output_names,
                                        node_group_ptr& group,
                                        int& out_idx) {
            auto e = expression_from_substrait(resource, expr, mapping, ctx);
            if (output_names && out_idx < static_cast<int>(output_names->size())) {
                e = expression_with_alias(resource, e, output_names->at(static_cast<size_t>(out_idx)));
            }
            group->append_expression(e);
            out_idx++;
        }

        bool append_deprecated_grouping_expressions(std::pmr::memory_resource* resource,
                                                    const substrait::AggregateRel_Grouping& grouping_set,
                                                    const field_mapping_t& mapping,
                                                    from_substrait_context_t& ctx,
                                                    const std::vector<std::string>* output_names,
                                                    node_group_ptr& group,
                                                    int& out_idx) {
            const auto* descriptor = grouping_set.GetDescriptor();
            const auto* reflection = grouping_set.GetReflection();
            const auto* expressions_field = descriptor ? descriptor->FindFieldByName("grouping_expressions") : nullptr;
            if (!expressions_field) {
                return true;
            }
            const auto count = reflection->FieldSize(grouping_set, expressions_field);
            for (int i = 0; i < count; ++i) {
                const auto& message = reflection->GetRepeatedMessage(grouping_set, expressions_field, i);
                const auto* expr = dynamic_cast<const substrait::Expression*>(&message);
                if (!expr) {
                    return false;
                }
                if (!is_valid_expression(*expr, mapping, ctx)) {
                    return false;
                }
                append_grouping_expression(resource, *expr, mapping, ctx, output_names, group, out_idx);
            }
            return true;
        }

        rel_parse_result_t update_to_plan(std::pmr::memory_resource* resource,
                                          const substrait::UpdateRel& update,
                                          from_substrait_context_t& ctx) {
            collection_full_name_t collection_to;
            if (update.has_named_table()) {
                collection_to = collection_from_names(update.named_table().names());
            }
            auto collection_from = parse_update_collection_from_extension(update);
            bool upsert = parse_update_upsert_extension(update);
            int limit_value = parse_update_limit_extension(update, -1);
            auto limit_node = make_node_limit(resource, collection_to, limit_t(limit_value));

            field_mapping_t mapping;
            if (update.has_table_schema()) {
                for (const auto& name : update.table_schema().names()) {
                    mapping.get_or_add(name);
                }
            }
            expressions::expression_ptr match_expr =
                expressions::make_compare_expression(resource, expressions::compare_type::all_true);
            if (update.has_condition()) {
                match_expr = expression_from_substrait(resource, update.condition(), mapping, ctx);
            }
            auto match = make_node_match(resource, collection_to, match_expr);

            std::pmr::vector<expressions::update_expr_ptr> updates(resource);
            for (const auto& t : update.transformations()) {
                auto idx = t.column_target();
                if (!mapping.contains(idx)) {
                    continue;
                }
                auto set = expressions::update_expr_ptr(
                    new expressions::update_expr_set_t(expressions::key_t(resource, mapping.name_or_empty(idx))));
                set->left() = update_expr_from_substrait(resource, t.transformation(), mapping, ctx);
                updates.push_back(set);
            }

            if (!collection_from.empty()) {
                return {make_node_update(resource, collection_to, collection_from, match, limit_node, updates, upsert), {}};
            }
            return {make_node_update(resource, collection_to, match, limit_node, updates, upsert), {}};
        }

        rel_parse_result_t rel_to_plan(std::pmr::memory_resource* resource,
                                       const substrait::Rel& rel,
                                       from_substrait_context_t& ctx,
                                       const std::vector<std::string>* output_names) {
            using components::logical_plan::node_type;
            switch (rel.rel_type_case()) {
                case substrait::Rel::kRead:
                    return read_to_plan(resource, rel.read(), ctx);
                case substrait::Rel::kFilter: {
                    if (!rel.filter().has_input() || !rel.filter().has_condition()) {
                        return invalid_rel(resource);
                    }
                    auto input = rel_to_plan(resource, rel.filter().input(), ctx, output_names);
                    auto root = input.plan;
                    if (!is_valid_expression(rel.filter().condition(), input.mapping, ctx)) {
                        return invalid_rel(resource);
                    }
                    auto match_expr =
                        expression_from_substrait(resource, rel.filter().condition(), input.mapping, ctx);
                    auto match = make_node_match(resource, root->collection_full_name(), match_expr);
                    if (root->type() == node_type::aggregate_t) {
                        root->append_child(match);
                        return {root, input.mapping};
                    }
                    auto agg = make_node_aggregate(resource, {});
                    agg->append_child(root);
                    agg->append_child(match);
                    return {agg, input.mapping};
                }
                case substrait::Rel::kProject: {
                    if (!rel.project().has_input()) {
                        return invalid_rel(resource);
                    }
                    auto input = rel_to_plan(resource, rel.project().input(), ctx);
                    auto root = input.plan;
                    auto group = make_node_group(resource, root->collection_full_name());
                    for (int i = 0; i < rel.project().expressions_size(); ++i) {
                        if (!is_valid_expression(rel.project().expressions(i), input.mapping, ctx)) {
                            return invalid_rel(resource);
                        }
                        auto expr = expression_from_substrait(
                            resource, rel.project().expressions(i), input.mapping, ctx);
                        if (output_names && i < static_cast<int>(output_names->size())) {
                            expr = expression_with_alias(resource, expr, output_names->at(static_cast<size_t>(i)));
                        }
                        group->append_expression(expr);
                    }
                    if (root->type() == node_type::aggregate_t) {
                        root->append_child(group);
                        return {root, output_mapping_from_group(group)};
                    }
                    auto agg = make_node_aggregate(resource, {});
                    agg->append_child(root);
                    agg->append_child(group);
                    return {agg, output_mapping_from_group(group)};
                }
                case substrait::Rel::kAggregate: {
                    if (!rel.aggregate().has_input()) {
                        return invalid_rel(resource);
                    }
                    auto input = rel_to_plan(resource, rel.aggregate().input(), ctx);
                    auto root = input.plan;
                    auto group = make_node_group(resource, root->collection_full_name());
                    int out_idx = 0;
                    for (const auto& grouping_set : rel.aggregate().groupings()) {
                        if (grouping_set.expression_references_size() == 0) {
                            if (!append_deprecated_grouping_expressions(
                                    resource, grouping_set, input.mapping, ctx, output_names, group, out_idx)) {
                                return invalid_rel(resource);
                            }
                            continue;
                        }
                        for (const auto expression_ref : grouping_set.expression_references()) {
                            if (expression_ref >= static_cast<uint32_t>(rel.aggregate().grouping_expressions_size())) {
                                return invalid_rel(resource);
                            }
                            if (!is_valid_expression(
                                    rel.aggregate().grouping_expressions(static_cast<int>(expression_ref)),
                                    input.mapping,
                                    ctx)) {
                                return invalid_rel(resource);
                            }
                            append_grouping_expression(
                                resource,
                                rel.aggregate().grouping_expressions(static_cast<int>(expression_ref)),
                                input.mapping,
                                ctx,
                                output_names,
                                group,
                                out_idx);
                        }
                    }
                    if (rel.aggregate().groupings_size() == 0) {
                        for (const auto& expr : rel.aggregate().grouping_expressions()) {
                            if (!is_valid_expression(expr, input.mapping, ctx)) {
                                return invalid_rel(resource);
                            }
                            append_grouping_expression(
                                resource, expr, input.mapping, ctx, output_names, group, out_idx);
                        }
                    }
                    for (const auto& measure : rel.aggregate().measures()) {
                        if (!is_valid_measure(measure, input.mapping, ctx)) {
                            return invalid_rel(resource);
                        }
                        auto func_name = normalize_aggregate_function_name(
                            get_function_name(ctx, measure.measure().function_reference()));
                        expressions::aggregate_expression_ptr aggr;
                        if (output_names && out_idx < static_cast<int>(output_names->size())) {
                            aggr = make_aggregate_expression(
                                resource,
                                func_name,
                                expressions::key_t(resource, output_names->at(static_cast<size_t>(out_idx))));
                        } else {
                            aggr = expressions::make_aggregate_expression(resource, func_name);
                        }
                        if (measure.measure().invocation() ==
                            substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT) {
                            aggr->set_distinct(true);
                        }
                        for (const auto& arg : measure.measure().arguments()) {
                            aggr->append_param(param_from_expression(resource, arg.value(), input.mapping, ctx));
                        }
                        group->append_expression(aggr);
                        out_idx++;
                    }
                    if (root->type() == node_type::aggregate_t) {
                        root->append_child(group);
                        return {root, output_mapping_from_group(group)};
                    }
                    auto agg = make_node_aggregate(resource, {});
                    agg->append_child(root);
                    agg->append_child(group);
                    return {agg, output_mapping_from_group(group)};
                }
                case substrait::Rel::kSort: {
                    if (!rel.sort().has_input()) {
                        return invalid_rel(resource);
                    }
                    auto input = rel_to_plan(resource, rel.sort().input(), ctx, output_names);
                    auto root = input.plan;
                    std::vector<expression_ptr> exprs;
                    exprs.reserve(static_cast<size_t>(rel.sort().sorts_size()));
                    for (const auto& sort : rel.sort().sorts()) {
                        if (!is_valid_expression(sort.expr(), input.mapping, ctx)) {
                            return invalid_rel(resource);
                        }
                        auto expr = expression_from_substrait(resource, sort.expr(), input.mapping, ctx);
                        if (expr->group() == expressions::expression_group::scalar) {
                            auto* scalar = static_cast<const expressions::scalar_expression_t*>(expr.get());
                            auto key = scalar->key();
                            if (!scalar->params().empty() && std::holds_alternative<expressions::key_t>(scalar->params().front())) {
                                key = std::get<expressions::key_t>(scalar->params().front());
                            }
                            const auto direction = sort.direction();
                            const bool is_desc =
                                direction == substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST ||
                                direction == substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST;
                            exprs.emplace_back(expressions::make_sort_expression(
                                key, is_desc ? expressions::sort_order::desc : expressions::sort_order::asc));
                        }
                    }
                    auto sort_node = make_node_sort(resource, root->collection_full_name(), exprs);
                    if (root->type() == node_type::aggregate_t) {
                        root->append_child(sort_node);
                        return {root, input.mapping};
                    }
                    auto agg = make_node_aggregate(resource, {});
                    agg->append_child(root);
                    agg->append_child(sort_node);
                    return {agg, input.mapping};
                }
                case substrait::Rel::kFetch: {
                    if (!rel.fetch().has_input()) {
                        return invalid_rel(resource);
                    }
                    auto input = rel_to_plan(resource, rel.fetch().input(), ctx, output_names);
                    auto root = input.plan;
                    auto value = fetch_count_value(rel.fetch());
                    auto limit_node = make_node_limit(resource, root->collection_full_name(), limit_t(value));
                    if (root->type() == node_type::aggregate_t) {
                        root->append_child(limit_node);
                        return {root, input.mapping};
                    }
                    auto agg = make_node_aggregate(resource, {});
                    agg->append_child(root);
                    agg->append_child(limit_node);
                    return {agg, input.mapping};
                }
                case substrait::Rel::kJoin:
                    return join_to_plan(resource, rel.join(), ctx);
                case substrait::Rel::kDdl:
                    return ddl_to_plan(resource, rel.ddl());
                case substrait::Rel::kWrite:
                    return write_to_plan(resource, rel.write(), ctx);
                case substrait::Rel::kUpdate:
                    return update_to_plan(resource, rel.update(), ctx);
                case substrait::Rel::kExtensionLeaf: {
                    break;
                }
                case substrait::Rel::kExtensionSingle: {
                    auto structured_node = parse_extension_single_struct(resource, rel.extension_single());
                    if (structured_node) {
                        return {structured_node, {}};
                    }
                    if (rel.extension_single().has_input()) {
                        return rel_to_plan(resource, rel.extension_single().input(), ctx, output_names);
                    }
                    break;
                }
                default:
                    break;
            }
            return {make_node_aggregate(resource, {}), {}};
        }

    } // namespace

    plan_with_params_t from_substrait_plan(std::pmr::memory_resource* resource, const substrait::Plan& plan) {
        return from_substrait_plan(resource, plan, import_profile_t::internal_roundtrip);
    }

    plan_with_params_t
    from_substrait_plan(std::pmr::memory_resource* resource, const substrait::Plan& plan, import_profile_t profile) {
        from_substrait_context_t ctx(resource, profile);
        for (const auto& ext : plan.extensions()) {
            if (ext.has_extension_function()) {
                ctx.functions.emplace(ext.extension_function().function_anchor(),
                                      ext.extension_function().name());
            }
        }
        if (plan.relations_size() == 0 || !plan.relations(0).has_root()) {
            return {make_node_aggregate(resource, {}), ctx.params};
        }
        std::vector<std::string> output_names;
        for (const auto& name : plan.relations(0).root().names()) {
            output_names.emplace_back(name);
        }
        auto result = rel_to_plan(resource, plan.relations(0).root().input(), ctx, &output_names);
        return {result.plan, ctx.params};
    }

    plan_with_params_t from_substrait_binary(std::pmr::memory_resource* resource, const std::string& binary) {
        return from_substrait_binary(resource, binary, import_profile_t::internal_roundtrip);
    }

    plan_with_params_t from_substrait_binary(std::pmr::memory_resource* resource,
                                             const std::string& binary,
                                             import_profile_t profile) {
        substrait::Plan plan;
        if (!plan.ParseFromString(binary)) {
            return {make_node_aggregate(resource, {}), make_parameter_node(resource)};
        }
        return from_substrait_plan(resource, plan, profile);
    }

    plan_with_params_t from_substrait_json(std::pmr::memory_resource* resource, const std::string& json) {
        return from_substrait_json(resource, json, import_profile_t::internal_roundtrip);
    }

    plan_with_params_t
    from_substrait_json(std::pmr::memory_resource* resource, const std::string& json, import_profile_t profile) {
        substrait::Plan plan;
        const auto status = google::protobuf::util::JsonStringToMessage(json, &plan);
        if (!status.ok()) {
            return {make_node_aggregate(resource, {}), make_parameter_node(resource)};
        }
        return from_substrait_plan(resource, plan, profile);
    }

} // namespace components::logical_plan::substrait_adapter
