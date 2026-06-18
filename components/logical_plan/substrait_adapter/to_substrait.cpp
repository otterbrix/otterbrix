#include "to_substrait.hpp"
#include "substrait_adapter_types.hpp"
#include "substrait_extension_contracts.hpp"
#include "substrait_expression_exporter.hpp"
#include "substrait_function_mapping.hpp"
#include "substrait_type_mapping.hpp"

#include <components/catalog/table_id.hpp>

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
#include <core/date/date_types.hpp>

#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "substrait/type.pb.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <unordered_map>

namespace components::logical_plan::substrait_adapter {

    namespace {
        struct rel_with_mapping_t {
            substrait::Rel* rel = nullptr;
            field_mapping_t mapping;
        };

        substrait::Rel* unsupported_node_rel(const node_ptr& node) {
            throw std::runtime_error("unsupported node type in to_substrait: " +
                                     components::logical_plan::to_string(node->type()));
        }

        substrait::Rel* build_function_extension_single_rel(const node_function_ptr& fn) {
            auto* rel = new substrait::Rel();
            auto* extension_single = rel->mutable_extension_single();
            google::protobuf::Struct payload;
            payload.mutable_fields()->operator[](kContractKey).set_string_value(kFunctionContract);
            payload.mutable_fields()->operator[](kContractVersionKey).set_number_value(kContractVersion);
            payload.mutable_fields()->operator[](kNodeKindKey).set_string_value(kFunctionNodeKind);
            payload.mutable_fields()->operator[](kFunctionNameKey).set_string_value(fn->name());
            set_function_args_extension(payload, fn->args());
            extension_single->mutable_detail()->PackFrom(payload);
            return rel;
        }

        substrait::Rel* build_rel(const node_ptr& node, field_mapping_t& mapping, to_substrait_context_t& ctx);
        substrait::Rel* build_match_rel(const node_match_ptr& match,
                                        substrait::Rel* input,
                                        field_mapping_t& mapping,
                                        to_substrait_context_t& ctx);
        substrait::Rel* build_limit_rel(const node_limit_ptr& limit, substrait::Rel* input);
        substrait::Rel* build_group_rel(const node_group_ptr& group,
                                        substrait::Rel* input,
                                        field_mapping_t& mapping,
                                        field_mapping_t* source_mapping,
                                        to_substrait_context_t& ctx);

        void populate_mapping_from_catalog_schema(const components::catalog::schema& schema, field_mapping_t& mapping) {
            for (const auto& column : schema.columns()) {
                auto name = column.name();
                if (name.empty()) {
                    continue;
                }
                mapping.get_or_add(name, to_substrait_type(column.type()));
            }
        }

        void populate_mapping_from_catalog(const collection_full_name_t& collection,
                                           std::pmr::memory_resource* resource,
                                           field_mapping_t& mapping,
                                           const to_substrait_context_t& ctx) {
            if (!ctx.catalog || collection.empty()) {
                return;
            }
            const auto table = components::catalog::table_id(resource ? resource : std::pmr::get_default_resource(), collection);
            if (!ctx.catalog->table_exists(table)) {
                return;
            }
            populate_mapping_from_catalog_schema(ctx.catalog->get_table_schema(table), mapping);
        }

        substrait::Rel* build_read_rel(const node_ptr& node, field_mapping_t& mapping, const to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* read = rel->mutable_read();
            auto* named = read->mutable_named_table();
            if (!node->collection_full_name().unique_identifier.empty()) {
                named->add_names(node->collection_full_name().unique_identifier);
            }
            if (!node->collection_full_name().database.empty()) {
                named->add_names(node->collection_full_name().database);
            }
            if (!node->collection_full_name().schema.empty()) {
                named->add_names(node->collection_full_name().schema);
            }
            if (!node->collection_full_name().collection.empty()) {
                named->add_names(node->collection_full_name().collection);
            }
            populate_mapping_from_catalog(node->collection_full_name(), node->resource(), mapping, ctx);

            return rel;
        }

        substrait::Rel*
        build_read_rel_from_collection(const collection_full_name_t& collection, field_mapping_t& mapping, const to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* read = rel->mutable_read();
            auto* named = read->mutable_named_table();
            if (!collection.unique_identifier.empty()) {
                named->add_names(collection.unique_identifier);
            }
            if (!collection.database.empty()) {
                named->add_names(collection.database);
            }
            if (!collection.schema.empty()) {
                named->add_names(collection.schema);
            }
            if (!collection.collection.empty()) {
                named->add_names(collection.collection);
            }
            populate_mapping_from_catalog(collection, std::pmr::get_default_resource(), mapping, ctx);
            return rel;
        }

        void apply_schema(substrait::NamedStruct* schema, const field_mapping_t& mapping) {
            schema->clear_names();
            schema->mutable_struct_()->clear_types();
            for (size_t i = 0; i < mapping.names.size(); ++i) {
                schema->add_names(mapping.names[i]);
                schema->mutable_struct_()->add_types()->CopyFrom(mapping.type_or_default(static_cast<int32_t>(i)));
            }
            schema->mutable_struct_()->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
        }

        void set_literal_value(substrait::Expression_Literal* literal, const components::types::logical_value_t& value) {
            using components::types::logical_type;
            if (!literal) {
                return;
            }
            if (value.is_null()) {
                literal->mutable_null()->CopyFrom(to_substrait_type(value.type()));
                return;
            }
            switch (value.type().type()) {
                case logical_type::BOOLEAN:
                    literal->set_boolean(value.value<bool>());
                    break;
                case logical_type::TINYINT:
                    literal->set_i8(value.value<int8_t>());
                    break;
                case logical_type::SMALLINT:
                    literal->set_i16(value.value<int16_t>());
                    break;
                case logical_type::INTEGER:
                case logical_type::INTEGER_LITERAL:
                    literal->set_i32(value.value<int32_t>());
                    break;
                case logical_type::UTINYINT:
                    literal->set_i32(value.value<uint8_t>());
                    break;
                case logical_type::USMALLINT:
                    literal->set_i32(value.value<uint16_t>());
                    break;
                case logical_type::UINTEGER:
                    literal->set_i32(static_cast<int32_t>(value.value<uint32_t>()));
                    break;
                case logical_type::BIGINT:
                    literal->set_i64(value.value<int64_t>());
                    break;
                case logical_type::UBIGINT:
                    literal->set_i64(static_cast<int64_t>(value.value<uint64_t>()));
                    break;
                case logical_type::FLOAT:
                    literal->set_fp32(value.value<float>());
                    break;
                case logical_type::DOUBLE:
                    literal->set_fp64(value.value<double>());
                    break;
                case logical_type::STRING_LITERAL:
                case logical_type::ENUM: {
                    const auto text = value.value<std::string_view>();
                    literal->set_string(std::string(text.data(), text.size()));
                    break;
                }
                case logical_type::BLOB: {
                    const auto bytes = value.value<std::string_view>();
                    literal->set_binary(std::string(bytes.data(), bytes.size()));
                    break;
                }
                case logical_type::TIMESTAMP:
                    literal->set_i64(value.value<core::date::timestamp_t>().value.count());
                    break;
                case logical_type::TIMESTAMP_TZ:
                    literal->set_i64(value.value<core::date::timestamptz_t>().value.count());
                    break;
                case logical_type::LIST:
                case logical_type::ARRAY: {
                    auto* list = literal->mutable_list();
                    for (const auto& child : value.children()) {
                        set_literal_value(list->add_values(), child);
                    }
                    break;
                }
                case logical_type::STRUCT:
                case logical_type::VARIANT: {
                    auto* struct_value = literal->mutable_struct_();
                    for (const auto& child : value.children()) {
                        set_literal_value(struct_value->add_fields(), child);
                    }
                    break;
                }
                case logical_type::MAP: {
                    auto* map = literal->mutable_map();
                    const auto& children = value.children();
                    if (children.size() < 2) {
                        break;
                    }
                    const auto& keys = children[0].children();
                    const auto& values = children[1].children();
                    const auto count = std::min(keys.size(), values.size());
                    for (size_t i = 0; i < count; ++i) {
                        auto* entry = map->add_key_values();
                        set_literal_value(entry->mutable_key(), keys[i]);
                        set_literal_value(entry->mutable_value(), values[i]);
                    }
                    break;
                }
                default:
                    literal->mutable_null()->CopyFrom(to_substrait_type(value.type()));
                    break;
            }
        }

        substrait::Rel* build_virtual_table_empty_rel(size_t rows) {
            auto* rel = new substrait::Rel();
            auto* read = rel->mutable_read();
            auto* virtual_table = read->mutable_virtual_table();
            for (size_t i = 0; i < rows; ++i) {
                virtual_table->add_expressions();
            }
            return rel;
        }

        substrait::Rel* build_data_read_rel(const node_data_ptr& data, field_mapping_t& mapping) {
            auto* rel = new substrait::Rel();
            auto* read = rel->mutable_read();
            auto* virtual_table = read->mutable_virtual_table();
            const auto chunk_types = data->data_chunk().types();
            for (size_t i = 0; i < chunk_types.size(); ++i) {
                const auto& type = chunk_types[i];
                auto name = type.has_alias() ? type.alias() : ("col_" + std::to_string(i));
                mapping.get_or_add(name, to_substrait_type(type));
            }
            for (uint64_t row_idx = 0; row_idx < data->data_chunk().size(); ++row_idx) {
                auto* row = virtual_table->add_expressions();
                for (uint64_t col_idx = 0; col_idx < data->data_chunk().column_count(); ++col_idx) {
                    set_literal_value(row->add_fields()->mutable_literal(), data->data_chunk().value(col_idx, row_idx));
                }
            }
            apply_schema(read->mutable_base_schema(), mapping);
            return rel;
        }

        void append_collection_name(substrait::NamedObjectWrite* named, const collection_full_name_t& collection) {
            if (!named) {
                return;
            }
            if (!collection.unique_identifier.empty()) {
                named->add_names(collection.unique_identifier);
            }
            if (!collection.database.empty()) {
                named->add_names(collection.database);
            }
            if (!collection.schema.empty()) {
                named->add_names(collection.schema);
            }
            if (!collection.collection.empty()) {
                named->add_names(collection.collection);
            }
        }

        void append_collection_name(substrait::NamedTable* named, const collection_full_name_t& collection) {
            if (!named) {
                return;
            }
            if (!collection.unique_identifier.empty()) {
                named->add_names(collection.unique_identifier);
            }
            if (!collection.database.empty()) {
                named->add_names(collection.database);
            }
            if (!collection.schema.empty()) {
                named->add_names(collection.schema);
            }
            if (!collection.collection.empty()) {
                named->add_names(collection.collection);
            }
        }

        void set_read_schema(substrait::Rel* rel, const field_mapping_t& mapping) {
            if (!rel) {
                return;
            }
            if (rel->has_read()) {
                apply_schema(rel->mutable_read()->mutable_base_schema(), mapping);
                return;
            }
            if (rel->has_filter()) {
                set_read_schema(rel->mutable_filter()->mutable_input(), mapping);
            } else if (rel->has_project()) {
                set_read_schema(rel->mutable_project()->mutable_input(), mapping);
            } else if (rel->has_sort()) {
                set_read_schema(rel->mutable_sort()->mutable_input(), mapping);
            } else if (rel->has_fetch()) {
                set_read_schema(rel->mutable_fetch()->mutable_input(), mapping);
            } else if (rel->has_aggregate()) {
                set_read_schema(rel->mutable_aggregate()->mutable_input(), mapping);
            }
        }

        void collect_update_fields(const expressions::update_expr_ptr& expr, field_mapping_t& mapping) {
            if (!expr) {
                return;
            }
            using expressions::update_expr_type;
            switch (expr->type()) {
                case update_expr_type::set:
                    mapping.get_or_add(static_cast<const expressions::update_expr_set_t*>(expr.get())->key());
                    break;
                case update_expr_type::get_value:
                    mapping.get_or_add(static_cast<const expressions::update_expr_get_value_t*>(expr.get())->key());
                    break;
                default:
                    break;
            }
            collect_update_fields(expr->left(), mapping);
            collect_update_fields(expr->right(), mapping);
        }

        std::string update_expr_function_name(expressions::update_expr_type type) {
            using expressions::update_expr_type;
            switch (type) {
                case update_expr_type::add:
                    return "add";
                case update_expr_type::sub:
                    return "subtract";
                case update_expr_type::mult:
                    return "multiply";
                case update_expr_type::div:
                    return "divide";
                case update_expr_type::mod:
                    return "mod";
                case update_expr_type::exp:
                    return "pow";
                case update_expr_type::sqr_root:
                    return "sqrt";
                case update_expr_type::cube_root:
                    return "cuberoot";
                case update_expr_type::factorial:
                    return "factorial";
                case update_expr_type::abs:
                    return "abs";
                case update_expr_type::AND:
                    return "bit_and";
                case update_expr_type::OR:
                    return "bit_or";
                case update_expr_type::XOR:
                    return "bit_xor";
                case update_expr_type::NOT:
                    return "bit_not";
                case update_expr_type::shift_left:
                    return "bit_shift_left";
                case update_expr_type::shift_right:
                    return "bit_shift_right";
                default:
                    return "";
            }
        }

        substrait::Expression* update_expr_to_substrait_expression(const expressions::update_expr_ptr& expr,
                                                                   field_mapping_t& mapping,
                                                                   to_substrait_context_t& ctx) {
            auto* out = new substrait::Expression();
            if (!expr) {
                return out;
            }
            using expressions::update_expr_type;
            switch (expr->type()) {
                case update_expr_type::set:
                    delete out;
                    return update_expr_to_substrait_expression(expr->left(), mapping, ctx);
                case update_expr_type::get_value: {
                    auto key = static_cast<const expressions::update_expr_get_value_t*>(expr.get())->key();
                    set_field_ref(out, mapping.get_or_add(key));
                    return out;
                }
                case update_expr_type::get_value_params: {
                    auto id = static_cast<const expressions::update_expr_get_const_value_t*>(expr.get())->id();
                    out->mutable_dynamic_parameter()->set_parameter_reference(static_cast<uint32_t>(id));
                    return out;
                }
                default: {
                    auto name = update_expr_function_name(expr->type());
                    auto* scalar = out->mutable_scalar_function();
                    scalar->set_function_reference(ctx.registry.register_function(name));
                    scalar->mutable_output_type()->CopyFrom(make_fp64_type());
                    if (expr->left()) {
                        auto* arg = scalar->mutable_arguments()->Add();
                        arg->set_allocated_value(update_expr_to_substrait_expression(expr->left(), mapping, ctx));
                    }
                    if (expr->right()) {
                        auto* arg = scalar->mutable_arguments()->Add();
                        arg->set_allocated_value(update_expr_to_substrait_expression(expr->right(), mapping, ctx));
                    }
                    return out;
                }
            }
        }

        substrait::Rel* build_ddl_rel(const node_ptr& node, to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* ddl = rel->mutable_ddl();
            auto* named = ddl->mutable_named_object();
            using components::logical_plan::node_type;
            switch (node->type()) {
                case node_type::create_database_t:
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_CREATE);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_object_extension(ddl, node->collection_full_name(), kDatabaseObjectKind, true);
                    break;
                case node_type::drop_database_t:
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_DROP);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_object_extension(ddl, node->collection_full_name(), kDatabaseObjectKind, false);
                    break;
                case node_type::create_collection_t:
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_CREATE);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_object_extension(ddl, node->collection_full_name(), kCollectionObjectKind, true);
                    break;
                case node_type::drop_collection_t:
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_DROP);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_object_extension(ddl, node->collection_full_name(), kCollectionObjectKind, false);
                    break;
                case node_type::create_index_t: {
                    auto idx = reinterpret_cast<const node_create_index_ptr&>(node);
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_CREATE);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_create_index_extension(ddl, idx);
                    if (!ctx.is_external_canonical()) {
                        named->add_names("__otterbrix_index__");
                        named->add_names("create");
                        named->add_names(idx->name());
                        named->add_names(std::to_string(static_cast<int>(idx->type())));
                        for (const auto& key : idx->keys()) {
                            named->add_names(key.as_string());
                        }
                    }
                    break;
                }
                case node_type::drop_index_t: {
                    auto idx = reinterpret_cast<const node_drop_index_ptr&>(node);
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_DROP);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
                    append_collection_name(named, node->collection_full_name());
                    set_ddl_drop_index_extension(ddl, idx);
                    if (!ctx.is_external_canonical()) {
                        named->add_names("__otterbrix_index__");
                        named->add_names("drop");
                        named->add_names(idx->name());
                    }
                    break;
                }
                case node_type::create_type_t: {
                    auto type_node = reinterpret_cast<const node_create_type_ptr&>(node);
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_CREATE);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_VIEW);
                    auto value = get_ddl_extension_struct(*ddl);
                    set_extension_contract(value, kDdlTypeContract);
                    value.mutable_fields()->operator[](kDdlActionKey).set_string_value(kCreateTypeAction);
                    value.mutable_fields()->operator[](kTypeNameKey).set_string_value(type_node->type().type_name());
                    const auto substrait_type = to_substrait_type(type_node->type());
                    std::string type_json;
                    const auto status = google::protobuf::util::MessageToJsonString(substrait_type, &type_json);
                    if (status.ok()) {
                        value.mutable_fields()->operator[](kTypeSubstraitJsonKey).set_string_value(type_json);
                    }
                    auto* field_names = value.mutable_fields()->operator[](kTypeFieldNamesKey).mutable_list_value();
                    field_names->clear_values();
                    for (const auto& child_type : type_node->type().child_types()) {
                        field_names->add_values()->set_string_value(child_type.has_alias() ? child_type.alias() : "");
                    }
                    ddl->mutable_advanced_extension()->mutable_enhancement()->PackFrom(value);
                    break;
                }
                case node_type::drop_type_t: {
                    auto type_node = reinterpret_cast<const node_drop_type_ptr&>(node);
                    ddl->set_op(substrait::DdlRel_DdlOp_DDL_OP_DROP);
                    ddl->set_object(substrait::DdlRel_DdlObject_DDL_OBJECT_VIEW);
                    auto value = get_ddl_extension_struct(*ddl);
                    set_extension_contract(value, kDdlTypeContract);
                    value.mutable_fields()->operator[](kDdlActionKey).set_string_value(kDropTypeAction);
                    value.mutable_fields()->operator[](kTypeNameKey).set_string_value(type_node->name());
                    ddl->mutable_advanced_extension()->mutable_enhancement()->PackFrom(value);
                    break;
                }
                default:
                    delete rel;
                    return unsupported_node_rel(node);
            }
            return rel;
        }

        substrait::Rel* build_write_rel(const node_ptr& node, field_mapping_t& mapping, to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* write = rel->mutable_write();
            auto* named = write->mutable_named_table();
            append_collection_name(named, node->collection_full_name());
            using components::logical_plan::node_type;
            switch (node->type()) {
                case node_type::insert_t: {
                    auto insert = reinterpret_cast<const node_insert_ptr&>(node);
                    write->set_op(substrait::WriteRel_WriteOp_WRITE_OP_INSERT);
                    if (!insert->children().empty() && insert->children().front()->type() == node_type::data_t) {
                        field_mapping_t input_mapping;
                        write->set_allocated_input(
                            build_data_read_rel(reinterpret_cast<const node_data_ptr&>(insert->children().front()),
                                                input_mapping));
                        return rel;
                    }
                    write->set_allocated_input(build_virtual_table_empty_rel(0));
                    return rel;
                }
                case node_type::delete_t: {
                    auto del = reinterpret_cast<const node_delete_ptr&>(node);
                    if (!del->collection_from().empty()) {
                        set_delete_collection_from_extension(write, del->collection_from());
                    }
                    write->set_op(substrait::WriteRel_WriteOp_WRITE_OP_DELETE);
                    substrait::Rel* input = del->collection_from().empty()
                                                ? build_read_rel(node, mapping, ctx)
                                                : build_read_rel_from_collection(del->collection_from(), mapping, ctx);
                    for (const auto& child : del->children()) {
                        if (child->type() == node_type::match_t) {
                            input = build_match_rel(reinterpret_cast<const node_match_ptr&>(child), input, mapping, ctx);
                        } else if (child->type() == node_type::limit_t) {
                            input = build_limit_rel(reinterpret_cast<const node_limit_ptr&>(child), input);
                        }
                    }
                    set_read_schema(input, mapping);
                    write->set_allocated_input(input);
                    return rel;
                }
                default:
                    break;
            }
            delete rel;
            return unsupported_node_rel(node);
        }

        substrait::Rel* build_update_rel(const node_ptr& node, field_mapping_t& mapping, to_substrait_context_t& ctx) {
            auto upd = reinterpret_cast<const node_update_ptr&>(node);
            auto* rel = new substrait::Rel();
            auto* update = rel->mutable_update();
            auto* named = update->mutable_named_table();
            append_collection_name(named, node->collection_full_name());
            if (upd->upsert()) {
                set_update_upsert_extension(update, true);
            }
            if (!upd->collection_from().empty()) {
                set_update_collection_from_extension(update, upd->collection_from());
            }
            if (upd->children().size() > 1 && upd->children().at(1)->type() == node_type::limit_t) {
                auto lim = reinterpret_cast<const node_limit_ptr&>(upd->children().at(1))->limit().limit();
                if (lim >= 0) {
                    set_update_limit_extension(update, static_cast<int64_t>(lim));
                }
            }

            for (const auto& expr : upd->updates()) {
                collect_update_fields(expr, mapping);
            }
            if (!upd->children().empty() && upd->children().at(0)->type() == node_type::match_t &&
                !upd->children().at(0)->expressions().empty()) {
                field_context_t fields{&mapping, nullptr, nullptr, 0};
                update->set_allocated_condition(
                    to_substrait_expression(upd->children().at(0)->expressions().front(), fields, ctx));
            }

            auto* schema = update->mutable_table_schema();
            apply_schema(schema, mapping);

            for (const auto& expr : upd->updates()) {
                if (!expr || expr->type() != expressions::update_expr_type::set) {
                    continue;
                }
                auto* set_expr = static_cast<const expressions::update_expr_set_t*>(expr.get());
                auto* t = update->add_transformations();
                t->set_column_target(mapping.get_or_add(set_expr->key()));
                t->set_allocated_transformation(update_expr_to_substrait_expression(set_expr->left(), mapping, ctx));
            }
            return rel;
        }

        substrait::Rel* build_match_rel(const node_match_ptr& match,
                                        substrait::Rel* input,
                                        field_mapping_t& mapping,
                                        to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* filter = rel->mutable_filter();
            filter->set_allocated_input(input);
            field_context_t fields{&mapping, nullptr, nullptr, 0};
            if (!match->expressions().empty()) {
                filter->set_allocated_condition(to_substrait_expression(match->expressions().front(), fields, ctx));
            }
            return rel;
        }

        substrait::Rel* build_sort_rel(const node_sort_ptr& sort,
                                       substrait::Rel* input,
                                       field_mapping_t& mapping,
                                       to_substrait_context_t& ctx) {
            auto* rel = new substrait::Rel();
            auto* s = rel->mutable_sort();
            s->set_allocated_input(input);
            field_context_t fields{&mapping, nullptr, nullptr, 0};
            for (const auto& expr : sort->expressions()) {
                auto* sort_field = s->mutable_sorts()->Add();
                auto* e = to_substrait_expression(expr, fields, ctx);
                sort_field->set_allocated_expr(e);
                sort_field->set_direction(static_cast<const expressions::sort_expression_t*>(expr.get())->order() ==
                                                  expressions::sort_order::desc
                                              ? substrait::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST
                                              : substrait::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
            }
            return rel;
        }

        substrait::Rel* build_limit_rel(const node_limit_ptr& limit, substrait::Rel* input) {
            auto* rel = new substrait::Rel();
            auto* fetch = rel->mutable_fetch();
            fetch->set_allocated_input(input);
            auto value = limit->limit().limit();
            if (value >= 0) {
                auto* count = fetch->mutable_count_expr();
                count->mutable_literal()->set_i64(static_cast<int64_t>(value));
            }
            return rel;
        }

        substrait::Rel* build_group_rel(const node_group_ptr& group,
                                        substrait::Rel* input,
                                        field_mapping_t& mapping,
                                        field_mapping_t* source_mapping,
                                        to_substrait_context_t& ctx) {
            using namespace components::expressions;
            bool has_aggregate = false;
            for (const auto& expr : group->expressions()) {
                if (expr->group() == expression_group::aggregate) {
                    has_aggregate = true;
                    break;
                }
            }
            field_context_t fields{&mapping, nullptr, nullptr, 0};
            if (!has_aggregate) {
                auto* rel = new substrait::Rel();
                auto* proj = rel->mutable_project();
                proj->set_allocated_input(input);
                for (const auto& expr : group->expressions()) {
                    proj->mutable_expressions()->AddAllocated(to_substrait_expression(expr, fields, ctx));
                }
                const auto input_size = static_cast<int32_t>(mapping.names.size());
                for (int32_t i = 0; i < static_cast<int32_t>(group->expressions().size()); ++i) {
                    proj->mutable_common()->mutable_emit()->add_output_mapping(input_size + i);
                }
                if (source_mapping) {
                    *source_mapping = mapping;
                }
                mapping = project_output_mapping(group->expressions(), fields);
                return rel;
            }
            auto* rel = new substrait::Rel();
            auto* agg = rel->mutable_aggregate();
            agg->set_allocated_input(input);
            substrait::AggregateRel_Grouping* grouping = nullptr;
            for (const auto& expr : group->expressions()) {
                if (expr->group() == expression_group::aggregate) {
                    auto* aggr = static_cast<const aggregate_expression_t*>(expr.get());
                    auto* measure = agg->mutable_measures()->Add();
                    auto* func = measure->mutable_measure();
                    func->set_function_reference(ctx.registry.register_function(
                        normalize_aggregate_function_name(aggr->function_name())));
                    if (aggr->is_distinct()) {
                        func->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_DISTINCT);
                    } else {
                        func->set_invocation(substrait::AggregateFunction::AGGREGATION_INVOCATION_ALL);
                    }
                    func->mutable_output_type()->CopyFrom(infer_expression_type(expr, fields));
                    for (const auto& param : aggr->params()) {
                        auto* arg = func->mutable_arguments()->Add();
                        arg->set_allocated_value(to_substrait_param(param, fields, ctx));
                    }
                } else if (expr->group() == expression_group::scalar) {
                    if (!grouping) {
                        grouping = agg->add_groupings();
                    }
                    const auto expression_ref = static_cast<uint32_t>(agg->grouping_expressions_size());
                    agg->mutable_grouping_expressions()->AddAllocated(to_substrait_expression(expr, fields, ctx));
                    grouping->add_expression_references(expression_ref);
                }
            }
            if (source_mapping) {
                *source_mapping = mapping;
            }
            mapping = aggregate_output_mapping(group->expressions(), fields);
            return rel;
        }

        substrait::Rel* build_join_rel(const node_join_ptr& join, field_mapping_t& mapping, to_substrait_context_t& ctx) {
            if (join->children().size() < 2) {
                return nullptr;
            }
            field_mapping_t left_mapping;
            field_mapping_t right_mapping;
            auto* left_rel = build_rel(join->children().at(0), left_mapping, ctx);
            auto* right_rel = build_rel(join->children().at(1), right_mapping, ctx);

            mapping.names = left_mapping.names;
            mapping.names.insert(mapping.names.end(), right_mapping.names.begin(), right_mapping.names.end());
            mapping.types = left_mapping.types;
            mapping.types.insert(mapping.types.end(), right_mapping.types.begin(), right_mapping.types.end());
            mapping.index.clear();
            for (size_t i = 0; i < mapping.names.size(); ++i) {
                mapping.index.emplace(mapping.names[i], static_cast<int32_t>(i));
            }

            auto* rel = new substrait::Rel();
            auto* j = rel->mutable_join();
            j->set_allocated_left(left_rel);
            j->set_allocated_right(right_rel);

            switch (join->type()) {
                case join_type::inner:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_INNER);
                    break;
                case join_type::left:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_LEFT);
                    break;
                case join_type::right:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT);
                    break;
                case join_type::full:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_OUTER);
                    break;
                case join_type::cross:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_INNER);
                    break;
                case join_type::invalid:
                    j->set_type(substrait::JoinRel_JoinType_JOIN_TYPE_UNSPECIFIED);
                    break;
            }

            field_context_t fields{&mapping, &left_mapping, &right_mapping, static_cast<int32_t>(left_mapping.names.size())};
            if (!join->expressions().empty()) {
                j->set_allocated_expression(to_substrait_expression(join->expressions().front(), fields, ctx));
            }
            set_read_schema(left_rel, left_mapping);
            set_read_schema(right_rel, right_mapping);
            return rel;
        }

        substrait::Rel* build_rel(const node_ptr& node, field_mapping_t& mapping, to_substrait_context_t& ctx) {
            using components::logical_plan::node_type;
            if (!node) {
                return nullptr;
            }
            switch (node->type()) {
                case node_type::aggregate_t: {
                    substrait::Rel* base = nullptr;
                    field_mapping_t source_mapping;
                    bool has_group = false;
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::join_t) {
                            base = build_join_rel(reinterpret_cast<const node_join_ptr&>(child), mapping, ctx);
                        }
                    }
                    if (!base) {
                        base = build_read_rel(node, mapping, ctx);
                    }
                    source_mapping = mapping;
                    // match -> group -> sort
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::match_t) {
                            base = build_match_rel(reinterpret_cast<const node_match_ptr&>(child), base, mapping, ctx);
                        }
                    }
                    source_mapping = mapping;
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::group_t) {
                            has_group = true;
                            base = build_group_rel(
                                reinterpret_cast<const node_group_ptr&>(child), base, mapping, &source_mapping, ctx);
                        }
                    }
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::sort_t) {
                            base = build_sort_rel(reinterpret_cast<const node_sort_ptr&>(child), base, mapping, ctx);
                        }
                    }
                    for (const auto& child : node->children()) {
                        if (child->type() == node_type::limit_t) {
                            base = build_limit_rel(reinterpret_cast<const node_limit_ptr&>(child), base);
                        }
                    }
                    if (!has_group) {
                        source_mapping = mapping;
                    }
                    set_read_schema(base, source_mapping);
                    return base;
                }
                case node_type::join_t:
                    return build_join_rel(reinterpret_cast<const node_join_ptr&>(node), mapping, ctx);
                case node_type::insert_t:
                case node_type::delete_t:
                    return build_write_rel(node, mapping, ctx);
                case node_type::update_t:
                    return build_update_rel(node, mapping, ctx);
                case node_type::match_t:
                case node_type::group_t:
                case node_type::sort_t:
                case node_type::limit_t:
                    return unsupported_node_rel(node);
                case node_type::create_collection_t:
                case node_type::create_database_t:
                case node_type::create_index_t:
                case node_type::drop_collection_t:
                case node_type::drop_database_t:
                case node_type::drop_index_t:
                case node_type::create_type_t:
                case node_type::drop_type_t:
                    return build_ddl_rel(node, ctx);
                case node_type::data_t:
                    return build_data_read_rel(reinterpret_cast<const node_data_ptr&>(node), mapping);
                case node_type::function_t:
                    return build_function_extension_single_rel(reinterpret_cast<const node_function_ptr&>(node));
                case node_type::alias_t:
                case node_type::unused:
                    throw std::runtime_error("unsupported node type in to_substrait: " +
                                             components::logical_plan::to_string(node->type()));
                default:
                    throw std::runtime_error("unsupported node type in to_substrait: " +
                                             components::logical_plan::to_string(node->type()));
            }
        }

        void validate_external_canonical_node(const node_ptr& node, bool is_root) {
            if (!node) {
                throw std::runtime_error("external canonical export: plan is null");
            }
            using components::logical_plan::node_type;
            switch (node->type()) {
                case node_type::aggregate_t: {
                    for (const auto& child : node->children()) {
                        switch (child->type()) {
                            case node_type::join_t:
                            case node_type::match_t:
                            case node_type::group_t:
                            case node_type::sort_t:
                            case node_type::limit_t:
                                break;
                            default:
                                throw std::runtime_error("external canonical export: aggregate contains unsupported child " +
                                                         components::logical_plan::to_string(child->type()));
                        }
                    }
                    for (const auto& child : node->children()) {
                        validate_external_canonical_node(child, false);
                    }
                    return;
                }
                case node_type::join_t:
                    for (const auto& child : node->children()) {
                        validate_external_canonical_node(child, false);
                    }
                    return;
                case node_type::create_collection_t:
                case node_type::create_database_t:
                case node_type::drop_collection_t:
                case node_type::drop_database_t:
                case node_type::insert_t:
                    return;
                case node_type::delete_t: {
                    return;
                }
                case node_type::update_t: {
                    return;
                }
                case node_type::match_t:
                case node_type::group_t:
                case node_type::sort_t:
                case node_type::limit_t:
                    if (is_root) {
                        throw std::runtime_error(
                            "external canonical export: root node is not representable: " +
                            components::logical_plan::to_string(node->type()));
                    }
                    return;
                case node_type::create_index_t:
                case node_type::drop_index_t:
                    return;
                case node_type::create_type_t:
                case node_type::data_t:
                case node_type::drop_type_t:
                case node_type::function_t:
                    return;
                case node_type::alias_t:
                case node_type::unused:
                default:
                    throw std::runtime_error("external canonical export: unsupported node type " +
                                             components::logical_plan::to_string(node->type()));
            }
        }

    } // namespace

    substrait::Plan to_substrait_plan(const node_ptr& plan) {
        return to_substrait_plan(plan, export_profile_t::internal_roundtrip, nullptr);
    }

    substrait::Plan to_substrait_plan(const node_ptr& plan, export_profile_t profile) {
        return to_substrait_plan(plan, profile, nullptr);
    }

    substrait::Plan to_substrait_plan(const node_ptr& plan, const components::catalog::catalog* catalog) {
        return to_substrait_plan(plan, export_profile_t::internal_roundtrip, catalog);
    }

    substrait::Plan
    to_substrait_plan(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog) {
        if (profile == export_profile_t::external_canonical) {
            validate_external_canonical_node(plan, true);
        }
        substrait::Plan result;
        auto* version = result.mutable_version();
        version->set_major_number(kSubstraitMajorVersion);
        version->set_minor_number(kSubstraitMinorVersion);
        version->set_patch_number(kSubstraitPatchVersion);
        version->set_producer(kSubstraitProducer);
        to_substrait_context_t ctx(&result, profile, catalog);
        field_mapping_t mapping;
        auto* rel = build_rel(plan, mapping, ctx);
        auto* root = result.mutable_relations()->Add()->mutable_root();
        root->set_allocated_input(rel);
        for (const auto& name : mapping.names) {
            root->add_names(name);
        }
        return result;
    }

    std::string to_substrait_binary(const node_ptr& plan) {
        return to_substrait_binary(plan, export_profile_t::internal_roundtrip, nullptr);
    }

    std::string to_substrait_binary(const node_ptr& plan, export_profile_t profile) {
        return to_substrait_binary(plan, profile, nullptr);
    }

    std::string to_substrait_binary(const node_ptr& plan, const components::catalog::catalog* catalog) {
        return to_substrait_binary(plan, export_profile_t::internal_roundtrip, catalog);
    }

    std::string
    to_substrait_binary(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog) {
        auto p = to_substrait_plan(plan, profile, catalog);
        return p.SerializeAsString();
    }

    std::string to_substrait_json(const node_ptr& plan) {
        return to_substrait_json(plan, export_profile_t::internal_roundtrip, nullptr);
    }

    std::string to_substrait_json(const node_ptr& plan, export_profile_t profile) {
        return to_substrait_json(plan, profile, nullptr);
    }

    std::string to_substrait_json(const node_ptr& plan, const components::catalog::catalog* catalog) {
        return to_substrait_json(plan, export_profile_t::internal_roundtrip, catalog);
    }

    std::string
    to_substrait_json(const node_ptr& plan, export_profile_t profile, const components::catalog::catalog* catalog) {
        auto p = to_substrait_plan(plan, profile, catalog);
        std::string out;
        const auto status = google::protobuf::util::MessageToJsonString(p, &out);
        if (!status.ok()) {
            throw std::runtime_error("Substrait JSON serialization failed");
        }
        return out;
    }

} // namespace components::logical_plan::substrait_adapter
