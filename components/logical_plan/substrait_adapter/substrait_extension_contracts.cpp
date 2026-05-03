#include "substrait_extension_contracts.hpp"
#include "substrait_adapter_types.hpp"
#include "substrait_type_mapping.hpp"

#include "node_create_index.hpp"
#include "node_drop_index.hpp"
#include "node_function.hpp"

#include "components/logical_plan/substrait_adapter/substrait/algebra.pb.h"
#include "components/logical_plan/substrait_adapter/substrait/type.pb.h"

#include <boost/json/src.hpp>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>

#include <algorithm>
#include <cmath>
#include <variant>

namespace components::logical_plan::substrait_adapter {

    namespace {
        bool extract_extension_struct(const google::protobuf::Any& any,
                                      google::protobuf::Struct& ext,
                                      const char* expected_contract) {
            if (!any.UnpackTo(&ext)) {
                return false;
            }
            const auto contract_it = ext.fields().find(kContractKey);
            if (contract_it == ext.fields().end()) {
                return true;
            }
            if (contract_it->second.kind_case() != google::protobuf::Value::kStringValue ||
                contract_it->second.string_value() != expected_contract) {
                return false;
            }
            const auto version_it = ext.fields().find(kContractVersionKey);
            if (version_it == ext.fields().end()) {
                return true;
            }
            if (version_it->second.kind_case() != google::protobuf::Value::kNumberValue) {
                return false;
            }
            return std::abs(version_it->second.number_value() - kContractVersion) < 1e-9;
        }

        std::string parse_string_field(const google::protobuf::Struct& ext, const std::string& key) {
            const auto it = ext.fields().find(key);
            if (it == ext.fields().end() || it->second.kind_case() != google::protobuf::Value::kStringValue) {
                return {};
            }
            return it->second.string_value();
        }

        void set_collection_extension(google::protobuf::Struct& ext, const collection_full_name_t& collection) {
            if (!collection.unique_identifier.empty()) {
                ext.mutable_fields()->operator[](kCollectionUidKey).set_string_value(collection.unique_identifier);
            }
            if (!collection.database.empty()) {
                ext.mutable_fields()->operator[](kCollectionDatabaseKey).set_string_value(collection.database);
            }
            if (!collection.schema.empty()) {
                ext.mutable_fields()->operator[](kCollectionSchemaKey).set_string_value(collection.schema);
            }
            if (!collection.collection.empty()) {
                ext.mutable_fields()->operator[](kCollectionNameKey).set_string_value(collection.collection);
            }
        }

        collection_full_name_t parse_collection_extension(const google::protobuf::Struct& ext) {
            collection_full_name_t collection;
            collection.unique_identifier = parse_string_field(ext, kCollectionUidKey);
            collection.database = parse_string_field(ext, kCollectionDatabaseKey);
            collection.schema = parse_string_field(ext, kCollectionSchemaKey);
            collection.collection = parse_string_field(ext, kCollectionNameKey);
            return collection;
        }

        google::protobuf::Struct get_update_extension_struct(const substrait::UpdateRel& update) {
            google::protobuf::Struct result;
            if (update.has_advanced_extension() && update.advanced_extension().has_enhancement()) {
                const auto& any = update.advanced_extension().enhancement();
                google::protobuf::Struct stored_struct;
                if (any.UnpackTo(&stored_struct)) {
                    set_extension_contract(stored_struct, kUpdateContract);
                    return stored_struct;
                }
            }
            set_extension_contract(result, kUpdateContract);
            return result;
        }

        google::protobuf::Struct get_write_extension_struct(const substrait::WriteRel& write) {
            google::protobuf::Struct result;
            if (write.has_advanced_extension() && write.advanced_extension().has_enhancement()) {
                const auto& any = write.advanced_extension().enhancement();
                google::protobuf::Struct stored_struct;
                if (any.UnpackTo(&stored_struct)) {
                    set_extension_contract(stored_struct, kDeleteContract);
                    return stored_struct;
                }
            }
            set_extension_contract(result, kDeleteContract);
            return result;
        }
    } // namespace

    void set_extension_contract(google::protobuf::Struct& ext, const std::string& contract) {
        ext.mutable_fields()->operator[](kContractKey).set_string_value(contract);
        ext.mutable_fields()->operator[](kContractVersionKey).set_number_value(kContractVersion);
    }

    google::protobuf::Struct get_ddl_extension_struct(const substrait::DdlRel& ddl) {
        google::protobuf::Struct result;
        if (ddl.has_advanced_extension() && ddl.advanced_extension().has_enhancement()) {
            const auto& any = ddl.advanced_extension().enhancement();
            google::protobuf::Struct stored_struct;
            if (any.UnpackTo(&stored_struct)) {
                return stored_struct;
            }
        }
        return result;
    }

    void set_function_args_extension(google::protobuf::Struct& ext,
                                     const std::pmr::vector<expressions::param_storage>& args) {
        auto* list = ext.mutable_fields()->operator[](kFunctionArgsKey).mutable_list_value();
        list->clear_values();
        for (const auto& arg : args) {
            auto* item = list->add_values()->mutable_struct_value();
            std::visit(
                [&](const auto& value) {
                    using value_t = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<value_t, core::parameter_id_t>) {
                        item->mutable_fields()->operator[](kFunctionArgKindKey)
                            .set_string_value(kFunctionArgParameterIdKind);
                        item->mutable_fields()->operator[](kFunctionArgValueKey)
                            .set_number_value(static_cast<double>(static_cast<uint16_t>(value)));
                    } else if constexpr (std::is_same_v<value_t, expressions::key_t>) {
                        item->mutable_fields()->operator[](kFunctionArgKindKey).set_string_value(kFunctionArgFieldKeyKind);
                        item->mutable_fields()->operator[](kFunctionArgValueKey).set_string_value(value.as_string());
                    } else if constexpr (std::is_same_v<value_t, expressions::expression_ptr>) {
                        item->mutable_fields()->operator[](kFunctionArgKindKey).set_string_value("unsupported_expression");
                    }
                },
                arg);
        }
    }

    void set_update_upsert_extension(substrait::UpdateRel* update, bool upsert) {
        if (!update) {
            return;
        }
        auto value = get_update_extension_struct(*update);
        value.mutable_fields()->operator[]("otterbrix_update_upsert").set_bool_value(upsert);
        auto* any = update->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_update_limit_extension(substrait::UpdateRel* update, int64_t limit) {
        if (!update || limit < 0) {
            return;
        }
        auto value = get_update_extension_struct(*update);
        value.mutable_fields()->operator[]("otterbrix_update_limit").set_number_value(static_cast<double>(limit));
        auto* any = update->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_update_collection_from_extension(substrait::UpdateRel* update,
                                              const collection_full_name_t& collection_from) {
        if (!update || collection_from.empty()) {
            return;
        }
        auto value = get_update_extension_struct(*update);
        if (!collection_from.unique_identifier.empty()) {
            value.mutable_fields()->operator[]("otterbrix_update_collection_from_uid")
                .set_string_value(collection_from.unique_identifier);
        }
        if (!collection_from.database.empty()) {
            value.mutable_fields()->operator[]("otterbrix_update_collection_from_db").set_string_value(collection_from.database);
        }
        if (!collection_from.schema.empty()) {
            value.mutable_fields()->operator[]("otterbrix_update_collection_from_schema").set_string_value(collection_from.schema);
        }
        if (!collection_from.collection.empty()) {
            value.mutable_fields()->operator[]("otterbrix_update_collection_from_name")
                .set_string_value(collection_from.collection);
        }
        auto* any = update->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_delete_collection_from_extension(substrait::WriteRel* write,
                                              const collection_full_name_t& collection_from) {
        if (!write || collection_from.empty()) {
            return;
        }
        auto value = get_write_extension_struct(*write);
        if (!collection_from.unique_identifier.empty()) {
            value.mutable_fields()->operator[]("otterbrix_delete_collection_from_uid")
                .set_string_value(collection_from.unique_identifier);
        }
        if (!collection_from.database.empty()) {
            value.mutable_fields()->operator[]("otterbrix_delete_collection_from_db").set_string_value(collection_from.database);
        }
        if (!collection_from.schema.empty()) {
            value.mutable_fields()->operator[]("otterbrix_delete_collection_from_schema").set_string_value(collection_from.schema);
        }
        if (!collection_from.collection.empty()) {
            value.mutable_fields()->operator[]("otterbrix_delete_collection_from_name")
                .set_string_value(collection_from.collection);
        }
        auto* any = write->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_ddl_object_extension(substrait::DdlRel* ddl,
                                  const collection_full_name_t& collection,
                                  const std::string& object_kind,
                                  bool create) {
        if (!ddl || collection.empty() || object_kind.empty()) {
            return;
        }
        auto value = get_ddl_extension_struct(*ddl);
        set_extension_contract(value, kDdlObjectContract);
        value.mutable_fields()->operator[](kObjectKindKey).set_string_value(object_kind);
        value.mutable_fields()->operator[](kDdlActionKey).set_string_value(create ? kCreateObjectAction : kDropObjectAction);
        set_collection_extension(value, collection);
        auto* any = ddl->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_ddl_create_index_extension(substrait::DdlRel* ddl, const node_create_index_ptr& idx) {
        if (!ddl || !idx) {
            return;
        }
        auto value = get_ddl_extension_struct(*ddl);
        set_extension_contract(value, kDdlIndexContract);
        value.mutable_fields()->operator[](kObjectKindKey).set_string_value(kIndexObjectKind);
        value.mutable_fields()->operator[](kIndexActionKey).set_string_value("create");
        value.mutable_fields()->operator[](kIndexNameKey).set_string_value(idx->name());
        value.mutable_fields()->operator[](kIndexTypeKey).set_number_value(static_cast<int>(idx->type()));
        set_collection_extension(value, idx->collection_full_name());
        auto* keys = value.mutable_fields()->operator[](kIndexKeysKey).mutable_list_value();
        keys->clear_values();
        for (const auto& key : idx->keys()) {
            keys->add_values()->set_string_value(key.as_string());
        }
        auto* any = ddl->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    void set_ddl_drop_index_extension(substrait::DdlRel* ddl, const node_drop_index_ptr& idx) {
        if (!ddl || !idx) {
            return;
        }
        auto value = get_ddl_extension_struct(*ddl);
        set_extension_contract(value, kDdlIndexContract);
        value.mutable_fields()->operator[](kObjectKindKey).set_string_value(kIndexObjectKind);
        value.mutable_fields()->operator[](kIndexActionKey).set_string_value("drop");
        value.mutable_fields()->operator[](kIndexNameKey).set_string_value(idx->name());
        set_collection_extension(value, idx->collection_full_name());
        auto* any = ddl->mutable_advanced_extension()->mutable_enhancement();
        any->PackFrom(value);
    }

    bool parse_update_upsert_extension(const substrait::UpdateRel& update) {
        if (!update.has_advanced_extension() || !update.advanced_extension().has_enhancement()) {
            return false;
        }
        const auto& any = update.advanced_extension().enhancement();
        google::protobuf::Struct ext;
        if (extract_extension_struct(any, ext, kUpdateContract)) {
            const auto it = ext.fields().find("otterbrix_update_upsert");
            if (it != ext.fields().end() && it->second.kind_case() == google::protobuf::Value::kBoolValue) {
                return it->second.bool_value();
            }
            return false;
        }
        return false;
    }

    int parse_update_limit_extension(const substrait::UpdateRel& update, int fallback) {
        if (!update.has_advanced_extension() || !update.advanced_extension().has_enhancement()) {
            return fallback;
        }
        const auto& any = update.advanced_extension().enhancement();
        google::protobuf::Struct ext;
        if (!extract_extension_struct(any, ext, kUpdateContract)) {
            return fallback;
        }
        const auto it = ext.fields().find("otterbrix_update_limit");
        if (it == ext.fields().end() || it->second.kind_case() != google::protobuf::Value::kNumberValue) {
            return fallback;
        }
        return static_cast<int>(it->second.number_value());
    }

    collection_full_name_t parse_delete_collection_from_extension(const substrait::WriteRel& write) {
        collection_full_name_t collection_from;
        if (!write.has_advanced_extension() || !write.advanced_extension().has_enhancement()) {
            return collection_from;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(write.advanced_extension().enhancement(), ext, kDeleteContract)) {
            return collection_from;
        }
        collection_from.unique_identifier = parse_string_field(ext, "otterbrix_delete_collection_from_uid");
        collection_from.database = parse_string_field(ext, "otterbrix_delete_collection_from_db");
        collection_from.schema = parse_string_field(ext, "otterbrix_delete_collection_from_schema");
        collection_from.collection = parse_string_field(ext, "otterbrix_delete_collection_from_name");
        return collection_from;
    }

    collection_full_name_t parse_update_collection_from_extension(const substrait::UpdateRel& update) {
        collection_full_name_t collection_from;
        if (!update.has_advanced_extension() || !update.advanced_extension().has_enhancement()) {
            return collection_from;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(update.advanced_extension().enhancement(), ext, kUpdateContract)) {
            return collection_from;
        }
        collection_from.unique_identifier = parse_string_field(ext, "otterbrix_update_collection_from_uid");
        collection_from.database = parse_string_field(ext, "otterbrix_update_collection_from_db");
        collection_from.schema = parse_string_field(ext, "otterbrix_update_collection_from_schema");
        collection_from.collection = parse_string_field(ext, "otterbrix_update_collection_from_name");
        return collection_from;
    }

    ddl_object_extension_t parse_ddl_object_extension(const substrait::DdlRel& ddl) {
        ddl_object_extension_t out;
        if (!ddl.has_advanced_extension() || !ddl.advanced_extension().has_enhancement()) {
            return out;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(ddl.advanced_extension().enhancement(), ext, kDdlObjectContract)) {
            return out;
        }
        const auto object_kind = parse_string_field(ext, kObjectKindKey);
        if (object_kind != kDatabaseObjectKind && object_kind != kCollectionObjectKind) {
            return out;
        }
        const auto action = parse_string_field(ext, kDdlActionKey);
        if (action != kCreateObjectAction && action != kDropObjectAction) {
            return out;
        }
        auto collection = parse_collection_extension(ext);
        if (collection.empty()) {
            return out;
        }
        out.valid = true;
        out.create = action == kCreateObjectAction;
        out.object_kind = object_kind;
        out.collection = std::move(collection);
        return out;
    }

    ddl_index_extension_t parse_ddl_index_extension(const substrait::DdlRel& ddl) {
        ddl_index_extension_t out;
        if (!ddl.has_advanced_extension() || !ddl.advanced_extension().has_enhancement()) {
            return out;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(ddl.advanced_extension().enhancement(), ext, kDdlIndexContract)) {
            return out;
        }
        const auto object_kind = parse_string_field(ext, kObjectKindKey);
        if (!object_kind.empty() && object_kind != kIndexObjectKind) {
            return out;
        }
        auto action = parse_string_field(ext, kIndexActionKey);
        if (action != "create" && action != "drop") {
            return out;
        }
        auto name = parse_string_field(ext, kIndexNameKey);
        if (name.empty()) {
            return out;
        }
        out.valid = true;
        out.create = action == "create";
        out.name = std::move(name);
        out.collection = parse_collection_extension(ext);
        const auto type_it = ext.fields().find(kIndexTypeKey);
        if (type_it != ext.fields().end() && type_it->second.kind_case() == google::protobuf::Value::kNumberValue) {
            out.type = static_cast<int>(type_it->second.number_value());
        }
        const auto keys_it = ext.fields().find(kIndexKeysKey);
        if (keys_it != ext.fields().end() && keys_it->second.kind_case() == google::protobuf::Value::kListValue) {
            for (const auto& value : keys_it->second.list_value().values()) {
                if (value.kind_case() == google::protobuf::Value::kStringValue) {
                    out.keys.emplace_back(value.string_value());
                }
            }
        }
        return out;
    }

    ddl_type_extension_t parse_ddl_type_extension(std::pmr::memory_resource* resource, const substrait::DdlRel& ddl) {
        (void) resource;
        ddl_type_extension_t out;
        if (!ddl.has_advanced_extension() || !ddl.advanced_extension().has_enhancement()) {
            return out;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(ddl.advanced_extension().enhancement(), ext, kDdlTypeContract)) {
            return out;
        }
        const auto action = parse_string_field(ext, kDdlActionKey);
        if (action == kCreateTypeAction) {
            const auto type_name = parse_string_field(ext, kTypeNameKey);
            if (type_name.empty()) {
                return out;
            }
            out.valid = true;
            out.create = true;
            out.name = type_name;
            const auto type_json_it = ext.fields().find(kTypeSubstraitJsonKey);
            if (type_json_it != ext.fields().end() &&
                type_json_it->second.kind_case() == google::protobuf::Value::kStringValue) {
                substrait::Type substrait_type;
                const auto status =
                    google::protobuf::util::JsonStringToMessage(type_json_it->second.string_value(), &substrait_type);
                if (status.ok()) {
                    auto parsed_type = from_substrait_type(substrait_type, type_name);
                    if (parsed_type.type() == types::logical_type::STRUCT) {
                        auto children = parsed_type.child_types();
                        const auto field_names_it = ext.fields().find(kTypeFieldNamesKey);
                        if (field_names_it != ext.fields().end() &&
                            field_names_it->second.kind_case() == google::protobuf::Value::kListValue) {
                            const auto& names = field_names_it->second.list_value().values();
                            const auto count = std::min(children.size(), static_cast<size_t>(names.size()));
                            for (size_t i = 0; i < count; ++i) {
                                if (names.Get(static_cast<int>(i)).kind_case() == google::protobuf::Value::kStringValue &&
                                    !names.Get(static_cast<int>(i)).string_value().empty()) {
                                    children[i].set_alias(names.Get(static_cast<int>(i)).string_value());
                                }
                            }
                        }
                        out.type = types::complex_logical_type::create_struct(type_name, children);
                    } else {
                        out.type = std::move(parsed_type);
                    }
                    return out;
                }
            }
            out.type = types::complex_logical_type::create_unknown(type_name);
            return out;
        }
        if (action == kDropTypeAction) {
            const auto name = parse_string_field(ext, kTypeNameKey);
            if (name.empty()) {
                return out;
            }
            out.valid = true;
            out.create = false;
            out.name = name;
        }
        return out;
    }

    node_ptr parse_extension_single_struct(std::pmr::memory_resource* resource, const substrait::ExtensionSingleRel& rel) {
        if (!rel.has_detail()) {
            return nullptr;
        }
        google::protobuf::Struct ext;
        if (!extract_extension_struct(rel.detail(), ext, kFunctionContract)) {
            return nullptr;
        }
        const auto kind = parse_string_field(ext, kNodeKindKey);
        if (kind != kFunctionNodeKind) {
            return nullptr;
        }
        auto name = parse_string_field(ext, kFunctionNameKey);
        if (name.empty()) {
            return nullptr;
        }
        std::pmr::vector<expressions::param_storage> args(resource);
        const auto args_it = ext.fields().find(kFunctionArgsKey);
        if (args_it != ext.fields().end()) {
            if (args_it->second.kind_case() != google::protobuf::Value::kListValue) {
                return nullptr;
            }
            const auto& values = args_it->second.list_value().values();
            args.reserve(static_cast<size_t>(values.size()));
            for (const auto& item_value : values) {
                if (item_value.kind_case() != google::protobuf::Value::kStructValue) {
                    return nullptr;
                }
                const auto& item = item_value.struct_value();
                const auto kind = parse_string_field(item, kFunctionArgKindKey);
                const auto value_it = item.fields().find(kFunctionArgValueKey);
                if (kind == kFunctionArgParameterIdKind && value_it != item.fields().end() &&
                    value_it->second.kind_case() == google::protobuf::Value::kNumberValue) {
                    args.emplace_back(core::parameter_id_t(static_cast<uint16_t>(value_it->second.number_value())));
                } else if (kind == kFunctionArgFieldKeyKind && value_it != item.fields().end() &&
                           value_it->second.kind_case() == google::protobuf::Value::kStringValue) {
                    args.emplace_back(expressions::key_t(resource, value_it->second.string_value()));
                } else {
                    return nullptr;
                }
            }
        }
        return make_node_function(resource, std::move(name), std::move(args));
    }

} // namespace components::logical_plan::substrait_adapter
