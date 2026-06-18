#pragma once

#include "substrait_type_mapping.hpp"

#include <components/expressions/key.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace components::logical_plan::substrait_adapter {

    inline constexpr const char* kExtensionUrn = "urn:otterbrix:substrait:functions";
    inline constexpr uint32_t kExtensionUrnAnchor = 1;
    inline constexpr uint32_t kSubstraitMajorVersion = 0;
    inline constexpr uint32_t kSubstraitMinorVersion = 57;
    inline constexpr uint32_t kSubstraitPatchVersion = 1;
    inline constexpr const char* kSubstraitProducer = "otterbrix";
    inline constexpr const char* kContractKey = "otterbrix_contract";
    inline constexpr const char* kContractVersionKey = "otterbrix_contract_version";
    inline constexpr const char* kDdlActionKey = "otterbrix_ddl_action";
    inline constexpr const char* kCreateTypeAction = "create_type";
    inline constexpr const char* kDropTypeAction = "drop_type";
    inline constexpr const char* kTypeNameKey = "otterbrix_type_name";
    inline constexpr const char* kTypeSubstraitJsonKey = "otterbrix_type_substrait_json";
    inline constexpr const char* kTypeFieldNamesKey = "otterbrix_type_field_names";
    inline constexpr const char* kObjectKindKey = "otterbrix_object_kind";
    inline constexpr const char* kCreateObjectAction = "create";
    inline constexpr const char* kDropObjectAction = "drop";
    inline constexpr const char* kDatabaseObjectKind = "database";
    inline constexpr const char* kCollectionObjectKind = "collection";
    inline constexpr const char* kIndexObjectKind = "index";
    inline constexpr const char* kIndexActionKey = "otterbrix_index_action";
    inline constexpr const char* kIndexNameKey = "otterbrix_index_name";
    inline constexpr const char* kIndexTypeKey = "otterbrix_index_type";
    inline constexpr const char* kIndexKeysKey = "otterbrix_index_keys";
    inline constexpr const char* kCollectionUidKey = "otterbrix_collection_uid";
    inline constexpr const char* kCollectionDatabaseKey = "otterbrix_collection_database";
    inline constexpr const char* kCollectionSchemaKey = "otterbrix_collection_schema";
    inline constexpr const char* kCollectionNameKey = "otterbrix_collection_name";
    inline constexpr const char* kNodeKindKey = "otterbrix_node_kind";
    inline constexpr const char* kFunctionNodeKind = "function";
    inline constexpr const char* kFunctionNameKey = "otterbrix_function_name";
    inline constexpr const char* kFunctionArgsKey = "otterbrix_function_args";
    inline constexpr const char* kFunctionArgKindKey = "kind";
    inline constexpr const char* kFunctionArgValueKey = "value";
    inline constexpr const char* kFunctionArgParameterIdKind = "parameter_id";
    inline constexpr const char* kFunctionArgFieldKeyKind = "key";
    inline constexpr const char* kUpdateContract = "otterbrix.update";
    inline constexpr const char* kDeleteContract = "otterbrix.delete";
    inline constexpr const char* kDdlIndexContract = "otterbrix.ddl.index";
    inline constexpr const char* kDdlObjectContract = "otterbrix.ddl.object";
    inline constexpr const char* kDdlTypeContract = "otterbrix.ddl.type";
    inline constexpr const char* kFunctionContract = "otterbrix.extension.function";
    inline constexpr double kContractVersion = 1.0;

    struct field_mapping_t {
        std::vector<std::string> names;
        std::vector<substrait::Type> types;
        std::unordered_map<std::string, int32_t> index;
        int32_t left_size = -1;

        int32_t get_or_add(const expressions::key_t& key);
        int32_t get_or_add(const std::string& name);
        int32_t get_or_add(const std::string& name, const substrait::Type& type);
        bool contains(int32_t idx) const;
        const std::string& name_or_empty(int32_t idx) const;
        void set_type(int32_t idx, const substrait::Type& type);
        substrait::Type type_or_default(int32_t idx) const;
    };

} // namespace components::logical_plan::substrait_adapter
