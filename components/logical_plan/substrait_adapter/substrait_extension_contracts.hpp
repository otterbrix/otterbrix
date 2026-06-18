#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/types/types.hpp>

#include <string>
#include <vector>

namespace google::protobuf {
    class Struct;
}

namespace substrait {
    class DdlRel;
    class ExtensionSingleRel;
    class UpdateRel;
    class WriteRel;
}

namespace components::logical_plan {
    using node_create_index_ptr = boost::intrusive_ptr<class node_create_index_t>;
    using node_drop_index_ptr = boost::intrusive_ptr<class node_drop_index_t>;
}

namespace components::logical_plan::substrait_adapter {

    struct ddl_index_extension_t {
        bool valid = false;
        bool create = false;
        std::string name;
        int type = 0;
        std::vector<std::string> keys;
        collection_full_name_t collection;
    };

    struct ddl_object_extension_t {
        bool valid = false;
        bool create = false;
        std::string object_kind;
        collection_full_name_t collection;
    };

    struct ddl_type_extension_t {
        bool valid = false;
        bool create = false;
        std::string name;
        types::complex_logical_type type;
    };

    void set_extension_contract(google::protobuf::Struct& ext, const std::string& contract);
    google::protobuf::Struct get_ddl_extension_struct(const substrait::DdlRel& ddl);
    void set_function_args_extension(google::protobuf::Struct& ext,
                                     const std::pmr::vector<expressions::param_storage>& args);

    void set_update_upsert_extension(substrait::UpdateRel* update, bool upsert);
    void set_update_limit_extension(substrait::UpdateRel* update, int64_t limit);
    void set_update_collection_from_extension(substrait::UpdateRel* update,
                                              const collection_full_name_t& collection_from);
    void set_delete_collection_from_extension(substrait::WriteRel* write,
                                              const collection_full_name_t& collection_from);
    void set_ddl_object_extension(substrait::DdlRel* ddl,
                                  const collection_full_name_t& collection,
                                  const std::string& object_kind,
                                  bool create);
    void set_ddl_create_index_extension(substrait::DdlRel* ddl, const node_create_index_ptr& idx);
    void set_ddl_drop_index_extension(substrait::DdlRel* ddl, const node_drop_index_ptr& idx);

    bool parse_update_upsert_extension(const substrait::UpdateRel& update);
    int parse_update_limit_extension(const substrait::UpdateRel& update, int fallback);
    collection_full_name_t parse_delete_collection_from_extension(const substrait::WriteRel& write);
    collection_full_name_t parse_update_collection_from_extension(const substrait::UpdateRel& update);
    ddl_object_extension_t parse_ddl_object_extension(const substrait::DdlRel& ddl);
    ddl_index_extension_t parse_ddl_index_extension(const substrait::DdlRel& ddl);
    ddl_type_extension_t parse_ddl_type_extension(std::pmr::memory_resource* resource, const substrait::DdlRel& ddl);
    node_ptr parse_extension_single_struct(std::pmr::memory_resource* resource, const substrait::ExtensionSingleRel& rel);

} // namespace components::logical_plan::substrait_adapter
