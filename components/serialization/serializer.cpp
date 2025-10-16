#include "serializer.hpp"

#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/expressions/key.hpp>

namespace components::serializer {

    base_serializer_t::base_serializer_t(std::pmr::memory_resource* resource)
        : result_(std::pmr::string(resource)) {}

    std::pmr::string base_serializer_t::result() const { return result_.str(); }

    void base_serializer_t::append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes) {
        start_array(nodes.size());
        for (const auto& n : nodes) {
            n->serialize(this);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents) {
        start_array(documents.size());
        for (const auto& doc : documents) {
            append(key, doc);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys) {
        start_array(keys.size());
        for (const auto& k : keys) {
            append(key, k);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params) {
        start_array(params.size());
        for (const auto& id : params) {
            append(key, id);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key,
                                   const std::pmr::vector<expressions::expression_ptr>& expressions) {
        start_array(expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key,
                                   const std::pmr::vector<expressions::update_expr_ptr>& expressions) {
        start_array(expressions.size());
        for (const auto& expr : expressions) {
            expr->serialize(this);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const std::pmr::vector<expressions::param_storage>& params) {
        start_array(params.size());
        for (const auto& p : params) {
            append(key, p);
        }
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const collection_full_name_t& collection) {
        start_array(2);
        append("database", collection.database);
        append("collection", collection.collection);
        end_array();
    }

    void base_serializer_t::append(std::string_view key, const expressions::param_storage& param) {
        std::visit(
            [&](const auto& value) {
                using param_type = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                    append(key, value);
                } else if constexpr (std::is_same_v<param_type, expressions::key_t>) {
                    append(key, value);
                } else if constexpr (std::is_same_v<param_type, expressions::expression_ptr>) {
                    value->serialize(this);
                } else {
                    assert(false);
                }
            },
            param);
    }

    void base_serializer_t::append(std::string_view key, const logical_plan::node_ptr& node) { node->serialize(this); }

    void base_serializer_t::append(std::string_view key, const expressions::expression_ptr& expr) {
        expr->serialize(this);
    }

    void base_serializer_t::append(std::string_view key, const expressions::update_expr_ptr& expr) {
        expr->serialize(this);
    }

    // TODO: use memory_resource to initialize root_arr_
    json_serializer_t::json_serializer_t(std::pmr::memory_resource* resource)
        : base_serializer_t(resource) {}

    void json_serializer_t::start_array(size_t size) {
        if (working_tree_.empty()) {
            working_tree_.emplace(&root_arr_);
        } else {
            working_tree_.top()->emplace_back(boost::json::array());
            working_tree_.emplace(&working_tree_.top()->back().as_array());
        }
        working_tree_.top()->reserve(size);
    }

    void json_serializer_t::end_array() {
        if (working_tree_.size() == 1) {
            result_ << root_arr_;
        } else {
            working_tree_.pop();
        }
    }

    void json_serializer_t::append(std::string_view key, bool val) { working_tree_.top()->emplace_back(val); }

    void json_serializer_t::append(std::string_view key, int64_t val) { working_tree_.top()->emplace_back(val); }

    void json_serializer_t::append(std::string_view key, uint64_t val) { working_tree_.top()->emplace_back(val); }

    void json_serializer_t::append(std::string_view key, core::parameter_id_t val) {
        working_tree_.top()->emplace_back(val.t);
    }

    void json_serializer_t::append(std::string_view key, serialization_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, logical_plan::node_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, logical_plan::index_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, logical_plan::join_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, expressions::aggregate_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, expressions::compare_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, expressions::scalar_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, expressions::sort_order order) {
        working_tree_.top()->emplace_back(static_cast<int8_t>(order));
    }

    void json_serializer_t::append(std::string_view key, expressions::update_expr_type type) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(type));
    }

    void json_serializer_t::append(std::string_view key, expressions::side_t side) {
        working_tree_.top()->emplace_back(static_cast<uint8_t>(side));
    }

    void json_serializer_t::append(std::string_view key, const std::string& str) {
        working_tree_.top()->emplace_back(str);
    }

    void json_serializer_t::append(std::string_view key, const document_ptr& doc) {
        working_tree_.top()->emplace_back(doc->to_json());
    }

    void json_serializer_t::append(std::string_view key, const document::value_t& val) {
        switch (val.physical_type()) {
            case types::physical_type::BOOL:
                working_tree_.top()->emplace_back(val.as_bool());
                break;
            case types::physical_type::UINT8:
            case types::physical_type::UINT16:
            case types::physical_type::UINT32:
            case types::physical_type::UINT64:
                working_tree_.top()->emplace_back(val.as_unsigned());
                break;
            case types::physical_type::INT8:
            case types::physical_type::INT16:
            case types::physical_type::INT32:
            case types::physical_type::INT64:
                working_tree_.top()->emplace_back(val.as_int());
                break;
            // case types::physical_type::UINT128:
            // case types::physical_type::INT128:
            //     working_tree_.top()->emplace_back(val.as_int128());
            //     break;
            case types::physical_type::FLOAT:
                working_tree_.top()->emplace_back(val.as_float());
                break;
            case types::physical_type::DOUBLE:
                working_tree_.top()->emplace_back(val.as_double());
                break;
            case types::physical_type::STRING:
                working_tree_.top()->emplace_back(val.as_string());
                break;
            case types::physical_type::NA:
                working_tree_.top()->emplace_back(nullptr);
                break;
            default:
                assert(false && "incorrect type");
        }
    }

    void json_serializer_t::append(std::string_view key, const expressions::key_t& key_val) {
        if (key_val.is_string()) {
            working_tree_.top()->emplace_back(key_val.as_string());
        } else if (key_val.is_int()) {
            working_tree_.top()->emplace_back(key_val.as_int());
        } else if (key_val.is_uint()) {
            working_tree_.top()->emplace_back(key_val.as_uint());
        } else {
            working_tree_.top()->emplace_back(nullptr);
        }
    }

    msgpack_serializer_t::msgpack_serializer_t(std::pmr::memory_resource* resource)
        : base_serializer_t(resource)
        , packer_(result_) {}

    void msgpack_serializer_t::start_array(size_t size) { packer_.pack_array(size); }

    void msgpack_serializer_t::end_array() {
        // nothing to do here
    }

    void msgpack_serializer_t::append(std::string_view key, bool val) { packer_.pack(val); }

    void msgpack_serializer_t::append(std::string_view key, int64_t val) { packer_.pack(val); }

    void msgpack_serializer_t::append(std::string_view key, uint64_t val) { packer_.pack(val); }

    void msgpack_serializer_t::append(std::string_view key, core::parameter_id_t val) { packer_.pack(val.t); }

    void msgpack_serializer_t::append(std::string_view key, serialization_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::node_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::index_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, logical_plan::join_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::aggregate_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::compare_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::scalar_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::sort_order order) {
        packer_.pack(static_cast<int8_t>(order));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::update_expr_type type) {
        packer_.pack(static_cast<uint8_t>(type));
    }

    void msgpack_serializer_t::append(std::string_view key, expressions::side_t side) {
        packer_.pack(static_cast<uint8_t>(side));
    }

    void msgpack_serializer_t::append(std::string_view key, const std::string& str) { packer_.pack(str); }

    void msgpack_serializer_t::append(std::string_view key, const document_ptr& doc) { packer_.pack(doc); }

    void msgpack_serializer_t::append(std::string_view key, const document::value_t& val) {
        to_msgpack_(packer_, val.get_element());
    }

    void msgpack_serializer_t::append(std::string_view key, const expressions::key_t& key_val) {
        if (key_val.is_string()) {
            packer_.pack(key_val.as_string());
        } else if (key_val.is_int()) {
            packer_.pack(key_val.as_int());
        } else if (key_val.is_uint()) {
            packer_.pack(key_val.as_uint());
        } else {
            packer_.pack_nil();
        }
    }

} // namespace components::serializer