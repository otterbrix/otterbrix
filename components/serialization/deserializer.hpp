#pragma once
#include "serializer.hpp"
#include <components/logical_plan/param_storage.hpp>

namespace components::serializer {

    class base_deserializer_t {
    public:
        using deserialization_result =
            std::variant<expressions::expression_ptr, logical_plan::node_ptr, logical_plan::parameter_node_ptr>;

        explicit base_deserializer_t(const std::pmr::string& input);
        virtual ~base_deserializer_t() = default;

        std::pmr::memory_resource* resource() const { return input_.get_allocator().resource(); }

        virtual size_t root_array_size() const = 0;
        virtual size_t current_array_size() const = 0;
        virtual void advance_array(size_t index) = 0;
        virtual void pop_array() = 0;
        virtual serialization_type current_type() = 0;

        virtual bool deserialize_bool(size_t index) = 0;
        virtual int64_t deserialize_int64(size_t index) = 0;
        virtual uint64_t deserialize_uint64(size_t index) = 0;
        virtual expressions::aggregate_type deserialize_aggregate_type(size_t index) = 0;
        virtual expressions::compare_type deserialize_compare_type(size_t index) = 0;
        virtual expressions::scalar_type deserialize_scalar_type(size_t index) = 0;
        virtual expressions::sort_order deserialize_sort_order(size_t index) = 0;
        virtual expressions::update_expr_type deserialize_update_expr_type(size_t index) = 0;
        virtual expressions::side_t deserialize_expr_side(size_t index) = 0;
        virtual logical_plan::index_type deserialize_index_type(size_t index) = 0;
        virtual logical_plan::join_type deserialize_join_type(size_t index) = 0;
        virtual core::parameter_id_t deserialize_param_id(size_t index) = 0;
        virtual expressions::key_t deserialize_key(size_t index) = 0;
        virtual std::string deserialize_string(size_t index) = 0;
        virtual document::value_t deserialize_value(document::impl::base_document* tape, size_t index) = 0;
        virtual document_ptr deserialize_document(size_t index) = 0;
        virtual collection_full_name_t deserialize_collection(size_t index) = 0;
        virtual expressions::param_storage deserialize_param_storage(size_t index) = 0;

        std::pmr::vector<core::parameter_id_t> deserialize_param_ids(size_t index);
        std::pmr::vector<expressions::key_t> deserialize_keys(size_t index);
        std::pmr::vector<expressions::param_storage> deserialize_param_storages(size_t index);
        std::pmr::vector<document_ptr> deserialize_documents(size_t index);
        std::pmr::vector<expressions::expression_ptr> deserialize_expressions(size_t index);
        std::pair<core::parameter_id_t, document::value_t> deserialize_param_pair(document::impl::base_document* tape,
                                                                                  size_t size);

        expressions::expression_ptr deserialize_expression(size_t index);

    protected:
        std::pmr::string input_;
    };

    class json_deserializer_t : public base_deserializer_t {
    public:
        explicit json_deserializer_t(const std::pmr::string& input);

        size_t root_array_size() const override;
        size_t current_array_size() const override;
        void advance_array(size_t index) override;
        void pop_array() override;
        serialization_type current_type() override;

        bool deserialize_bool(size_t index) override;
        int64_t deserialize_int64(size_t index) override;
        uint64_t deserialize_uint64(size_t index) override;
        expressions::aggregate_type deserialize_aggregate_type(size_t index) override;
        expressions::compare_type deserialize_compare_type(size_t index) override;
        expressions::scalar_type deserialize_scalar_type(size_t index) override;
        expressions::sort_order deserialize_sort_order(size_t index) override;
        expressions::update_expr_type deserialize_update_expr_type(size_t index) override;
        expressions::side_t deserialize_expr_side(size_t index) override;
        logical_plan::index_type deserialize_index_type(size_t index) override;
        logical_plan::join_type deserialize_join_type(size_t index) override;
        core::parameter_id_t deserialize_param_id(size_t index) override;
        expressions::key_t deserialize_key(size_t index) override;
        std::string deserialize_string(size_t index) override;
        document::value_t deserialize_value(document::impl::base_document* tape, size_t index) override;
        document_ptr deserialize_document(size_t index) override;
        collection_full_name_t deserialize_collection(size_t index) override;
        expressions::param_storage deserialize_param_storage(size_t index) override;

    private:
        boost::json::array root_arr_;
        std::stack<boost::json::array*> working_tree_;
    };

    class msgpack_deserializer_t : public base_deserializer_t {
    public:
        explicit msgpack_deserializer_t(const std::pmr::string& input);

        size_t root_array_size() const override;
        size_t current_array_size() const override;
        void advance_array(size_t index) override;
        void pop_array() override;
        serialization_type current_type() override;

        bool deserialize_bool(size_t index) override;
        int64_t deserialize_int64(size_t index) override;
        uint64_t deserialize_uint64(size_t index) override;
        expressions::aggregate_type deserialize_aggregate_type(size_t index) override;
        expressions::compare_type deserialize_compare_type(size_t index) override;
        expressions::scalar_type deserialize_scalar_type(size_t index) override;
        expressions::sort_order deserialize_sort_order(size_t index) override;
        expressions::update_expr_type deserialize_update_expr_type(size_t index) override;
        expressions::side_t deserialize_expr_side(size_t index) override;
        logical_plan::index_type deserialize_index_type(size_t index) override;
        logical_plan::join_type deserialize_join_type(size_t index) override;
        core::parameter_id_t deserialize_param_id(size_t index) override;
        expressions::key_t deserialize_key(size_t index) override;
        std::string deserialize_string(size_t index) override;
        document::value_t deserialize_value(document::impl::base_document* tape, size_t index) override;
        document_ptr deserialize_document(size_t index) override;
        collection_full_name_t deserialize_collection(size_t index) override;
        expressions::param_storage deserialize_param_storage(size_t index) override;

    private:
        msgpack::unpacked msg;
        msgpack::object_array root_arr_;
        std::stack<msgpack::object_array*> working_tree_;
    };

} // namespace components::serializer