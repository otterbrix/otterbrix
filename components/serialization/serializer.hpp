#pragma once

#include <components/document/document.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/key.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <boost/json.hpp>
#include <memory_resource>
#include <stack>

#include <msgpack.hpp>

namespace components::logical_plan {
    class limit_t;
    enum class join_type : uint8_t;
    enum class index_type : uint8_t;
} // namespace components::logical_plan

namespace components::serializer {

    using pmr_string_stream =
        std::basic_stringstream<char, std::char_traits<char>, std::pmr::polymorphic_allocator<char>>;

    enum class serialization_type : uint8_t
    {
        logical_node_aggregate,
        logical_node_create_collection,
        logical_node_create_database,
        logical_node_create_index,
        logical_node_data,
        logical_node_delete,
        logical_node_drop_collection,
        logical_node_drop_database,
        logical_node_drop_index,
        logical_node_insert,
        logical_node_join,
        logical_node_limit,
        logical_node_match,
        logical_node_group,
        logical_node_sort,
        logical_node_function,
        logical_node_update,

        expression_compare,
        expression_aggregate,
        expression_scalar,
        expression_sort,
        expression_update,

        parameters,

        invalid = 255
    };

    class base_serializer_t {
    public:
        explicit base_serializer_t(std::pmr::memory_resource* resource);
        virtual ~base_serializer_t() = default;

        std::pmr::string result() const;

        virtual void start_array(size_t size) = 0;
        virtual void end_array() = 0;

        virtual void append(std::string_view key, bool val) = 0;
        virtual void append(std::string_view key, int64_t val) = 0;
        virtual void append(std::string_view key, uint64_t val) = 0;
        virtual void append(std::string_view key, core::parameter_id_t val) = 0;
        virtual void append(std::string_view key, serialization_type type) = 0;
        virtual void append(std::string_view key, logical_plan::node_type type) = 0;
        virtual void append(std::string_view key, logical_plan::index_type type) = 0;
        virtual void append(std::string_view key, logical_plan::join_type type) = 0;
        virtual void append(std::string_view key, expressions::aggregate_type type) = 0;
        virtual void append(std::string_view key, expressions::compare_type type) = 0;
        virtual void append(std::string_view key, expressions::scalar_type type) = 0;
        virtual void append(std::string_view key, expressions::sort_order order) = 0;
        virtual void append(std::string_view key, expressions::update_expr_type type) = 0;
        virtual void append(std::string_view key, expressions::side_t side) = 0;

        void append(std::string_view key, const std::pmr::vector<logical_plan::node_ptr>& nodes);
        void append(std::string_view key, const std::pmr::vector<document::document_ptr>& documents);
        void append(std::string_view key, const std::pmr::vector<expressions::key_t>& keys);
        void append(std::string_view key, const std::pmr::vector<core::parameter_id_t>& params);
        void append(std::string_view key, const std::pmr::vector<expressions::expression_ptr>& expressions);
        void append(std::string_view key, const std::pmr::vector<expressions::update_expr_ptr>& expressions);
        void append(std::string_view key, const std::pmr::vector<expressions::param_storage>& params);
        void append(std::string_view key, const collection_full_name_t& collection);
        void append(std::string_view key, const expressions::param_storage& param);

        virtual void append(std::string_view key, const std::string& str) = 0;
        virtual void append(std::string_view key, const document::document_ptr& doc) = 0;
        virtual void append(std::string_view key, const document::value_t& val) = 0;
        virtual void append(std::string_view key, const expressions::key_t& key_val) = 0;

        void append(std::string_view key, const logical_plan::node_ptr& node);
        void append(std::string_view key, const expressions::expression_ptr& expr);
        void append(std::string_view key, const expressions::update_expr_ptr& expr);

    protected:
        pmr_string_stream result_;
    };

    class json_serializer_t : public base_serializer_t {
    public:
        using base_serializer_t::append;

        explicit json_serializer_t(std::pmr::memory_resource* resource);

        void start_array(size_t size) override;
        void end_array() override;

        void append(std::string_view key, bool val) override;
        void append(std::string_view key, int64_t val) override;
        void append(std::string_view key, uint64_t val) override;
        void append(std::string_view key, core::parameter_id_t val) override;
        void append(std::string_view key, serialization_type type) override;
        void append(std::string_view key, logical_plan::node_type type) override;
        void append(std::string_view key, logical_plan::index_type type) override;
        void append(std::string_view key, logical_plan::join_type type) override;
        void append(std::string_view key, expressions::aggregate_type type) override;
        void append(std::string_view key, expressions::compare_type type) override;
        void append(std::string_view key, expressions::scalar_type type) override;
        void append(std::string_view key, expressions::sort_order order) override;
        void append(std::string_view key, expressions::update_expr_type type) override;
        void append(std::string_view key, expressions::side_t side) override;
        void append(std::string_view key, const std::string& str) override;
        void append(std::string_view key, const document::document_ptr& doc) override;
        void append(std::string_view key, const document::value_t& val) override;
        void append(std::string_view key, const expressions::key_t& key_val) override;

    private:
        boost::json::array root_arr_;
        std::stack<boost::json::array*> working_tree_;
    };

    class msgpack_serializer_t : public base_serializer_t {
    public:
        using base_serializer_t::append;

        explicit msgpack_serializer_t(std::pmr::memory_resource* resource);

        void start_array(size_t size) override;
        void end_array() override;

        void append(std::string_view key, bool val) override;
        void append(std::string_view key, int64_t val) override;
        void append(std::string_view key, uint64_t val) override;
        void append(std::string_view key, core::parameter_id_t val) override;
        void append(std::string_view key, serialization_type type) override;
        void append(std::string_view key, logical_plan::node_type type) override;
        void append(std::string_view key, logical_plan::index_type type) override;
        void append(std::string_view key, logical_plan::join_type type) override;
        void append(std::string_view key, expressions::aggregate_type type) override;
        void append(std::string_view key, expressions::compare_type type) override;
        void append(std::string_view key, expressions::scalar_type type) override;
        void append(std::string_view key, expressions::sort_order order) override;
        void append(std::string_view key, expressions::update_expr_type type) override;
        void append(std::string_view key, expressions::side_t side) override;
        void append(std::string_view key, const std::string& str) override;
        void append(std::string_view key, const document::document_ptr& doc) override;
        void append(std::string_view key, const document::value_t& val) override;
        void append(std::string_view key, const expressions::key_t& key_val) override;

    private:
        msgpack::packer<pmr_string_stream> packer_;
    };

} // namespace components::serializer