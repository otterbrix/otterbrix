#include "expression.hpp"

#include "aggregate_expression.hpp"
#include "compare_expression.hpp"
#include "function_expression.hpp"
#include "scalar_expression.hpp"
#include "sort_expression.hpp"

#include <boost/container_hash/hash.hpp>
#include <components/serialization/deserializer.hpp>
#include <sstream>

namespace components::expressions {

    expression_group expression_i::group() const { return group_; }

    hash_t expression_i::hash() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, group_);
        boost::hash_combine(hash_, hash_impl());
        return hash_;
    }

    std::string expression_i::to_string() const { return to_string_impl(); }

    bool expression_i::operator==(const expression_i& rhs) const { return group_ == rhs.group_ && equal_impl(&rhs); }

    bool expression_i::operator!=(const expression_i& rhs) const { return !operator==(rhs); }

    const std::string& expression_i::result_alias() const { return result_alias_; }

    void expression_i::set_result_alias(const std::string& alias) { result_alias_ = alias; }

    void expression_i::serialize(serializer::msgpack_serializer_t* serializer) const {
        return serialize_impl(serializer);
    }

    boost::intrusive_ptr<expression_i> expression_i::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->current_type();
        switch (type) {
            case serializer::serialization_type::expression_compare:
                return compare_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_aggregate:
                return aggregate_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_scalar:
                return scalar_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_sort:
                return sort_expression_t::deserialize(deserializer);
            case serializer::serialization_type::expression_function:
                return function_expression_t::deserialize(deserializer);
            default:
                return {nullptr};
        }
    }

    expression_i::expression_i(expression_group group)
        : group_(group) {}

    enum class param_storage_tag : uint8_t
    {
        parameter_id,
        key,
        expression,
        null
    };

    param_storage deserialize_param_storage(serializer::msgpack_deserializer_t* deserializer, size_t index) {
        param_storage res;
        deserializer->advance_array(index);
        auto tag = deserializer->deserialize_enum<param_storage_tag>(0);
        if (tag == param_storage_tag::parameter_id) {
            res = deserializer->deserialize_param_id(1);
        } else if (tag == param_storage_tag::expression) {
            deserializer->advance_array(1);
            res = expressions::expression_i::deserialize(deserializer);
            deserializer->pop_array();
        } else if (tag == param_storage_tag::key) {
            res = deserializer->deserialize_key(1);
        } else {
            res = nullptr;
        }
        deserializer->pop_array();
        return res;
    }

    void serialize_param_storage(serializer::msgpack_serializer_t* serializer, const param_storage& param) {
        std::visit(
            [&](const auto& value) {
                serializer->start_array(2);
                using param_type = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                    serializer->append_enum(param_storage_tag::parameter_id);
                    serializer->append(value);
                } else if constexpr (std::is_same_v<param_type, expressions::key_t>) {
                    serializer->append_enum(param_storage_tag::key);
                    serializer->append(value);
                } else if constexpr (std::is_same_v<param_type, expressions::expression_ptr>) {
                    if (value) {
                        serializer->append_enum(param_storage_tag::expression);
                        value->serialize(serializer);
                    } else {
                        serializer->append_enum(param_storage_tag::null);
                        serializer->append_null();
                    }
                } else {
                    assert(false);
                }
                serializer->end_array();
            },
            param);
    }

} // namespace components::expressions
