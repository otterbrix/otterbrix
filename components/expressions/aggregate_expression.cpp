#include "aggregate_expression.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <sstream>

namespace components::expressions {

    template<class OStream>
    OStream& operator<<(OStream& stream, const aggregate_expression_t* expr) {
        if (expr->params().empty()) {
            stream << expr->key();
        } else {
            if (!expr->key().is_null()) {
                stream << expr->key() << ": ";
            }
            stream << "{$" << expr->function_name() << ": ";
            if (expr->params().size() > 1) {
                stream << "[";
                bool is_first = true;
                for (const auto& param : expr->params()) {
                    if (is_first) {
                        is_first = false;
                    } else {
                        stream << ", ";
                    }
                    stream << param;
                }
                stream << "]";
            } else {
                stream << expr->params().at(0);
            }
            stream << "}";
        }
        return stream;
    }

    aggregate_expression_t::aggregate_expression_t(std::pmr::memory_resource* resource,
                                                   const std::string& function_name,
                                                   const key_t& key)
        : expression_i(expression_group::aggregate)
        , function_name_(function_name)
        , key_(key)
        , params_(resource) {}

    const key_t& aggregate_expression_t::key() const { return key_; }

    const std::string& aggregate_expression_t::function_name() const { return function_name_; }

    void aggregate_expression_t::add_function_uid(compute::function_uid uid) { function_uid_ = uid; }

    compute::function_uid aggregate_expression_t::function_uid() const { return function_uid_; }

    const std::pmr::vector<param_storage>& aggregate_expression_t::params() const { return params_; }

    void aggregate_expression_t::append_param(const param_storage& param) { params_.push_back(param); }

    expression_ptr aggregate_expression_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto func = deserializer->deserialize_string(1);
        auto key = deserializer->deserialize_key(2);
        std::pmr::vector<expressions::param_storage> params(deserializer->resource());
        deserializer->advance_array(3);
        params.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            params.emplace_back(expressions::deserialize_param_storage(deserializer, i));
        }
        deserializer->pop_array();
        auto res = make_aggregate_expression(deserializer->resource(), func, key);
        for (const auto& param : params) {
            res->append_param(param);
        }
        return res;
    }

    hash_t aggregate_expression_t::hash_impl() const {
        hash_t hash_{0};
        boost::hash_combine(hash_, std::hash<std::string>{}(function_name_));
        boost::hash_combine(hash_, key_.hash());
        for (const auto& param : params_) {
            auto param_hash = std::visit(
                [](const auto& value) {
                    using param_type = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<param_type, core::parameter_id_t>) {
                        return std::hash<uint64_t>()(value);
                    } else if constexpr (std::is_same_v<param_type, key_t>) {
                        return value.hash();
                    } else if constexpr (std::is_same_v<param_type, expression_ptr>) {
                        return value->hash();
                    }
                },
                param);
            boost::hash_combine(hash_, param_hash);
        }
        return hash_;
    }

    std::string aggregate_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << this;
        return stream.str();
    }

    bool aggregate_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const aggregate_expression_t*>(rhs);
        return function_name_ == other->function_name_ && key_ == other->key_ &&
               params_.size() == other->params_.size() &&
               std::equal(params_.begin(), params_.end(), other->params_.begin());
    }

    void aggregate_expression_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::expression_aggregate);
        serializer->append(function_name_);
        serializer->append(key_);
        serializer->start_array(params_.size());
        for (const auto& p : params_) {
            serialize_param_storage(serializer, p);
        }
        serializer->end_array();
        serializer->end_array();
    }
    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key) {
        return new aggregate_expression_t(resource, function_name, key);
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name) {
        return make_aggregate_expression(resource, function_name, key_t(resource));
    }

    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name,
                                                       const key_t& key,
                                                       const key_t& field) {
        auto expr = make_aggregate_expression(resource, function_name, key);
        expr->append_param(field);
        return expr;
    }

} // namespace components::expressions