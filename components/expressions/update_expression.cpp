#include "update_expression.hpp"

#include <components/logical_plan/param_storage.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

namespace components::expressions {

    update_expr_t::expr_output_t::expr_output_t(types::logical_value_t value)
        : output_(std::move(value)) {}

    types::logical_value_t& update_expr_t::expr_output_t::value() { return output_; }

    const types::logical_value_t& update_expr_t::expr_output_t::value() const { return output_; }

    update_expr_t::update_expr_t(update_expr_type type)
        : type_(type) {}

    bool update_expr_t::execute(document::document_ptr& to,
                                const document::document_ptr& from,
                                document::impl::base_document* tape,
                                const logical_plan::storage_parameters* parameters) {
        if (left_) {
            left_->execute(to, from, tape, parameters);
        }
        if (right_) {
            right_->execute(to, from, tape, parameters);
        }
        return execute_impl(to, from, tape, parameters);
    }

    bool update_expr_t::execute(vector::data_chunk_t& to,
                                const vector::data_chunk_t& from,
                                size_t row_to,
                                size_t row_from,
                                const logical_plan::storage_parameters* parameters) {
        if (left_) {
            left_->execute(to, from, row_to, row_from, parameters);
        }
        if (right_) {
            right_->execute(to, from, row_to, row_from, parameters);
        }
        return execute_impl(to, from, row_to, row_from, parameters);
    }

    update_expr_type update_expr_t::type() const noexcept { return type_; }

    update_expr_ptr& update_expr_t::left() { return left_; }

    const update_expr_ptr& update_expr_t::left() const { return left_; }

    update_expr_ptr& update_expr_t::right() { return right_; }

    const update_expr_ptr& update_expr_t::right() const { return right_; }

    update_expr_t::expr_output_t& update_expr_t::output() { return output_; }

    const update_expr_t::expr_output_t& update_expr_t::output() const { return output_; }

    update_expr_ptr update_expr_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<update_expr_type>(1);
        switch (type) {
            case update_expr_type::set:
                return update_expr_set_t::deserialize(deserializer);
            case update_expr_type::get_value_doc:
                return update_expr_get_value_t::deserialize(deserializer);
            case update_expr_type::get_value_params:
                return update_expr_get_const_value_t::deserialize(deserializer);
            case update_expr_type::add:
            case update_expr_type::sub:
            case update_expr_type::mult:
            case update_expr_type::div:
            case update_expr_type::mod:
            case update_expr_type::exp:
            case update_expr_type::sqr_root:
            case update_expr_type::cube_root:
            case update_expr_type::factorial:
            case update_expr_type::abs:
            case update_expr_type::AND:
            case update_expr_type::OR:
            case update_expr_type::XOR:
            case update_expr_type::NOT:
            case update_expr_type::shift_left:
            case update_expr_type::shift_right:
                return update_expr_calculate_t::deserialize(deserializer);
            default:
                assert(false && "incorrect update_expr_type");
        }
    }

    bool operator==(const update_expr_ptr& lhs, const update_expr_ptr& rhs) {
        if (lhs.get() == rhs.get()) {
            // same address
            return true;
        }
        // XOR
        if ((lhs != nullptr) != (rhs != nullptr)) {
            // only one is nullptr
            return false;
        }
        if (lhs->type() != rhs->type()) {
            return false;
        }

        switch (lhs->type()) {
            case update_expr_type::set:
                return *reinterpret_cast<const update_expr_set_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_set_ptr&>(rhs);
            case update_expr_type::get_value_doc:
                return *reinterpret_cast<const update_expr_get_value_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_get_value_ptr&>(rhs);
            case update_expr_type::get_value_params:
                return *reinterpret_cast<const update_expr_get_const_value_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_get_const_value_ptr&>(rhs);
            case update_expr_type::add:
            case update_expr_type::sub:
            case update_expr_type::mult:
            case update_expr_type::div:
            case update_expr_type::mod:
            case update_expr_type::exp:
            case update_expr_type::sqr_root:
            case update_expr_type::cube_root:
            case update_expr_type::factorial:
            case update_expr_type::abs:
            case update_expr_type::AND:
            case update_expr_type::OR:
            case update_expr_type::XOR:
            case update_expr_type::NOT:
            case update_expr_type::shift_left:
            case update_expr_type::shift_right:
                return *reinterpret_cast<const update_expr_calculate_ptr&>(lhs) ==
                       *reinterpret_cast<const update_expr_calculate_ptr&>(rhs);
            default:
                assert(false && "incorrect update_expr_type");
        }
    }

    update_expr_set_t::update_expr_set_t(key_t key)
        : update_expr_t(update_expr_type::set)
        , key_(std::move(key)) {}

    const key_t& update_expr_set_t::key() const noexcept { return key_; }

    bool update_expr_set_t::operator==(const update_expr_set_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_;
    }

    void update_expr_set_t::serialize(serializer::msgpack_serializer_t* serializer) {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::expression_update);
        serializer->append_enum(type_);
        serializer->append(key_);
        left_->serialize(serializer);
        serializer->end_array();
    }

    update_expr_ptr update_expr_set_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        update_expr_ptr res = new update_expr_set_t(deserializer->deserialize_key(2));
        deserializer->advance_array(3);
        res->left() = update_expr_t::deserialize(deserializer);
        deserializer->pop_array();
        return res;
    }

    bool update_expr_set_t::execute_impl(document::document_ptr& to,
                                         const document::document_ptr& from,
                                         document::impl::base_document*,
                                         const logical_plan::storage_parameters*) {
        if (left_) {
            return to->update(key_.as_string(), left_->output().value());
        }
        return false;
    }

    bool update_expr_set_t::execute_impl(vector::data_chunk_t& to,
                                         const vector::data_chunk_t& from,
                                         size_t row_to,
                                         size_t row_from,
                                         const logical_plan::storage_parameters* parameters) {
        // TODO: find fix for complex keys e.g. "countArray/0"
        if (left_) {
            size_t index = to.column_index(key_.as_string());
            assert(index < to.column_count());
            auto prev_value = to.data[index].value(row_to);
            auto res = prev_value != left_->output().value();
            to.data[index].set_value(row_to, left_->output().value());
            return res;
        }
        return false;
    }

    update_expr_get_value_t::update_expr_get_value_t(key_t key)
        : update_expr_t(update_expr_type::get_value_doc)
        , key_(std::move(key)) {}

    const key_t& update_expr_get_value_t::key() const noexcept { return key_; }

    bool update_expr_get_value_t::operator==(const update_expr_get_value_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_ && key_.side() == rhs.key_.side();
    }

    void update_expr_get_value_t::serialize(serializer::msgpack_serializer_t* serializer) {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::expression_update);
        serializer->append_enum(type_);
        serializer->append(key_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_get_value_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return {new update_expr_get_value_t(deserializer->deserialize_key(2))};
    }

    bool update_expr_get_value_t::execute_impl(document::document_ptr& to,
                                               const document::document_ptr& from,
                                               document::impl::base_document* tape,
                                               const logical_plan::storage_parameters*) {
        auto side = key_.side();
        if (side == side_t::undefined) {
            if (to->is_exists(key_.as_string())) {
                side = side_t::left;
            } else if (from->is_exists(key_.as_string())) {
                side = side_t::right;
            } else {
                output_ = types::logical_value_t();
            }
        }
        if (side == side_t::right) {
            output_ = from->get_value(key_.as_string()).as_logical_value();
        } else if (side == side_t::left) {
            output_ = to->get_value(key_.as_string()).as_logical_value();
        }
        return false;
    }

    bool update_expr_get_value_t::execute_impl(vector::data_chunk_t& to,
                                               const vector::data_chunk_t& from,
                                               size_t row_to,
                                               size_t row_from,
                                               const logical_plan::storage_parameters* parameters) {
        auto side = key_.side();
        // TODO: find fix for complex keys e.g. "countArray/0"
        if (side == side_t::undefined) {
            // TODO: deduce which side to use
            assert(false);
        }
        if (side == side_t::right) {
            size_t index = from.column_index(key_.as_string());
            assert(index < from.column_count());
            output_ = from.data[index].value(row_from);
        } else if (side == side_t::left) {
            size_t index = to.column_index(key_.as_string());
            assert(index < to.column_count());
            output_ = to.data[index].value(row_to);
        }
        return false;
    }

    update_expr_get_const_value_t::update_expr_get_const_value_t(core::parameter_id_t id)
        : update_expr_t(update_expr_type::get_value_params)
        , id_(id) {}

    core::parameter_id_t update_expr_get_const_value_t::id() const noexcept { return id_; }

    bool update_expr_get_const_value_t::operator==(const update_expr_get_const_value_t& rhs) const {
        return id_ == rhs.id_;
    }

    void update_expr_get_const_value_t::serialize(serializer::msgpack_serializer_t* serializer) {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::expression_update);
        serializer->append_enum(type_);
        serializer->append(id_);
        serializer->end_array();
    }

    update_expr_ptr update_expr_get_const_value_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        return {new update_expr_get_const_value_t(deserializer->deserialize_param_id(2))};
    }

    bool update_expr_get_const_value_t::execute_impl(document::document_ptr& to,
                                                     const document::document_ptr& from,
                                                     document::impl::base_document*,
                                                     const logical_plan::storage_parameters* parameters) {
        output_ = parameters->parameters.at(id_);
        return false;
    }

    bool update_expr_get_const_value_t::execute_impl(vector::data_chunk_t& to,
                                                     const vector::data_chunk_t& from,
                                                     size_t row_to,
                                                     size_t row_from,
                                                     const logical_plan::storage_parameters* parameters) {
        output_ = parameters->parameters.at(id_);
        return false;
    }

    update_expr_calculate_t::update_expr_calculate_t(update_expr_type type)
        : update_expr_t(type) {}

    bool update_expr_calculate_t::operator==(const update_expr_calculate_t& rhs) const {
        return left_ == rhs.left_ && right_ == rhs.right_;
    }

    void update_expr_calculate_t::serialize(serializer::msgpack_serializer_t* serializer) {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::expression_update);
        serializer->append_enum(type_);
        left_->serialize(serializer);
        right_->serialize(serializer);
        serializer->end_array();
    }

    update_expr_ptr update_expr_calculate_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        update_expr_ptr res = new update_expr_calculate_t(deserializer->deserialize_enum<update_expr_type>(1));
        deserializer->advance_array(2);
        res->left() = update_expr_t::deserialize(deserializer);
        deserializer->pop_array();
        deserializer->advance_array(3);
        res->right() = update_expr_t::deserialize(deserializer);
        deserializer->pop_array();
        return res;
    }

    bool update_expr_calculate_t::execute_impl(document::document_ptr&,
                                               const document::document_ptr&,
                                               document::impl::base_document* tape,
                                               const logical_plan::storage_parameters*) {
        switch (type_) {
            case update_expr_type::add:
                output_ = types::logical_value_t::sum(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::sub:
                output_ = types::logical_value_t::subtract(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::mult:
                output_ = types::logical_value_t::mult(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::div:
                output_ = types::logical_value_t::divide(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::mod:
                output_ = types::logical_value_t::modulus(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::exp:
                output_ = types::logical_value_t::exponent(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::sqr_root:
                output_ = types::logical_value_t::sqr_root(left_->output().value());
                break;
            case update_expr_type::cube_root:
                output_ = types::logical_value_t::cube_root(left_->output().value());
                break;
            case update_expr_type::factorial:
                output_ = types::logical_value_t::factorial(left_->output().value());
                break;
            case update_expr_type::abs:
                output_ = types::logical_value_t::absolute(left_->output().value());
                break;
            case update_expr_type::AND:
                output_ = types::logical_value_t::bit_and(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::OR:
                output_ = types::logical_value_t::bit_or(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::XOR:
                output_ = types::logical_value_t::bit_xor(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::NOT:
                output_ = types::logical_value_t::bit_not(left_->output().value());
                break;
            case update_expr_type::shift_left:
                output_ = types::logical_value_t::bit_shift_l(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::shift_right:
                output_ = types::logical_value_t::bit_shift_r(left_->output().value(), right_->output().value());
                break;
            default:
                break;
        }
        return false;
    }

    bool update_expr_calculate_t::execute_impl(vector::data_chunk_t& to,
                                               const vector::data_chunk_t& from,
                                               size_t row_to,
                                               size_t row_from,
                                               const logical_plan::storage_parameters* parameters) {
        switch (type_) {
            case update_expr_type::add:
                output_ = types::logical_value_t::sum(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::sub:
                output_ = types::logical_value_t::subtract(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::mult:
                output_ = types::logical_value_t::mult(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::div:
                output_ = types::logical_value_t::divide(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::mod:
                output_ = types::logical_value_t::modulus(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::exp:
                output_ = types::logical_value_t::exponent(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::sqr_root:
                output_ = types::logical_value_t::sqr_root(left_->output().value());
                break;
            case update_expr_type::cube_root:
                output_ = types::logical_value_t::cube_root(left_->output().value());
                break;
            case update_expr_type::factorial:
                output_ = types::logical_value_t::factorial(left_->output().value());
                break;
            case update_expr_type::abs:
                output_ = types::logical_value_t::absolute(left_->output().value());
                break;
            case update_expr_type::AND:
                output_ = types::logical_value_t::bit_and(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::OR:
                output_ = types::logical_value_t::bit_or(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::XOR:
                output_ = types::logical_value_t::bit_xor(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::NOT:
                output_ = types::logical_value_t::bit_not(left_->output().value());
                break;
            case update_expr_type::shift_left:
                output_ = types::logical_value_t::bit_shift_l(left_->output().value(), right_->output().value());
                break;
            case update_expr_type::shift_right:
                output_ = types::logical_value_t::bit_shift_r(left_->output().value(), right_->output().value());
                break;
            default:
                break;
        }
        return false;
    }

} // namespace components::expressions