#include "update_expression.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace components::expressions {

    update_expr_t::expr_output_t::expr_output_t()
        : output_(nullptr, false) {}

    update_expr_t::expr_output_t::expr_output_t(types::logical_value_t value)
        : output_(std::move(value)) {}

    types::logical_value_t& update_expr_t::expr_output_t::value() { return output_; }

    const types::logical_value_t& update_expr_t::expr_output_t::value() const { return output_; }

    update_expr_t::update_expr_t(update_expr_type type)
        : type_(type) {}

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
            case update_expr_type::get_value:
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
                return false;
        }
    }

    update_expr_set_t::update_expr_set_t(key_t key)
        : update_expr_t(update_expr_type::set)
        , key_(std::move(key)) {}

    key_t& update_expr_set_t::key() noexcept { return key_; }

    const key_t& update_expr_set_t::key() const noexcept { return key_; }

    bool update_expr_set_t::operator==(const update_expr_set_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_;
    }

    bool update_expr_set_t::execute_impl(vector::data_chunk_t& to,
                                         const vector::data_chunk_t&,
                                         size_t row_to,
                                         size_t,
                                         const logical_plan::storage_parameters*) {
        if (left_) {
            assert(key_.path().front() != size_t(-1));
            auto prev_value = to.value(key_.path(), row_to);
            auto res = prev_value != left_->output().value();
            to.set_value(key_.path(), row_to, left_->output().value());
            return res;
        }
        return false;
    }

    update_expr_get_value_t::update_expr_get_value_t(key_t key)
        : update_expr_t(update_expr_type::get_value)
        , key_(std::move(key)) {}

    key_t& update_expr_get_value_t::key() noexcept { return key_; }

    const key_t& update_expr_get_value_t::key() const noexcept { return key_; }

    bool update_expr_get_value_t::operator==(const update_expr_get_value_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_ && key_.side() == rhs.key_.side();
    }

    bool update_expr_get_value_t::execute_impl(vector::data_chunk_t& to,
                                               const vector::data_chunk_t& from,
                                               size_t row_to,
                                               size_t row_from,
                                               const logical_plan::storage_parameters*) {
        auto side = key_.side();
        assert(side != side_t::undefined && "validation must resolve side before execution");
        if (side == side_t::right) {
            assert(key_.path().front() != size_t(-1));
            output_ = from.value(key_.path(), row_from);
        } else if (side == side_t::left) {
            assert(key_.path().front() != size_t(-1));
            output_ = to.value(key_.path(), row_to);
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

    bool update_expr_get_const_value_t::execute_impl(vector::data_chunk_t&,
                                                     const vector::data_chunk_t&,
                                                     size_t,
                                                     size_t,
                                                     const logical_plan::storage_parameters* parameters) {
        output_ = parameters->parameters.at(id_);
        return false;
    }

    update_expr_calculate_t::update_expr_calculate_t(update_expr_type type)
        : update_expr_t(type) {}

    bool update_expr_calculate_t::operator==(const update_expr_calculate_t& rhs) const {
        return left_ == rhs.left_ && right_ == rhs.right_;
    }

    namespace {
        // Binary arithmetic dispatch for update expressions.
        // Covers basic arithmetic (add/sub/mult/div/mod), exponent, and bitwise ops.
        types::logical_value_t apply_binary_update_op(update_expr_type type,
                                                      const types::logical_value_t& left,
                                                      const types::logical_value_t& right) {
            switch (type) {
                case update_expr_type::add:
                    return types::logical_value_t::sum(left, right);
                case update_expr_type::sub:
                    return types::logical_value_t::subtract(left, right);
                case update_expr_type::mult:
                    return types::logical_value_t::mult(left, right);
                case update_expr_type::div:
                    return types::logical_value_t::divide(left, right);
                case update_expr_type::mod:
                    return types::logical_value_t::modulus(left, right);
                case update_expr_type::exp:
                    return types::logical_value_t::exponent(left, right);
                case update_expr_type::AND:
                    return types::logical_value_t::bit_and(left, right);
                case update_expr_type::OR:
                    return types::logical_value_t::bit_or(left, right);
                case update_expr_type::XOR:
                    return types::logical_value_t::bit_xor(left, right);
                case update_expr_type::shift_left:
                    return types::logical_value_t::bit_shift_l(left, right);
                case update_expr_type::shift_right:
                    return types::logical_value_t::bit_shift_r(left, right);
                default:
                    throw std::logic_error("apply_binary_update_op: unsupported update_expr_type");
            }
        }

        // Unary ops dispatch for update expressions.
        types::logical_value_t apply_unary_update_op(update_expr_type type, const types::logical_value_t& operand) {
            switch (type) {
                case update_expr_type::sqr_root:
                    return types::logical_value_t::sqr_root(operand);
                case update_expr_type::cube_root:
                    return types::logical_value_t::cube_root(operand);
                case update_expr_type::factorial:
                    return types::logical_value_t::factorial(operand);
                case update_expr_type::abs:
                    return types::logical_value_t::absolute(operand);
                case update_expr_type::NOT:
                    return types::logical_value_t::bit_not(operand);
                default:
                    throw std::logic_error("apply_unary_update_op: unsupported update_expr_type");
            }
        }

        bool is_unary_update_op(update_expr_type type) {
            return type == update_expr_type::sqr_root || type == update_expr_type::cube_root ||
                   type == update_expr_type::factorial || type == update_expr_type::abs ||
                   type == update_expr_type::NOT;
        }
    } // anonymous namespace

    bool update_expr_calculate_t::execute_impl(vector::data_chunk_t&,
                                               const vector::data_chunk_t&,
                                               size_t,
                                               size_t,
                                               const logical_plan::storage_parameters*) {
        if (is_unary_update_op(type_)) {
            output_ = apply_unary_update_op(type_, left_->output().value());
        } else {
            output_ = apply_binary_update_op(type_, left_->output().value(), right_->output().value());
        }
        return false;
    }

} // namespace components::expressions