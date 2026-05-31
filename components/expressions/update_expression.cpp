#include "update_expression.hpp"

#include <components/logical_plan/param_storage.hpp>
#include <components/vector/arithmetic.hpp>
#include <components/vector/vector_operations.hpp>

using namespace components::vector;

namespace components::expressions {

    update_expr_t::update_expr_t(update_expr_type type)
        : type_(type) {}

    std::pmr::vector<bool> update_expr_t::execute(std::pmr::memory_resource* resource,
                                                  vector::data_chunk_t& to,
                                                  const vector::data_chunk_t& from,
                                                  uint64_t count,
                                                  const logical_plan::storage_parameters* parameters,
                                                  core::date::timezone_offset_t session_tz) {
        if (left_) {
            left_->execute(resource, to, from, count, parameters, session_tz);
        }
        if (right_) {
            right_->execute(resource, to, from, count, parameters, session_tz);
        }
        return execute_impl(resource, to, from, count, parameters, session_tz);
    }

    update_expr_type update_expr_t::type() const noexcept { return type_; }

    update_expr_ptr& update_expr_t::left() { return left_; }
    const update_expr_ptr& update_expr_t::left() const { return left_; }

    update_expr_ptr& update_expr_t::right() { return right_; }
    const update_expr_ptr& update_expr_t::right() const { return right_; }

    const vector::vector_t* update_expr_t::output_vec() const noexcept { return output_vec_; }

    bool operator==(const update_expr_ptr& lhs, const update_expr_ptr& rhs) {
        if (lhs.get() == rhs.get()) {
            return true;
        }
        if ((lhs != nullptr) != (rhs != nullptr)) {
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

    std::pmr::vector<bool> update_expr_set_t::execute_impl(std::pmr::memory_resource* resource,
                                                           vector::data_chunk_t& to,
                                                           const vector::data_chunk_t&,
                                                           uint64_t count,
                                                           const logical_plan::storage_parameters*,
                                                           core::date::timezone_offset_t) {
        std::pmr::vector<bool> modified(count, false, resource);
        if (!left_ || count == 0) {
            return modified;
        }

        assert(key_.path().front() != size_t(-1));
        auto* col_vec = to.at(key_.path());
        auto* new_vec = left_->output_vec();

        // Cast new_vec to col_vec's type if they differ (e.g. DOUBLE→FLOAT after arithmetic).
        std::optional<vector_t> casted;
        if (new_vec->type().to_physical_type() != col_vec->type().to_physical_type()) {
            casted.emplace(vector_ops::cast_vector(resource, *new_vec, col_vec->type(), count));
            new_vec = &casted.value();
        }

        // For ARRAY-element paths the parent is an ARRAY vector; element j of row i
        // lives at i*stride+j in the flat child vector returned by at().
        if (key_.path().size() > 1) {
            const vector_t* parent = &to.data[key_.path().front()];
            for (size_t depth = 1; depth + 1 < key_.path().size(); ++depth) {
                parent = parent->entries()[key_.path()[depth]].get();
            }
            if (parent->type().type() == types::logical_type::ARRAY) {
                auto stride =
                    static_cast<const types::array_logical_type_extension*>(parent->type().extension())->size();
                vector_ops::copy_strided_target(*new_vec, *col_vec, count, stride, key_.path().back());
                std::fill(modified.begin(), modified.end(), true);
                return modified;
            }
        }

        vector_ops::copy(*new_vec, *col_vec, count, 0, 0);
        std::fill(modified.begin(), modified.end(), true);
        return modified;
    }

    update_expr_get_value_t::update_expr_get_value_t(key_t key)
        : update_expr_t(update_expr_type::get_value)
        , key_(std::move(key)) {}

    key_t& update_expr_get_value_t::key() noexcept { return key_; }
    const key_t& update_expr_get_value_t::key() const noexcept { return key_; }

    bool update_expr_get_value_t::operator==(const update_expr_get_value_t& rhs) const {
        return left_ == rhs.left_ && key_ == rhs.key_ && key_.side() == rhs.key_.side();
    }

    std::pmr::vector<bool> update_expr_get_value_t::execute_impl(std::pmr::memory_resource* resource,
                                                                 vector::data_chunk_t& to,
                                                                 const vector::data_chunk_t& from,
                                                                 uint64_t,
                                                                 const logical_plan::storage_parameters*,
                                                                 core::date::timezone_offset_t) {
        auto side = key_.side();
        assert(side != side_t::undefined && "validation must resolve side before execution");
        if (side == side_t::right) {
            assert(key_.path().front() != size_t(-1));
            output_vec_ = from.at(key_.path());
        } else {
            assert(key_.path().front() != size_t(-1));
            output_vec_ = to.at(key_.path());
        }
        return std::pmr::vector<bool>(resource);
    }

    update_expr_get_const_value_t::update_expr_get_const_value_t(core::parameter_id_t id)
        : update_expr_t(update_expr_type::get_value_params)
        , id_(id) {}

    core::parameter_id_t update_expr_get_const_value_t::id() const noexcept { return id_; }

    bool update_expr_get_const_value_t::operator==(const update_expr_get_const_value_t& rhs) const {
        return id_ == rhs.id_;
    }

    std::pmr::vector<bool>
    update_expr_get_const_value_t::execute_impl(std::pmr::memory_resource* resource,
                                                vector::data_chunk_t&,
                                                const vector::data_chunk_t&,
                                                uint64_t count,
                                                const logical_plan::storage_parameters* parameters,
                                                core::date::timezone_offset_t) {
        const auto& param = parameters->parameters.at(id_);
        uint64_t vec_count = count > 0 ? count : 1;
        owned_output_.emplace(resource, param, vec_count);
        owned_output_->flatten(vec_count);
        output_vec_ = &owned_output_.value();
        return std::pmr::vector<bool>(resource);
    }

    update_expr_calculate_t::update_expr_calculate_t(update_expr_type type)
        : update_expr_t(type) {}

    bool update_expr_calculate_t::operator==(const update_expr_calculate_t& rhs) const {
        return left_ == rhs.left_ && right_ == rhs.right_;
    }

    namespace {
        std::optional<arithmetic_op> to_arith_op(update_expr_type type) {
            switch (type) {
                case update_expr_type::add:
                    return arithmetic_op::add;
                case update_expr_type::sub:
                    return arithmetic_op::subtract;
                case update_expr_type::mult:
                    return arithmetic_op::multiply;
                case update_expr_type::div:
                    return arithmetic_op::divide;
                case update_expr_type::mod:
                    return arithmetic_op::mod;
                default:
                    return std::nullopt;
            }
        }

        std::optional<vector_ops::unary_vector_op> to_unary_vec_op(update_expr_type type) {
            switch (type) {
                case update_expr_type::sqr_root:
                    return vector_ops::unary_vector_op::sqr_root;
                case update_expr_type::cube_root:
                    return vector_ops::unary_vector_op::cube_root;
                case update_expr_type::factorial:
                    return vector_ops::unary_vector_op::factorial;
                case update_expr_type::abs:
                    return vector_ops::unary_vector_op::abs;
                case update_expr_type::NOT:
                    return vector_ops::unary_vector_op::bit_not;
                default:
                    return std::nullopt;
            }
        }

        vector_ops::binary_vector_op to_binary_vec_op(update_expr_type type) {
            switch (type) {
                case update_expr_type::exp:
                    return vector_ops::binary_vector_op::exp;
                case update_expr_type::AND:
                    return vector_ops::binary_vector_op::bit_and;
                case update_expr_type::OR:
                    return vector_ops::binary_vector_op::bit_or;
                case update_expr_type::XOR:
                    return vector_ops::binary_vector_op::bit_xor;
                case update_expr_type::shift_left:
                    return vector_ops::binary_vector_op::shift_left;
                case update_expr_type::shift_right:
                    return vector_ops::binary_vector_op::shift_right;
                default:
                    throw std::logic_error("to_binary_vec_op: unsupported update_expr_type");
            }
        }
    } // anonymous namespace

    std::pmr::vector<bool> update_expr_calculate_t::execute_impl(std::pmr::memory_resource* resource,
                                                                 vector::data_chunk_t&,
                                                                 const vector::data_chunk_t&,
                                                                 uint64_t count,
                                                                 const logical_plan::storage_parameters*,
                                                                 core::date::timezone_offset_t) {
        uint64_t vec_count = count > 0 ? count : 1;
        auto* left_vec = left_->output_vec();

        if (auto unary_op = to_unary_vec_op(type_)) {
            owned_output_.emplace(vector_ops::apply_unary_vector_op(resource, *unary_op, *left_vec, vec_count));
        } else if (auto arith_op = to_arith_op(type_)) {
            owned_output_.emplace(
                compute_binary_arithmetic(resource, *arith_op, *left_vec, *right_->output_vec(), vec_count));
        } else {
            owned_output_.emplace(vector_ops::apply_binary_vector_op(resource,
                                                                     to_binary_vec_op(type_),
                                                                     *left_vec,
                                                                     *right_->output_vec(),
                                                                     vec_count));
        }

        output_vec_ = &owned_output_.value();
        return std::pmr::vector<bool>(resource);
    }

} // namespace components::expressions
