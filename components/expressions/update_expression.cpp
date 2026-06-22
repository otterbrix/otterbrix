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

        // Full-column assignment into an array-like column (e.g. UPDATE v = ARRAY[...]
        // where v is a fixed ARRAY or a variadic LIST). The value is itself an
        // array-like vector whose flat support vector (entry()) holds the actual
        // elements, with each row carrying an (offset,length) slice into it. Rather
        // than casting and writing each row through logical_value_t, cast that whole
        // support vector to the column's element type once via cast_vector, then
        // re-point the per-row entries: a wider element literal is narrowed in a
        // single pass, a fixed ARRAY reconciles its length to the declared size, and
        // a LIST grows/shrinks the row's flat-child slot to the new length.
        const bool full_column_assignment = key_.path().size() == 1;
        const bool array_like_target =
            col_vec->type().type() == types::logical_type::ARRAY || col_vec->type().type() == types::logical_type::LIST;
        if (full_column_assignment && array_like_target) {
            const auto& target_elem_type = col_vec->type().child_type();
            const vector_t& src_entry = new_vec->entry();

            // Per-row (offset,length) slice of the value into its support vector. A
            // LIST stores this explicitly; a fixed ARRAY derives it from the stride.
            auto src_slice = [&](uint64_t row) -> types::list_entry_t {
                if (new_vec->type().type() == types::logical_type::LIST) {
                    return new_vec->data<types::list_entry_t>()[row];
                }
                auto stride =
                    static_cast<const types::array_logical_type_extension*>(new_vec->type().extension())->size();
                return types::list_entry_t{row * stride, stride};
            };

            // Cast only the populated prefix of the support vector to the column's
            // element type. A matching physical type needs no cast and the source
            // support vector is used directly (this also preserves string elements,
            // which cast_vector does not handle).
            uint64_t src_elem_count = 0;
            for (uint64_t row = 0; row < count; ++row) {
                auto slice = src_slice(row);
                src_elem_count = std::max(src_elem_count, slice.offset + slice.length);
            }
            std::optional<vector_t> casted_elems;
            const vector_t* elems = &src_entry;
            if (src_entry.type().to_physical_type() != target_elem_type.to_physical_type()) {
                casted_elems.emplace(vector_ops::cast_vector(resource, src_entry, target_elem_type, src_elem_count));
                elems = &casted_elems.value();
            }

            if (col_vec->type().type() == types::logical_type::LIST) {
                // Variadic LIST target: rebuild the column's flat child buffer by
                // appending each row's (cast) element slice, then re-point the row's
                // (offset,length) slot at the freshly appended segment.
                auto* row_entries = col_vec->data<types::list_entry_t>();
                col_vec->set_list_size(0);
                uint64_t target_offset = 0;
                for (uint64_t row = 0; row < count; ++row) {
                    auto slice = src_slice(row);
                    if (new_vec->is_null(row)) {
                        col_vec->set_null(row, true);
                        row_entries[row] = types::list_entry_t{target_offset, 0};
                        modified[row] = true;
                        continue;
                    }
                    col_vec->append(*elems, slice.offset + slice.length, slice.offset);
                    row_entries[row] = types::list_entry_t{target_offset, slice.length};
                    target_offset += slice.length;
                    modified[row] = true;
                }
                col_vec->set_list_size(target_offset);
                return modified;
            }

            // Fixed ARRAY target: write exactly stride elements per row, truncating a
            // longer literal and NULL-padding a shorter one to the declared size.
            auto target_stride =
                static_cast<const types::array_logical_type_extension*>(col_vec->type().extension())->size();
            auto& target_child = col_vec->entry();
            for (uint64_t row = 0; row < count; ++row) {
                auto slice = src_slice(row);
                if (new_vec->is_null(row)) {
                    col_vec->set_null(row, true);
                    for (uint64_t j = 0; j < target_stride; ++j) {
                        target_child.set_null(row * target_stride + j, true);
                    }
                    modified[row] = true;
                    continue;
                }
                uint64_t copied = std::min<uint64_t>(slice.length, target_stride);
                if (copied > 0) {
                    vector_ops::copy(*elems, target_child, slice.offset + copied, slice.offset, row * target_stride);
                }
                for (uint64_t j = copied; j < target_stride; ++j) {
                    target_child.set_null(row * target_stride + j, true);
                }
                modified[row] = true;
            }
            return modified;
        }

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
            if (parent->type().type() == types::logical_type::LIST) {
                // LIST element j of row r lives at list_entry[r].offset + j in the flat
                // child vector; offsets vary per row so we write each one individually.
                // Rows whose list is shorter than j are left untouched.
                const auto* offlen = parent->data<types::list_entry_t>();
                const auto element_index = key_.path().back();
                for (uint64_t row = 0; row < count; ++row) {
                    if (element_index >= offlen[row].length) {
                        continue;
                    }
                    col_vec->set_value(offlen[row].offset + element_index, new_vec->value(row));
                    modified[row] = true;
                }
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
