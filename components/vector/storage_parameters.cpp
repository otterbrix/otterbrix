#include "storage_parameters.hpp"

#include "vector_builders.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <limits>

namespace components::vector {

    namespace {
        [[noreturn]] void refuse(const char* what) {
            std::fprintf(stderr, "storage_parameters: contract violated: %s\n", what);
            std::abort();
        }

        bool fits(const types::complex_logical_type& placeholder, const types::complex_logical_type& type) {
            if (placeholder.type() == types::logical_type::NA) {
                return true;
            }
            if (placeholder.type() == types::logical_type::LIST && type.type() == types::logical_type::LIST) {
                return fits(placeholder.child_type(), type.child_type());
            }
            return placeholder == type;
        }

    } // namespace

    bool is_placeholder(const types::complex_logical_type& type) noexcept {
        if (type.type() == types::logical_type::NA) {
            return true;
        }
        return type.type() == types::logical_type::LIST && is_placeholder(type.child_type());
    }

    types::complex_logical_type filled_placeholder(const types::complex_logical_type& placeholder,
                                                   const types::complex_logical_type& column) {
        if (placeholder.type() == types::logical_type::NA) {
            return column;
        }
        if (placeholder.type() == types::logical_type::LIST) {
            return types::complex_logical_type::create_list(filled_placeholder(placeholder.child_type(), column));
        }
        return placeholder;
    }

    storage_parameters::storage_parameters(std::pmr::memory_resource* resource)
        : resource_(resource)
        , slots_(resource) {}

    core::parameter_id_t storage_parameters::add(vector_t&& value, parameter_kind kind) {
        if (slots_.size() > std::numeric_limits<uint32_t>::max()) {
            refuse("more than 2^32-1 parameters");
        }
        assert(value.get_vector_type() == vector_type::CONSTANT);
        const auto id = core::parameter_id_t(static_cast<uint32_t>(slots_.size()));
        slots_.push_back(slot_t{std::move(value), kind});
        return id;
    }

    void storage_parameters::set(core::parameter_id_t id, vector_t&& value) {
        if (!is_valid(id)) {
            refuse("set: id out of range");
        }
        assert(value.get_vector_type() == vector_type::CONSTANT);
        auto& slot = slots_[id.t];
        // transform_result refuses a bind after finalize(), so the last bind of an external slot wins whatever
        // the type: no graph has been built against the old one.
        const auto& slot_type = slot.value.type();
        const bool accepted = slot.kind == parameter_kind::external ||
                              (is_placeholder(slot_type) ? fits(slot_type, value.type()) : slot_type == value.type());
        if (!accepted) {
            refuse("set: type differs from the slot's");
        }
        slot.value = std::move(value);
    }

    void storage_parameters::set_cell(core::parameter_id_t id, const vector_t& column, uint64_t row) {
        set(id, make_constant_copy(resource_, column, row));
    }

    core::parameter_id_t storage_parameters::add_cell(const vector_t& column, uint64_t row, parameter_kind kind) {
        return add(make_constant_copy(resource_, column, row), kind);
    }

    const vector_t& storage_parameters::at(core::parameter_id_t id) const {
        if (!is_valid(id)) {
            refuse("at: id out of range");
        }
        return slots_[id.t].value;
    }

    const types::complex_logical_type& storage_parameters::type(core::parameter_id_t id) const { return at(id).type(); }

    parameter_kind storage_parameters::kind(core::parameter_id_t id) const {
        if (!is_valid(id)) {
            refuse("kind: id out of range");
        }
        return slots_[id.t].kind;
    }

    bool storage_parameters::is_null(core::parameter_id_t id) const { return at(id).is_null(0); }

    storage_parameters storage_parameters::clone(std::pmr::memory_resource* target) const {
        storage_parameters result(target);
        result.slots_.reserve(slots_.size());
        for (const auto& slot : slots_) {
            result.slots_.push_back(slot_t{make_constant_copy(target, slot.value), slot.kind});
        }
        return result;
    }

} // namespace components::vector
