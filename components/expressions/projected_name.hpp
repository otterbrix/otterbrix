#pragma once

#include <components/expressions/scalar_expression.hpp>

#include <string>

namespace components::expressions {

    // The name a projected column runs under: the key of the expression, else the column a bare read reads, else
    // none. The validator answers RETURNING by it, physgen names the output column by it.
    [[nodiscard]] inline std::string projected_name(const expression_ptr& expression) {
        if (expression->group() != expression_group::scalar) {
            return expression->key().as_string();
        }
        if (!expression->key().storage().empty()) {
            return std::string{expression->key().storage().back()};
        }
        const auto* scalar = static_cast<const scalar_expression_t*>(expression.get());
        if (scalar->type() == scalar_type::get_field && !scalar->params().empty()) {
            const auto* field = std::get_if<key_t>(&scalar->params().front());
            if (field != nullptr && !field->storage().empty()) {
                return std::string{field->storage().back()};
            }
        }
        return {};
    }

} // namespace components::expressions
