#include "prepared_expansion.hpp"
#include "view_expansion.hpp"

#include <components/sql/transformer/transformer.hpp>
#include <core/pmr.hpp>

namespace components::planner {

    core::result_wrapper_t<logical_plan::execution_plan_t> expand_prepared(std::pmr::memory_resource* resource,
                                                                           const std::string& text,
                                                                           std::span<const vector::vector_t> values,
                                                                           prepared_use_t use) {
        core::pmr::arena_resource_t parser_arena(resource);
        auto statement = parse_stored_body(resource, &parser_arena, text, "prepared statement");
        if (statement.has_error()) {
            return statement.error();
        }
        // PostgreSQL 18's grammar lets PREPARE hold these only (PreparableStmt); a host's text is held to it.
        switch (nodeTag(statement.value())) {
            case T_SelectStmt:
            case T_InsertStmt:
            case T_UpdateStmt:
            case T_DeleteStmt:
                break;
            default:
                return core::error_t{core::error_code_t::protocol_violation,
                                     std::pmr::string{"the body of a prepared statement must be SELECT, INSERT, "
                                                      "UPDATE or DELETE",
                                                      resource}};
        }
        components::sql::transform::transformer local_transformer(resource, text.c_str());
        auto binder = local_transformer.transform(*statement.value());
        for (std::size_t i = 0; i < values.size(); ++i) {
            // PostgreSQL 18 keeps a declared parameter the body never names: it has its declared type.
            if (binder.names_parameter(i + 1)) {
                binder.bind(i + 1, values[i]);
            } else {
                binder.declare(i + 1, values[i].type());
            }
        }
        auto finalized = use == prepared_use_t::execute ? binder.finalize() : binder.finalize_for_describe();
        if (finalized.has_error()) {
            return core::error_on(resource, finalized.error());
        }
        return std::move(finalized.value());
    }

} // namespace components::planner
