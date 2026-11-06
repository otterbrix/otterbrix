#include <components/logical_plan/node_cursor.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_declare_cursor(DeclareCursorStmt& node, logical_plan::execution_plan_t* plan) {
        if (node.options & CURSOR_OPT_SCROLL) {
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"DECLARE ... SCROLL is not supported: a cursor reads forward only", resource_});
        }
        if (node.options & CURSOR_OPT_HOLD) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"DECLARE ... WITH HOLD is not supported: a cursor ends with its "
                                                  "transaction",
                                                  resource_});
        }
        auto query = transform(*node.query, plan);
        if (query.has_error()) {
            return query;
        }
        auto declared = logical_plan::make_node_declare_cursor(resource_, node.portalname);
        declared->append_child(query.value());
        return logical_plan::node_ptr{declared};
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_fetch(FetchStmt& node) {
        if (node.direction != FETCH_FORWARD || node.howMany < 0) {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"FETCH and MOVE read forward only: PRIOR, FIRST, LAST, ABSOLUTE, "
                                                  "RELATIVE and BACKWARD are not supported",
                                                  resource_});
        }
        const uint64_t count =
            node.howMany == FETCH_ALL ? logical_plan::fetch_all : static_cast<uint64_t>(node.howMany);
        return logical_plan::node_ptr{logical_plan::make_node_fetch(resource_, node.portalname, count, node.ismove)};
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_close_cursor(ClosePortalStmt& node) {
        return logical_plan::node_ptr{
            logical_plan::make_node_close_cursor(resource_, node.portalname ? node.portalname : std::string{})};
    }

} // namespace components::sql::transform
