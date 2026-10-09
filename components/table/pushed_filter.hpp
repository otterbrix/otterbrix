#pragma once

#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/clone_expression.hpp>
#include <components/table/column_state.hpp>
#include <components/types/parameter_map.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory>
#include <memory_resource>

namespace components::table {

    // A scan predicate as it crosses the mailbox to a disk agent: a detached expression tree carrying
    // its own copies of the functions it calls, its parameters by value on the receiver's resource,
    // and the evaluation context without the fill-value pointer. The agent builds its own
    // table_filter_t from it.
    struct pushed_filter_t final {
        pushed_filter_t(expressions::detached_expression_t&& expression,
                        types::parameter_map_t&& parameters,
                        const graph_execution_context& context);

        expressions::detached_expression_t expression;
        types::parameter_map_t parameters;
        core::date::timezone_offset_t timezone_offset;
        uint8_t decimal_width;
        uint8_t decimal_scale;
    };

    // Sender side: deep copies onto `target`, the receiving actor's resource.
    [[nodiscard]] std::unique_ptr<pushed_filter_t> make_pushed_filter(std::pmr::memory_resource* target,
                                                                      const expressions::expression_ptr& expression,
                                                                      const types::parameter_map_t& parameters,
                                                                      const graph_execution_context& context);

    // Receiver side: everything the built filter holds lives on `resource`.
    [[nodiscard]] core::result_wrapper_t<std::unique_ptr<table_filter_t>>
    build_table_filter(std::pmr::memory_resource* resource,
                       const pushed_filter_t& filter,
                       const std::pmr::vector<types::complex_logical_type>& types);

} // namespace components::table
