#pragma once

#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::dispatcher::validation {

    // Position of a column inside a schema: path[0] is the top-level column index,
    // each further entry descends one nesting level (struct field / array or list element).
    using column_path = std::pmr::vector<size_t>;

    // One column of a node's output schema.
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
        components::expressions::side_t side = components::expressions::side_t::undefined;
        // Set when this column is a bare NULL literal (a scalar constant whose value is NULL, whose type was
        // defaulted to text). Lets a UNION reconcile the column to the other branch's type (PostgreSQL).
        bool from_null_literal = false;
    };

    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Type as it should read in a user-facing message: the declared name for a named
    // type (ENUM / STRUCT / UDT), the pg_type name for everything else.
    std::string describe_type(const components::types::complex_logical_type& type);

    // JOIN schema = pure concatenation of both sides. The runtime join
    // operators (join_utils.hpp: res_types = left.types() + all right
    // types) emit every column of both inputs, including same-named join
    // keys. Dropping a duplicate here desynchronizes the validator's
    // column indices from the runtime chunk layout: every key path
    // resolved after the dropped column points one column to the left
    // (wrong values for same-typed columns, kernel errors or worse for
    // mismatched ones). Notably both sides carry an empty result_alias
    // when they are raw node_data inputs, so a same-named join key used
    // to be deduplicated exactly there.
    named_schema merge_schemas(std::pmr::memory_resource* resource, named_schema lhs, named_schema rhs);

    // Resolve `key` against `schema`, returning every column it addresses ('*' and
    // 'table.*' expand to many). Stores the resolved path back into the key.
    [[nodiscard]] core::result_wrapper_t<type_paths>
    find_types(std::pmr::memory_resource* resource, components::expressions::key_t& key, const named_schema& schema);

    // Two-sided form for JOIN / UPDATE..FROM / DELETE..USING contexts. Resolves against the
    // side the key already names, otherwise tries both and stores the side that matched.
    // A null right schema is a single input: nothing to be ambiguous against.
    [[nodiscard]] core::result_wrapper_t<type_paths> validate_key(std::pmr::memory_resource* resource,
                                                                  components::expressions::key_t& key,
                                                                  const named_schema* schema_left,
                                                                  const named_schema* schema_right = nullptr);

} // namespace services::dispatcher::validation
