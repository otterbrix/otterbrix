#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::dispatcher::validation {

    // Position of a column inside a schema: path[0] is the top-level column index,
    // each further entry descends one nesting level (struct field / array or list element).
    using column_path = std::pmr::vector<size_t>;

    // A catalog column some key of the plan resolved to.
    struct column_use_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
    };
    using column_uses_t = std::pmr::vector<column_use_t>;

    // One column of a node's output schema.
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
        // The source element of the column
        components::expressions::source_element_id_t source_element = components::expressions::NO_SOURCE_ELEMENT;
        // The nested joins this column passed through as a join input
        std::pmr::vector<components::expressions::source_element_id_t> joined_inputs{};
        // Set when this column is a bare NULL literal (a scalar constant whose value is NULL, whose type was
        // defaulted to text). Lets a UNION reconcile the column to the other branch's type (PostgreSQL).
        bool from_null_literal = false;
        // A catalog table's own column; INVALID_OID for any other. find_types records it into the collector it is
        // given (CREATE VIEW) whenever a key resolves to the column.
        column_use_t origin{};
    };

    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

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

    // Does `column` come from `relation`: that source element, or a nested join it passed through as an input?
    bool of_relation(const type_from_t& column, components::expressions::source_element_id_t relation);

    // Resolve `key` against `schema`, returning every column it addresses ('*' and
    // 'table.*' expand to many). Stores the resolved path back into the key. `uses`, when not null, collects the catalog
    // column of each addressed top-level column (CREATE VIEW's dependencies).
    [[nodiscard]] core::result_wrapper_t<type_paths> find_types(std::pmr::memory_resource* resource,
                                                                components::expressions::key_t& key,
                                                                const named_schema& schema,
                                                                column_uses_t* uses);

    // Two-input form for JOIN / UPDATE..FROM / DELETE..USING contexts. Resolves against the
    // input holding the relation the key names, otherwise tries both. The stored path indexes
    // the inputs laid side by side: a right column sits after every left one.
    // A null right schema is a single input: nothing to be ambiguous against.
    [[nodiscard]] core::result_wrapper_t<type_paths> validate_key(std::pmr::memory_resource* resource,
                                                                  components::expressions::key_t& key,
                                                                  const named_schema* schema_left,
                                                                  const named_schema* schema_right,
                                                                  column_uses_t* uses);

} // namespace services::dispatcher::validation
