#include "check_predicate_compiler.hpp"

// TODO (Etap 3.5): replace stub with full compilation:
//   1. components::sql::transform::transformer t(resource); auto [expr, params] = t.parse_where_expr(conexpr);
//   2. resolve column key_t paths against col_types (similar to executor::resolve_check_key_paths)
//   3. components::operators::predicates::create_predicate(resource, &functions, expr, col_types, col_types, &params)
//   4. return [pred](const chunk, row) { return pred->check(chunk, row); }
// Adding those includes here (not in .hpp) keeps the catalog→physical_plan dep link-local.

namespace components::catalog {

    row_predicate_fn compile_check(
        std::pmr::memory_resource*,
        const compute::function_registry_t&,
        const std::string&                              conexpr,
        const std::vector<types::complex_logical_type>&)
    {
        if (conexpr.empty()) {
            return [](const vector::data_chunk_t&, std::uint64_t) { return true; };
        }
        // Stub: always-true. Replaced in Etap 3.5.
        return [](const vector::data_chunk_t&, std::uint64_t) { return true; };
    }

} // namespace components::catalog
