#include "explain_renderer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>

namespace services::collection {

    namespace {
        namespace ops = components::operators;

        // Map operator_type to a PostgreSQL-style label. EXHAUSTIVE over operator_type with NO
        // `default`: -Wswitch (Linux CI -Werror) then forces any newly-added op to be given a label,
        // so nothing silently blanks out and there is no fallback branch (Rule 6). The
        // proven-unreachable ops (they never sit on an EXPLAINed SELECT/DML spine) share
        // one grouped "?" arm whose label is never actually emitted.
        std::pmr::string pg_label(std::pmr::memory_resource* mr, const explain_plan_node& n) {
            std::pmr::string label(mr);
            switch (n.type) {
                case ops::operator_type::full_scan:
                case ops::operator_type::transfer_scan:
                    label = "Seq Scan";
                    break;
                case ops::operator_type::index_scan:
                    label = "Index Scan";
                    break;
                case ops::operator_type::pushed_reduce_scan:
                    label = "Pushed Aggregate Scan";
                    break;
                case ops::operator_type::hash_join:
                    label = "Hash Join";
                    break;
                case ops::operator_type::join:
                    label = "Nested Loop";
                    break;
                case ops::operator_type::aggregate:
                    label = "Aggregate";
                    break;
                case ops::operator_type::group_merge:
                    label = "Finalize Aggregate";
                    break;
                case ops::operator_type::sort:
                    label = "Sort";
                    break;
                case ops::operator_type::match:
                    label = "Filter";
                    break;
                case ops::operator_type::select:
                    label = "Project";
                    break;
                case ops::operator_type::insert:
                    label = "Insert";
                    break;
                case ops::operator_type::remove:
                    label = "Delete";
                    break;
                case ops::operator_type::update:
                    label = "Update";
                    break;
                case ops::operator_type::union_op:
                    label = "Append";
                    break;
                case ops::operator_type::recursive_cte:
                    label = "Recursive Union";
                    break;
                case ops::operator_type::cte_scan:
                    label = "CTE Scan";
                    break;
                case ops::operator_type::raw_data:
                    label = "Values Scan";
                    break;
                case ops::operator_type::function:
                    label = "Function Scan";
                    break;
                case ops::operator_type::check_constraint:
                    label = "Check Constraint";
                    break;
                case ops::operator_type::unique_constraint:
                    label = "Unique Check";
                    break;
                case ops::operator_type::fk_check:
                    label = "FK Check";
                    break;
                case ops::operator_type::fk_cascade:
                    label = "FK Cascade";
                    break;
                case ops::operator_type::computed_field_register:
                    label = "Computed Fields";
                    break;
                // Proven unreachable on an EXPLAINed SELECT/INSERT/UPDATE/DELETE spine (DDL/txn/utility
                // statements are rejected by transform_explain; resolve_* live in separate resolve
                // subplans; sequence is flattened; empty/batch/unused are never rendered). Grouped so
                // the switch stays exhaustive with no `default` — the "?" is never actually emitted.
                case ops::operator_type::unused:
                case ops::operator_type::empty:
                case ops::operator_type::sequence:
                case ops::operator_type::create_collection:
                case ops::operator_type::alter_column_add:
                case ops::operator_type::alter_column_rename:
                case ops::operator_type::alter_column_drop:
                case ops::operator_type::dynamic_cascade_delete:
                case ops::operator_type::checkpoint:
                case ops::operator_type::set_timezone:
                case ops::operator_type::vacuum:
                case ops::operator_type::register_udf:
                case ops::operator_type::unregister_udf:
                case ops::operator_type::commit_transaction:
                case ops::operator_type::abort_transaction:
                case ops::operator_type::begin_transaction:
                case ops::operator_type::computed_field_unregister:
                case ops::operator_type::resolve_table:
                case ops::operator_type::resolve_namespace:
                case ops::operator_type::resolve_database:
                case ops::operator_type::resolve_type:
                case ops::operator_type::resolve_constraint:
                case ops::operator_type::allocate_oids:
                case ops::operator_type::batch:
                    label = "?";
                    break;
            }
            if (!n.relation.empty()) {
                label += " on ";
                label.append(n.relation.data(), n.relation.size());
            }
            return label;
        }

        // PG-faithful per-loop stats; loops==0 -> "(never executed)" (also avoids divide-by-zero).
        std::pmr::string analyze_suffix(std::pmr::memory_resource* mr, const explain_plan_node& n) {
            if (n.loops == 0) {
                return std::pmr::string("  (never executed)", mr);
            }
            const double ms =
                std::chrono::duration<double, std::milli>(n.time).count() / static_cast<double>(n.loops);
            // PG rounds actual per-loop rows to nearest (rint); integer round-half-up avoids float drift.
            // loops>=1 here (loops==0 returned "(never executed)" above), so n.loops/2 < n.loops.
            const unsigned long long rows_per = static_cast<unsigned long long>((n.rows + n.loops / 2) / n.loops);
            char buf[160];
            std::snprintf(buf,
                          sizeof(buf),
                          "  (actual time=%.3fms rows=%llu loops=%llu)",
                          ms,
                          rows_per,
                          static_cast<unsigned long long>(n.loops));
            return std::pmr::string(buf, mr);
        }

        void render_node(std::pmr::memory_resource* mr,
                         const explain_plan_node& n,
                         int depth,
                         bool analyze,
                         std::pmr::vector<std::pmr::string>& lines) {
            std::pmr::string line(mr);
            if (depth > 0) {
                line.assign(static_cast<size_t>(depth) * 2, ' ');
                line += "->  ";
            }
            line += pg_label(mr, n);
            if (analyze) {
                line += analyze_suffix(mr, n);
            }
            lines.push_back(std::move(line));
            for (const auto& c : n.children) {
                render_node(mr, c, depth + 1, analyze, lines);
            }
        }
    } // namespace

    components::cursor::cursor_t_ptr
    render_postgres(std::pmr::memory_resource* mr, const explain_plan_node& root, bool analyze) {
        std::pmr::vector<std::pmr::string> lines(mr);
        render_node(mr, root, 0, analyze, lines);

        std::pmr::vector<components::types::complex_logical_type> types(mr);
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "QUERY PLAN");

        // Emit <=DEFAULT_VECTOR_CAPACITY (1024)-row chunks: a single data_chunk_t caps at 1024.
        std::pmr::vector<components::vector::data_chunk_t> chunks(mr);
        const size_t cap = components::vector::DEFAULT_VECTOR_CAPACITY;
        size_t i = 0;
        while (i < lines.size()) {
            const size_t n = std::min(cap, lines.size() - i);
            components::vector::data_chunk_t chunk(mr, types, n);
            chunk.set_cardinality(n);
            for (size_t r = 0; r < n; ++r) {
                chunk.set_value(0, r, std::string_view(lines[i + r]));
            }
            chunks.push_back(std::move(chunk));
            i += n;
        }
        if (chunks.empty()) {
            // Keep at least one (empty) chunk so the cursor has column metadata.
            components::vector::data_chunk_t chunk(mr, types, 1);
            chunk.set_cardinality(0);
            chunks.push_back(std::move(chunk));
        }
        return components::cursor::make_cursor(mr, std::move(chunks));
    }

} // namespace services::collection
