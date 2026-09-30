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

        // The operator names itself (operator_t::explain_label); a scan adds the relation it reads.
        std::pmr::string pg_label(std::pmr::memory_resource* mr, const explain_plan_node& n) {
            std::pmr::string label(n.label, mr);
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
            const double ms = std::chrono::duration<double, std::milli>(n.time).count() / static_cast<double>(n.loops);
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
                         std::pmr::vector<std::pmr::string>& lines,
                         int& initplan_no) {
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
            // PostgreSQL prints a node's details two columns right of where its label starts.
            const std::size_t detail_indent = depth > 0 ? static_cast<std::size_t>(depth) * 2 + 6 : 2;
            for (const auto& detail : n.details) {
                std::pmr::string detail_line(detail_indent, ' ', mr);
                detail_line += detail;
                lines.push_back(std::move(detail_line));
            }
            for (const auto& c : n.children) {
                render_node(mr, c, depth + 1, analyze, lines, initplan_no);
            }
            // PostgreSQL-style InitPlans: each flattened sub-query hangs on the node that holds it (here,
            // always the root — see explain_plan.hpp). `InitPlan k` is numbered globally across the whole
            // query (PG numbering is global, not per-node); `$M` is the returned parameter slot.
            for (const auto& sp : n.subplans) {
                ++initplan_no;
                std::pmr::string hdr(mr);
                hdr.assign(static_cast<size_t>(depth + 1) * 2, ' ');
                char buf[64];
                std::snprintf(buf, sizeof(buf), "InitPlan %d (returns $%u)", initplan_no, sp.subplan_returns);
                hdr += buf;
                lines.push_back(std::move(hdr));
                render_node(mr, sp, depth + 2, analyze, lines, initplan_no);
            }
        }
    } // namespace

    components::cursor::cursor_t_ptr
    render_postgres(std::pmr::memory_resource* mr, const explain_plan_node& root, bool analyze) {
        std::pmr::vector<std::pmr::string> lines(mr);
        int initplan_no = 0;
        render_node(mr, root, 0, analyze, lines, initplan_no);

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
