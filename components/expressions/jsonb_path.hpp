#pragma once

#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

// Single source of truth for the jsonb path <-> flattened-column-name mapping.
//
// otterbrix stores a nested jsonb path as one flat column on a "computing" table:
// the path a.b is the column named "a/b", segments joined by the reserved
// separator '/'. Every jsonb operator (navigation ->/->>, path #>/#>>, existence
// ?/?|/?&, expansion, deletion) and the INSERT target flattener build and match
// these names, so the read and write sides MUST agree byte-for-byte — this header
// is the one place that defines the split (operand text -> segments) and the join
// (segments -> flattened name).
//
// NOTE on the '/' separator: because a computing-table column name and a nested
// path share one namespace, a column whose literal name contains '/' (e.g. a
// quoted "a/b") is indistinguishable from the two-segment path a.b — both are the
// storage name "a/b". That is an inherent property of the flattened
// representation, not something this codec can resolve without escaping every
// ordinary column identifier system-wide; callers that care must reserve '/'.
namespace components::expressions::jsonb_path {

    inline constexpr char separator = '/';

    // Join path segments into the flattened storage-column name (segments are used
    // verbatim; the caller guarantees they do not contain the separator).
    inline std::pmr::string flatten(const std::pmr::vector<std::pmr::string>& segments,
                                    std::pmr::memory_resource* resource) {
        std::pmr::string out(resource);
        bool first = true;
        for (const auto& seg : segments) {
            if (!first) {
                out += separator;
            }
            out += seg;
            first = false;
        }
        return out;
    }

    // Split a jsonb path OPERAND string into its segments. Two spellings are
    // accepted, matching PostgreSQL's #> path operators:
    //   dotted        'a.b'   -> ["a", "b"]
    //   pg text-array '{a,b}' -> ["a", "b"]   (braces stripped, per-segment spaces trimmed)
    // Empty / all-space segments are dropped. This is the single splitter used by
    // every path-taking operator.
    inline std::pmr::vector<std::pmr::string> split_operand(std::string_view raw, std::pmr::memory_resource* resource) {
        std::pmr::vector<std::pmr::string> segments(resource);
        auto push = [&](std::string_view s) {
            size_t b = s.find_first_not_of(' ');
            if (b == std::string_view::npos) {
                return; // empty / all spaces
            }
            size_t e = s.find_last_not_of(' ');
            segments.emplace_back(std::pmr::string{s.substr(b, e - b + 1), resource});
        };
        std::string_view body = raw;
        char delim = '.';
        if (raw.size() >= 2 && raw.front() == '{' && raw.back() == '}') {
            body = raw.substr(1, raw.size() - 2);
            delim = ',';
        }
        size_t start = 0;
        while (true) {
            size_t next = body.find(delim, start);
            push(next == std::string_view::npos ? body.substr(start) : body.substr(start, next - start));
            if (next == std::string_view::npos) {
                break;
            }
            start = next + 1;
        }
        return segments;
    }

} // namespace components::expressions::jsonb_path
