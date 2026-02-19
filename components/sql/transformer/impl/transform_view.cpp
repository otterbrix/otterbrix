#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <cctype>

namespace components::sql::transform {

    namespace {
        // Case-insensitive search for a whole word in a string
        std::string extract_view_query(const char* sql) {
            std::string s(sql);
            // Convert to uppercase for searching
            std::string upper(s);
            std::transform(upper.begin(), upper.end(), upper.begin(),
                           [](unsigned char c) { return std::toupper(c); });

            // Find " AS " keyword — marks start of the query
            auto pos = upper.find(" AS ");
            if (pos == std::string::npos) {
                return "SELECT *";
            }

            // Extract everything after "AS "
            auto query_start = pos + 4; // skip " AS "
            auto query = s.substr(query_start);

            // Trim trailing semicolons and whitespace
            while (!query.empty() && (query.back() == ';' || query.back() == ' '
                                      || query.back() == '\n' || query.back() == '\r'
                                      || query.back() == '\t')) {
                query.pop_back();
            }

            return query.empty() ? "SELECT *" : query;
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_create_view(ViewStmt& node) {
        auto name = rangevar_to_collection(node.view);

        std::string query_sql;
        if (raw_sql_) {
            query_sql = extract_view_query(raw_sql_);
        } else {
            query_sql = "SELECT *";
        }

        return logical_plan::make_node_create_view(resource_, name, std::move(query_sql));
    }

} // namespace components::sql::transform
