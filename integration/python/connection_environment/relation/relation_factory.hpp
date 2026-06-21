#pragma once
#include "../expression/expression_factory.hpp"
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/table/column_definition.hpp>
#include <components/tableref/tableref.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <memory>

#include <memory_resource>
#include <vector>

using namespace components::logical_plan;

namespace otterbrix {

    // A relation in the Python integration is now just an eagerly-built
    // logical_plan node together with the column schema it produces. The
    // schema is carried alongside the node because the Python API needs
    // column names/types before the plan is executed; it is recomputed at
    // each chaining op, mirroring the former ColumnsVisitor's per-op logic.
    struct built_relation_t {
        components::logical_plan::node_ptr node;
        std::pmr::vector<components::table::column_definition_t> columns;
    };

    class relation_factory_t {
    public:
        relation_factory_t(const boost::intrusive_ptr<otterbrix_t>& space);
        virtual ~relation_factory_t();
        void set_null_space();

        // Chaining operations: each takes the source node + its column schema
        // and returns the new node + its (eagerly recomputed) column schema.
        built_relation_t filter_relation(const built_relation_t& relation, const expression_wrapper_t& condition);
        built_relation_t sort_relation(const built_relation_t& relation,
                                       const std::vector<expression_wrapper_t>& exprs);
        built_relation_t group_relation(const built_relation_t& relation,
                                        const std::vector<expression_wrapper_t>& exprs);
        built_relation_t select_relation(const built_relation_t& relation,
                                         const std::vector<expression_wrapper_t>& exprs);

        built_relation_t join_relation(const built_relation_t& relation,
                                       const built_relation_t& other,
                                       const std::vector<expression_wrapper_t>& exprs,
                                       components::logical_plan::join_type type);

        built_relation_t limit_relation(const built_relation_t& relation, int64_t count);

        built_relation_t create_df_relation(std::unique_ptr<components::tableref::table_ref_t> tableref);

    private:
        // Wrap a source node in an aggregate node carrying one of
        // group/match/sort/select/limit children. Allocates the tmp.tN table
        // exactly as the former make_aggregate_relation did and returns the
        // composed aggregate node (column schema is computed by the callers).
        components::logical_plan::node_ptr
        make_aggregate_node(const components::logical_plan::node_ptr& from,
                            components::logical_plan::node_group_ptr group,
                            components::logical_plan::node_match_ptr match,
                            components::logical_plan::node_sort_ptr sort,
                            components::logical_plan::node_select_ptr select,
                            components::logical_plan::node_limit_ptr limit = nullptr);

        boost::intrusive_ptr<otterbrix_t> space;
    };

} // namespace otterbrix
