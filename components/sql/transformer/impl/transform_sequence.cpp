#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_sequence(CreateSeqStmt& node) {
        auto qn = rangevar_to_qualified_name(node.sequence);
        const std::string db_for_resolve = qn.dbname;

        int64_t start = 1;
        int64_t increment = 1;
        int64_t min_value = 1;
        int64_t max_value = std::numeric_limits<int64_t>::max();

        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname)
                    continue;
                std::string opt_name(def->defname);
                if (opt_name == "start" && def->arg) {
                    start = intVal(def->arg);
                } else if (opt_name == "increment" && def->arg) {
                    increment = intVal(def->arg);
                } else if (opt_name == "minvalue" && def->arg) {
                    min_value = intVal(def->arg);
                } else if (opt_name == "maxvalue" && def->arg) {
                    max_value = intVal(def->arg);
                }
            }
        }

        auto seq = logical_plan::make_node_create_sequence(resource_,
                                                           core::seqname_t{std::move(qn.relname)},
                                                           start,
                                                           increment,
                                                           min_value,
                                                           max_value);
        // Wrap with namespace resolve so enrich's create_sequence_t case can
        // read ns_oid from plan-tree idx.
        return maybe_wrap_with_catalog_resolve_namespace(resource_, db_for_resolve, std::move(seq));
    }

} // namespace components::sql::transform
