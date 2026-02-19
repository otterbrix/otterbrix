#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_sequence(CreateSeqStmt& node) {
        auto name = rangevar_to_collection(node.sequence);

        int64_t start = 1;
        int64_t increment = 1;
        int64_t min_value = 1;
        int64_t max_value = std::numeric_limits<int64_t>::max();

        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (!def->defname) continue;
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

        return logical_plan::make_node_create_sequence(resource_, name, start, increment, min_value, max_value);
    }

} // namespace components::sql::transform
