#include "index/forward.hpp"

#include <components/logical_plan/node_create_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <cstring>

using namespace components::expressions;

namespace components::sql::transform {
    namespace {
        logical_plan::index_type detect_index_type(const char* method) {
            if (method != nullptr && std::strcmp(method, "hash") == 0) {
                return logical_plan::index_type::hashed;
            }
            return logical_plan::index_type::single;
        }
    }

    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        if (!(node.relation && node.relation->relname && node.idxname)) {
            throw parser_exception_t{"incorrect create index arguments", ""};
        }

        auto create_index = logical_plan::make_node_create_index(resource_,
                                                                 rangevar_to_collection(node.relation),
                                                                 node.idxname,
                                                                 detect_index_type(node.accessMethod));
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(resource_, pg_ptr_cast<IndexElem>(key.data)->name);
        }
        return create_index;
    }

} // namespace components::sql::transform
