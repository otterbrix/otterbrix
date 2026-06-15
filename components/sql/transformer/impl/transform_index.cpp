#include <components/logical_plan/node_create_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <algorithm>
#include <cstring>

using namespace components::expressions;

namespace components::sql::transform {

    namespace {
        logical_plan::index_type detect_index_type(const char* method) {
            if (method != nullptr && std::strcmp(method, "hash") == 0) {
                return logical_plan::index_type::hashed;
            }
            if (method != nullptr && std::strcmp(method, "hnsw") == 0) {
                return logical_plan::index_type::vector_hnsw;
            }
            return logical_plan::index_type::single;
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_create_index(IndexStmt& node) {
        if (!(node.relation && node.relation->relname && node.relation->catalogname && node.idxname)) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"incorrect create index arguments", resource_});
            return nullptr;
        }

        auto qn = rangevar_to_qualified_name(node.relation);
        const std::string dbname_for_resolve = qn.dbname;
        const std::string relname_for_resolve = qn.relname;
        auto index_type = detect_index_type(node.accessMethod);
        auto create_index = logical_plan::make_node_create_index(resource_,
                                                                 core::indexname_t{std::string(node.idxname)},
                                                                 index_type);
        for (auto key : node.indexParams->lst) {
            create_index->keys().emplace_back(resource_, pg_ptr_cast<IndexElem>(key.data)->name);
        }

        if (index_type == logical_plan::index_type::vector_hnsw) {
            // Opclass selects the metric (default L2)
            auto metric = vector_search::metric_type::l2;
            for (auto key : node.indexParams->lst) {
                auto elem = pg_ptr_cast<IndexElem>(key.data);
                if (!elem->opclass || elem->opclass->lst.empty()) {
                    continue;
                }
                std::string opclass(strVal(elem->opclass->lst.back().data));
                if (opclass == "vector_l2_ops") {
                    metric = vector_search::metric_type::l2;
                } else if (opclass == "vector_cosine_ops") {
                    metric = vector_search::metric_type::cosine;
                } else if (opclass == "vector_ip_ops") {
                    metric = vector_search::metric_type::inner_product;
                } else {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"operator class \"" + opclass + "\" does not exist",
                                                            resource_});
                    return nullptr;
                }
            }

            // WITH (m, ef_construction)
            uint64_t hnsw_m = 16;
            uint64_t hnsw_ef = 64;
            if (node.options) {
                for (auto data : node.options->lst) {
                    auto def = pg_ptr_cast<DefElem>(data.data);
                    if (!def->defname || !def->arg)
                        continue;
                    std::string opt_name(def->defname);
                    if (opt_name == "m") {
                        hnsw_m = static_cast<uint64_t>(intVal(def->arg));
                    } else if (opt_name == "ef_construction") {
                        hnsw_ef = static_cast<uint64_t>(intVal(def->arg));
                    } else {
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"unrecognized parameter \"" + opt_name + "\"",
                                                                resource_});
                        return nullptr;
                    }
                }
            }
            create_index->set_vector_params(metric, hnsw_m, hnsw_ef);
        }
        // Wrap with catalog_resolve so Pass 1 stamps ns_oid + table_oid +
        // columns; enrich_logical_plan reads from the plan-tree idx.
        return maybe_wrap_with_catalog_resolve_table(resource_,
                                                     dbname_for_resolve,
                                                     relname_for_resolve,
                                                     std::move(create_index));
    }

} // namespace components::sql::transform
