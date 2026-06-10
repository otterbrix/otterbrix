#pragma once

// Plan-tree catalog lookup index.
//
// The transformer wraps DML/DDL plans with catalog_resolve_*_t leaf nodes.
// The dispatcher/executor runs those leaves through operator_resolve_*_t
// (which queries pg_catalog via the standard executor pipeline) and stamps
// the resolved OID + metadata onto each leaf via back-pointer.
//
// gather_plan_resolve_index() walks the tree and harvests those stamps into
// a flat map keyed by (dbname[, relname|type_name|fn_name|table_oid]) so
// validate / enrich / dispatcher can read OIDs + metadata synchronously
// instead of issuing async catalog reads.

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/table_id.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve_constraint.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>

#include <components/types/types.hpp>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

// Under `services::catalog_resolve` so both dispatcher and executor share the
// same gather + lookup primitives; re-exported into `dispatcher::impl` below.
namespace services::catalog_resolve {

    struct cte_schema_column_t {
        std::pmr::string name;
        components::types::complex_logical_type type;
    };
    using cte_schema_t = std::vector<cte_schema_column_t>;

    struct plan_resolve_index_t {
        // Namespace name -> ns_oid (from node_catalog_resolve_namespace_t
        // AND from node_catalog_resolve_table_t::namespace_oid()).
        std::unordered_map<std::string, components::catalog::oid_t> ns_by_dbname;
        // "dbname|relname" -> const resolved_table_metadata_t*. Points into
        // the resolve_table node's resolved_metadata() optional (stamped by
        // operator_resolve_table_t). Validate reads columns/relkind here.
        std::unordered_map<std::string, const components::logical_plan::resolved_table_metadata_t*> tbl_md_by_qname;
        // table_oid -> const resolved_table_metadata_t*. Oid-keyed mirror.
        std::unordered_map<components::catalog::oid_t, const components::logical_plan::resolved_table_metadata_t*>
            tbl_md_by_oid;
        // "dbname|typename" -> type_oid (from resolve_type nodes).
        std::unordered_map<std::string, components::catalog::oid_t> type_oid_by_qname;
        // "dbname|typename" -> const resolved_type_metadata_t*. Points
        // into the resolve_type node's resolved_metadata() optional.
        // resolve_type.cpp + enrich(drop_type_t) + dispatcher UDT existence
        // checks read decoded complex_logical_type via this map.
        std::unordered_map<std::string, const components::logical_plan::resolved_type_metadata_t*> type_md_by_qname;
        // "dbname|fnname" -> fn_oid (from resolve_function nodes).
        std::unordered_map<std::string, components::catalog::oid_t> fn_oid_by_qname;
        // Constraint snapshots keyed by parent table_oid.
        // outgoing_fks_by_oid: FKs where the target is the CHILD (INSERT/UPDATE).
        // referencing_fks_by_oid: FKs where the target is the PARENT (DELETE).
        // check_exprs_by_oid: CHECK constraint expressions for INSERT/UPDATE.
        std::unordered_map<components::catalog::oid_t, std::vector<components::catalog::fk_info_t>> outgoing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t, std::vector<components::catalog::fk_info_t>>
            referencing_fks_by_oid;
        std::unordered_map<components::catalog::oid_t, std::vector<std::pair<std::string, std::string>>>
            check_exprs_by_oid;

        // CTE name → anchor column schema for recursive CTE working-set resolution.
        // Populated by validate_schema when processing node_recursive_cte_t;
        // read by validate_schema when processing node_cte_scan_t.
        std::unordered_map<std::pmr::string, cte_schema_t> cte_schemas;

        bool empty() const noexcept {
            return ns_by_dbname.empty() && type_oid_by_qname.empty() && fn_oid_by_qname.empty() &&
                   outgoing_fks_by_oid.empty() && referencing_fks_by_oid.empty() && check_exprs_by_oid.empty();
        }
    };

    // Walk plan tree once; collect every node_catalog_resolve_*_t leaf
    // and populate the index. Leaves whose oid is still INVALID_OID
    // (resolve operator did not stamp them — name did not resolve) are skipped.
    inline void gather_plan_resolve_index(components::logical_plan::node_t* root, plan_resolve_index_t* out) {
        using namespace components::logical_plan;
        if (!root)
            return;
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            switch (n->type()) {
                case node_type::catalog_resolve_namespace_t: {
                    auto* rn = static_cast<node_catalog_resolve_namespace_t*>(n);
                    if (rn->namespace_oid() != components::catalog::INVALID_OID) {
                        out->ns_by_dbname[rn->dbname()] = rn->namespace_oid();
                    }
                    break;
                }
                case node_type::catalog_resolve_table_t: {
                    auto* rt = static_cast<node_catalog_resolve_table_t*>(n);
                    if (rt->namespace_oid() != components::catalog::INVALID_OID) {
                        out->ns_by_dbname[rt->dbname()] = rt->namespace_oid();
                        std::string key;
                        key.reserve(rt->dbname().size() + 1 + rt->relname().size());
                        key.append(rt->dbname()).push_back('|');
                        key.append(rt->relname());
                        // Stamp table metadata pointer so validate /
                        // dispatcher can read columns + relkind from the
                        // plan-tree idx.
                        if (rt->resolved_metadata().has_value()) {
                            const auto* md_ptr = &rt->resolved_metadata().value();
                            out->tbl_md_by_qname[std::move(key)] = md_ptr;
                            if (rt->table_oid() != components::catalog::INVALID_OID) {
                                out->tbl_md_by_oid[rt->table_oid()] = md_ptr;
                            }
                        }
                    }
                    break;
                }
                case node_type::catalog_resolve_type_t: {
                    auto* tr = static_cast<node_catalog_resolve_type_t*>(n);
                    if (tr->type_oid() != components::catalog::INVALID_OID) {
                        std::string key;
                        key.reserve(tr->dbname().size() + 1 + tr->type_name().size());
                        key.append(tr->dbname()).push_back('|');
                        key.append(tr->type_name());
                        out->type_oid_by_qname[key] = tr->type_oid();
                        if (tr->resolved_metadata().has_value()) {
                            out->type_md_by_qname[std::move(key)] = &tr->resolved_metadata().value();
                        }
                    }
                    break;
                }
                case node_type::catalog_resolve_function_t: {
                    auto* fr = static_cast<node_catalog_resolve_function_t*>(n);
                    if (fr->function_oid() != components::catalog::INVALID_OID) {
                        std::string key;
                        key.reserve(fr->dbname().size() + 1 + fr->function_name().size());
                        key.append(fr->dbname()).push_back('|');
                        key.append(fr->function_name());
                        out->fn_oid_by_qname[std::move(key)] = fr->function_oid();
                    }
                    break;
                }
                case node_type::catalog_resolve_constraint_t: {
                    auto* cr = static_cast<node_catalog_resolve_constraint_t*>(n);
                    if (!cr->target())
                        break;
                    const auto& md = cr->target()->resolved_metadata();
                    if (!md.has_value() || md->table_oid == components::catalog::INVALID_OID) {
                        break;
                    }
                    using direction_t = node_catalog_resolve_constraint_t::direction_t;
                    if (cr->direction() == direction_t::outgoing) {
                        out->outgoing_fks_by_oid[md->table_oid] = cr->fks();
                        out->check_exprs_by_oid[md->table_oid] = cr->check_exprs();
                    } else {
                        out->referencing_fks_by_oid[md->table_oid] = cr->fks();
                    }
                    break;
                }
                default:
                    break;
            }
            for (const auto& c : n->children()) {
                if (c)
                    q.push(c.get());
            }
        }
    }

    inline const components::logical_plan::resolved_table_metadata_t*
    tbl_md_for(const plan_resolve_index_t* idx, std::string_view dbname, std::string_view relname) {
        if (!idx)
            return nullptr;
        std::string key;
        key.reserve(dbname.size() + 1 + relname.size());
        key.append(dbname.data(), dbname.size()).push_back('|');
        key.append(relname.data(), relname.size());
        auto it = idx->tbl_md_by_qname.find(key);
        return it != idx->tbl_md_by_qname.end() ? it->second : nullptr;
    }

    inline const components::logical_plan::resolved_type_metadata_t*
    type_md_for(const plan_resolve_index_t* idx, std::string_view dbname, std::string_view type_name) {
        if (!idx)
            return nullptr;
        std::string key;
        key.reserve(dbname.size() + 1 + type_name.size());
        key.append(dbname.data(), dbname.size()).push_back('|');
        key.append(type_name.data(), type_name.size());
        auto it = idx->type_md_by_qname.find(key);
        return it != idx->type_md_by_qname.end() ? it->second : nullptr;
    }

    inline const components::logical_plan::resolved_table_metadata_t* tbl_md_for_oid(const plan_resolve_index_t* idx,
                                                                                     components::catalog::oid_t oid) {
        if (!idx)
            return nullptr;
        auto it = idx->tbl_md_by_oid.find(oid);
        return it != idx->tbl_md_by_oid.end() ? it->second : nullptr;
    }

    inline components::catalog::oid_t ns_oid_for_dbname(const plan_resolve_index_t* idx, std::string_view dbname) {
        if (dbname.empty())
            return components::catalog::INVALID_OID;
        if (!idx)
            return components::catalog::INVALID_OID;
        if (auto it = idx->ns_by_dbname.find(std::string(dbname)); it != idx->ns_by_dbname.end()) {
            return it->second;
        }
        return components::catalog::INVALID_OID;
    }

} // namespace services::catalog_resolve

// Lets dispatcher/validate/resolve_type keep spelling these as `impl::...`.
namespace services::dispatcher::impl {
    using catalog_resolve::gather_plan_resolve_index;
    using catalog_resolve::ns_oid_for_dbname;
    using catalog_resolve::plan_resolve_index_t;
    using catalog_resolve::tbl_md_for;
    using catalog_resolve::tbl_md_for_oid;
    using catalog_resolve::type_md_for;
} // namespace services::dispatcher::impl