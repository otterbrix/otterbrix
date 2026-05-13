#pragma once

// Phase 13 T16 / M2a: plan-tree-based catalog lookup index.
//
// When the transformer emits node_catalog_resolve_*_t leaf nodes (gated
// by toggle g_emit_catalog_resolve_enabled) and the dispatcher's Pass 1
// (M1) executes them PRE-validate, each resolve node carries the OID(s)
// stamped by operator_resolve_*_t back-pointers. This index gathers
// those OIDs into name-keyed maps so validate / enrich can probe them
// instead of falling back to async catalog_view::get_*.
//
// Originally lived (anonymous-namespace) inside validate_logical_plan.cpp.
// M2a moves it to this shared header so enrich_logical_plan.cpp (and
// future M5 sites) can use the same probe-then-fallback pattern.

#include "catalog_view.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/table_id.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>

#include <atomic>
#include <cstddef>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>

namespace services::dispatcher::impl {

    struct plan_resolve_index_t {
        // Namespace name -> ns_oid (from node_catalog_resolve_namespace_t
        // AND from node_catalog_resolve_table_t::namespace_oid()).
        std::unordered_map<std::string, components::catalog::oid_t> ns_by_dbname;
        // "dbname|relname" -> namespace_oid (from resolve_table nodes).
        std::unordered_map<std::string, components::catalog::oid_t> tbl_ns_by_qname;
        // "dbname|typename" -> type_oid (from resolve_type nodes).
        std::unordered_map<std::string, components::catalog::oid_t> type_oid_by_qname;
        // "dbname|fnname" -> fn_oid (from resolve_function nodes).
        std::unordered_map<std::string, components::catalog::oid_t> fn_oid_by_qname;

        bool empty() const noexcept {
            return ns_by_dbname.empty() && tbl_ns_by_qname.empty() &&
                   type_oid_by_qname.empty() && fn_oid_by_qname.empty();
        }
    };

    // Telemetry: count how often the plan-tree probe beats / falls
    // through to the catalog_view fallback. observability only.
    inline std::atomic<std::size_t>& plan_resolve_ns_hit_counter() {
        static std::atomic<std::size_t> v{0};
        return v;
    }
    inline std::atomic<std::size_t>& plan_resolve_ns_miss_counter() {
        static std::atomic<std::size_t> v{0};
        return v;
    }

    // Thread-local active index pointer. Set via scoped_plan_resolve_index_t
    // at the top of validate / enrich; queried by ns_oid_for and similar.
    // nullptr means "no M2a path active — use catalog_view fallback only".
    inline const plan_resolve_index_t*& active_plan_resolve_index() {
        thread_local const plan_resolve_index_t* p = nullptr;
        return p;
    }

    // RAII setter. Stacks (saves prior value on entry, restores on exit)
    // so nested validates / enriches from tests don't leak across calls.
    struct scoped_plan_resolve_index_t {
        explicit scoped_plan_resolve_index_t(const plan_resolve_index_t* idx) noexcept
            : prev_(active_plan_resolve_index()) {
            active_plan_resolve_index() = idx;
        }
        ~scoped_plan_resolve_index_t() noexcept {
            active_plan_resolve_index() = prev_;
        }
        scoped_plan_resolve_index_t(const scoped_plan_resolve_index_t&) = delete;
        scoped_plan_resolve_index_t& operator=(const scoped_plan_resolve_index_t&) = delete;
    private:
        const plan_resolve_index_t* prev_;
    };

    // Walk plan tree once; collect every node_catalog_resolve_*_t leaf
    // and populate the index. Leaves whose oid is still INVALID_OID
    // (Pass 1 did not stamp them — name did not resolve) are skipped.
    inline void gather_plan_resolve_index(components::logical_plan::node_t* root,
                                          plan_resolve_index_t& out) {
        using namespace components::logical_plan;
        if (!root) return;
        std::queue<node_t*> q;
        q.push(root);
        while (!q.empty()) {
            auto* n = q.front();
            q.pop();
            switch (n->type()) {
                case node_type::catalog_resolve_namespace_t: {
                    auto* rn = static_cast<node_catalog_resolve_namespace_t*>(n);
                    if (rn->namespace_oid() != components::catalog::INVALID_OID) {
                        out.ns_by_dbname[rn->dbname()] = rn->namespace_oid();
                    }
                    break;
                }
                case node_type::catalog_resolve_table_t: {
                    auto* rt = static_cast<node_catalog_resolve_table_t*>(n);
                    if (rt->namespace_oid() != components::catalog::INVALID_OID) {
                        out.ns_by_dbname[rt->dbname()] = rt->namespace_oid();
                        std::string key;
                        key.reserve(rt->dbname().size() + 1 + rt->relname().size());
                        key.append(rt->dbname()).push_back('|');
                        key.append(rt->relname());
                        out.tbl_ns_by_qname[std::move(key)] = rt->namespace_oid();
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
                        out.type_oid_by_qname[std::move(key)] = tr->type_oid();
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
                        out.fn_oid_by_qname[std::move(key)] = fr->function_oid();
                    }
                    break;
                }
                default:
                    break;
            }
            for (const auto& c : n->children()) {
                if (c) q.push(c.get());
            }
        }
    }

    // ns_oid_for resolves the namespace_oid for a table_id by probing
    // the plan-tree index first, then falling back to the catalog_view's
    // sync in-memory cache. Returns INVALID_OID if not found anywhere.
    inline components::catalog::oid_t ns_oid_for(const catalog_view_t& view,
                                                  const components::catalog::table_id& id) {
        auto& ns = id.get_namespace();
        if (ns.empty()) return components::catalog::INVALID_OID;
        if (auto* idx = active_plan_resolve_index(); idx) {
            std::string ns_key(ns.front().data(), ns.front().size());
            std::string qkey;
            qkey.reserve(ns_key.size() + 1 + id.table_name().size());
            qkey.append(ns_key).push_back('|');
            qkey.append(id.table_name().data(), id.table_name().size());
            if (auto it = idx->tbl_ns_by_qname.find(qkey); it != idx->tbl_ns_by_qname.end()) {
                plan_resolve_ns_hit_counter().fetch_add(1, std::memory_order_relaxed);
                return it->second;
            }
            if (auto it = idx->ns_by_dbname.find(ns_key); it != idx->ns_by_dbname.end()) {
                plan_resolve_ns_hit_counter().fetch_add(1, std::memory_order_relaxed);
                return it->second;
            }
            plan_resolve_ns_miss_counter().fetch_add(1, std::memory_order_relaxed);
        }
        auto* n = view.try_get_namespace(std::string_view(ns.front()));
        return n ? n->oid : components::catalog::INVALID_OID;
    }

    inline components::catalog::oid_t ns_oid_for_dbname(const catalog_view_t& view,
                                                         std::string_view dbname) {
        if (dbname.empty()) return components::catalog::INVALID_OID;
        if (auto* idx = active_plan_resolve_index(); idx) {
            if (auto it = idx->ns_by_dbname.find(std::string(dbname));
                it != idx->ns_by_dbname.end()) {
                plan_resolve_ns_hit_counter().fetch_add(1, std::memory_order_relaxed);
                return it->second;
            }
            plan_resolve_ns_miss_counter().fetch_add(1, std::memory_order_relaxed);
        }
        auto* n = view.try_get_namespace(dbname);
        return n ? n->oid : components::catalog::INVALID_OID;
    }

} // namespace services::dispatcher::impl