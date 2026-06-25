#include "operator_resolve_constraint.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_resolve_constraint_t::operator_resolve_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        components::logical_plan::node_catalog_resolve_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_constraint)
        , target_node_(target_node)
        , output_schema_(resource) {
        // Single-column placeholder schema, built once (TASK C10). This
        // operator stamps data on the target logical node rather than emitting
        // rows, so the chunk it produces is always an empty placeholder.
        output_schema_.emplace_back(types::logical_type::UINTEGER);
    }

    void operator_resolve_constraint_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_resolve_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgConstraint = catalog::well_known_oid::pg_constraint_table;
        constexpr catalog::oid_t kPgAttribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Empty single-column placeholder chunk — this operator's purpose is to
        // stamp data on the target logical node, not to emit rows. Schema is
        // cached on the operator (output_schema_), built once in the ctor.
        output_ = make_operator_data(resource_, output_schema_, 0);
        output_->chunks().front().set_cardinality(0);

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }
        if (!target_node_ || !target_node_->target()) {
            mark_executed();
            co_return;
        }

        // table_oid comes from the sibling resolve_table node; the pipeline
        // guarantees resolve_table ran first and stamped resolved_metadata().
        const auto& md_opt = target_node_->target()->resolved_metadata();
        if (!md_opt.has_value() || md_opt->table_oid == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }
        const catalog::oid_t table_oid = md_opt->table_oid;
        const auto direction = target_node_->direction();
        using direction_t = components::logical_plan::resolve_direction;

        const std::string key_col =
            (direction == direction_t::outgoing) ? std::string{"conrelid"} : std::string{"confrelid"};

        std::vector<catalog::fk_info_t> fks;
        std::vector<std::pair<std::string, std::string>> check_exprs;

        // scan pg_constraint by (conrelid|confrelid).
        std::pmr::vector<std::string> con_keys(resource_);
        con_keys.emplace_back(key_col);
        auto [_c, fut_con] = actor_zeta::send(ctx->disk_address,
                                              &services::disk::manager_disk_t::read_chunks_by_key,
                                              exec_ctx,
                                              kPgConstraint,
                                              std::move(con_keys),
                                              components::operators::make_key_chunk(resource_, table_oid));
        auto con_batches = co_await std::move(fut_con);

        // PASS 1: decode every pg_constraint row. FK rows ('f') build a partially-filled
        // fk_info_t plus the child/parent attoid CSVs needed to resolve column names; CHECK
        // rows ('c', outgoing only) emit check_exprs directly. The per-FK child/parent
        // pg_attribute reads below are independent (each keys on a table oid known here from
        // the constraint row, their results feed disjoint fk fields, and nothing in this
        // operator WRITES the catalog), so they are deferred and issued as two batched
        // read_chunks_by_keys calls after this pass — one key per FK, in FK order. The order
        // of `fks` is preserved: every FK candidate keeps its slot in pending_fks and is
        // pushed (if valid) during pass 2 in the same order it was decoded here.
        struct pending_fk_t {
            catalog::fk_info_t fk;
            // parse_oid_csv returns std::vector (not pmr), so these mirror that type.
            std::vector<catalog::oid_t> child_attoids;
            std::vector<catalog::oid_t> parent_attoids;
        };
        std::pmr::vector<pending_fk_t> pending_fks(resource_);
        std::pmr::vector<catalog::oid_t> child_oids(resource_);
        std::pmr::vector<catalog::oid_t> parent_oids(resource_);

        for (auto& con_chunk : con_batches) {
            if (con_chunk.column_count() <= catalog::pg_constraint_col::confupdtype)
                continue;
            for (uint64_t ci = 0; ci < con_chunk.size(); ++ci) {
                const auto contype_cell =
                    con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::contype, ci);
                if (!contype_cell || contype_cell->empty())
                    continue;
                const char contype = (*contype_cell)[0];

                if (contype == 'f') {
                    pending_fk_t pending;
                    catalog::fk_info_t& fk = pending.fk;
                    fk.constraint_oid = static_cast<catalog::oid_t>(
                        con_chunk.get_value_unchecked<std::uint32_t>(catalog::pg_constraint_col::oid, ci));
                    if (direction == direction_t::outgoing) {
                        fk.child_table_oid = table_oid;
                        fk.parent_table_oid = static_cast<catalog::oid_t>(
                            con_chunk.get_value_unchecked<std::uint32_t>(catalog::pg_constraint_col::confrelid, ci));
                    } else {
                        fk.child_table_oid = static_cast<catalog::oid_t>(
                            con_chunk.get_value_unchecked<std::uint32_t>(catalog::pg_constraint_col::conrelid, ci));
                        fk.parent_table_oid = table_oid;
                    }
                    const auto confmatch =
                        con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::confmatch, ci);
                    fk.matchtype = confmatch ? (*confmatch)[0] : 's';
                    const auto confdeltype =
                        con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::confdeltype, ci);
                    fk.del_action = confdeltype ? (*confdeltype)[0] : 'a';
                    const auto confupdtype =
                        con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::confupdtype, ci);
                    fk.upd_action = confupdtype ? (*confupdtype)[0] : 'a';

                    pending.child_attoids = catalog::parse_oid_csv(
                        std::string(con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conkey, ci)
                                        .value_or(std::string_view{})));
                    pending.parent_attoids = catalog::parse_oid_csv(
                        std::string(con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::confkey, ci)
                                        .value_or(std::string_view{})));

                    // One key row per FK, positionally aligned to pending_fks — child by
                    // child_table_oid, parent by parent_table_oid (both keyed "attrelid").
                    child_oids.push_back(fk.child_table_oid);
                    parent_oids.push_back(fk.parent_table_oid);

                    pending_fks.push_back(std::move(pending));
                } else if (contype == 'c' && direction == direction_t::outgoing) {
                    const auto conexpr_cell =
                        con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conexpr, ci);
                    if (!conexpr_cell || conexpr_cell->empty())
                        continue;
                    const auto conexpr_sv = *conexpr_cell;
                    std::string name;
                    if (auto conname_cell =
                            con_chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, ci)) {
                        name = std::string(*conname_cell);
                    }
                    check_exprs.emplace_back(std::move(name), std::string(conexpr_sv));
                }
            }
        }

        if (!pending_fks.empty()) {
            // Batched child + parent pg_attribute reads, one key per FK in FK order. The two
            // batches are mutually independent (disjoint key columns / disjoint fk fields), so
            // issue both before awaiting either — same independence the prior per-FK code relied
            // on, now amortised across the whole constraint set in two mailbox hops total.
            // child_results[k] / parent_results[k] correspond to pending_fks[k].
            std::pmr::vector<std::string> attr_c_keys(resource_);
            attr_c_keys.emplace_back("attrelid");
            auto [_a, fut_attr_c] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::read_chunks_by_keys,
                                                     exec_ctx,
                                                     kPgAttribute,
                                                     std::move(attr_c_keys),
                                                     components::operators::make_keys_chunk(resource_, child_oids));

            std::pmr::vector<std::string> attr_p_keys(resource_);
            attr_p_keys.emplace_back("attrelid");
            auto [_b, fut_attr_p] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::read_chunks_by_keys,
                                                     exec_ctx,
                                                     kPgAttribute,
                                                     std::move(attr_p_keys),
                                                     components::operators::make_keys_chunk(resource_, parent_oids));

            std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> child_results =
                co_await std::move(fut_attr_c);
            std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> parent_results =
                co_await std::move(fut_attr_p);

            // PASS 2: per-FK column-name resolution + (for referencing) the chained
            // pg_class / pg_namespace reads. Identical extraction logic to the prior
            // per-FK code, just driven off the batched results indexed by FK slot k.
            for (std::size_t k = 0; k < pending_fks.size(); ++k) {
                catalog::fk_info_t fk = std::move(pending_fks[k].fk);
                const auto& child_attoids = pending_fks[k].child_attoids;
                const auto& parent_attoids = pending_fks[k].parent_attoids;
                std::pmr::vector<components::vector::data_chunk_t> empty_child(resource_);
                std::pmr::vector<components::vector::data_chunk_t> empty_parent(resource_);
                auto& child_attr = k < child_results.size() ? child_results[k] : empty_child;
                auto& parent_attr = k < parent_results.size() ? parent_results[k] : empty_parent;

                {
                    std::vector<std::string> names;
                    names.reserve(child_attoids.size());
                    for (const auto& wanted_oid : child_attoids) {
                        for (auto& attr_chunk : child_attr) {
                            if (attr_chunk.column_count() <= catalog::pg_attribute_col::attname)
                                continue;
                            bool found = false;
                            for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                auto row_attoid = static_cast<catalog::oid_t>(
                                    attr_chunk.get_value_unchecked<std::uint32_t>(catalog::pg_attribute_col::attoid,
                                                                                  ai));
                                if (row_attoid == wanted_oid) {
                                    names.emplace_back(std::string(attr_chunk.get_value_unchecked<std::string_view>(
                                        catalog::pg_attribute_col::attname,
                                        ai)));
                                    found = true;
                                    break;
                                }
                            }
                            if (found)
                                break;
                        }
                    }
                    fk.child_col_names = std::move(names);
                }

                // Also resolve child schema positions + defspec for each FK
                // column (used by operator_fk_cascade_t SET NULL / SET DEFAULT).
                if (direction == direction_t::referencing) {
                    // Build (attname → attnum-1, attdefspec) over child's
                    // pg_attribute rows sorted by attnum.
                    struct row_meta_t {
                        std::int32_t attnum{0};
                        std::string attname;
                        std::string attdefspec;
                    };
                    std::vector<row_meta_t> ordered;
                    constexpr std::uint64_t kAttnum = 4;
                    constexpr std::uint64_t kAttisdropped = 7;
                    constexpr std::uint64_t kAttdefspec = 9;
                    for (auto& attr_chunk : child_attr) {
                        if (attr_chunk.column_count() <= kAttisdropped)
                            continue;
                        for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                            if (attr_chunk.get_value<bool>(kAttisdropped, ai).value_or(false))
                                continue;
                            row_meta_t r;
                            if (auto attname =
                                    attr_chunk.get_value<std::string_view>(catalog::pg_attribute_col::attname, ai)) {
                                r.attname.assign(*attname);
                            }
                            r.attnum = attr_chunk.get_value<std::int32_t>(kAttnum, ai).value_or(0);
                            if (attr_chunk.column_count() > kAttdefspec) {
                                if (auto attdefspec = attr_chunk.get_value<std::string_view>(kAttdefspec, ai)) {
                                    r.attdefspec.assign(*attdefspec);
                                }
                            }
                            ordered.push_back(std::move(r));
                        }
                    }
                    std::sort(ordered.begin(), ordered.end(), [](const row_meta_t& a, const row_meta_t& b) {
                        return a.attnum < b.attnum;
                    });
                    for (const auto& col_name : fk.child_col_names) {
                        std::size_t pos = std::numeric_limits<std::size_t>::max();
                        std::string def_spec;
                        for (std::size_t i = 0; i < ordered.size(); ++i) {
                            if (ordered[i].attname == col_name) {
                                pos = i;
                                def_spec = ordered[i].attdefspec;
                                break;
                            }
                        }
                        fk.child_col_schema_indices.push_back(pos);
                        fk.child_col_default_specs.push_back(std::move(def_spec));
                    }
                }

                {
                    std::vector<std::string> names;
                    names.reserve(parent_attoids.size());
                    for (const auto& wanted_oid : parent_attoids) {
                        for (auto& attr_chunk : parent_attr) {
                            if (attr_chunk.column_count() <= catalog::pg_attribute_col::attname)
                                continue;
                            bool found = false;
                            for (uint64_t ai = 0; ai < attr_chunk.size(); ++ai) {
                                auto row_attoid = static_cast<catalog::oid_t>(
                                    attr_chunk.get_value_unchecked<std::uint32_t>(catalog::pg_attribute_col::attoid,
                                                                                  ai));
                                if (row_attoid == wanted_oid) {
                                    names.emplace_back(std::string(attr_chunk.get_value_unchecked<std::string_view>(
                                        catalog::pg_attribute_col::attname,
                                        ai)));
                                    found = true;
                                    break;
                                }
                            }
                            if (found)
                                break;
                        }
                    }
                    fk.parent_col_names = std::move(names);
                }

                if (direction == direction_t::referencing) {
                    // For DELETE FK cascade we also need the child's table name
                    // + schema (so operator_fk_cascade_t can locate the
                    // descendant collection without a back-resolve). The pg_namespace
                    // read keys on an oid DERIVED from the pg_class read result, so
                    // this remains a 2-hop chained read per FK (NOT batchable).
                    std::pmr::vector<std::string> cls_keys(resource_);
                    cls_keys.emplace_back("oid");
                    auto [_cls, fut_cls] =
                        actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::read_chunks_by_key,
                                         exec_ctx,
                                         kPgClass,
                                         std::move(cls_keys),
                                         components::operators::make_key_chunk(resource_, fk.child_table_oid));
                    auto cls_batches = co_await std::move(fut_cls);
                    if (!cls_batches.empty() && cls_batches[0].size() != 0 &&
                        cls_batches[0].column_count() > catalog::pg_class_col::relname) {
                        fk.child_collection_name = std::string(
                            cls_batches[0].get_value_unchecked<std::string_view>(catalog::pg_class_col::relname, 0));
                        fk.child_database = "";
                        const auto ns_oid = static_cast<catalog::oid_t>(
                            cls_batches[0].get_value_unchecked<std::uint32_t>(catalog::pg_class_col::relnamespace, 0));
                        std::pmr::vector<std::string> ns_keys(resource_);
                        ns_keys.emplace_back("oid");
                        auto [_ns, fut_ns] = actor_zeta::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::read_chunks_by_key,
                                                              exec_ctx,
                                                              kPgNamespace,
                                                              std::move(ns_keys),
                                                              components::operators::make_key_chunk(resource_, ns_oid));
                        auto ns_batches = co_await std::move(fut_ns);
                        if (!ns_batches.empty() && ns_batches[0].size() != 0 &&
                            ns_batches[0].column_count() > catalog::pg_namespace_col::nspname) {
                            fk.child_schema = std::string(
                                ns_batches[0].get_value_unchecked<std::string_view>(catalog::pg_namespace_col::nspname,
                                                                                    0));
                        }
                    }
                }

                if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
                    fks.push_back(std::move(fk));
                }
            }
        }

        target_node_->set_fks(std::move(fks));
        target_node_->set_check_exprs(std::move(check_exprs));
        mark_executed();
        co_return;
    }

} // namespace components::operators