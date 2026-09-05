#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Catalog DDL routers. The crash-safe WAL write + catalog scan + storage mutation
    // now run on the owning agent (agent-0 / CATALOG) in append_pg_catalog_row_inner /
    // delete_pg_catalog_rows_inner / update_pg_attribute_commit_id_field_inner /
    // compact_relkind_g_storage_inner, so the manager no longer borrows the agent's
    // slice across the actor boundary. Every catalog OID routes to agents_[0] via
    // pool_idx_for_oid. Routers mirror storage_append: pool_idx_for_oid → otterbrix::send
    // → if needs_sched enqueue → co_return co_await.

    // Router to the agent twin. Both routing legs REFUSE rather than answer with an empty
    // range: a manager with no agents, or an agent slot holding nothing, is a catalog write
    // that did not happen, and reporting it as "appended 0 rows" is indistinguishable from a
    // healthy no-op — the reading that let CREATE TABLE report success over a pg_class row it
    // never wrote. Same rule and same shape as storage_delete_rows' routing legs.
    manager_disk_t::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                          components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t row) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"append_pg_catalog_row: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"append_pg_catalog_row: owning disk agent is null", resource()}};
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::append_pg_catalog_row_inner,
                                                              ctx,
                                                              table_oid,
                                                              std::move(row));
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<void> manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::int64_t oid_col_idx,
                                                                               components::catalog::oid_t target_oid) {
        // Single route to the agent inner body (the same body delete_pg_catalog_rows_many
        // loops, so both paths emit identical WAL records).
        //
        // BLOCKING DEBT — THIS ROUTE HAS NO ERROR CHANNEL, AND TWO CALLERS CAN NOW LEAVE A
        // DUPLICATE ROW BEHIND. Name them, because the shape of the damage changed:
        //
        //   * operator_alter_column_rename_t::await_async_and_resume
        //     (components/physical_plan/operators/operator_alter_column_rename.cpp, the send at
        //     ~:219) deletes the pg_attribute row of `attoid`, then appends a replacement
        //     carrying the new attname (~:247; that append's own refusal IS read). A refused
        //     delete leaves BOTH rows live for one attoid: the column answers to its old name
        //     and its new one at the same time.
        //   * operator_create_index_backfill_t::await_async_and_resume
        //     (components/physical_plan/operators/operator_create_index_backfill.cpp, ~:546)
        //     deletes the pg_index row of `index_oid_`, then appends the indisvalid=true
        //     replacement (~:563). A refused delete leaves indisvalid=false AND indisvalid=true
        //     for one indexrelid.
        //   * operator_delete::await_async_and_resume's catalog branch
        //     (components/physical_plan/operators/operator_delete.cpp, ~:390) appends nothing,
        //     so it keeps the older, milder failure: ONE row that should be gone, and a success
        //     reported over it.
        //
        // WHY IT IS NEW. agent_disk_t::delete_pg_catalog_rows_inner used to log a refused
        // PHYSICAL_DELETE and delete the rows anyway; storage then ran ahead of the journal but
        // held ONE row. It now refuses to delete what it could not journal — correct in itself —
        // and with no answer reaching the caller, the append that follows makes it two.
        //
        // WHAT IS LEFT OF IT. The two-row state had a SECOND, likelier cause that is now gone:
        // the delete's scan carried no transaction, so inside a BEGIN it silently missed the row
        // it was told to remove and the append landed on top. That happened on ordinary
        // statements, no fault injection needed, and both operators are gated on it now —
        // integration/cpp/test/test_catalog_delete_refusal.cpp,
        // an_in_transaction_rename_leaves_one_attribute_row (it observed live 'c' AND live 'd')
        // and an_in_transaction_create_index_leaves_one_pg_index_row (two pg_index rows for one
        // indexrelid). What remains reachable is the refusal path itself: a journal that would
        // not take the PHYSICAL_DELETE, or an owning agent holding no storage for the catalog
        // oid.
        //
        // WHY IT IS NOT FIXED HERE. The fix is the one delete_pg_catalog_rows_many just had:
        // widen the return to core::result_wrapper_t and make each caller read it. All three
        // callers are outside this change's scope, and there is no half-measure available —
        // widening the signature alone would compile, because a discarded co_await is legal, and
        // would leave exactly the silent ignore rule 6 forbids. Routing through the batched twin
        // with a one-element spec list (the move operator_alter_column_drop_t made for its live
        // pg_attribute row) is the other way, and it edits the same three files.
        //
        // Until one of those happens the refusal is REPORTED here rather than dropped in silence.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::delete_pg_catalog_rows_inner,
                                                                      ctx,
                                                                      table_oid,
                                                                      oid_col_idx,
                                                                      target_oid);
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                auto deleted = co_await std::move(fut);
                if (deleted.has_error()) {
                    error(log_,
                          "manager_disk::delete_pg_catalog_rows: the scrub of catalog oid={} for target oid={} "
                          "was refused and this route cannot report it: {}",
                          static_cast<unsigned>(table_oid),
                          static_cast<unsigned>(target_oid),
                          deleted.error().what);
                }
            }
        }
        co_return;
    }

    // Loop-route per spec; each spec emits the same WAL + storage records as one singular
    // delete_pg_catalog_rows call. Serialized (send, await, next send) so the WAL ordering
    // matches N successive singular calls — which also means there is never more than one
    // outstanding future here, so the first refusal can end the loop without abandoning a
    // future whose reply would land on a frame that has already finished.
    //
    // BOTH ROUTING LEGS REFUSE rather than answer with a short count vector, for the reason
    // append_pg_catalog_row's routing legs give: a manager with no agents, or an agent slot
    // holding nothing, is a catalog scrub that did not happen, and "deleted 0 rows" is exactly
    // what a healthy no-op looks like at every call site.
    //
    // AND IT STOPS AT THE FIRST REFUSAL. Continuing would be another mutation taken after the
    // answer was already known — the ordering rule the callers above follow, applied to the
    // loop itself.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<std::uint64_t>>>
    manager_disk_t::delete_pg_catalog_rows_many(execution_context_t ctx,
                                                std::pmr::vector<pg_catalog_delete_spec_t> specs) {
        std::pmr::vector<std::uint64_t> deleted_per_spec(resource());
        // The one legitimate no-op: nothing was asked to be deleted. Zero specs in, zero counts
        // out, no agent touched — and, unlike the legs below, no storage that should have been
        // there is missing.
        if (specs.empty()) {
            co_return std::move(deleted_per_spec);
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"delete_pg_catalog_rows_many: no disk agents", resource()}};
        }
        deleted_per_spec.reserve(specs.size());
        for (const auto& spec : specs) {
            const std::size_t idx = pool_idx_for_oid(spec.table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent == nullptr) {
                co_return core::error_t{
                    core::error_code_t::io_error,
                    std::pmr::string{"delete_pg_catalog_rows_many: owning disk agent is null", resource()}};
            }
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::delete_pg_catalog_rows_inner,
                                                                  ctx,
                                                                  spec.table_oid,
                                                                  spec.oid_col_idx,
                                                                  spec.target_oid);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto deleted = co_await std::move(fut);
            if (deleted.has_error()) {
                co_return deleted.convert_error<std::pmr::vector<std::uint64_t>>();
            }
            deleted_per_spec.push_back(deleted.value());
        }
        co_return std::move(deleted_per_spec);
    }

    manager_disk_t::unique_future<core::error_t> manager_disk_t::update_pg_attribute_commit_id_fields(
        execution_context_t ctx,
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills,
        std::uint64_t commit_id) {
        // Loop-route per backfill with the shared commit_id; each emits its own
        // physical_update WAL record. pg_attribute always routes to agents_[0].
        // Serialized (co_await per item) so the per-backfill WAL records are emitted
        // in order.
        //
        // THE TWO ROUTING LEGS ARE REFUSALS, NOT NO-OPS, and both used to be a bare co_return —
        // the same reading direct_delete_sync's router already rejects one file over. A
        // caller's markers do not evaporate because this manager has no agents.
        //
        // AND THE ONE LEGITIMATE EMPTY IS SPLIT OFF FIRST, the same split direct_append_sync and
        // storage_delete_rows_inner keep: a batch that names no marker asks for nothing, and
        // must not be answered with the refusal that belongs to a batch with nowhere to land.
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        if (backfills.empty()) {
            co_return core::error_t::no_error();
        }
        if (agents_.empty()) {
            co_return core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: no disk agents; no commit_id stamp "
                                 "was applied",
                                 resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(pg_attr_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{
                core::error_code_t::io_error,
                std::pmr::string{"update_pg_attribute_commit_id_fields: the agent owning pg_attribute is null; "
                                 "no commit_id stamp was applied",
                                 resource()}};
        }
        // EVERY MARKER IS ATTEMPTED, and the answer carries the COUNT of refusals with the
        // FIRST one's text. Stopping at the first would cost the remaining markers a stamp
        // they could have had, on a path that is already below the durable commit marker and
        // cannot be retried by aborting — and an answer that names only the first refusal
        // reads, one level up, as if the WHOLE batch went unstamped: on a mixed batch that is
        // the old "patched in-place" lie with the sign flipped. The counts make "1 of N" and
        // "N of N" different answers.
        core::error_t first_refusal = core::error_t::no_error();
        std::size_t refused_count = 0;
        for (const auto& b : backfills) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent->address(),
                                            &agent_disk_t::update_pg_attribute_commit_id_field_inner,
                                            ctx,
                                            b.attoid,
                                            b.kind,
                                            commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            auto stamped = co_await std::move(fut);
            if (stamped.contains_error()) {
                ++refused_count;
                if (!first_refusal.contains_error()) {
                    first_refusal = core::error_on(resource(), stamped);
                }
            }
        }

        // RN-oid — the added_at marker's SECOND half, and the only leg of "every storage column
        // carries its attoid" that has to travel forward in time.
        //
        // ALTER TABLE ADD COLUMN writes a pg_attribute row and stops; the STORAGE column is
        // materialised later, by the schema-growth stage of storage_append_inner, out of an
        // INSERT chunk that carries nothing but an alias-tagged type — and on an agent that
        // owns no pg_attribute and may not take a second cross-actor await inside an append.
        // So the identity is parked on the owning agent NOW, keyed by the attname the future
        // chunk will carry, and stamped onto the column at the moment it is created.
        //
        // FIRE-AND-FORGET, deliberately: this handler already co_awaits per backfill above, and
        // the ordering that matters is not "before this returns" but "before the client's next
        // statement". Both are satisfied by the mailbox — the note is ENQUEUED on the target
        // agent before this coroutine returns, the COMMIT's reply is what unblocks the client,
        // and the agent's mailbox is FIFO, so any later INSERT is processed after it.
        //
        // A note that finds no owner no-ops (note_column_identity_inner). That is not a silent
        // degradation: it means the storage this attoid describes is not loaded, in which case
        // there is no column to materialise, and bootstrap re-derives the same publication from
        // pg_attribute the next time the table IS loaded.
        for (const auto& b : backfills) {
            if (b.kind != components::pg_attribute_commit_id_backfill_t::kind_t::added_at ||
                b.release_attname.empty() || b.release_table_oid == components::catalog::INVALID_OID ||
                b.attoid == components::catalog::INVALID_OID) {
                continue;
            }
            const std::size_t owner_idx = pool_idx_for_oid(b.release_table_oid, agents_.size());
            if (owner_idx >= agents_.size() || agents_[owner_idx] == nullptr) {
                continue;
            }
            auto& owner = agents_[owner_idx];
            auto [owner_sched, note_fut] = actor_zeta::otterbrix::send(owner->address(),
                                                                       &agent_disk_t::note_column_identity_inner,
                                                                       b.release_table_oid,
                                                                       b.release_attname,
                                                                       static_cast<std::uint32_t>(b.attoid),
                                                                       b.added_column_type);
            [[maybe_unused]] auto dropped_note_future = std::move(note_fut);
            if (owner_sched) {
                scheduler_disk_->enqueue(owner.get());
            }
        }
        // The RN-oid notes above are deliberately fire-and-forget (see the block comment), so
        // the answer carries only what the STAMPS did: how many of the batch were refused,
        // how many landed, and the first refusal's own words.
        if (refused_count > 0) {
            std::pmr::string what{"update_pg_attribute_commit_id_fields: ", resource()};
            what.append(std::to_string(refused_count).c_str());
            what.append(" of ");
            what.append(std::to_string(backfills.size()).c_str());
            what.append(" commit_id stamp(s) were refused (");
            what.append(std::to_string(backfills.size() - refused_count).c_str());
            what.append(" applied); first refusal: ");
            what.append(first_refusal.what.c_str());
            co_return core::error_t{first_refusal.type, std::move(what)};
        }
        co_return core::error_t::no_error();
    }

    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t /*ctx*/,
                                              components::catalog::oid_t table_oid,
                                              std::set<std::string> live_attnames) {
        // Single route: the whole compaction (read mode + columns, compute to_drop,
        // drop each, count) runs intra-agent in compact_relkind_g_storage_inner —
        // no per-column manager↔agent round-trips.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::compact_relkind_g_storage_inner,
                                                                      table_oid,
                                                                      std::move(live_attnames));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        co_return 0;
    }

    // B3c1 — ALTER TABLE DROP COLUMN's physical half. Thin router, the same shape as every
    // other DDL leg: pool_idx_for_oid -> otterbrix::send -> enqueue if needed -> await. The
    // work itself runs intra-agent (drop_storage_column_inner) on that agent's own slice; the
    // manager never borrows a storage entry across the actor boundary.
    //
    // Not folded into compact_relkind_g_storage: that leg is SUBTRACTIVE (live set in, the
    // complement dropped), so any gap in the caller's derivation of the live set becomes a
    // physical drop of a SURVIVING column. This one is told WHICH column to drop — see the
    // contract note in disk_contract.hpp.
    manager_disk_t::unique_future<core::result_wrapper_t<bool>>
    manager_disk_t::drop_storage_column(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        std::string attname) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::drop_storage_column_inner,
                                                                      table_oid,
                                                                      std::move(attname));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        // No agent to route to at all. The caller is a COMMITTED ALTER whose tombstone is
        // already durable, so answering "done" here would be exactly the silent degradation
        // rule 6 forbids: the column would be hidden forever and its space never named again.
        std::pmr::string msg{"manager_disk::drop_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
    }

    // ALTER TABLE RENAME COLUMN's physical half. Same thin-router shape as the DROP leg above:
    // pool_idx_for_oid -> otterbrix::send -> enqueue if needed -> await, with the work running
    // intra-agent on that agent's own slice. The manager never borrows a storage entry across
    // the actor boundary.
    //
    // The point of the leg is in disk_contract.hpp: the storage's column name is a cache of
    // the catalog's, and the write path still addresses columns by it. Identity is the attoid
    // (RN-oid), so a rename the storage never saw is repaired at the next bootstrap rather than
    // read as a drop — but repairing it only at a restart would leave the live append path
    // expanding chunks against a stale name in the meantime.
    manager_disk_t::unique_future<core::result_wrapper_t<bool>>
    manager_disk_t::rename_storage_column(session_id_t /*session*/,
                                          components::catalog::oid_t table_oid,
                                          std::string old_attname,
                                          std::string new_attname) {
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[idx];
            if (agent != nullptr) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                      &agent_disk_t::rename_storage_column_inner,
                                                                      table_oid,
                                                                      std::move(old_attname),
                                                                      std::move(new_attname));
                if (needs_sched) {
                    scheduler_disk_->enqueue(agent.get());
                }
                co_return co_await std::move(fut);
            }
        }
        // No agent to route to at all. The caller is a COMMITTED ALTER whose new attname is
        // already durable, so answering "done" here would leave the storage carrying the OLD
        // name against a catalog carrying the new one — precisely the divergence the bootstrap
        // walk reads as a DROP. Rule 6: refuse loudly instead.
        std::pmr::string msg{"manager_disk::rename_storage_column: no disk agent owns table oid ", resource()};
        msg += std::pmr::string{std::to_string(static_cast<unsigned>(table_oid)), resource()};
        co_return core::result_wrapper_t<bool>(core::error_t{core::error_code_t::other_error, std::move(msg)});
    }

} // namespace services::disk
