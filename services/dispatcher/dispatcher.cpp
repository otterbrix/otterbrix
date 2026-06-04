#include "dispatcher.hpp"
#include "plan_resolve_index.hpp"
#include "validate_logical_plan.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>

#include <components/context/context.hpp>
#include <components/logical_plan/node_abort_transaction.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_alter_column_add.hpp>
#include <components/logical_plan/node_alter_column_drop.hpp>
#include <components/logical_plan/node_alter_column_rename.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_database.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_abort_transaction.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_udf.hpp>
#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>

#include <algorithm>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/planner.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include "enrich_logical_plan.hpp"

#include <services/collection/context_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

#include <set>
#include <span>
#include <string_view>
#include <vector>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {

    namespace catalog = components::catalog;
    // Resolve helpers shared with the executor live in services::catalog_resolve;
    // this alias keeps dispatcher.cpp call sites short.
    namespace catalog_resolve = services::catalog_resolve;

    // Back-compat using-decls so dispatcher call sites can keep spelling these
    // helpers unqualified (definitions in enrich_logical_plan.{hpp,cpp}).
    using catalog_resolve::build_type_search_path_str;
    using catalog_resolve::drop_target_names_from_resolves;
    using catalog_resolve::effective_root_node;
    using catalog_resolve::probe_type_in_path;

    namespace {

        components::logical_plan::node_type effective_root_type(const components::logical_plan::node_t* n) {
            auto* r = effective_root_node(n);
            return r ? r->type() : components::logical_plan::node_type::unused;
        }

        // subscriber-kind discriminator. Shared between
        // on_drop_resource_marked / on_subscriber_empty (dispatcher side) and the
        // subscriber's `on_horizon_advanced` ack send back to the dispatcher.
        // Two kinds today (disk + index); future subscribers (e.g. matview
        // refresh tracker) would extend this enum.
        constexpr uint8_t DISK_KIND = 1;
        constexpr uint8_t INDEX_KIND = 2;

        // Routes the unrewritten logical plan to executor_t::execute_plan_full
        // (vs. the dispatcher running resolve/validate/enrich/plan upstream and
        // calling executor_t::execute_plan). MUST stay paired with executor.cpp's
        // `enable_pass2_rewrites`: if this is true but the dispatcher also runs
        // its pre-execute rewrites, the plan is rewritten TWICE (broken double-wrap).
        constexpr bool use_executor_full_pipeline = true;
    } // namespace

    // 0xFFFFFFFF is unreachable: oid allocation aborts at OID_HARD_LIMIT=0xFFFF0000 (catalog_oids.hpp)
    static constexpr components::catalog::oid_t kSentinelOid = 0xFFFFFFFF;

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , collections_(resource_ptr)
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr)
        , txn_manager_(resource_ptr)
        , pending_void_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");

        // Event-loop-in-thread model. enqueue_impl (any sender thread) only
        // pushes the message into the lock-free inbox_ and notifies pump_cv_;
        // this thread owns ALL processing. The in-flight slot list is LOCAL to
        // the loop (allocated here; resource() is a synchronized_pool_resource,
        // thread-safe) so no mutex is needed across the phase logic.
        loop_thread_ = std::thread([this] {
            std::pmr::list<in_flight_entry_t> in_flight(resource());
            uint32_t loop_ticks = 0;
            while (loop_running_.load(std::memory_order_acquire)) {
                // Drain the lock-free inbox into local slots. release()/re-wrap
                // round-trips ownership of the message back into a message_ptr;
                // the coroutine created below holds a raw pointer into it, so
                // pending_msg must outlive the behavior.
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                }

                bool progress = true;
                while (progress) {
                    progress = false;

                    // (a) Create behavior for the first slot that still needs
                    //     one. pending_msg STAYS in the slot — the coroutine
                    //     holds a raw pointer to the message across suspension
                    //     points. The behavior coroutine runs HERE, on the loop
                    //     thread, until its first co_await. Marker = behavior
                    //     handle null (not yet created).
                    {
                        in_flight_entry_t* slot = nullptr;
                        for (auto& e : in_flight) {
                            if (e.pending_msg && !e.behavior) {
                                slot = &e;
                                break;
                            }
                        }
                        if (slot) {
                            slot->behavior = behavior(slot->pending_msg.get());
                            progress = true;
                            continue;
                        }
                    }

                    // (b) Resume the first ready behavior; reset its staleness.
                    {
                        in_flight_entry_t* ready_slot = nullptr;
                        actor_zeta::detail::coroutine_handle<> cont{};
                        for (auto& e : in_flight) {
                            if (e.behavior.is_awaited_ready()) {
                                cont = e.behavior.take_awaited_continuation();
                                if (cont) {
                                    ready_slot = &e;
                                    break;
                                }
                            } else if (e.behavior && !e.behavior.done() && e.behavior.is_busy()) {
                                ++e.stale_ticks;
                            }
                        }
                        if (cont) {
                            ready_slot->stale_ticks = 0;
                            cont.resume();
                            poll_pending();
                            progress = true;
                            continue;
                        }
                    }

                    // (c) Erase ONE done slot per pass. "Done" = behavior
                    //     created (handle non-null) AND completed. behavior +
                    //     pending_msg destruct naturally on this thread.
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }

                    poll_pending();
                }

                // WATCHDOG for the actor-zeta parking race on an executor
                // (docs/actor-zeta-lost-wakeup.md): the executor's mailbox can be
                // reader_blocked while its awaited future is READY, and
                // resume_impl's blocked-check precedes the busy/ready check, so it
                // never wakes. A mailbox PUSH unblocks it. A slot stuck busy &&
                // !ready past the staleness threshold signals this; we poke with a
                // no-op message (unregister_collection_local with an absent key).
                // Firing early on a legitimately long executor operation is
                // harmless (one warn line + erase of an absent key).
                bool any_stale = false;
                for (auto& e : in_flight)
                    if (e.behavior && !e.behavior.done() && e.behavior.is_busy()
                        && !e.behavior.is_awaited_ready() && e.stale_ticks > 20) {
                        any_stale = true;
                        break;
                    }
                if (any_stale) {
                    warn(log_,
                         "dispatcher loop: stale await detected — poking executors (see docs/actor-zeta-lost-wakeup.md)");
                    for (auto& ex : executors_) {
                        if (ex) {
                            auto [ns, f] = actor_zeta::send(ex.get(),
                                                            &collection::executor::executor_t::unregister_collection_local,
                                                            components::session::session_id_t{},
                                                            kSentinelOid);
                            if (ns)
                                scheduler_->enqueue(ex.get());
                            (void) f; // dropped future is memory-safe (Last-One-Out)
                        }
                    }
                    for (auto& e : in_flight) e.stale_ticks = 0; // backoff: re-arm threshold
                }

                ++loop_ticks;
                (void) loop_ticks;
                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk, std::chrono::microseconds(100));
                }
                // NOTE: lock-free inbox trade — a push+notify may slip between
                // empty() and wait_for; bounded by the 100µs timeout
                // (staleness, not loss).
            }
            // Local in_flight destructs HERE on the loop thread: any still-
            // suspended behaviors are destroyed safely (~behavior_t destroys
            // suspended frames; Last-One-Out frees state).
        });
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any leftover inbox_ raw pointers: re-wrap each into a
        // message_ptr temporary so its PMR memory is freed (the loop is gone).
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drop{raw};
        }
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Delivery only — ALL processing happens on loop_thread_. The lock-free
        // inbox takes ownership of the raw message* (release()); the loop
        // re-wraps it into a message_ptr. notify without holding mutex_ is fine
        // (the loop re-checks inbox_.empty() under the lock before sleeping).
        inbox_.push(msg.release());
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    void manager_dispatcher_t::poll_pending() {
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(),
                           [](auto& f) { return f.is_ready(); }),
            pending_void_.end());
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::unregister_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::begin_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::begin_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::commit_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::commit_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::abort_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::abort_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_begin_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_begin_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_commit_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_commit_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_abort_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_abort_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_publish_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_publish_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::txn_lowest_active_msg>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::txn_lowest_active_msg, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_drop_resource_marked>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::on_drop_resource_marked, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::on_subscriber_empty>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::on_subscriber_empty, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_dispatcher_t::sync(sync_pack pack) {
        constexpr static int wal_idx = 0;
        constexpr static int disk_idx = 1;
        constexpr static int index_idx = 2;
        wal_address_ = std::get<wal_idx>(pack);
        disk_address_ = std::get<disk_idx>(pack);
        index_address_ = std::get<index_idx>(pack);

        executors_.reserve(executor_pool_size_);
        executor_addresses_.reserve(executor_pool_size_);
        for (std::size_t i = 0; i < executor_pool_size_; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            &txn_manager_,
                                                                            log_.clone());
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_, "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses", executor_pool_size_);
    }

    // selective-broadcast helper. Called inline from commit_txn /
    // abort_txn handler bodies (wiring deferred to follow-up). When
    // the lowest_active_start_time advances past the cached
    // `last_broadcast_horizon_`, fan out `on_horizon_advanced(new_lowest)` to
    // every subscriber whose dropped-resource flag is set. Subscribers without
    // outstanding drops are skipped entirely (no message bursts on commits
    // when nothing is queued for GC).
    void manager_dispatcher_t::try_trigger_cleanup_if_horizon_advanced() noexcept {
        auto new_lowest = txn_manager_.lowest_active_start_time();
        if (new_lowest > last_broadcast_horizon_) {
            last_broadcast_horizon_ = new_lowest;
            if (disk_has_dropped_ && disk_address_ != actor_zeta::address_t::empty_address()) {
                // Fire-and-forget: subscriber acks asynchronously via
                // on_subscriber_empty mailbox message. pending_void_ is pure GC
                // bookkeeping for these broadcast futures — poll_pending() drains
                // them non-blockingly via is_ready(). Dropping the future
                // immediately would also be memory-safe (the library's
                // Last-One-Out protocol — future.hpp release_future only flags,
                // the dealloc happens on the later of future/promise release), so
                // this is bookkeeping, not a lifetime requirement.
                auto disk_send_result =
                    actor_zeta::send(disk_address_,
                                     &services::disk::manager_disk_t::on_horizon_advanced,
                                     new_lowest);
                pending_void_.emplace_back(std::move(disk_send_result.second));
            }
            if (index_has_dropped_ && index_address_ != actor_zeta::address_t::empty_address()) {
                auto index_send_result =
                    actor_zeta::send(index_address_,
                                     &services::index::manager_index_t::on_horizon_advanced,
                                     new_lowest);
                pending_void_.emplace_back(std::move(index_send_result.second));
            }
        }
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::on_drop_resource_marked(uint8_t subscriber_kind) {
        // DROP TABLE / DROP INDEX path emits this on the dispatcher's mailbox
        // so the broadcast helper above starts sending horizon-advance
        // notifications to the owning subscriber.
        if (subscriber_kind == DISK_KIND) {
            disk_has_dropped_ = true;
        } else if (subscriber_kind == INDEX_KIND) {
            index_has_dropped_ = true;
        }
        co_return;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::on_subscriber_empty(uint8_t subscriber_kind) {
        // Subscriber's dropped_storages_ queue drained — stop broadcasting
        // horizon updates to it until the next DROP marks something pending
        // again.
        if (subscriber_kind == DISK_KIND) {
            disk_has_dropped_ = false;
        } else if (subscriber_kind == INDEX_KIND) {
            index_has_dropped_ = false;
        }
        co_return;
    }

    void manager_dispatcher_t::set_replay_horizon_sync(uint64_t commit_id) {
        // publish() takes the manager's own lock_ — safe even outside a started
        // scheduler. publish() is monotonic (CAS keeps the max), so passing a
        // stale id is a no-op.
        if (commit_id > 0) {
            txn_manager_.publish(commit_id);
            trace(log_, "manager_dispatcher_t::set_replay_horizon_sync , advanced published_horizon to {}", commit_id);
        }
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       node_ptr plan,
                                       parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());

        auto params_for_wal = make_parameter_node(resource());
        params_for_wal->set_parameters(params->parameters());

        // All catalog reads go through the plan-tree resolve idx (validate /
        // enrich / DDL paths). The collections_ map is maintained incrementally by
        // init_from_state, post-create (line ~1251), and post-drop (line
        // ~1259-1269), so re-fetching pg_namespace + pg_class on every plan
        // is unnecessary.
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};

        // Save original node type — used after planner rewrite to dispatch DDL/DML paths.
        // When transformer wraps DML/DDL in
        //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <consumer>)
        // we route on <consumer>'s type, not the wrapping sequence_t. For non-wrapped
        // plans effective_root_type() is identity (n->type()), so this is a no-op for
        // existing DDL flows (planner wraps are applied AFTER this line — see notes
        // inside the helper).
        const auto original_type = effective_root_type(plan.get());
        // Capture the drop target before the planner rewrites it into a
        // node_dynamic_cascade_delete_t (which carries only OIDs, not names).
        // Used after successful execution to clean up the in-memory collections_
        // routing map so a subsequent execute_plan does not see a stale entry.
        // Descend through transformer's sequence_t(catalog_resolve_*,
        // <drop_node>) wrapper to reach the real drop node before casting.
        std::string drop_target_database;
        qualified_name_t drop_target_collection;
        if (original_type == node_type::drop_database_t) {
            auto names = drop_target_names_from_resolves(plan.get());
            drop_target_database = std::move(names.first);
        } else if (original_type == node_type::drop_collection_t) {
            auto names = drop_target_names_from_resolves(plan.get());
            drop_target_collection = qualified_name_t{names.first, names.second};
        }
        // SET TIMEZONE: capture name before the planner consumes the node so
        // the dispatcher can update default_tz_cat_ (single-owner per rule 10)
        // after the operator pipeline confirms pg_settings was persisted.
        std::pmr::string pending_set_tz_name{resource()};
        if (original_type == node_type::set_timezone_t) {
            auto* tz_node = static_cast<components::logical_plan::node_set_timezone_t*>(
                effective_root_node(plan.get()));
            pending_set_tz_name.assign(tz_node->timezone_name().c_str(),
                                       tz_node->timezone_name().size());
        }
        auto logic_plan = std::move(plan);
        context_storage_t collections_context_storage(resource(), log_.clone(), session_tz(session));
        // Optimizer: constant folding, etc.
        logic_plan = components::planner::optimize(resource(), logic_plan, params.get());

        // Wrap the plan with catalog_resolve_namespace + catalog_resolve_table
        // for every (db, rel) pair found in the tree. Validate/enrich consume
        // OIDs through the plan-tree idx; the SQL transformer only emits
        // resolves for the outermost target (e.g. INSERT FROM SELECT wraps
        // CopyTestCollection but not the SELECT source TestCollection), so we
        // need to top-up missing tables here. For direct-API callers
        // (wrapper_dispatcher::execute_plan, find, etc.) this builds the full
        // wrap from scratch. Existing resolves in sequence_t already cover
        // their (db, rel) tuples — set-based dedup avoids re-emitting them.
        {
            // Collect resolves that already exist in the plan tree so we don't
            // re-emit them. Operates on the immediate front children of
            // sequence_t (where the transformer puts its resolves); a deeper
            // walk is unnecessary because resolve only consumes front-children.
            std::set<std::string> existing_dbs;
            std::set<std::pair<std::string, std::string>> existing_tbls;
            if (logic_plan->type() == node_type::sequence_t) {
                for (const auto& c : logic_plan->children()) {
                    if (!c)
                        continue;
                    if (c->type() == node_type::catalog_resolve_namespace_t) {
                        auto* r = static_cast<const node_catalog_resolve_namespace_t*>(c.get());
                        existing_dbs.insert(r->dbname());
                    } else if (c->type() == node_type::catalog_resolve_table_t) {
                        auto* r = static_cast<const node_catalog_resolve_table_t*>(c.get());
                        existing_tbls.insert({r->dbname(), r->relname()});
                        existing_dbs.insert(r->dbname());
                    }
                }
            }
            std::set<std::string> wrap_dbs;
            std::set<std::pair<std::string, std::string>> wrap_tbls;
            auto add_dbrel = [&](std::string db, std::string rel) {
                if (db.empty())
                    return;
                wrap_dbs.insert(db);
                if (!rel.empty()) {
                    wrap_tbls.insert({std::move(db), std::move(rel)});
                }
            };
            // Iterative pre-order walk (no recursion → no std::function).
            std::vector<const node_t*> stack;
            stack.push_back(logic_plan.get());
            while (!stack.empty()) {
                const node_t* n = stack.back();
                stack.pop_back();
                if (!n)
                    continue;
                switch (n->type()) {
                    // DML consumers no longer carry (db, rel) — names
                    // for collection-set tracking come from the sibling
                    // resolve_table inside the wrapping sequence_t (the
                    // catalog_resolve_table_t branch below picks them up).
                    case node_type::insert_t:
                    case node_type::update_t:
                    case node_type::delete_t:
                        break;
                    case node_type::aggregate_t: {
                        auto* d = static_cast<const node_aggregate_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::match_t: {
                        auto* d = static_cast<const node_match_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::join_t: {
                        auto* d = static_cast<const node_join_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::create_database_t: {
                        auto* d = static_cast<const node_create_database_t*>(n);
                        if (!d->dbname().empty())
                            wrap_dbs.insert(d->dbname());
                        break;
                    }
                    // create_collection_t / create_index_t no longer
                    // carry parent dbname/relname; the transformer always wraps
                    // them with sibling catalog_resolve_namespace / resolve_table
                    // so wrap_dbs/wrap_tbls is already populated from
                    // existing_dbs/existing_tbls above.
                    // drop_database_t / drop_collection_t / drop_index_t
                    // no longer carry names; the transformer always wraps them
                    // with sibling catalog_resolve_* nodes so wrap_dbs/wrap_tbls
                    // already has the (db, rel) covered.
                    default:
                        break;
                }
                for (const auto& c : n->children()) stack.push_back(c.get());
            }
            // Drop resolves already present so we don't duplicate them.
            for (const auto& db : existing_dbs) wrap_dbs.erase(db);
            for (const auto& t : existing_tbls) wrap_tbls.erase(t);
            if (!wrap_dbs.empty() || !wrap_tbls.empty()) {
                // Collect new resolves to prepend.
                std::vector<components::logical_plan::node_ptr> new_resolves;
                std::set<std::string> resolved_dbs = existing_dbs;
                for (const auto& db : wrap_dbs) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                }
                for (const auto& [db, rel] : wrap_tbls) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                    new_resolves.push_back(
                        components::logical_plan::make_node_catalog_resolve_table(resource(),
                                                                                  core::dbname_t{db},
                                                                                  core::relname_t{rel}));
                }
                if (logic_plan->type() == node_type::sequence_t) {
                    // Splice new resolves AFTER existing leading resolve_*
                    // siblings but BEFORE the consumer node. Order matters:
                    // stamp_oids_from_resolves picks the FIRST resolve_table
                    // as the DML target — preserving original-target priority
                    // means walker-added scan resolves don't shadow it.
                    auto is_resolve_local = [](node_type t) {
                        return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                               t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                               t == node_type::catalog_resolve_constraint_t;
                    };
                    auto& kids = logic_plan->children();
                    std::vector<components::logical_plan::node_ptr> merged;
                    merged.reserve(kids.size() + new_resolves.size());
                    std::size_t split = 0;
                    while (split < kids.size() && kids[split] && is_resolve_local(kids[split]->type())) {
                        merged.push_back(std::move(kids[split]));
                        ++split;
                    }
                    for (auto& r : new_resolves) merged.push_back(std::move(r));
                    for (; split < kids.size(); ++split) {
                        merged.push_back(std::move(kids[split]));
                    }
                    kids.clear();
                    for (auto& m : merged) kids.push_back(std::move(m));
                } else {
                    auto seq = boost::intrusive_ptr<components::logical_plan::node_t>(
                        new components::logical_plan::node_sequence_t(resource()));
                    for (auto& r : new_resolves) seq->append_child(std::move(r));
                    seq->append_child(std::move(logic_plan));
                    logic_plan = seq;
                }
            }
        }

        // DDL needs a real (non-zero) txn so that mid-DDL crash → WAL replay rolls back
        // partially-written pg_catalog.* records.
        components::table::transaction_data txn_data{0, 0};
        // create_collection_t/create_constraint_t are checked via original_type:
        // after the DDL planner rewrite they become sequence_t, but still need a
        // DDL txn so that append_pg_catalog_row records ranges on
        // txn_t->pg_catalog_appends and storage_publish_commits rebuilds
        // table_to_oid_ on success.
        const bool needs_ddl_txn =
            original_type == node_type::create_collection_t || original_type == node_type::create_constraint_t ||
            original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
            original_type == node_type::create_macro_t || original_type == node_type::create_type_t ||
            original_type == node_type::create_index_t || original_type == node_type::drop_index_t ||
            original_type == node_type::drop_database_t || original_type == node_type::drop_collection_t ||
            original_type == node_type::drop_type_t || original_type == node_type::drop_sequence_t ||
            original_type == node_type::drop_view_t || original_type == node_type::drop_macro_t ||
            original_type == node_type::create_database_t || original_type == node_type::alter_table_t ||
            original_type == node_type::create_matview_t;
        // DML txn begins here (not in executor::execute_plan) to avoid the
        // cross-actor await + cooperative_actor.hpp framework bug. The executor
        // receives txn_data via the execute_plan(_full) parameter and uses it for
        // its DML path, skipping its own redundant txn_begin_msg send. Hoisted out
        // of an inner scope so the DML auto-commit block below can read it.
        const bool needs_dml_txn =
            original_type == node_type::insert_t || original_type == node_type::update_t ||
            original_type == node_type::delete_t;
        if (needs_ddl_txn || needs_dml_txn) {
            txn_data = txn_manager_.begin_transaction(session).data();
            trace(log_,
                  "manager_dispatcher_t::execute_plan: {} began txn {}",
                  needs_ddl_txn ? "DDL" : "DML",
                  txn_data.transaction_id);
        }

        collection::executor::execute_result_t exec_result;
        // Route execution by the effective consumer type;
        // see comments above the validate switch.
        switch (original_type) {
            case node_type::alter_table_t: {
                // ALTER TABLE is normally rewritten by the planner into
                // sequence_t(alter_column_{add,rename,drop}_t × N). Reaching this
                // case means rewrite_alter_table bailed out because table_oid was
                // not resolved by enrich (table not found); return no-op success
                // and let the validate/enrich layer surface a hard error.
                //
                // This switch routes by `original_type`, so we still see
                // alter_table_t here even AFTER the planner has rewritten the
                // plan into sequence_t. Distinguish the genuine bailout
                // (logic_plan->type() still alter_table_t) from the
                // already-rewritten case (logic_plan is sequence_t) — the
                // latter must run through the executor like every other DDL.
                if (logic_plan->type() == node_type::alter_table_t) {
                    exec_result = {make_cursor(resource()), {}, {}, {}, {}};
                } else {
                    exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data, &collections_context_storage);
                }
                break;
            }
            default:
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data, &collections_context_storage);
                break;
        }

        // ===== DML auto-commit / abort phase =====
        // Runs here, not in services/collection/executor.cpp, to escape the
        // cooperative_actor.hpp framework bug that hangs the executor on
        // multi-await sequences. actor_mixin pump (this actor) drives the
        // chain — txn_manager_.commit() → storage_publish_* fanout →
        // index commit_insert/commit_delete fanout → WAL commit_txn (real
        // commit_id) → txn_manager_.publish() — and mirrors the abort cascade
        // on failure. txn_manager_ is dispatcher's own member (intra-actor
        // sync access is OK, no shared state across actors). All cross-actor
        // calls go through actor_zeta::send + unique_future<value-type>.
        // No exceptions; index commit errors flip cursor via make_cursor
        // and run the abort cascade by disjunction (no goto).
        if (needs_dml_txn && !exec_result.explicit_txn_no_commit) {
            bool need_abort = false;

            if (exec_result.cursor->is_success()) {
                // Allocate commit_id (sync intra-actor member access).
                uint64_t commit_id = txn_manager_.commit(session);
                core::error_t commit_err = core::error_t::no_error();

                // Per-range storage publish + index commit_insert.
                for (auto& app : exec_result.dml_appends) {
                    components::execution_context_t ctx{session,
                                                        txn_data,
                                                        exec_result.session_tz,
                                                        app.table_oid};
                    auto [_p, pf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::storage_publish_commit,
                                                    ctx,
                                                    app.table_oid,
                                                    commit_id,
                                                    app.row_start,
                                                    app.row_count);
                    co_await std::move(pf);

                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_ci, cif] = actor_zeta::send(index_address_,
                                                          &index::manager_index_t::commit_insert,
                                                          ctx,
                                                          app.table_oid,
                                                          commit_id);
                        auto ci_result = co_await std::move(cif);
                        if (ci_result.contains_error()) {
                            commit_err = std::move(ci_result);
                            need_abort = true;
                            break;
                        }
                    }
                }

                // Per-range delete publish + index commit_delete.
                if (!need_abort) {
                    for (auto& del : exec_result.dml_deletes) {
                        components::execution_context_t del_ctx{session,
                                                                txn_data,
                                                                exec_result.session_tz,
                                                                del.table_oid};
                        auto [_pd, cdf] = actor_zeta::send(disk_address_,
                                                          &disk::manager_disk_t::storage_publish_delete,
                                                          del_ctx,
                                                          del.table_oid,
                                                          commit_id);
                        co_await std::move(cdf);

                        if (index_address_ != actor_zeta::address_t::empty_address()) {
                            auto [_cd, cdif] = actor_zeta::send(index_address_,
                                                               &index::manager_index_t::commit_delete,
                                                               del_ctx,
                                                               del.table_oid,
                                                               commit_id);
                            auto cd_result = co_await std::move(cdif);
                            if (cd_result.contains_error()) {
                                commit_err = std::move(cd_result);
                                need_abort = true;
                                break;
                            }
                        }
                    }
                }

                // Batched pg_catalog publish (DML typically empty; kept for
                // symmetry with explicit-txn commit operator path). Moves the
                // vectors out of exec_result so the downstream merge block
                // below is a no-op (no double-push onto transaction_t).
                if (!need_abort && !exec_result.pg_catalog_appends.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pc, pcf] = actor_zeta::send(disk_address_,
                                                      &disk::manager_disk_t::storage_publish_commits,
                                                      pgc_ctx,
                                                      commit_id,
                                                      std::move(exec_result.pg_catalog_appends));
                    co_await std::move(pcf);
                }
                if (!need_abort && !exec_result.pg_catalog_delete_tables.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pdl, pdf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_publish_deletes,
                                                       pgc_ctx,
                                                       commit_id,
                                                       std::move(exec_result.pg_catalog_delete_tables));
                    co_await std::move(pdf);
                }

                if (need_abort) {
                    // Flip cursor; the abort phase below runs by disjunction
                    // (need_abort || cursor->is_error()) — no goto.
                    exec_result.cursor = make_cursor(resource(), std::move(commit_err));
                } else {
                    // WAL commit_txn carries the real commit_id (mirrors the
                    // ordering the old executor's commit phase used:
                    // storage_publish_* → WAL commit_txn → txn_manager publish).
                    // Snapshot-aware replay restores published_horizon_ from
                    // this record's commit_id.
                    if (wal_address_ != actor_zeta::address_t::empty_address()) {
                        constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                        auto [_wc, wcf] = actor_zeta::send(wal_address_,
                                                          &wal::manager_wal_replicate_t::commit_txn,
                                                          session,
                                                          txn_data.transaction_id,
                                                          wal::wal_sync_mode::FULL,
                                                          db_oid,
                                                          commit_id);
                        co_await std::move(wcf);
                    }
                    // ProcArray publish barrier (sync intra-actor) — after
                    // this call concurrent readers see commit_id as visible.
                    txn_manager_.publish(commit_id);
                }
            }

            // Abort cascade (disjunction: executor-side error OR commit-phase
            // index error). Reverts every accumulated range, then aborts the
            // txn in txn_manager_.
            if (need_abort || exec_result.cursor->is_error()) {
                for (auto& app : exec_result.dml_appends) {
                    components::execution_context_t abort_ctx{session,
                                                              txn_data,
                                                              exec_result.session_tz,
                                                              app.table_oid};
                    auto [_ra, raf] = actor_zeta::send(disk_address_,
                                                      &disk::manager_disk_t::storage_revert_append,
                                                      abort_ctx,
                                                      app.table_oid,
                                                      app.row_start,
                                                      app.row_count);
                    co_await std::move(raf);

                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_ri, rif] = actor_zeta::send(index_address_,
                                                          &index::manager_index_t::revert_insert,
                                                          abort_ctx,
                                                          app.table_oid);
                        co_await std::move(rif);
                    }
                }

                if (!exec_result.pg_catalog_appends.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                      &disk::manager_disk_t::storage_revert_appends,
                                                      pgc_ctx,
                                                      std::move(exec_result.pg_catalog_appends));
                    co_await std::move(paf);
                }

                txn_manager_.abort(session);
            }
        }

        // Hand pg_catalog swap-info up to the transaction so commit/abort
        // operators (or the inline DDL commit blocks below) can apply
        // storage_publish_commits / storage_revert_appends after txn_manager_.commit()/abort().
        // Skip txn=0 (auto-commit / bootstrap path).
        // Note: for the DML auto-commit path above, exec_result.pg_catalog_*
        // were moved out into storage_publish_commits/_deletes (or
        // storage_revert_appends), so the loops below iterate empty ranges —
        // no double-merge onto txn_t.
        // THREADING: transaction_t body is single-owner-thread per session
        // (see transaction.hpp) — this merge runs on the dispatcher loop thread
        // strictly AFTER the executor's accumulate_* (we co_awaited its result
        // future above; release/acquire edge). Never mutate the body concurrently.
        if (txn_data.transaction_id != 0) {
            if (auto* txn_t = txn_manager_.find_transaction(session)) {
                for (auto& a : exec_result.pg_catalog_appends) {
                    txn_t->pg_catalog_appends.push_back(std::move(a));
                }
                for (auto& d : exec_result.pg_catalog_delete_tables) {
                    txn_t->pg_catalog_delete_tables.insert(std::move(d));
                }
                // backfill markers onto the txn so operator_commit_transaction
                // can drain them after commit_id allocation.
                txn_t->accumulate_pg_attribute_commit_id_backfills(
                    std::move(exec_result.pg_attribute_commit_id_backfills));
            }
        }

        auto& result = exec_result.cursor;
        trace(log_, "manager_dispatcher_t::execute_plan: result received, success: {}", result->is_success());

        if (result->is_success()) {
            // Use original_type for dispatch: planner may have wrapped DML nodes,
            // changing logic_plan->type() to a constraint wrapper type.
            const auto t = original_type;
            // SET TIMEZONE — operator pipeline persisted the ('TimeZone', <name>)
            // row to pg_settings. Refresh the dispatcher's session_catalog_t
            // cache so subsequent session_tz(session) reads see the new value.
            // Only this dispatcher coroutine mutates default_tz_cat_, so no
            // synchronization is needed.
            if (t == node_type::set_timezone_t && !pending_set_tz_name.empty()) {
                (void) default_tz_cat_.set_timezone(
                    resource(),
                    std::string_view{pending_set_tz_name.data(), pending_set_tz_name.size()});
                co_return result;
            }
            // ALTER TABLE flows through the executor pipeline as
            // sequence_t(alter_column_{add,rename,drop}_t × N).
            if (t == node_type::insert_t) {
                trace(log_, "manager_dispatcher_t::execute_plan: DML {} completed by executor", to_string(t));
                co_return result;
            }
            if (t == node_type::create_collection_t || t == node_type::create_database_t ||
                t == node_type::create_constraint_t || t == node_type::create_sequence_t ||
                t == node_type::create_view_t || t == node_type::create_macro_t || t == node_type::create_type_t ||
                t == node_type::create_index_t || t == node_type::drop_index_t || t == node_type::drop_database_t ||
                t == node_type::drop_collection_t || t == node_type::drop_type_t || t == node_type::drop_sequence_t ||
                t == node_type::drop_view_t || t == node_type::drop_macro_t || t == node_type::alter_table_t ||
                t == node_type::create_matview_t) {
                // DDL commit goes through operator_commit_transaction_t in
                // ddl-commit mode. The operator performs flush (durability
                // barrier) + wal::commit_txn + txn_manager.commit +
                // storage_publish_commits/deletes (steps 1-6). Step 7
                // (CREATE INDEX backfill commit_insert) stays inline below
                // because it depends on plan structure.
                std::uint64_t commit_id = 0;
                {
                    constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                    auto ddl_commit_node =
                        boost::intrusive_ptr(new components::logical_plan::node_commit_transaction_t(resource()));
                    ddl_commit_node->set_is_ddl_commit(true);
                    ddl_commit_node->set_txn_id(txn_data.transaction_id);
                    ddl_commit_node->set_database_oid(db_oid);

                    services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
                    components::compute::function_registry_t fn_registry{resource()};
                    auto commit_op = services::planner::create_plan(cstor,
                                                                    fn_registry,
                                                                    ddl_commit_node,
                                                                    components::logical_plan::limit_t::unlimit(),
                                                                    /*params=*/nullptr);
                    if (commit_op) {
                        commit_op->set_as_root();
                        components::logical_plan::storage_parameters cparams(resource());
                        components::pipeline::context_t pctx{session,
                                                             actor_zeta::address_t::empty_address(),
                                                             actor_zeta::address_t::empty_address(),
                                                             &fn_registry,
                                                             cparams};
                        pctx.disk_address = disk_address_;
                        pctx.wal_address = wal_address_;
                        pctx.txn_manager = &txn_manager_;
                        pctx.txn = txn_data;
                        commit_op->prepare();
                        commit_op->on_execute(&pctx);
                        while (!commit_op->is_executed()) {
                            auto waiting = commit_op->find_waiting_operator();
                            if (!waiting)
                                break;
                            co_await waiting->await_async_and_resume(&pctx);
                            commit_op->on_execute(&pctx);
                        }
                        if (pctx.has_pending_disk_futures()) {
                            auto futures = pctx.take_pending_disk_futures();
                            for (auto& f : futures) co_await std::move(f);
                        }
                        commit_id = static_cast<components::operators::operator_commit_transaction_t*>(commit_op.get())
                                        ->commit_id();
                    }
                }
                // Step 7 (inline, see above): drive index commit_insert for CREATE INDEX.
                if (commit_id > 0 && original_type == node_type::create_index_t &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    auto* root_after_plan = effective_root_node(logic_plan.get());
                    components::catalog::oid_t indexed_tbl_oid = components::catalog::INVALID_OID;
                    if (root_after_plan && !root_after_plan->children().empty()) {
                        auto* back = root_after_plan->children().back().get();
                        if (back && back->type() == node_type::create_index_t) {
                            auto* ci = static_cast<const node_create_index_t*>(back);
                            indexed_tbl_oid = ci->table_oid();
                        }
                    }
                    if (indexed_tbl_oid != components::catalog::INVALID_OID) {
                        components::execution_context_t swap_ctx{session, txn_data, {}};
                        auto [_ci, cif] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::commit_insert,
                                                           swap_ctx,
                                                           indexed_tbl_oid,
                                                           commit_id);
                        // Today bitcask is assert+abort terminal, so
                        // contains_error() never trips on the CREATE INDEX path.
                        auto ci_result = co_await std::move(cif);
                        if (ci_result.contains_error()) {
                            // TODO: index-side abort path for CREATE INDEX
                            // failure (revert pg_index row, drop disk agent).
                        }
                    }
                }
                // CREATE MATERIALIZED VIEW — register routing for SELECT * FROM mv.
                // The matview heap + INSERT-SELECT populate is handled atomically by
                // operator_create_matview_t (composite physical operator); dispatcher
                // only needs to register the new collection in its routing map.
                if (t == node_type::create_matview_t) {
                    auto* mv_node = effective_root_node(logic_plan.get());
                    if (mv_node && mv_node->type() == node_type::create_matview_t) {
                        auto* cm = static_cast<const node_create_matview_t*>(mv_node);
                        auto names = drop_target_names_from_resolves(logic_plan.get());
                        collections_.insert(qualified_name_t{names.first, cm->matviewname()});
                        // Fan-out the new oid to the owning executor slice
                        // (single send, NOT broadcast) so it can populate its
                        // local_collections_ slot with a by-value POD entry — a
                        // shared collection_t pointer would be mutable state shared
                        // across actors. The executor uses find_local_collection(oid)
                        // in execute_plan for intra-partition probes; cross-partition
                        // queries still read dispatcher.collections_ (not yet migrated).
                        const auto mv_oid = cm->matview_oid();
                        if (mv_oid != components::catalog::INVALID_OID && !executors_.empty()) {
                            const std::size_t pool_idx =
                                static_cast<std::size_t>(mv_oid) % executors_.size();
                            collection::executor::executor_t::local_collection_entry_t entry;
                            entry.oid = mv_oid;
                            entry.database = names.first;
                            // schema not surfaced by drop_target_names_from_resolves;
                            // left empty until a later migration carries it through.
                            entry.name = cm->matviewname();
                            auto reg = actor_zeta::otterbrix::send(
                                executor_addresses_[pool_idx],
                                &collection::executor::executor_t::register_collection_local,
                                session,
                                mv_oid,
                                std::move(entry));
                            if (reg.first && executors_[pool_idx]) {
                                scheduler_->enqueue(executors_[pool_idx].get());
                            }
                            // Park the unique_future<void> on pending_void_ as
                            // GC bookkeeping; poll_pending() drains it via
                            // is_ready() once register_collection_local completes
                            // on the executor side. Not a lifetime requirement —
                            // dropping the future immediately would also be
                            // memory-safe (library Last-One-Out protocol);
                            // parking merely batches the release.
                            pending_void_.emplace_back(std::move(reg.second));
                        }
                    }
                }
                // Update routing map: for create_collection_t logic_plan is sequence_t whose
                // first child is the create_collection_t carrying the new collection name.
                // create_constraint_t and create_database_t have no collection to register.
                if (t == node_type::create_collection_t) {
                    auto* root_after_plan = effective_root_node(logic_plan.get());
                    if (root_after_plan && !root_after_plan->children().empty()) {
                        auto* cc_child =
                            static_cast<node_create_collection_t*>(root_after_plan->children().front().get());
                        auto names = drop_target_names_from_resolves(logic_plan.get());
                        collections_.insert(qualified_name_t{names.first, cc_child->relname()});
                        // See create_matview_t fanout above. The planner stamps
                        // the freshly-allocated oid on the create_collection_t
                        // node via set_table_oid (OID allocation lives in
                        // executor_t::execute_plan_full). Entry is a by-value POD —
                        // a shared collection_t pointer between dispatcher and
                        // executor would be mutable state shared across actors.
                        const auto cc_oid = cc_child->table_oid();
                        if (cc_oid != components::catalog::INVALID_OID && !executors_.empty()) {
                            const std::size_t pool_idx =
                                static_cast<std::size_t>(cc_oid) % executors_.size();
                            collection::executor::executor_t::local_collection_entry_t entry;
                            entry.oid = cc_oid;
                            entry.database = names.first;
                            // schema not surfaced by drop_target_names_from_resolves;
                            // left empty until a later migration carries it through.
                            entry.name = cc_child->relname();
                            auto reg = actor_zeta::otterbrix::send(
                                executor_addresses_[pool_idx],
                                &collection::executor::executor_t::register_collection_local,
                                session,
                                cc_oid,
                                std::move(entry));
                            if (reg.first && executors_[pool_idx]) {
                                scheduler_->enqueue(executors_[pool_idx].get());
                            }
                            // Park unique_future<void> on pending_void_ — see
                            // matview branch above for rationale.
                            pending_void_.emplace_back(std::move(reg.second));
                        }
                    }
                }
                // Drop side: the cascade operator removed pg_class/pg_namespace rows on
                // disk, but the dispatcher's in-memory collections_ map is rebuilt from
                // pg_catalog only on the *next* execute_plan. Clean up here so any
                // immediate follow-up call (in the same handler chain) does not see a
                // stale entry. Names captured before the planner rewrite.
                if (t == node_type::drop_database_t && !drop_target_database.empty()) {
                    for (auto it = collections_.begin(); it != collections_.end();) {
                        if (it->database == drop_target_database) {
                            it = collections_.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
                if (t == node_type::drop_collection_t && !drop_target_collection.collection.empty()) {
                    collections_.erase(drop_target_collection);
                }
                co_return result;
            }
            if (t == node_type::update_t || t == node_type::delete_t) {
                co_return result;
            }
            trace(log_, "manager_dispatcher_t::execute_plan: non processed type - {}", to_string(t));
        } else {
            // Executor handles abort + revert for DML errors
            trace(log_, "manager_dispatcher_t::execute_plan: error: \"{}\"", result->get_error().what);
        }

        co_return std::move(result);
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());

        // Go through the operator pipeline. The logical leaf
        // node_register_udf_t carries the function payload; create_plan lowers
        // it to operator_register_udf_t which fans out to per-executor
        // registries, mirrors into function_registry_t::get_default(), and
        // persists pg_proc rows.
        //
        // We invoke the operator directly here rather than routing through
        // execute_plan_impl: register_udf has a custom return type (bool, not
        // cursor) and needs the executor address list which only the
        // dispatcher has.

        // Wrap the unique_ptr function in a shared_ptr so the logical node can
        // copy without consuming. The operator deep-copies via get_copy() when
        // fanning out, leaving the shared_ptr's payload untouched for the
        // pg_proc encode step.
        std::shared_ptr<components::compute::function> shared_fn(function.release());
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_register_udf_t(resource(), shared_fn));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        // Build the executor fan-out callable. The dispatcher captures
        // executor_addresses_, scheduler_, and executors_ so the operator can
        // drive per-executor register_udf without needing direct scheduler
        // access. needs_sched is honoured here (matching the legacy inline
        // path) so the executor's mailbox is processed.
        using fanout_result_t = components::operators::operator_register_udf_t::executor_register_result_t;
        auto fanout = [this](components::session::session_id_t s,
                             components::compute::function_ptr fcopy,
                             std::size_t i) -> actor_zeta::unique_future<std::unique_ptr<fanout_result_t>> {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[i],
                                                                     &collection::executor::executor_t::register_udf,
                                                                     s,
                                                                     std::move(fcopy));
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            return std::move(future);
        };
        auto op = services::planner::impl::create_plan_register_udf(cstor,
                                                                    plan,
                                                                    executor_addresses_.size(),
                                                                    std::move(fanout));
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting)
                break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* ru = static_cast<components::operators::operator_register_udf_t*>(op.get());
        co_return ru->success();
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);

        // Operator-pipeline replacement. The logical leaf
        // node_unregister_udf_t carries the (name, inputs) signature; the
        // operator probes function_registry_t::get_default(), removes the
        // matching overload, and purges pg_proc + pg_depend rows.
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                core::function_name_t{std::move(function_name)},
                                                                std::move(inputs)));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting)
                break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* uu = static_cast<components::operators::operator_unregister_udf_t*>(op.get());
        co_return uu->success();
    }

    manager_dispatcher_t::unique_future<collection::executor::execute_result_t>
    manager_dispatcher_t::execute_plan_impl(components::session::session_id_t session,
                                            node_ptr logical_plan,
                                            storage_parameters parameters,
                                            components::table::transaction_data txn,
                                            context_storage_t* collections_context_storage) {
        auto context_copy = *collections_context_storage;
        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: node_type: {}, table_oid: {}, session: {}",
              components::logical_plan::to_string(logical_plan->type()),
              logical_plan->table_oid(),
              session.data());

        // Oid-only routing. Plan generators ask context_storage_t
        // whether a given resolved table_oid is known (i.e. we have an actor
        // for it). Walk the plan, collect every table_oid stamped by enrich,
        // and forward the set to the executor. Wrapper / parser-window / DDL
        // nodes contribute INVALID_OID and are filtered.
        auto dependency_oids = logical_plan->table_oid_dependencies();

        for (auto oid : dependency_oids) {
            context_copy.known_oids.insert(oid);
        }
        // Forward resolve_table metadata (relkind + live columns)
        // so plan generators can build transfer_scan with the right
        // projection mask instead of inlining pg_class / pg_computed_column
        // scans. Gather a local index from the plan tree (execute_plan_impl
        // is callable from sub-plan execution where the caller's
        // dispatcher_idx isn't visible).
        {
            catalog_resolve::plan_resolve_index_t local_idx;
            catalog_resolve::gather_plan_resolve_index(logical_plan.get(), local_idx);
            for (const auto& [oid, md_ptr] : local_idx.tbl_md_by_oid) {
                context_copy.table_metadata[oid] = md_ptr;
            }
        }

        context_copy.parameters = &parameters;

        assert(!executors_.empty());
        // Oid-only pool routing. For wrapper nodes (sequence_t etc.)
        // table_oid is INVALID at the root; peek at the first child which is
        // the inner DML/DDL bearing the resolved oid. When no oid is resolvable
        // (db/ns DDL — no table involved) we route to executor[0] deterministically.
        std::size_t pool_idx = 0;
        components::catalog::oid_t routing_oid = logical_plan->table_oid();
        if (routing_oid == components::catalog::INVALID_OID && !logical_plan->children().empty()) {
            routing_oid = logical_plan->children().front()->table_oid();
        }
        if (routing_oid != components::catalog::INVALID_OID) {
            pool_idx = static_cast<std::size_t>(routing_oid) % executors_.size();
        }
        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: calling executor[{}] (full_pipeline={})",
              pool_idx,
              use_executor_full_pipeline ? "yes" : "no");
        // execute_plan and execute_plan_full share the exact same call signature
        // (see executor.hpp) so they have identical member-function-pointer types.
        // The constexpr ternary collapses at compile time to a single pointer —
        // no runtime branch, no dead-code warning. The flag must stay paired with
        // executor.cpp's `enable_pass2_rewrites` (see the note at its definition).
        constexpr auto execute_method =
            use_executor_full_pipeline ? &collection::executor::executor_t::execute_plan_full
                                       : &collection::executor::executor_t::execute_plan;
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 execute_method,
                                                                 session,
                                                                 logical_plan,
                                                                 parameters,
                                                                 std::move(context_copy),
                                                                 txn);
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto result = co_await std::move(future);

        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: executor returned, success: {}",
              result.cursor->is_success());
        co_return result;
    }

    manager_dispatcher_t::unique_future<components::table::transaction_data>
    manager_dispatcher_t::begin_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::begin_transaction, session: {}", session.data());
        auto& txn = txn_manager_.begin_transaction(session);
        co_return txn.data();
    }

    // low-level wrappers. Each handler runs inside
    // the dispatcher's actor context, so the underlying txn_manager_ mutation
    // happens on a single owner — the executor only ever talks via mailbox.
    manager_dispatcher_t::unique_future<components::table::transaction_data>
    manager_dispatcher_t::txn_begin_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_begin_msg, session: {}", session.data());
        auto& txn = txn_manager_.begin_transaction(session);
        co_return txn.data();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::txn_commit_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_commit_msg, session: {}", session.data());
        co_return txn_manager_.commit(session);
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_abort_msg(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::txn_abort_msg, session: {}", session.data());
        txn_manager_.abort(session);
        co_return;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::txn_publish_msg(uint64_t commit_id) {
        trace(log_, "manager_dispatcher_t::txn_publish_msg, commit_id: {}", commit_id);
        txn_manager_.publish(commit_id);
        co_return;
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::txn_lowest_active_msg() {
        co_return txn_manager_.lowest_active_start_time();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::commit_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::commit_transaction, session: {}", session.data());

        // Go through the operator pipeline instead of inline
        // txn_manager + disk sends. The leaf node carries no fields; the
        // operator reads txn_manager / disk_address / session off the
        // pipeline::context_t we build here.
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_commit_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            // Should not happen — create_plan_commit_transaction is unconditional.
            co_return 0;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn_manager = &txn_manager_;
        // txn snapshot is unused by the operator (it re-reads via find_transaction)
        // but carry a sane default so any nested debug logs see a zero value.
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) {
                break;
            }
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* commit_op = static_cast<components::operators::operator_commit_transaction_t*>(op.get());
        co_return commit_op->commit_id();
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::abort_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::abort_transaction, session: {}", session.data());

        // Operator-pipeline replacement (mirrors commit above).
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_abort_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            co_return;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                             actor_zeta::address_t::empty_address(),
                                             actor_zeta::address_t::empty_address(),
                                             &fn_registry,
                                             params};
        pctx.disk_address = disk_address_;
        pctx.txn_manager = &txn_manager_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) {
                break;
            }
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    node_ptr manager_dispatcher_t::create_logic_plan(node_ptr plan) {
        // Retained for any callers outside execute_plan; the primary path now
        // calls planner_t::create_plan directly after enrich_plan.
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan));
    }

} // namespace services::dispatcher
