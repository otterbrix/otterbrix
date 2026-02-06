#include "manager_disk.hpp"
#include "result.hpp"
#include <actor-zeta/spawn.hpp>
#include <chrono>
#include <thread>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <core/executor.hpp>
#include <services/collection/collection.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

namespace services::disk {

    using components::document::document_id_t;
    using namespace core::filesystem;

    namespace {
        std::vector<components::logical_plan::node_create_index_ptr>
        make_unique(std::vector<components::logical_plan::node_create_index_ptr> indexes) {
            std::vector<components::logical_plan::node_create_index_ptr> result;
            result.reserve(indexes.size());

            for (auto&& index : indexes) {
                result.emplace_back(std::move(index));
            }
            return result;
        }
    } // namespace

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* resource,
                                   actor_zeta::scheduler_raw scheduler,
                                   actor_zeta::scheduler_raw scheduler_disk,
                                   configuration::config_disk config,
                                   log_t& log,
                                   run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_disk_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , scheduler_disk_(scheduler_disk)
        , run_fn_(std::move(run_fn))
        , log_(log.clone())
        , fs_(core::filesystem::local_file_system_t())
        , config_(std::move(config))
        , metafile_indexes_(nullptr)
        , removed_indexes_(resource)
        , pending_void_(resource)
        , pending_load_(resource)
        , pending_find_(resource) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            create_directories(config_.path);
            metafile_indexes_ = open_file(fs_,
                                          config_.path / "indexes_METADATA",
                                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                          file_lock_type::NO_LOCK);
        }
        create_agent(config.agent);
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() { trace(log_, "delete manager_disk_t"); }

    void manager_disk_t::poll_pending() {
        if (is_polling_) {
            return;
        }
        is_polling_ = true;

        if (pending_void_.empty() && pending_load_.empty() && pending_find_.empty()) {
            is_polling_ = false;
            return;
        }

        size_t i = 0;
        while (i < pending_void_.size()) {
            if (!pending_void_[i].valid() || pending_void_[i].available()) {
                if (i + 1 < pending_void_.size()) {
                    std::swap(pending_void_[i], pending_void_.back());
                }
                pending_void_.pop_back();
            } else {
                ++i;
            }
        }

        i = 0;
        while (i < pending_load_.size()) {
            if (!pending_load_[i].valid() || pending_load_[i].available()) {
                if (i + 1 < pending_load_.size()) {
                    std::swap(pending_load_[i], pending_load_.back());
                }
                pending_load_.pop_back();
            } else {
                ++i;
            }
        }

        i = 0;
        while (i < pending_find_.size()) {
            if (!pending_find_[i].valid() || pending_find_[i].available()) {
                if (i + 1 < pending_find_.size()) {
                    std::swap(pending_find_[i], pending_find_.back());
                }
                pending_find_.pop_back();
            } else {
                ++i;
            }
        }

        is_polling_ = false;
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        std::lock_guard<spin_lock> guard(lock_);

        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::load_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::load_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::write_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::write_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_index_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_index_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_index_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_index_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_index_agent_success>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_index_agent_success, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_insert_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_insert_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_insert>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_remove>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_remove, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_insert_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_insert_by_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_remove_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_remove_by_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::index_find_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::index_find_by_agent, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_disk_t::sync(address_pack pack) {
        constexpr static int manager_wal = 0;
        manager_wal_ = std::get<manager_wal>(pack);
    }

    void manager_disk_t::create_agent(int count_agents) {
        for (int i = 0; i < count_agents; i++) {
            auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), this, config_.path, log_);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<result_load_t> manager_disk_t::load(session_id_t session) {
        trace(log_, "manager_disk_t::load , session : {}", session.data());
        auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::load, session);
        if (needs_sched) {
            scheduler_->enqueue(agents_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_disk_t::unique_future<void> manager_disk_t::load_indexes(session_id_t session,
                                                                      actor_zeta::address_t dispatcher_address) {
        trace(log_, "manager_disk_t::load_indexes , session : {}", session.data());
        load_session_ = session;
        co_await load_indexes_impl(session, dispatcher_address);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::append_database(session_id_t session,
                                                                         database_name_t database) {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        command_append_database_t command{database};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::remove_database(session_id_t session,
                                                                         database_name_t database) {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        command_remove_database_t command{database};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::append_collection(session_id_t session,
                                                                           database_name_t database,
                                                                           collection_name_t collection) {
        trace(log_,
              "manager_disk_t::append_collection , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_append_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::remove_collection(session_id_t session,
                                                                           database_name_t database,
                                                                           collection_name_t collection) {
        trace(log_,
              "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_remove_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::write_documents(session_id_t session,
                                                                         database_name_t database,
                                                                         collection_name_t collection,
                                                                         std::pmr::vector<document_ptr> documents) {
        trace(log_,
              "manager_disk_t::write_documents , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_write_documents_t command{std::move(database), std::move(collection), std::move(documents)};
        append_command(commands_, session, command_t(std::move(command)));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::remove_documents(session_id_t session,
                                                                          database_name_t database,
                                                                          collection_name_t collection,
                                                                          document_ids_t documents) {
        trace(log_,
              "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_remove_documents_t command{std::move(database), std::move(collection), std::move(documents)};
        append_command(commands_, session, command_t(command));
        co_return;
    }


    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : it->second) {
                if (command.name() == actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_collection>) {
                    const auto& drop_collection = command.get<command_remove_collection_t>();
                    std::vector<index_agent_disk_t*> indexes;
                    for (const auto& index : index_agents_) {
                        if (index.second->collection_name() == drop_collection.collection) {
                            indexes.push_back(index.second.get());
                        }
                    }
                    if (indexes.empty()) {
                        auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_collection, command);
                        if (needs_sched) {
                            scheduler_->enqueue(agents_[0].get());
                        }
                        co_await std::move(future);
                    } else {
                        removed_indexes_.emplace(session, removed_index_t{indexes.size(), command});
                        for (auto* index : indexes) {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(index->address(), &index_agent_disk_t::drop, session);
                            if (needs_sched) {
                                scheduler_disk_->enqueue(index);
                            }
                            co_await std::move(future);
                        }
                    }
                } else {
                    switch (command.name()) {
                        case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_database>: {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::append_database, command);
                            if (needs_sched) {
                                scheduler_->enqueue(agents_[0].get());
                            }
                            co_await std::move(future);
                            break;
                        }
                        case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_database>: {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_database, command);
                            if (needs_sched) {
                                scheduler_->enqueue(agents_[0].get());
                            }
                            co_await std::move(future);
                            break;
                        }
                        case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_collection>: {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::append_collection, command);
                            if (needs_sched) {
                                scheduler_->enqueue(agents_[0].get());
                            }
                            co_await std::move(future);
                            break;
                        }
                        case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::write_documents>: {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::write_documents, command);
                            if (needs_sched) {
                                scheduler_->enqueue(agents_[0].get());
                            }
                            co_await std::move(future);
                            break;
                        }
                        case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_documents>: {
                            auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_documents, command);
                            if (needs_sched) {
                                scheduler_->enqueue(agents_[0].get());
                            }
                            co_await std::move(future);
                            break;
                        }
                        default:
                            break;
                    }
                }
            }
            commands_.erase(session);
        }
        auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::fix_wal_id, wal_id);
        if (needs_sched) {
            scheduler_->enqueue(agents_[0].get());
        }
        co_await std::move(future);
        co_return;
    }

    manager_disk_t::unique_future<actor_zeta::address_t> manager_disk_t::create_index_agent(
        session_id_t session,
        components::logical_plan::node_create_index_ptr index,
        services::collection::context_collection_t* collection) {
        auto name = index->name();
        trace(log_, "manager_disk: create_index_agent : {}", name);
        if (index_agents_.contains(name) && !index_agents_.at(name)->is_dropped()) {
            error(log_, "manager_disk: index {} already exists", name);
            co_return actor_zeta::address_t::empty_address();
        } else {
            trace(log_, "manager_disk: create_index_agent : {}", name);
            index_agents_.erase(name);
            auto index_agent = actor_zeta::spawn<index_agent_disk_t>(resource(), this, config_.path, collection, name, log_);
            auto agent_address = index_agent->address();
            index_agents_.insert_or_assign(name, std::move(index_agent));
            if (session.data() != load_session_.data()) {
                trace(log_, "manager_disk: write_index_impl, index valid: {}", static_cast<bool>(index));
                write_index_impl(index);
            }
            co_return agent_address;
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_index_agent(
        session_id_t session,
        index_name_t index_name,
        services::collection::context_collection_t* /*collection*/) {
        if (index_agents_.contains(index_name)) {
            trace(log_, "manager_disk: drop_index_agent : {}", index_name);
            command_drop_index_t command{index_name, actor_zeta::address_t::empty_address()};
            append_command(commands_, session, command_t(command));
            auto* index_agent = index_agents_.at(index_name).get();
            auto [needs_sched, future] = actor_zeta::otterbrix::send(index_agent->address(),
                                        &index_agent_disk_t::drop, session);
            if (needs_sched) {
                scheduler_disk_->enqueue(index_agent);
            }
            remove_index_impl(index_name);
        } else {
            error(log_, "manager_disk: index {} not exists", index_name);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_index_agent_success(session_id_t session) {
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                auto command_drop = command.get<command_drop_index_t>();
                trace(log_, "manager_disk: drop_index_agent : {} : success", command_drop.index_name);
            }
            commands_.erase(session);
        } else {
            auto it_all_drop = removed_indexes_.find(session);
            if (it_all_drop != removed_indexes_.end()) {
                if (--it_all_drop->second.size == 0) {
                    const auto& drop_collection = it_all_drop->second.command.get<command_remove_collection_t>();
                    auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_collection,
                                                it_all_drop->second.command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    remove_all_indexes_from_collection_impl(drop_collection.collection);
                }
            }
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::index_insert_many(
        session_id_t session,
        index_name_t index_name,
        std::vector<std::pair<components::document::value_t, components::document::document_id_t>> values) {
        trace(log_, "manager_disk: index_insert_many : {} , {} values", index_name, values.size());
        if (index_agents_.contains(index_name)) {
            auto* index_agent = index_agents_.at(index_name).get();
            auto [needs_sched, future] = actor_zeta::otterbrix::send(index_agent->address(),
                                        &index_agent_disk_t::insert_many, session, std::move(values));
            if (needs_sched) {
                scheduler_disk_->enqueue(index_agent);
            }
        } else {
            error(log_, "manager_disk: index {} not exists for insert_many", index_name);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::index_insert(
        session_id_t session,
        index_name_t index_name,
        components::types::logical_value_t key,
        components::document::document_id_t doc_id) {
        trace(log_, "manager_disk: index_insert : {}", index_name);
        if (index_agents_.contains(index_name)) {
            auto* index_agent = index_agents_.at(index_name).get();
            auto [needs_sched, future] = actor_zeta::otterbrix::send(index_agent->address(),
                                        &index_agent_disk_t::insert, session, std::move(key), doc_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(index_agent);
            }
        } else {
            error(log_, "manager_disk: index {} not exists for insert", index_name);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::index_remove(
        session_id_t session,
        index_name_t index_name,
        components::types::logical_value_t key,
        components::document::document_id_t doc_id) {
        trace(log_, "manager_disk: index_remove : {}", index_name);
        if (index_agents_.contains(index_name)) {
            auto* index_agent = index_agents_.at(index_name).get();
            auto [needs_sched, future] = actor_zeta::otterbrix::send(index_agent->address(),
                                        &index_agent_disk_t::remove, session, std::move(key), doc_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(index_agent);
            }
        } else {
            error(log_, "manager_disk: index {} not exists for remove", index_name);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::index_insert_by_agent(
        session_id_t session,
        actor_zeta::address_t agent_address,
        components::types::logical_value_t key,
        components::document::document_id_t doc_id) {
        trace(log_, "manager_disk: index_insert_by_agent");
        index_agent_disk_t* found_agent = nullptr;
        for (auto& [name, ptr] : index_agents_) {
            if (ptr->address() == agent_address) {
                found_agent = ptr.get();
                break;
            }
        }
        if (found_agent) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(found_agent->address(),
                                        &index_agent_disk_t::insert, session, std::move(key), doc_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(found_agent);
            }
            co_await std::move(future);
        } else {
            error(log_, "manager_disk: agent not found for insert_by_agent");
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::index_remove_by_agent(
        session_id_t session,
        actor_zeta::address_t agent_address,
        components::types::logical_value_t key,
        components::document::document_id_t doc_id) {
        trace(log_, "manager_disk: index_remove_by_agent");
        index_agent_disk_t* found_agent = nullptr;
        for (auto& [name, ptr] : index_agents_) {
            if (ptr->address() == agent_address) {
                found_agent = ptr.get();
                break;
            }
        }
        if (found_agent) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(found_agent->address(),
                                        &index_agent_disk_t::remove, session, std::move(key), doc_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(found_agent);
            }
            co_await std::move(future);
        } else {
            error(log_, "manager_disk: agent not found for remove_by_agent");
        }
        co_return;
    }

    manager_disk_t::unique_future<index_disk_t::result> manager_disk_t::index_find_by_agent(
        session_id_t session,
        actor_zeta::address_t agent_address,
        components::types::logical_value_t key,
        components::expressions::compare_type compare) {
        trace(log_, "manager_disk: index_find_by_agent");
        index_agent_disk_t* found_agent = nullptr;
        for (auto& [name, ptr] : index_agents_) {
            if (ptr->address() == agent_address) {
                found_agent = ptr.get();
                break;
            }
        }
        if (found_agent) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(found_agent->address(),
                                        &index_agent_disk_t::find, session, std::move(key), compare);
            if (needs_sched) {
                scheduler_disk_->enqueue(found_agent);
            }
            co_return co_await std::move(future);
        } else {
            error(log_, "manager_disk: agent not found for find_by_agent");
            co_return index_disk_t::result{resource()};
        }
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }

    void manager_disk_t::write_index_impl(const components::logical_plan::node_create_index_ptr& index) {
        if (metafile_indexes_) {
            components::serializer::msgpack_serializer_t serializer(resource());
            serializer.start_array(1);
            index->serialize(&serializer);
            serializer.end_array();
            auto buf = serializer.result();
            auto size = buf.size();
            metafile_indexes_->write(&size, sizeof(size), metafile_indexes_->file_size());
            metafile_indexes_->write(buf.data(), buf.size(), metafile_indexes_->file_size());
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::load_indexes_impl(
        session_id_t /*session*/, actor_zeta::address_t dispatcher_address) {
        auto indexes = make_unique(read_indexes_impl());
        metafile_indexes_->seek(metafile_indexes_->file_size());

        for (auto& index : indexes) {
            trace(log_, "manager_disk: load_indexes_impl : {}", index->name());

            auto [_cr, cursor_future] = actor_zeta::send(
                dispatcher_address,
                &dispatcher::manager_dispatcher_t::execute_plan,
                session_id_t::generate_uid(),
                boost::static_pointer_cast<components::logical_plan::node_t>(index),
                components::logical_plan::make_parameter_node(resource()));
            auto cursor = co_await std::move(cursor_future);

            if (cursor && cursor->is_error()) {
                error(log_, "manager_disk: failed to create index {}: {}",
                      index->name(), cursor->get_error().what);
            }
        }

        trace(log_, "manager_disk: load_indexes_impl completed, {} indexes", indexes.size());
        co_return;
    }

    std::vector<components::logical_plan::node_create_index_ptr>
    manager_disk_t::read_indexes_impl(const collection_name_t& collection) const {
        std::vector<components::logical_plan::node_create_index_ptr> res;
        if (metafile_indexes_) {
            constexpr auto count_byte_by_size = sizeof(size_t);
            size_t size;
            size_t offset = 0;
            std::unique_ptr<char[]> size_str(new char[count_byte_by_size]);
            while (true) {
                metafile_indexes_->seek(offset);
                auto bytes_read = metafile_indexes_->read(size_str.get(), count_byte_by_size);
                if (bytes_read == count_byte_by_size) {
                    offset += count_byte_by_size;
                    std::memcpy(&size, size_str.get(), count_byte_by_size);
                    std::pmr::string buf(resource());
                    buf.resize(size);
                    metafile_indexes_->read(buf.data(), size, offset);
                    offset += size;
                    components::serializer::msgpack_deserializer_t deserializer(buf);

                    deserializer.advance_array(0);
                    auto index = components::logical_plan::node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                    if (collection.empty() || index->collection_name() == collection) {
                        res.push_back(
                            boost::polymorphic_pointer_downcast<components::logical_plan::node_create_index_t>(index));
                    }
                } else {
                    break;
                }
            }
        }
        return res;
    }

    std::vector<components::logical_plan::node_create_index_ptr> manager_disk_t::read_indexes_impl() const {
        return read_indexes_impl("");
    }

    void manager_disk_t::remove_index_impl(const index_name_t& index_name) {
        if (metafile_indexes_) {
            auto indexes = read_indexes_impl();
            indexes.erase(std::remove_if(indexes.begin(),
                                         indexes.end(),
                                         [&index_name](const components::logical_plan::node_create_index_ptr& index) {
                                             return index->name() == index_name;
                                         }),
                          indexes.end());
            metafile_indexes_->truncate(0);
            for (const auto& index : indexes) {
                write_index_impl(index);
            }
        }
    }

    void manager_disk_t::remove_all_indexes_from_collection_impl(const collection_name_t& collection) {
        if (metafile_indexes_) {
            auto indexes = read_indexes_impl();
            indexes.erase(std::remove_if(indexes.begin(),
                                         indexes.end(),
                                         [&collection](const components::logical_plan::node_create_index_ptr& index) {
                                             return index->collection_name() == collection;
                                         }),
                          indexes.end());
            metafile_indexes_->truncate(0);
            for (const auto& index : indexes) {
                write_index_impl(index);
            }
        }
    }

    manager_disk_empty_t::manager_disk_empty_t(std::pmr::memory_resource* mr,
                                               actor_zeta::scheduler::sharing_scheduler* scheduler)
        : actor_zeta::actor::actor_mixin<manager_disk_empty_t>()
        , resource_(mr)
        , scheduler_(scheduler)
        , pending_void_(mr)
        , pending_load_(mr)
        , pending_find_(mr) {}

    auto manager_disk_empty_t::make_type() const noexcept -> const char* { return "manager_disk"; }

    void manager_disk_empty_t::sync(address_pack /*pack*/) {}

    actor_zeta::behavior_t manager_disk_empty_t::behavior(actor_zeta::mailbox::message* msg) {
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(),
                           [](const auto& f) { return !f.valid() || f.available(); }),
            pending_void_.end());
        pending_load_.erase(
            std::remove_if(pending_load_.begin(), pending_load_.end(),
                           [](const auto& f) { return !f.valid() || f.available(); }),
            pending_load_.end());

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::load_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::load_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::append_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::append_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::append_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::append_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::write_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::write_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::create_index_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::create_index_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::drop_index_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::drop_index_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::drop_index_agent_success>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::drop_index_agent_success, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_insert_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_insert_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_insert>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_remove>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_remove, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_insert_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_insert_by_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_remove_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_remove_by_agent, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::index_find_by_agent>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::index_find_by_agent, msg);
                break;
            }
            default:
                break;
        }
    }

    manager_disk_empty_t::unique_future<result_load_t> manager_disk_empty_t::load(session_id_t /*session*/) {
        co_return result_load_t::empty();
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::load_indexes(
        session_id_t /*session*/, actor_zeta::address_t /*dispatcher_address*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::append_database(
        session_id_t /*session*/, database_name_t /*database*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::remove_database(
        session_id_t /*session*/, database_name_t /*database*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::append_collection(
        session_id_t /*session*/, database_name_t /*database*/, collection_name_t /*collection*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::remove_collection(
        session_id_t /*session*/, database_name_t /*database*/, collection_name_t /*collection*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::write_documents(
        session_id_t /*session*/, database_name_t /*database*/, collection_name_t /*collection*/,
        std::pmr::vector<document_ptr> /*documents*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::remove_documents(
        session_id_t /*session*/, database_name_t /*database*/, collection_name_t /*collection*/,
        document_ids_t /*documents*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::flush(
        session_id_t /*session*/, wal::id_t /*wal_id*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<actor_zeta::address_t> manager_disk_empty_t::create_index_agent(
        session_id_t /*session*/,
        components::logical_plan::node_create_index_ptr /*index*/,
        services::collection::context_collection_t* /*collection*/) {
        co_return actor_zeta::address_t::empty_address();
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::drop_index_agent(
        session_id_t /*session*/,
        index_name_t /*index_name*/,
        services::collection::context_collection_t* /*collection*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::drop_index_agent_success(
        session_id_t /*session*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::index_insert_many(
        session_id_t /*session*/,
        index_name_t /*index_name*/,
        std::vector<std::pair<components::document::value_t, components::document::document_id_t>> /*values*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::index_insert(
        session_id_t /*session*/,
        index_name_t /*index_name*/,
        components::types::logical_value_t /*key*/,
        components::document::document_id_t /*doc_id*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::index_remove(
        session_id_t /*session*/,
        index_name_t /*index_name*/,
        components::types::logical_value_t /*key*/,
        components::document::document_id_t /*doc_id*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::index_insert_by_agent(
        session_id_t /*session*/,
        actor_zeta::address_t /*agent_address*/,
        components::types::logical_value_t /*key*/,
        components::document::document_id_t /*doc_id*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::index_remove_by_agent(
        session_id_t /*session*/,
        actor_zeta::address_t /*agent_address*/,
        components::types::logical_value_t /*key*/,
        components::document::document_id_t /*doc_id*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<index_disk_t::result> manager_disk_empty_t::index_find_by_agent(
        session_id_t /*session*/,
        actor_zeta::address_t /*agent_address*/,
        components::types::logical_value_t /*key*/,
        components::expressions::compare_type /*compare*/) {
        co_return index_disk_t::result{resource()};
    }

} //namespace services::disk