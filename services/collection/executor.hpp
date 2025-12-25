#pragma once

#include <components/catalog/table_metadata.hpp>

#include <services/collection/collection.hpp>
#include <services/memory_storage/context_storage.hpp>
#include <stack>

namespace services::collection::executor {

    struct plan_t {
        std::stack<components::collection::operators::operator_ptr> sub_plans;
        components::logical_plan::storage_parameters parameters;
        services::context_storage_t context_storage_;

        explicit plan_t(std::stack<components::collection::operators::operator_ptr>&& sub_plans,
                        components::logical_plan::storage_parameters parameters,
                        services::context_storage_t&& context_storage);
    };
    using plan_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, plan_t>;

    class executor_t final : public actor_zeta::basic_actor<executor_t> {
    public:
        executor_t(services::memory_storage_t* memory_storage, log_t&& log);
        ~executor_t() = default;

        void execute_plan(const components::session::session_id_t& session,
                          components::logical_plan::node_ptr logical_plan,
                          components::logical_plan::storage_parameters parameters,
                          services::context_storage_t&& context_storage,
                          components::catalog::used_format_t data_format);

        void create_documents(const session_id_t& session,
                              context_collection_t* collection,
                              const std::pmr::vector<document_ptr>& documents);

        void create_index_finish(const session_id_t& session,
                                 const std::string& name,
                                 const actor_zeta::address_t& index_address,
                                 context_collection_t* collection);
        void create_index_finish_index_exist(const session_id_t& session,
                                             const std::string& name,
                                             context_collection_t* collection);
        void index_modify_finish(const session_id_t& session, context_collection_t* collection);
        void index_find_finish(const session_id_t& session,
                               const std::pmr::vector<document_id_t>& result,
                               context_collection_t* collection);

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior();

    private:
        void traverse_plan_(const components::session::session_id_t& session,
                            components::collection::operators::operator_ptr&& plan,
                            components::logical_plan::storage_parameters&& parameters,
                            services::context_storage_t&& context_storage);

        void execute_sub_plan_(const components::session::session_id_t& session,
                               components::collection::operators::operator_ptr plan,
                               components::logical_plan::storage_parameters parameters);

        void
        execute_sub_plan_finish_(const components::session::session_id_t& session,
                                 components::cursor::cursor_t_ptr cursor,
                                 components::base::operators::operator_write_data_t::updated_types_map_t updates = {});

        void execute_plan_finish_(const components::session::session_id_t& session,
                                  components::cursor::cursor_t_ptr&& cursor,
                                  components::base::operators::operator_write_data_t::updated_types_map_t updates = {});

        void aggregate_document_impl(const components::session::session_id_t& session,
                                     context_collection_t* context_,
                                     components::collection::operators::operator_ptr plan);
        void update_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  components::collection::operators::operator_ptr plan);
        void insert_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  components::collection::operators::operator_ptr plan);
        void delete_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  components::collection::operators::operator_ptr plan);

    private:
        actor_zeta::address_t memory_storage_ = actor_zeta::address_t::empty_address();
        plan_storage_t plans_;
        log_t log_;

        // Behaviors
        actor_zeta::behavior_t execute_plan_;
        actor_zeta::behavior_t create_documents_;
        actor_zeta::behavior_t create_index_finish_;
        actor_zeta::behavior_t create_index_finish_index_exist_;
        actor_zeta::behavior_t index_modify_finish_;
        actor_zeta::behavior_t index_find_finish_;
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor