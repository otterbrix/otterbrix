#include "operator_add_index.hpp"
#include <components/cursor/cursor.hpp>
#include <components/index/single_field_index.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/pmr.hpp>
#include <core/executor.hpp>
#include <services/collection/collection.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::base::operators {

    operator_add_index::operator_add_index(services::collection::context_collection_t* context,
                                           logical_plan::node_create_index_ptr node)
        : read_write_operator_t(context, operator_type::add_index)
        , index_node_{std::move(node)}
        , index_name_{index_node_->name()} {}

    void operator_add_index::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(),
              "operator_add_index::on_execute_impl session: {}, index: {}",
              pipeline_context->session.data(),
              index_node_->name());
        switch (index_node_->type()) {
            case logical_plan::index_type::single: {
                const bool index_exist = context_->index_engine()->has_index(index_node_->name());
                id_index_ = index_exist
                            ? index::INDEX_ID_UNDEFINED
                            : index::make_index<index::single_field_index_t>(context_->index_engine(),
                                                                             index_node_->name(),
                                                                             index_node_->keys());

                auto [_, future] = actor_zeta::send(context_->disk(),
                                 &services::disk::manager_disk_t::create_index_agent,
                                 pipeline_context->session,
                                 std::move(index_node_),
                                 context_);
                bool tmp_disk_future_ready_ = future.available();
                disk_future_ = std::make_unique<actor_zeta::unique_future<actor_zeta::address_t>>(std::move(future)); //TODO: research std::unique_ptr<actor_zeta::unique_future<actor_zeta::address_t>>
                disk_future_ready_ = tmp_disk_future_ready_;
                break;
            }
            case logical_plan::index_type::composite:
            case logical_plan::index_type::multikey:
            case logical_plan::index_type::hashed:
            case logical_plan::index_type::wildcard: {
                trace(context_->log(), "index_type not implemented");
                assert(false && "index_type not implemented");
                break;
            }
            case logical_plan::index_type::no_valid: {
                trace(context_->log(), "index_type not valid");
                assert(false && "index_type not valid");
                break;
            }
        }
    }

} // namespace components::base::operators
