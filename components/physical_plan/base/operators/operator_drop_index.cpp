#include "operator_drop_index.hpp"

#include <components/cursor/cursor.hpp>
#include <components/index/single_field_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <core/pmr.hpp>
#include <core/executor.hpp>
#include <services/collection/collection.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::base::operators {

    using logical_plan::index_type;

    using index::single_field_index_t;

    using namespace cursor;
    using namespace core::pmr;

    operator_drop_index::operator_drop_index(services::collection::context_collection_t* context,
                                             logical_plan::node_drop_index_ptr node)
        : read_write_operator_t(context, operator_type::drop_index)
        , node_(std::move(node)) {}

    void operator_drop_index::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(),
              "operator_drop_index::on_execute_impl session: {}, index: {}",
              pipeline_context->session.data(),
              node_->name());

        auto index_ptr = index::search_index(context_->index_engine(), node_->name());
        if (!index_ptr) {
            error_cursor_ = make_cursor(context_->resource(), error_code_t::index_not_exists);
            return;
        }

        if (index_ptr->is_disk()) {
            auto [_, future] = actor_zeta::send(context_->disk(),
                             &services::disk::manager_disk_t::drop_index_agent,
                             pipeline_context->session,
                             node_->name(),
                             context_);

            pipeline_context->add_pending_disk_future(std::move(future));
        }
        index::drop_index(context_->index_engine(), index_ptr);
    }
} // namespace components::base::operators
