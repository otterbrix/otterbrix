#include "create_plan.hpp"

#include "impl/create_plan_aggregate.hpp"
#include "impl/create_plan_data.hpp"
#include "impl/create_plan_delete.hpp"
#include "impl/create_plan_group.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_join.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_sort.hpp"
#include "impl/create_plan_update.hpp"

namespace services::planner {

    using components::logical_plan::node_type;

    components::operators::operator_ptr create_plan(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit,
                                                          const components::logical_plan::storage_parameters* params) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, node, std::move(limit), params);
            case node_type::data_t:
                return impl::create_plan_data(node);
            case node_type::delete_t:
                return impl::create_plan_delete(context, node);
            case node_type::insert_t:
                return impl::create_plan_insert(context, node, std::move(limit));
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::having_t:
                return impl::create_plan_having(context, node, std::move(limit));
            case node_type::group_t:
                return impl::create_plan_group(context, node, params);
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, node);
            case node_type::join_t:
                return impl::create_plan_join(context, node, std::move(limit));
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::planner