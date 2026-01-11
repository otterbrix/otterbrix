#pragma once

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/callable_trait.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/send.hpp>

namespace actor_zeta {

    using shared_work = scheduler::sharing_scheduler;
    using scheduler_ptr = std::unique_ptr<shared_work>;
    using scheduler_raw = shared_work*;

    namespace otterbrix {

        // Custom send for otterbrix actors via address_t
        // Unlike standard actor_zeta::send which requires is_interface,
        // this version works with concrete actors accessed via address_t
        // If target is empty, returns a ready no-op future (for disabled services like disk)
        template<typename Sender, typename Method, typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(
            actor::address_t target,
            Sender sender,
            Method method,
            Args&&... args
        ) -> detail::dispatch_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;

            static_assert(
                type_traits::is_unique_future_v<result_type>,
                "Method must return unique_future<T>");

            // If target is empty (e.g., disk manager when disk is disabled),
            // return a ready no-op future instead of crashing
            if (!target) {
                // Get resource from sender (sender is always valid address_t)
                auto* resource = sender.resource();
                if constexpr (std::is_void_v<value_type>) {
                    return make_ready_future(resource);
                } else if constexpr (std::is_same_v<value_type, actor::address_t>) {
                    // For address_t, create a ready future with empty address
                    return make_ready_future<value_type>(resource, actor::address_t::empty_address());
                } else {
                    return make_ready_future<value_type>(resource);
                }
            }

            using methods = typename Actor::dispatch_traits::methods;

            return runtime_dispatch_helper<Actor, Method, methods>::dispatch(
                method, target, std::move(sender), std::forward<Args>(args)...);
        }

    } // namespace otterbrix

} // namespace actor_zeta
