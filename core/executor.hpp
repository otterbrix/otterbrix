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

        template<typename Method, typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(
            actor::address_t target,
            Method method,
            Args&&... args
        ) -> detail::send_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;

            static_assert(
                type_traits::is_unique_future_v<result_type>,
                "Method must return unique_future<T>");

            if (!target) {
                auto* resource = target.resource();
                if constexpr (std::is_void_v<value_type>) {
                    return {false, make_ready_future(resource)};
                } else if constexpr (std::is_same_v<value_type, actor::address_t>) {
                    return {false, make_ready_future<value_type>(resource, actor::address_t::empty_address())};
                } else {
                    return {false, make_ready_future<value_type>(resource)};
                }
            }

            auto* actor = static_cast<Actor*>(target.get());
            using methods = typename Actor::dispatch_traits::methods;

            return runtime_dispatch_helper<Actor, Method, methods>::dispatch(
                method, actor, std::forward<Args>(args)...);
        }

    } // namespace otterbrix

} // namespace actor_zeta
