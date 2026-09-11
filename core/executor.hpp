#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/callable_trait.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/send.hpp>

#include <core/result_wrapper.hpp>

#include <cstdio>
#include <cstdlib>

namespace actor_zeta {

    using shared_work = scheduler::sharing_scheduler;
    using scheduler_ptr = std::unique_ptr<shared_work>;
    using scheduler_raw = shared_work*;

    namespace otterbrix {

        // A Release actor-zeta build compiles out its empty-address assert, so an unguarded send() SIGSEGVs silently.
        template<typename Method,
                 typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(actor::address_t target, Method method, Args&&... args)
            -> detail::send_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;

            static_assert(type_traits::is_unique_future_v<result_type>, "Method must return unique_future<T>");

            // Checked by signature, not identity: un-registering a same-signature method won't fail this static_assert.
            static_assert(detail::validate_method_for_send<Actor, Method>::valid,
                          "send(): Method validation failed - see above for details");

            if (!target) {
                // A default-valued ready future would disguise "nobody is listening" as valid, needing a null resource.
                std::fputs("actor_zeta::otterbrix::send: refusing to send to an empty target address\n", stderr);
                std::abort();
            }

            auto* actor = static_cast<Actor*>(target.get());
            using methods = typename Actor::dispatch_traits::methods;

            return runtime_dispatch_helper<Actor, Method, methods>::dispatch(method,
                                                                             actor,
                                                                             std::forward<Args>(args)...);
        }

        // Checks by identity: find_method_index returns 0 (a valid index) for "not found", risking silent misdispatch.
        template<auto SearchPtr, typename MethodList>
        struct contract_declares_method;

        template<auto SearchPtr, auto... MethodPtrs>
        struct contract_declares_method<SearchPtr, type_traits::type_list<method_map_entry<MethodPtrs>...>> {
            static constexpr bool value = (detail::is_same_ptr_v<SearchPtr, MethodPtrs> || ...);
        };

        // The message id is the method's position in dispatch_traits, fixed at compile time, not a runtime compare.
        template<auto MethodPtr,
                 typename... Args,
                 typename Interface = typename type_traits::callable_trait<decltype(MethodPtr)>::class_type>
        requires detail::is_interface<Interface>
        [[nodiscard]] inline auto send(actor::address_t target, Args&&... args)
            -> detail::send_result_t<Interface,
                                     typename type_traits::callable_trait<decltype(MethodPtr)>::result_type> {
            using result_type = typename type_traits::callable_trait<decltype(MethodPtr)>::result_type;
            static_assert(type_traits::is_unique_future_v<result_type>, "Method must return unique_future<T>");
            static_assert(
                contract_declares_method<MethodPtr, typename Interface::dispatch_traits::methods>::value,
                "send<&contract::method>(): the method is not in the contract's dispatch_traits list; "
                "the message id would silently resolve to the contract's first method");

            constexpr uint64_t action_id =
                action_id_impl<Interface, MethodPtr, typename Interface::dispatch_traits::methods>::value;

            return detail::dispatch_method_impl_address<Interface, MethodPtr, action_id>(
                std::move(target),
                std::forward<Args>(args)...);
        }

    } // namespace otterbrix

} // namespace actor_zeta
