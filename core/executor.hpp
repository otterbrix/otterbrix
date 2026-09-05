#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/callable_trait.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/send.hpp>

#include <core/result_wrapper.hpp>

namespace actor_zeta {

    using shared_work = scheduler::sharing_scheduler;
    using scheduler_ptr = std::unique_ptr<shared_work>;
    using scheduler_raw = shared_work*;

    namespace otterbrix {

        template<typename Method,
                 typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(actor::address_t target, Method method, Args&&... args)
            -> detail::send_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;

            static_assert(type_traits::is_unique_future_v<result_type>, "Method must return unique_future<T>");

            if (!target) {
                auto* resource = target.resource();
                if constexpr (std::is_void_v<value_type>) {
                    return {false, make_ready_future(resource)};
                } else if constexpr (std::is_same_v<value_type, actor::address_t>) {
                    return {false, make_ready_future<value_type>(resource, actor::address_t::empty_address())};
                } else if constexpr (std::is_same_v<value_type, core::error_t>) {
                    // core::error_t is intentionally not default-constructible (its no-error
                    // state is only reachable via no_error()); the null-target branch yields a
                    // ready future carrying the no-error sentinel.
                    return {false, make_ready_future<value_type>(resource, core::error_t::no_error())};
                } else if constexpr (std::is_constructible_v<value_type, std::pmr::memory_resource*> &&
                                     !std::is_convertible_v<std::pmr::memory_resource*, value_type>) {
                    return {false, make_ready_future<value_type>(resource, value_type{resource})};
                } else {
                    return {false, make_ready_future<value_type>(resource)};
                }
            }

            auto* actor = static_cast<Actor*>(target.get());
            using methods = typename Actor::dispatch_traits::methods;

            return runtime_dispatch_helper<Actor, Method, methods>::dispatch(method,
                                                                             actor,
                                                                             std::forward<Args>(args)...);
        }

        // Is this exact method pointer one of the contract's registered methods? Asked by
        // IDENTITY, not by signature: actor_zeta::detail::find_method_index answers 0 for a
        // method it cannot find, which would silently address the contract's FIRST handler,
        // and method_signature_exists_v cannot tell two same-shaped methods apart. Purely
        // type-level, so it takes no addresses.
        template<auto SearchPtr, typename MethodList>
        struct contract_declares_method;

        template<auto SearchPtr, auto... MethodPtrs>
        struct contract_declares_method<SearchPtr, type_traits::type_list<method_map_entry<MethodPtrs>...>> {
            static constexpr bool value = (detail::is_same_ptr_v<SearchPtr, MethodPtrs> || ...);
        };

        // SEND THROUGH A CONTRACT, NAMING THE METHOD AT COMPILE TIME.
        //
        // Use this when one address may belong to any of several unrelated actor classes
        // that implement the same contract (actor_zeta::implements<>), so the caller has
        // no class to name: services::index::manager_index_t addressing an index agent,
        // which is a bitcask_index_agent_t or a btree_index_agent_t and it does not know
        // which. The message id is the method's POSITION in the contract's dispatch_traits
        // list, and `implements<>` is what guarantees every implementation agrees on it.
        //
        // WHY NOT actor_zeta::send(target, &contract::method, ...). The library's own
        // interface-polymorphic send takes the method pointer as a VALUE and then compares
        // it at runtime against each entry of the contract's list
        // (runtime_dispatch_helper_address). That comparison ODR-USES the address of every
        // contract method, so the linker demands a BODY for each -- it turns a pure
        // message vocabulary into a set of functions that exist only to be never called.
        // Naming the method as a template argument resolves the id through the same
        // positional rule (action_id_impl -> find_method_index, which compares TYPES) at
        // compile time, and asks nothing of the contract but its declarations.
        //
        // The empty-target shorthand of the send() above is deliberately NOT repeated
        // here: every caller of this overload holds an address it got from a live agent,
        // and a ready future for an empty one would turn "nobody is listening" into
        // "answered, with nothing". actor-zeta asserts on the empty address instead.
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
