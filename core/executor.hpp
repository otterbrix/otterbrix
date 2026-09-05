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

        // The only way this tree sends to an address; a bare actor_zeta::send(address, ...)
        // outside this file is a regression. The library's own overload guards the same
        // precondition with an assert, which a Release actor-zeta PACKAGE compiles out
        // regardless of how we build -- an empty address then walks a null resource into
        // address.ipp's enqueue_fn_ (SIGSEGV, no message). fputs+abort here is identical in
        // Debug/Release and whichever package variant is linked.
        //
        // Does not make an unguarded send correct: sites that legitimately hold a possibly-empty
        // address (a WAL manager never spawned) still need their own `!= empty_address()` check.
        template<typename Method,
                 typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(actor::address_t target, Method method, Args&&... args)
            -> detail::send_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;

            static_assert(type_traits::is_unique_future_v<result_type>, "Method must return unique_future<T>");

            // Kept from the library's send(): the method must be registered in
            // Actor::dispatch_traits and belong to Actor. Checked by SIGNATURE
            // (method_signature_exists_v), not identity, so two registered methods sharing a
            // signature are indistinguishable to it -- un-registering one won't fail this
            // static_assert; runtime_dispatch_helper's terminal abort catches it instead, loud
            // in every build but not from the compiler.
            static_assert(detail::validate_method_for_send<Actor, Method>::valid,
                          "send(): Method validation failed - see above for details");

            if (!target) {
                // A ready future with a default value would dress "nobody is listening" as
                // "answered, with nothing" -- and couldn't even do that, since it would build on
                // target.resource(), null for an empty address (assert-abort in Debug, SIGSEGV
                // under NDEBUG). Every live call site targets a spawned actor, so this branch
                // only needs to refuse cleanly instead of via UB.
                std::fputs("actor_zeta::otterbrix::send: refusing to send to an empty target address\n", stderr);
                std::abort();
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

        // Send through a contract, naming the method at compile time -- for when one address may
        // belong to any of several actor classes implementing the same contract (implements<>)
        // and the caller has no concrete class to name (manager_index_t addressing either a
        // bitcask or a btree index agent). The message id is the method's POSITION in the
        // contract's dispatch_traits list.
        //
        // Not actor_zeta::send(target, &contract::method, ...): the library's runtime-polymorphic
        // send compares the method pointer by VALUE against every contract entry at runtime,
        // which ODR-uses each one and forces the linker to demand a body nothing ever calls.
        // Naming the method as a template argument resolves the same positional id at compile
        // time instead, asking nothing of the contract but its declarations.
        //
        // Empty target refused exactly like send() above; every caller here holds an address
        // from a live agent.
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
