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

        template<typename Method,
                 typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(actor::address_t target, Method method, Args&&... args)
            -> detail::send_result_t<Actor, typename type_traits::callable_trait<Method>::result_type> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;

            static_assert(type_traits::is_unique_future_v<result_type>, "Method must return unique_future<T>");

            if (!target) {
                // An empty target gets a LOUD refusal, in every build mode. A ready future
                // with a default value here is "nobody is listening" dressed as "answered, with
                // nothing" (the interface overload below spells out why that shape is
                // forbidden), and it cannot even deliver that: it would be built on
                // target.resource(), which is null for an empty address, so what arrives is
                // an assert-abort in Debug (make_ready_future's null-resource assert) and a
                // null memory_resource dereference (SIGSEGV) under NDEBUG. Every live call
                // site targets a spawned actor; the one contract an unreachable branch may
                // keep is to refuse deliberately, with a message, instead of via UB.
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

        // SEND THROUGH A CONTRACT, NAMING THE METHOD AT COMPILE TIME.
        //
        // Use this when one address may belong to any of several unrelated actor classes that
        // implement the same contract (actor_zeta::implements<>), so the caller has no class to
        // name: services::index::manager_index_t addressing an index agent, which is a
        // bitcask_index_agent_t or a btree_index_agent_t and it does not know which. The message
        // id is the method's POSITION in the contract's dispatch_traits list, and `implements<>`
        // is what guarantees every implementation agrees on it.
        //
        // WHY NOT actor_zeta::send(target, &contract::method, ...). The library's own
        // interface-polymorphic send takes the method pointer as a VALUE and compares it at
        // runtime against each entry of the contract's list (runtime_dispatch_helper_address).
        // That comparison ODR-USES the address of every contract method, so the linker demands a
        // BODY for each -- it turns a pure message vocabulary into a set of functions that exist
        // only to be never called. Naming the method as a template argument resolves the id
        // through the same positional rule (action_id_impl -> find_method_index, which compares
        // TYPES) at compile time, and asks nothing of the contract but its declarations.
        //
        // An empty target is refused here exactly like in the send() above: a ready future for
        // an empty address would turn "nobody is listening" into "answered, with nothing", and
        // every caller of this overload holds an address it got from a live agent.
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
