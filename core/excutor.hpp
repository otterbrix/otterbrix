#pragma once

// Suppress GCC false positive -Wmaybe-uninitialized in actor-zeta future_state.hpp
// See docs/gcc-maybe-uninitialized-false-positive.md for details
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/callable_trait.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/mailbox/make_message.hpp>
#include <actor-zeta/mailbox/id.hpp>

#pragma GCC diagnostic pop

namespace actor_zeta {

    using shared_work = scheduler::sharing_scheduler;
    using scheduler_ptr = std::unique_ptr<shared_work>;
    using scheduler_raw = shared_work*;

    namespace otterbrix {

        namespace detail {

            // dispatch_method_impl_sync - uses enqueue_sync_impl for actor_mixin actors
            template<typename Actor, auto MethodPtr, uint64_t ActionId, typename Sender, typename... Args>
            inline auto dispatch_method_impl_sync(Actor* actor, Sender sender, Args&&... args)
                -> typename Actor::template unique_future<
                    typename type_traits::is_unique_future<
                        typename type_traits::callable_trait<decltype(MethodPtr)>::result_type
                    >::value_type
                > {
                using callable_trait = type_traits::callable_trait<decltype(MethodPtr)>;
                using method_result_type = typename callable_trait::result_type;
                using value_type = typename type_traits::is_unique_future<method_result_type>::value_type;

                return actor->template enqueue_sync_impl<value_type>(
                    std::move(sender),
                    mailbox::make_message_id(ActionId),
                    [actor](mailbox::message* msg) { actor->behavior(msg); },
                    std::forward<Args>(args)...);
            }

        } // namespace detail

        // dispatch_one_impl_sync - similar to actor-zeta's dispatch_one_impl but for sync
        template<typename Actor, typename Method, typename Sender, typename... Args>
        struct dispatch_one_impl_sync {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;
            using dispatch_return_type = typename Actor::template unique_future<value_type>;

            template<std::size_t... Is>
            static auto dispatch(Method, Actor*, Sender, std::index_sequence<>, Args&&...)
                -> dispatch_return_type {
                assert(false && "Method not found in dispatch_traits");
                std::abort();
            }

            template<auto FirstMethod, auto... RestMethods, std::size_t FirstIndex, std::size_t... RestIndices>
            static auto dispatch(Method method, Actor* actor, Sender sender,
                                 std::index_sequence<FirstIndex, RestIndices...>, Args&&... args)
                -> dispatch_return_type {
                if constexpr (std::same_as<Method, decltype(FirstMethod)>) {
                    if (method == FirstMethod) {
                        return detail::dispatch_method_impl_sync<Actor, FirstMethod, FirstIndex>(
                            actor, std::move(sender), std::forward<Args>(args)...);
                    }
                }

                return dispatch<RestMethods...>(
                    method, actor, std::move(sender),
                    std::index_sequence<RestIndices...>{},
                    std::forward<Args>(args)...);
            }
        };

        template<typename Actor, typename Method, auto... MethodPtrs, typename Sender, typename... Args, std::size_t... Is>
        static auto dispatch_impl_sync(Method method, Actor* actor, Sender sender, std::index_sequence<Is...>, Args&&... args)
            -> typename Actor::template unique_future<
                typename type_traits::is_unique_future<
                    typename type_traits::callable_trait<Method>::result_type
                >::value_type
            > {
            return dispatch_one_impl_sync<Actor, Method, Sender, Args...>::template dispatch<MethodPtrs...>(
                method, actor, std::move(sender),
                std::index_sequence<Is...>{},
                std::forward<Args>(args)...);
        }

        // runtime_dispatch_helper_sync - like runtime_dispatch_helper but for sync dispatch
        template<typename Actor, typename Method, typename MethodList>
        struct runtime_dispatch_helper_sync;

        template<typename Actor, typename Method>
        struct runtime_dispatch_helper_sync<Actor, Method, type_traits::type_list<>> {
            template<typename Sender, typename... Args>
            static auto dispatch(Method, Actor*, Sender, Args&&...)
                -> typename Actor::template unique_future<
                    typename type_traits::is_unique_future<
                        typename type_traits::callable_trait<Method>::result_type
                    >::value_type
                > {
                assert(false && "Method not found in dispatch_traits");
                std::abort();
            }
        };

        template<typename Actor, typename Method, auto... MethodPtrs>
        struct runtime_dispatch_helper_sync<Actor, Method, type_traits::type_list<method_map_entry<MethodPtrs>...>> {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;
            using dispatch_return_type = typename Actor::template unique_future<value_type>;

            template<typename Sender, typename... Args>
            static auto dispatch(Method method, Actor* actor, Sender sender, Args&&... args)
                -> dispatch_return_type {
                return dispatch_impl_sync<Actor, Method, MethodPtrs...>(
                    method, actor, std::move(sender),
                    std::index_sequence_for<method_map_entry<MethodPtrs>...>{},
                    std::forward<Args>(args)...);
            }

            template<typename Sender, typename... Args>
            static auto dispatch(Method method, actor::address_t target, Sender sender, Args&&... args)
                -> dispatch_return_type {
                auto* actor = static_cast<Actor*>(target.get());
                return dispatch_impl_sync<Actor, Method, MethodPtrs...>(
                    method, actor, std::move(sender),
                    std::index_sequence_for<method_map_entry<MethodPtrs>...>{},
                    std::forward<Args>(args)...);
            }
        };

        // Helper to create a ready future for empty target addresses
        // Used when an optional service (like disk) is disabled
        template<typename T>
        [[nodiscard]] inline auto make_ready_future() -> unique_future<T> {
            auto* resource = std::pmr::get_default_resource();
            promise<T> p(resource);
            if constexpr (std::is_void_v<T>) {
                p.set_value();
            } else if constexpr (std::is_same_v<T, actor::address_t>) {
                p.set_value(actor::address_t::empty_address());
            } else {
                p.set_value(T{});
            }
            return p.get_future();
        }

        // Custom send for otterbrix actors that use actor_mixin
        // Uses enqueue_sync_impl which calls behavior synchronously
        // If target is empty, returns a ready no-op future (for disabled services like disk)
        template<typename Sender, typename Method, typename... Args,
                 typename Actor = typename type_traits::callable_trait<Method>::class_type>
        [[nodiscard]] inline auto send(
            actor::address_t target,
            Sender sender,
            Method method,
            Args&&... args
        ) -> typename Actor::template unique_future<
            typename type_traits::is_unique_future<
                typename type_traits::callable_trait<Method>::result_type
            >::value_type
        > {
            using result_type = typename type_traits::callable_trait<Method>::result_type;
            using value_type = typename type_traits::is_unique_future<result_type>::value_type;

            static_assert(
                type_traits::is_unique_future_v<result_type>,
                "Method must return unique_future<T>");

            // If target is empty (e.g., disk manager when disk is disabled),
            // return a ready no-op future instead of crashing
            if (!target) {
                return make_ready_future<value_type>();
            }

            using methods = typename Actor::dispatch_traits::methods;

            return runtime_dispatch_helper_sync<Actor, Method, methods>::dispatch(
                method, target, std::move(sender), std::forward<Args>(args)...);
        }

    } // namespace otterbrix

} // namespace actor_zeta