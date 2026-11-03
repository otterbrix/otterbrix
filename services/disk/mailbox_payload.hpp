#pragma once

#include <actor-zeta/detail/callable_trait.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <components/execution_dag/execution_dag.hpp>
#include <components/logical_plan/detached_block.hpp>
#include <components/table/column_state.hpp>

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

// What may ride a message to a disk agent: values the receiver owns outright. Not a built graph (it
// holds the sender's function pointers and memory), not a refcounted tree the sender still points
// into, not a raw pointer.
namespace services::disk::mailbox {

    template<class T>
    struct owns_its_payload : std::true_type {};

    template<class T>
    struct owns_its_payload<T*> : std::false_type {};

    template<class T>
    struct owns_its_payload<boost::intrusive_ptr<T>> : std::false_type {};

    template<class T, class D>
    struct owns_its_payload<std::unique_ptr<T, D>> : owns_its_payload<T> {};

    template<class T, class A>
    struct owns_its_payload<std::vector<T, A>> : owns_its_payload<T> {};

    template<>
    struct owns_its_payload<components::execution_dag::execution_dag_t> : std::false_type {};

    template<>
    struct owns_its_payload<components::table::table_filter_t> : std::false_type {};

    // A block's nodes are an intrusive tree, yet detach_block made them on the receiver's resource and nothing of
    // the sender points into them (the precedent: detached_expression_t).
    template<>
    struct owns_its_payload<components::logical_plan::detached_block_t> : std::true_type {};

    template<class List>
    struct arguments_own_their_payload;

    template<class... Args>
    struct arguments_own_their_payload<actor_zeta::type_traits::type_list<Args...>>
        : std::conjunction<owns_its_payload<std::decay_t<Args>>...> {};

    template<auto Method>
    inline constexpr bool owns_its_arguments = arguments_own_their_payload<
        typename actor_zeta::type_traits::callable_trait<decltype(Method)>::args_types>::value;

} // namespace services::disk::mailbox
