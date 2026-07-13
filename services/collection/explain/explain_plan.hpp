#pragma once

#include <chrono>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace services {
    struct context_storage_t;
} // namespace services

namespace services::collection {

    // Engine-neutral EXPLAIN IR node (one per physical operator). MOVE-ONLY: a std::pmr copy would
    // re-anchor to get_default_resource() (Rule 14), so nodes are built with `mr` and moved into
    // their parent's children.
    struct explain_plan_node {
        components::operators::operator_type type{components::operators::operator_type::unused};
        std::pmr::string relation; // scans only (resolved oid->name; empty = none)
        uint64_t rows{0};
        std::chrono::nanoseconds time{std::chrono::nanoseconds::zero()};
        uint64_t loops{0};
        std::pmr::vector<explain_plan_node> children; // in explain(sink) recursion order

        explicit explain_plan_node(std::pmr::memory_resource* mr)
            : relation(mr)
            , children(mr) {}
        explain_plan_node(const explain_plan_node&) = delete;            // a COPY re-anchors to
        explain_plan_node& operator=(const explain_plan_node&) = delete; // get_default_resource() (Rule 14)
        explain_plan_node(explain_plan_node&&) = default;
        explain_plan_node& operator=(explain_plan_node&&) = default;
    };

    using explain_name_map_t = std::pmr::unordered_map<components::catalog::oid_t, std::pmr::string>;

    // Pre-move pass: drive root->explain(collector.sink()) while context_storage is still alive, to
    // snapshot each scan's oid->name into `out`. Non-scan nodes (INVALID_OID) and metadata misses are
    // skipped; the IR builder's "t"+oid default names an unresolved scan oid.
    class explain_name_collector {
    public:
        explain_name_collector(const context_storage_t& cs, explain_name_map_t& out) noexcept
            : cs_(cs)
            , out_(out) {}
        components::operators::explain_sink sink() noexcept { return {&node_cb, &end_cb, this}; }

    private:
        void node(components::operators::operator_type,
                  components::catalog::oid_t oid,
                  uint64_t,
                  std::chrono::nanoseconds,
                  uint64_t);
        static void node_cb(void* c,
                            components::operators::operator_type t,
                            components::catalog::oid_t o,
                            uint64_t r,
                            std::chrono::nanoseconds ti,
                            uint64_t l);
        static void end_cb(void*) {}

        const context_storage_t& cs_;
        explain_name_map_t& out_;
    };

    // Post-execution pass: drive root->explain(builder.sink()) to build the IR from the pre-collected
    // name map + the ANALYZE counters the pipeline accumulated on the same instances. Tree assembly is
    // a stack of nodes BY VALUE (realloc-safe — no dangling pointers into child vectors): begin pushes
    // a filled node; end pops it into its parent's children, or into root_ when the stack empties.
    class explain_ir_builder {
    public:
        // `cs` (optional): when non-null the builder resolves scan oid->name straight from the LIVE
        // catalog (the plan-only EXPLAIN path, where context_storage is still alive), so the separate
        // pre-move name-collector walk is skipped. When null, names come from the pre-collected `names`
        // map (the ANALYZE path builds AFTER context_storage is moved).
        explain_ir_builder(std::pmr::memory_resource* mr,
                           const explain_name_map_t& names,
                           const context_storage_t* cs = nullptr)
            : mr_(mr)
            , names_(names)
            , cs_(cs)
            , stack_(mr)
            , root_(mr) {}
        components::operators::explain_sink sink() noexcept { return {&node_cb, &end_cb, this}; }
        explain_plan_node release() { return std::move(root_); }

    private:
        void node(components::operators::operator_type t,
                  components::catalog::oid_t oid,
                  uint64_t rows,
                  std::chrono::nanoseconds time,
                  uint64_t loops);
        void end();
        static void node_cb(void* c,
                            components::operators::operator_type t,
                            components::catalog::oid_t o,
                            uint64_t r,
                            std::chrono::nanoseconds ti,
                            uint64_t l);
        static void end_cb(void* c);

        std::pmr::memory_resource* mr_;
        const explain_name_map_t& names_;
        const context_storage_t* cs_;
        std::pmr::vector<explain_plan_node> stack_;
        explain_plan_node root_;
    };

} // namespace services::collection
