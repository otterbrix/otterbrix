#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Physical operator that resolves a function by
    // (pronamespace, proname) against pg_proc.
    //
    // Mirrors services::disk::manager_disk_t::resolve_function but lives in
    // the operator pipeline. The operator drives a read_rows_by_key actor
    // send keyed on {"proname", "pronamespace"} and materialises matching
    // pg_proc rows into output_.
    //
    // Output chunk schema (matches pg_proc column order):
    //   col 0: oid             UINTEGER  — pg_proc.oid (function oid)
    //   col 1: proname         STRING
    //   col 2: pronamespace    UINTEGER
    //   col 3: pronargs        INTEGER   — arity of the first signature
    //   col 4: prouid          BIGINT    — registered function_uid
    //   col 5: proargmatchers  STRING    — encoded per-arg input matchers
    //   col 6: prorettype      STRING    — encoded output-type list
    //
    // Cardinality of output_ equals the number of pg_proc rows matched. The
    // operator emits an empty chunk when no rows match (preserving schema so
    // downstream consumers can introspect columns).
    class operator_resolve_function_t final : public read_write_operator_t {
    public:
        operator_resolve_function_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    components::catalog::oid_t namespace_oid,
                                    std::string name);

        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t namespace_oid_;
        std::string name_;
    };

} // namespace components::operators
