#pragma once

#include "wrapper_dispatcher.hpp"

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>
#include <services/engine/engine.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <memory>

namespace otterbrix {

    // The memory the engine allocates from: what a host keeps of the engine's answers holds it, not the engine.
    struct engine_arena_t final : boost::intrusive_ref_counter<engine_arena_t> {
        ~engine_arena_t();
        core::pmr::otterbrix_resource resource;
        log_t log;
    };

    using engine_arena_ptr = boost::intrusive_ptr<engine_arena_t>;

    // A complete host over services::engine: owns the memory resource, the logger, the three pools,
    // the engine and the blocking wrapper_dispatcher_t.
    class base_otterbrix_t {
    public:
        struct host_t;
        struct host_deleter_t final {
            void operator()(host_t* host) const noexcept;
        };
        using host_ptr = std::unique_ptr<host_t, host_deleter_t>;

        // A refused start answers the error; its message lives on new_delete_resource, since the
        // engine's own arena is gone by the time the caller reads it.
        [[nodiscard]] static core::result_wrapper_t<host_ptr> open(const configuration::config& config,
                                                                   components::planner::primitives_t primitives = {});

        base_otterbrix_t(const base_otterbrix_t&) = delete;
        base_otterbrix_t& operator=(const base_otterbrix_t&) = delete;
        ~base_otterbrix_t();

        log_t& get_log();
        wrapper_dispatcher_t* dispatcher();
        const engine_arena_ptr& arena() const noexcept;

    protected:
        explicit base_otterbrix_t(host_ptr host);
        const services::engine::engine_t& engine() const;

    private:
        host_ptr host_;
    };

} // namespace otterbrix
