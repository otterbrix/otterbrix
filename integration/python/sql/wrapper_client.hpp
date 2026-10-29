#pragma once

#include <memory>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <components/log/log.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "forward.hpp"
#include "integration/cpp/wrapper_dispatcher.hpp"
#include "spaces.hpp"
#include "wrapper_cursor.hpp"

namespace py = pybind11;
namespace otterbrix {
    class PYBIND11_EXPORT wrapper_client final : public boost::intrusive_ref_counter<wrapper_client> {
    public:
        wrapper_client(spaces_ptr space);
        ~wrapper_client();
        auto execute(const std::string& query) -> wrapper_cursor_ptr;
        // Runs in the client's session; once the client is closed it answers connection_closed.
        auto execute_sql(const std::string& query) -> components::cursor::cursor_t_ptr;
        // Ends the session and lets the engine go; a second call does nothing.
        void close();
        const engine_arena_ptr& arena() const noexcept { return arena_; }

    private:
        const std::string name_;
        spaces_ptr ptr_;
        const engine_arena_ptr arena_;
        const session_id_t session_{};
        bool closed_{false};
        mutable log_t log_;
    };

    using wrapper_client_ptr = boost::intrusive_ptr<wrapper_client>;
} // namespace otterbrix