#pragma once

// Block B Parallel A.B3: typed result for manager_index commit_insert /
// commit_delete. Today bitcask write-path methods are assert+abort terminal
// (Block A.B1), so the returned result_t will always carry success — this is
// boundary plumbing that prepares the API for the day when core::error_t
// propagation lands. Once that happens, no caller signature has to change a
// second time.
//
// Rule 14 compliance: no std::variant, no std::shared_ptr, no std::any, no
// std::function. result_t is a simple POD-ish struct: a bool ok flag plus a
// core::error_t payload.

#include <core/result_wrapper.hpp>

#include <utility>

namespace services::index {

    struct result_t {
        bool ok{true};
        core::error_t error{core::error_t::no_error()};

        [[nodiscard]] bool failed() const noexcept { return !ok; }

        static result_t success() noexcept { return {}; }
        static result_t failure(core::error_t e) noexcept {
            return {false, std::move(e)};
        }
    };

} // namespace services::index
