#include "node_limit.hpp"

#include <cassert>
#include <sstream>

namespace components::logical_plan {

    int64_t limit_t::count() const {
        assert(count_.has_value());
        return *count_;
    }

    core::error_t limit_t::validate(std::pmr::memory_resource* resource) const {
        if (offset_ < 0) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"OFFSET must not be negative", resource});
        }
        if (count_.has_value() && *count_ < 0) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"LIMIT must not be negative", resource});
        }
        return core::error_t::no_error();
    }

    row_cap_t limit_t::read_cap() const {
        if (!count_.has_value() || *count_ < 0 || offset_ < 0) {
            return row_cap_t::uncapped();
        }
        return row_cap_t(static_cast<uint64_t>(*count_) + static_cast<uint64_t>(offset_));
    }

    node_limit_t::node_limit_t(std::pmr::memory_resource* resource, const limit_t& limit)
        : node_t(resource, node_type::limit_t)
        , limit_(limit) {}

    const limit_t& node_limit_t::limit() const { return limit_; }

    hash_t node_limit_t::hash_impl() const { return 0; }

    std::string node_limit_t::to_string_impl() const {
        std::stringstream stream;
        // LIMIT ALL shows nothing, as PostgreSQL shows no Limit node for it; a written -1 prints as written.
        bool is_first = true;
        if (limit_.has_count()) {
            stream << "$limit: " << limit_.count();
            is_first = false;
        }
        if (limit_.offset() != 0) {
            stream << (is_first ? "" : ", ") << "$offset: " << limit_.offset();
        }
        return stream.str();
    }

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource, const limit_t& limit) {
        return {new node_limit_t{resource, limit}};
    }

} // namespace components::logical_plan
