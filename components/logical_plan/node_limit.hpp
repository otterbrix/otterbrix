#pragma once

#include "node.hpp"

#include <core/result_wrapper.hpp>

#include <cstdint>
#include <limits>
#include <optional>

namespace components::logical_plan {

    // How many rows a source or a window may take. The physical form of a limit: it is only ever built from a
    // limit_t::read_cap(), which has refused nothing yet, so it holds no invalid state. The one place that speaks
    // the disk protocol's "-1 = no cap" is to_disk().
    class row_cap_t {
    public:
        row_cap_t() = default;
        explicit row_cap_t(uint64_t rows)
            : rows_(rows) {}

        static row_cap_t uncapped() { return {}; }

        bool is_capped() const { return rows_ != std::numeric_limits<uint64_t>::max(); }
        // May the row that comes after `seen` rows be taken?
        bool admits(uint64_t seen) const { return seen < rows_; }
        uint64_t rows() const { return rows_; }

        int64_t to_disk() const {
            if (!is_capped()) {
                return -1;
            }
            return rows_ > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())
                       ? std::numeric_limits<int64_t>::max()
                       : static_cast<int64_t>(rows_);
        }

    private:
        uint64_t rows_ = std::numeric_limits<uint64_t>::max();
    };

    // LIMIT/OFFSET as written, NOT validated: PostgreSQL checks the values when the Limit node starts to run
    // (nodeLimit.c recompute_limits), so a negative count or offset lives in the plan until operator_limit refuses it.
    // No count is LIMIT ALL / LIMIT NULL.
    class limit_t {
    public:
        limit_t() = default;
        explicit limit_t(int64_t count, int64_t offset = 0)
            : count_(count)
            , offset_(offset) {}

        static limit_t all() { return {}; }
        static limit_t all_from(int64_t offset) {
            limit_t limit;
            limit.offset_ = offset;
            return limit;
        }
        static limit_t limit_one() { return limit_t(1); }

        bool has_count() const { return count_.has_value(); }
        int64_t count() const;
        int64_t offset() const { return offset_; }

        // A window that changes nothing needs no Limit node (PostgreSQL limit_needed). A negative one is a window:
        // the node has to run to refuse it.
        bool is_effective() const { return has_count() || offset_ != 0; }

        // OFFSET is checked first, as nodeLimit.c does. The texts are PostgreSQL's.
        core::error_t validate(std::pmr::memory_resource* resource) const;

        // What a source has to produce for the window [offset, offset + count) to be filled. A window that
        // validate() would refuse caps nothing.
        row_cap_t read_cap() const;

    private:
        std::optional<int64_t> count_;
        int64_t offset_ = 0;
    };

    class node_limit_t final : public node_t {
    public:
        explicit node_limit_t(std::pmr::memory_resource* resource, const limit_t& limit);

        const limit_t& limit() const;
        void set_limit(const limit_t& limit) { limit_ = limit; }

    private:
        limit_t limit_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_limit_ptr = boost::intrusive_ptr<node_limit_t>;

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource, const limit_t& limit);

} // namespace components::logical_plan
