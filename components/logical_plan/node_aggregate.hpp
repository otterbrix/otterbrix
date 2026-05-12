#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        // Phase 10.D: ctor takes role-named strings instead of cfn struct.
        explicit node_aggregate_t(std::pmr::memory_resource* resource, std::string dbname, std::string relname);

        void set_distinct(bool d) { distinct_ = d; }
        bool is_distinct() const { return distinct_; }

        // Phase 9.W/10.D: role-named accessors. The aggregate node carries the
        // source table identity through the parser-window for downstream
        // operator dispatch; routing in resolved-stage code uses table_oid().
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        bool distinct_{false};
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           std::string dbname,
                                           std::string relname);

} // namespace components::logical_plan
