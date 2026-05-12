#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class limit_t {
        static constexpr int unlimit_ = -1;

    public:
        limit_t() = default;
        explicit limit_t(int data);

        static limit_t unlimit();
        static limit_t limit_one();

        int limit() const;
        bool check(int count) const;

    private:
        int limit_ = unlimit_;
    };

    class node_limit_t final : public node_t {
    public:
        explicit node_limit_t(std::pmr::memory_resource* resource,
                              std::string dbname,
                              std::string relname,
                              const limit_t& limit);

        const limit_t& limit() const;

        // Phase 9.W/10.D: role-named accessors. Carries source table identity for parser-window.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        limit_t limit_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_limit_ptr = boost::intrusive_ptr<node_limit_t>;

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource,
                                   std::string dbname,
                                   std::string relname,
                                   const limit_t& limit);

} // namespace components::logical_plan
