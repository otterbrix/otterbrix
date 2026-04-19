#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class limit_t {
        static constexpr int64_t unlimit_ = -1;

    public:
        limit_t() = default;
        explicit limit_t(int64_t limit, int64_t offset = 0);

        static limit_t unlimit();
        static limit_t limit_one();

        int64_t limit() const;
        int64_t offset() const;
        bool check(int64_t count) const;
        bool is_skipping(int64_t count) const;

    private:
        int64_t limit_ = unlimit_;
        int64_t offset_ = 0;
    };

    class node_limit_t final : public node_t {
    public:
        explicit node_limit_t(std::pmr::memory_resource* resource,
                              const collection_full_name_t& collection,
                              const limit_t& limit);

        const limit_t& limit() const;

    private:
        limit_t limit_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_limit_ptr = boost::intrusive_ptr<node_limit_t>;

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const limit_t& limit);

} // namespace components::logical_plan