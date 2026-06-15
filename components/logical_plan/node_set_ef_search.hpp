#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // SET hnsw.ef_search = N (0 = reset to default).
    class node_set_ef_search_t final : public node_t {
    public:
        node_set_ef_search_t(std::pmr::memory_resource* resource, std::size_t ef_search);

        std::size_t ef_search() const noexcept { return ef_search_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::size_t ef_search_;
    };

    using node_set_ef_search_ptr = boost::intrusive_ptr<node_set_ef_search_t>;
    node_set_ef_search_ptr make_node_set_ef_search(std::pmr::memory_resource* resource, std::size_t ef_search);

} // namespace components::logical_plan
