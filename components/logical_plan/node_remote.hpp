#pragma once

#include "node.hpp"

#include <components/table/storage_address.hpp>

#include <memory_resource>
#include <vector>

namespace components::logical_plan {

    // A part of the plan a backend took when otterbrix offered it a block (storage_offer): the backend runs `request`
    // and otterbrix reads its rows (a read) or its receipt (a write: the rows changed, and their values for
    // RETURNING, which otterbrix computes). It stands where the taken node stood; a read's output_types() are the
    // taken node's.
    class node_remote_t final : public node_t {
    public:
        enum class kind_t : uint8_t
        {
            read,
            write
        };

        node_remote_t(std::pmr::memory_resource* resource,
                      kind_t kind,
                      table::storage_address_t address,
                      std::pmr::string request,
                      std::pmr::vector<std::pmr::string> explain);

        kind_t kind() const noexcept { return kind_; }
        const table::storage_address_t& address() const noexcept { return address_; }
        const std::pmr::string& request() const noexcept { return request_; }
        // The lines EXPLAIN prints under the node, as the backend wrote them.
        const std::pmr::vector<std::pmr::string>& explain() const noexcept { return explain_; }
        // The tables of the block: the backend keeps their part of the transaction.
        std::pmr::vector<catalog::oid_t>& tables() noexcept { return tables_; }
        const std::pmr::vector<catalog::oid_t>& tables() const noexcept { return tables_; }
        // A write's RETURNING, over the written table's columns.
        std::pmr::vector<expressions::expression_ptr>& returning() noexcept { return returning_; }
        const std::pmr::vector<expressions::expression_ptr>& returning() const noexcept { return returning_; }
        // A taken DELETE's scan columns: its RETURNING is bound to them.
        std::vector<size_t>& scan_columns() noexcept { return scan_columns_; }
        const std::vector<size_t>& scan_columns() const noexcept { return scan_columns_; }

    private:
        kind_t kind_;
        table::storage_address_t address_;
        std::pmr::string request_;
        std::pmr::vector<std::pmr::string> explain_;
        std::pmr::vector<catalog::oid_t> tables_;
        std::pmr::vector<expressions::expression_ptr> returning_;
        std::vector<size_t> scan_columns_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_remote_ptr = boost::intrusive_ptr<node_remote_t>;

} // namespace components::logical_plan
