#pragma once

#include "metadata_transaction.hpp"
#include "transaction_list.hpp"

#include <memory>
#include <optional>

namespace components::catalog {
    class catalog;

    class transaction_scope {
    public:
        transaction_scope(std::pmr::memory_resource* resource,
                          std::weak_ptr<transaction_list> transactions,
                          const table_id& id,
                          namespace_storage* ns_storage);

        transaction_scope(std::pmr::memory_resource* resource, catalog_error error);

        ~transaction_scope();

        transaction_scope(const transaction_scope&) = delete;
        transaction_scope(transaction_scope&& other) noexcept;

        transaction_scope& operator=(const transaction_scope&) = delete;
        transaction_scope& operator=(transaction_scope&& other) noexcept;

        [[nodiscard]] metadata_transaction& transaction();
        [[nodiscard]] const catalog_error& error() const;

        void commit();
        void abort();

    private:
        bool is_committed_ = false;
        bool is_aborted_ = false;
        table_id id_;
        catalog_error error_;
        std::weak_ptr<transaction_list> transaction_list_;
        namespace_storage* ns_storage_ptr_;
        std::unique_ptr<metadata_transaction> transaction_;
    };
} // namespace components::catalog
