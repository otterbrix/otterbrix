#pragma once

#include <cstdint>
#include <string>

namespace components::catalog {
    enum class catalog_mistake_t : uint8_t
    {
        NO_MISTAKE,
        DUPLICATE_COLUMN,
        FIELD_MISSING,
        MISSING_PRIMARY_KEY_ID,
        MISSING_NAMESPACE,
        ALREADY_EXISTS,
    };

    enum class transaction_mistake_t : uint8_t
    {
        NO_MISTAKE,
        TRANSACTION_INACTIVE,
        TRANSACTION_FINALIZED,
        MISSING_TABLE,
        MISSING_SAVEPOINT,
        COMMIT_FAILED,
    };

    class catalog_error {
    public:
        catalog_error();
        catalog_error(catalog_mistake_t mistake, std::string what = "");
        catalog_error(transaction_mistake_t mistake, std::string what = "");

        explicit operator bool() const;
        [[nodiscard]] catalog_mistake_t schema_mistake() const;
        [[nodiscard]] transaction_mistake_t transaction_mistake() const;
        [[nodiscard]] const std::string& what() const;

    private:
        catalog_mistake_t schema_mistake_;
        transaction_mistake_t transaction_mistake_;
        std::string what_;
    };

} // namespace components::catalog
