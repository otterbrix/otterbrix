#pragma once

#include "metadata_diff.hpp"
#include <components/catalog/namespace_storage.hpp>
#include <components/catalog/table_metadata.hpp>

#include <map>

namespace components::catalog {
    class transaction_scope;

    class metadata_transaction {
    public:
        enum class State
        {
            ACTIVE,
            COMMITTED,
            ABORTED
        };

        metadata_transaction& add_column(const std::string& name,
                                         const components::types::complex_logical_type& type,
                                         bool required = false,
                                         const std::pmr::string& doc = "");
        metadata_transaction& delete_column(const std::string& name);
        metadata_transaction& rename_column(const std::string& name, const std::string& new_name);
        metadata_transaction& update_column_type(const std::string& name,
                                                 const components::types::complex_logical_type& new_type);
        metadata_transaction& update_column_doc(const std::string& name, const std::pmr::string& doc);
        metadata_transaction& make_optional(const std::string& name);
        metadata_transaction& make_required(const std::string& name);
        metadata_transaction& update_description(const std::pmr::string& desc);

        metadata_transaction& savepoint(const std::pmr::string& name);
        metadata_transaction& rollback_to_savepoint(const std::pmr::string& name);

        [[nodiscard]] State state() const;
        [[nodiscard]] const catalog_error& error() const;

    private:
        metadata_transaction(std::pmr::memory_resource* resource);
        metadata_transaction(std::pmr::memory_resource* resource, catalog_error error);

        bool ensure_active();

        template<typename F,
                 typename = std::enable_if_t<std::is_invocable_v<F, metadata_diff> &&
                                             std::is_same_v<std::invoke_result_t<F, metadata_diff>, catalog_error>>>
        catalog_error commit(F&& fun) {
            if (ensure_active()) {
                metadata_diff_.use_schema_diff(std::move(schema_diff_));

                if (metadata_diff_.has_changes()) {
                    error_ = fun(std::move(metadata_diff_));
                }

                state_ = static_cast<bool>(error_) ? State::ABORTED : State::COMMITTED;
            }
            return error_;
        }

        void abort();

        State state_ = State::ACTIVE;
        metadata_diff metadata_diff_;
        schema_diff schema_diff_;
        catalog_error error_;
        std::pmr::map<std::pmr::string, std::pair<metadata_diff, schema_diff>> savepoints_;

        friend class transaction_scope;
    };
} // namespace components::catalog
