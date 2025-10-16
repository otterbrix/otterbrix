#include "metadata_transaction.hpp"

namespace components::catalog {
    metadata_transaction::metadata_transaction(std::pmr::memory_resource* resource)
        : metadata_diff_(resource)
        , schema_diff_(resource)
        , error_()
        , savepoints_(resource) {}

    metadata_transaction::metadata_transaction(std::pmr::memory_resource* resource,
                                               components::catalog::catalog_error error)
        : metadata_diff_(resource)
        , schema_diff_(resource)
        , error_(std::move(error))
        , savepoints_(resource) {}

    bool metadata_transaction::ensure_active() {
        if (static_cast<bool>(error_)) {
            return false;
        }

        if (state_ != State::ACTIVE) {
            error_ = catalog_error(transaction_mistake_t::TRANSACTION_INACTIVE, "Transaction is not active");
            return false;
        }

        return true;
    }

    metadata_transaction& metadata_transaction::add_column(const std::string& name,
                                                           const components::types::complex_logical_type& type,
                                                           bool required,
                                                           const std::pmr::string& doc) {
        if (ensure_active()) {
            schema_diff_.add_column(name, type, required, doc);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::delete_column(const std::string& name) {
        if (ensure_active()) {
            schema_diff_.delete_column(name);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::rename_column(const std::string& name, const std::string& new_name) {
        if (ensure_active()) {
            schema_diff_.rename_column(name, new_name);
        }
        return *this;
    }

    metadata_transaction&
    metadata_transaction::update_column_type(const std::string& name,
                                             const components::types::complex_logical_type& new_type) {
        if (ensure_active()) {
            schema_diff_.update_column_type(name, new_type);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::update_column_doc(const std::string& name,
                                                                  const std::pmr::string& doc) {
        if (ensure_active()) {
            schema_diff_.update_column_doc(name, doc);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::make_optional(const std::string& name) {
        if (ensure_active()) {
            schema_diff_.make_optional(name);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::make_required(const std::string& name) {
        if (ensure_active()) {
            schema_diff_.make_required(name);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::update_description(const std::pmr::string& desc) {
        if (ensure_active()) {
            metadata_diff_.update_description(desc);
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::savepoint(const std::pmr::string& name) {
        if (ensure_active()) {
            savepoints_.emplace(name, std::make_pair(metadata_diff_, schema_diff_));
        }
        return *this;
    }

    metadata_transaction& metadata_transaction::rollback_to_savepoint(const std::pmr::string& name) {
        if (ensure_active()) {
            auto it = savepoints_.find(name);
            if (it == savepoints_.end()) {
                error_ = catalog_error(transaction_mistake_t::MISSING_SAVEPOINT, "Savepoint not found");
                return *this;
            }

            metadata_diff_ = it->second.first;
            schema_diff_ = it->second.second;
        }
        return *this;
    }

    metadata_transaction::State metadata_transaction::state() const { return state_; }

    const catalog_error& metadata_transaction::error() const { return error_; }

    void metadata_transaction::abort() {
        if (state_ == State::ACTIVE) {
            state_ = State::ABORTED;
        }
    }
} // namespace components::catalog
