#include "transaction_scope.hpp"

namespace components::catalog {
    transaction_scope::transaction_scope(std::pmr::memory_resource* resource,
                                         std::weak_ptr<transaction_list> transactions,
                                         const table_id& id,
                                         namespace_storage* ns_storage)

        : id_(id)
        , error_(core::error_t::no_error())
        , transaction_list_(transactions)
        , ns_storage_ptr_(ns_storage)
        , resource_(resource) {
        transaction_ = std::unique_ptr<metadata_transaction>(new metadata_transaction(resource));
    }

    transaction_scope::transaction_scope(std::pmr::memory_resource* resource, core::error_t error)
        : id_(table_id(resource, table_namespace_t{}, ""))
        , error_(std::move(error))
        , transaction_list_()
        , ns_storage_ptr_(nullptr)
        , resource_(resource) {
        transaction_ = std::unique_ptr<metadata_transaction>(new metadata_transaction(resource, error_));
    }

    transaction_scope::~transaction_scope() {
        if (!is_committed_ && !is_aborted_) {
            abort();
        }
    }

    transaction_scope::transaction_scope(transaction_scope&& other) noexcept
        : is_committed_(other.is_committed_)
        , is_aborted_(other.is_aborted_)
        , id_(std::move(other.id_))
        , error_(std::move(other.error_))
        , transaction_list_(std::move(other.transaction_list_))
        , ns_storage_ptr_(other.ns_storage_ptr_)
        , transaction_(std::move(other.transaction_)) {
        other.is_aborted_ = true;
    }

    transaction_scope& transaction_scope::operator=(transaction_scope&& other) noexcept {
        if (this != &other) {
            if (!is_committed_ && !is_aborted_) {
                abort();
            }

            is_committed_ = other.is_committed_;
            is_aborted_ = other.is_aborted_;
            id_ = std::move(other.id_);
            error_ = std::move(other.error_);
            transaction_list_ = std::move(other.transaction_list_);
            ns_storage_ptr_ = other.ns_storage_ptr_;
            transaction_ = std::move(other.transaction_);

            other.is_aborted_ = true;
        }
        return *this;
    }

    metadata_transaction& transaction_scope::transaction() { return *transaction_; }

    const core::error_t& transaction_scope::error() const { return error_; }

    void transaction_scope::commit() {
        if (is_committed_ || is_aborted_) {
            error_ = core::error_t(core::error_code_t::transaction_finalized,
                                   core::error_tag_t::catalog,
                                   std::pmr::string{"Transaction already finalized!", resource_});
        }

        if (transaction_list_.expired()) {
            error_ = core::error_t(core::error_code_t::commit_failed,
                                   core::error_tag_t::catalog,
                                   std::pmr::string{"Transaction list has been destroyed!", resource_});
        }

        if (!error_.contains_error() && ns_storage_ptr_) {
            auto list = transaction_list_.lock();
            error_ = transaction_->commit([&](metadata_diff&& diff) {
                auto& info = ns_storage_ptr_->get_namespace_info(id_.get_namespace()).tables;

                if (auto it = info.find(id_.table_name()); it != info.end()) {
                    auto new_diff = diff.apply(it->second);
                    it->second = new_diff;
                    return core::error_t::no_error();
                }

                return core::error_t(core::error_code_t::commit_failed,
                                     core::error_tag_t::catalog,
                                     std::pmr::string{"Table not found!", resource_});
            });

            list->remove_transaction(id_);

            if (!error_.contains_error()) {
                is_committed_ = true;
                return;
            }
        }

        abort();
    }

    void transaction_scope::abort() {
        if (is_committed_) {
            error_ = core::error_t(core::error_code_t::transaction_finalized,
                                   core::error_tag_t::catalog,
                                   std::pmr::string{"Transaction already committed!", resource_});
            return;
        }

        if (is_aborted_) {
            return;
        }

        if (!transaction_list_.expired()) {
            transaction_list_.lock()->remove_transaction(id_);
        }

        if (transaction_) {
            transaction_->abort();
        }

        is_aborted_ = true;
    }
} // namespace components::catalog
