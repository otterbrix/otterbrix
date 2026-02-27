#include "index.hpp"
#include <components/expressions/forward.hpp>

namespace components::index {

    std::pmr::vector<int64_t> index_t::search(expressions::compare_type compare, const value_t& value) const {
        std::pmr::vector<int64_t> result(resource_);

        switch (compare) {
            case expressions::compare_type::eq: {
                auto range = find(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::lt: {
                auto range = lower_bound(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::lte: {
                auto ub = upper_bound(value);
                for (auto iter = cbegin(); iter != ub.first; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::gt: {
                auto range = upper_bound(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::gte: {
                auto lb = lower_bound(value);
                for (auto iter = lb.second; iter != cend(); ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::ne: {
                auto eq_range = find(value);
                for (auto iter = cbegin(); iter != cend(); ++iter) {
                    bool in_eq = false;
                    for (auto eq_it = eq_range.first; eq_it != eq_range.second; ++eq_it) {
                        if (eq_it->row_index == iter->row_index) {
                            in_eq = true;
                            break;
                        }
                    }
                    if (!in_eq) {
                        result.push_back(iter->row_index);
                    }
                }
                break;
            }
            default:
                break;
        }

        return result;
    }

    index_t::index_t(std::pmr::memory_resource* resource,
                     components::logical_plan::index_type type,
                     std::string name,
                     const keys_base_storage_t& keys)
        : resource_(resource)
        , type_(type)
        , name_(std::move(name))
        , keys_(keys) {
        assert(resource != nullptr);
    }

    index_t::range index_t::find(const value_t& value) const { return find_impl(value); }

    index_t::range index_t::lower_bound(const value_t& value) const { return lower_bound_impl(value); }

    index_t::range index_t::upper_bound(const value_t& value) const { return upper_bound_impl(value); }

    index_t::iterator index_t::cbegin() const { return cbegin_impl(); }

    index_t::iterator index_t::cend() const { return cend_impl(); }

    auto index_t::insert(value_t key, index_value_t value) -> void { return insert_impl(key, std::move(value)); }

    auto index_t::insert(value_t key, int64_t row_index) -> void { return insert_impl(key, index_value_t(row_index)); }

    auto index_t::remove(value_t key) -> void { remove_impl(key); }

    auto index_t::keys() -> std::pair<std::pmr::vector<key_t>::iterator, std::pmr::vector<key_t>::iterator> {
        return std::make_pair(keys_.begin(), keys_.end());
    }

    std::pmr::memory_resource* index_t::resource() const noexcept { return resource_; }

    logical_plan::index_type index_t::type() const noexcept { return type_; }

    const std::string& index_t::name() const noexcept { return name_; }

    bool index_t::is_disk() const noexcept { return disk_agent_ != actor_zeta::address_t::empty_address(); }

    const actor_zeta::address_t& index_t::disk_agent() const noexcept { return disk_agent_; }

    const actor_zeta::address_t& index_t::disk_manager() const noexcept { return disk_manager_; }

    void index_t::set_disk_agent(actor_zeta::address_t agent, actor_zeta::address_t manager) noexcept {
        disk_agent_ = std::move(agent);
        disk_manager_ = std::move(manager);
    }

    std::pmr::vector<int64_t> index_t::search(expressions::compare_type compare,
                                              const value_t& value,
                                              uint64_t start_time,
                                              uint64_t txn_id) const {
        std::pmr::vector<int64_t> result(resource_);

        auto filter = [&](auto begin, auto end) {
            for (auto iter = begin; iter != end; ++iter) {
                if (index_entry_visible(*iter, start_time, txn_id)) {
                    result.push_back(iter->row_index);
                }
            }
        };

        switch (compare) {
            case expressions::compare_type::eq: {
                auto range = find(value);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::lt: {
                auto range = lower_bound(value);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::lte: {
                auto ub = upper_bound(value);
                filter(cbegin(), ub.first);
                break;
            }
            case expressions::compare_type::gt: {
                auto range = upper_bound(value);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::gte: {
                auto lb = lower_bound(value);
                filter(lb.second, cend());
                break;
            }
            case expressions::compare_type::ne: {
                auto eq_range = find(value);
                for (auto iter = cbegin(); iter != cend(); ++iter) {
                    if (!index_entry_visible(*iter, start_time, txn_id)) {
                        continue;
                    }
                    bool in_eq = false;
                    for (auto eq_it = eq_range.first; eq_it != eq_range.second; ++eq_it) {
                        if (eq_it->row_index == iter->row_index) {
                            in_eq = true;
                            break;
                        }
                    }
                    if (!in_eq) {
                        result.push_back(iter->row_index);
                    }
                }
                break;
            }
            default:
                break;
        }

        return result;
    }

    auto index_t::insert(value_t key, int64_t row_index, uint64_t txn_id) -> void {
        insert_txn_impl(std::move(key), row_index, txn_id);
    }

    auto index_t::mark_delete(value_t key, int64_t row_index, uint64_t txn_id) -> void {
        mark_delete_impl(std::move(key), row_index, txn_id);
    }

    void index_t::commit_insert(uint64_t txn_id, uint64_t commit_id) { commit_insert_impl(txn_id, commit_id); }

    void index_t::commit_delete(uint64_t txn_id, uint64_t commit_id) { commit_delete_impl(txn_id, commit_id); }

    void index_t::revert_insert(uint64_t txn_id) { revert_insert_impl(txn_id); }

    void index_t::cleanup_versions(uint64_t lowest_active) { cleanup_versions_impl(lowest_active); }

    void index_t::for_each_pending_insert(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const {
        for_each_pending_insert_impl(txn_id, fn);
    }

    void index_t::for_each_pending_delete(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const {
        for_each_pending_delete_impl(txn_id, fn);
    }

    void index_t::clean_memory_to_new_elements(std::size_t count) noexcept { clean_memory_to_new_elements_impl(count); }

    index_t::iterator_t::reference index_t::iterator_t::operator*() const { return impl_->value_ref(); }

    index_t::iterator_t::pointer index_t::iterator_t::operator->() const { return &impl_->value_ref(); }

    index_t::iterator_t& index_t::iterator_t::operator++() {
        impl_->next();
        return *this;
    }

    bool index_t::iterator_t::operator==(const iterator_t& other) const { return impl_->equals(other.impl_); }

    bool index_t::iterator_t::operator!=(const iterator_t& other) const { return impl_->not_equals(other.impl_); }

    index_t::iterator_t::iterator_t(index_t::iterator_t::iterator_impl_t* ptr)
        : impl_(ptr) {}

    index_t::iterator_t::~iterator_t() { delete impl_; }

    index_t::iterator_t::iterator_t(const iterator_t& other)
        : impl_(other.impl_->copy()) {}

    index_t::iterator_t& index_t::iterator_t::operator=(const iterator_t& other) {
        delete impl_;
        impl_ = other.impl_->copy();
        return *this;
    }

    index_t::~index_t() = default;

} // namespace components::index