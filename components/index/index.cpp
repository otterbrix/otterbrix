#include "index.hpp"
#include <components/expressions/forward.hpp>

namespace components::index {

    std::pmr::vector<int64_t> index_t::search(expressions::compare_type compare,
                                              const value_t& value,
                                              core::date::timezone_offset_t local_timezone) const {
        std::pmr::vector<int64_t> result(resource_);

        switch (compare) {
            case expressions::compare_type::eq: {
                auto range = find(value, local_timezone);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::lt: {
                auto range = lower_bound(value, local_timezone);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::lte: {
                auto ub = upper_bound(value, local_timezone);
                for (auto iter = cbegin(); iter != ub.first; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::gt: {
                auto range = upper_bound(value, local_timezone);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::gte: {
                auto lb = lower_bound(value, local_timezone);
                for (auto iter = lb.second; iter != cend(); ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case expressions::compare_type::ne: {
                auto eq_range = find(value, local_timezone);
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

    // An index stores exactly the NON-NULL keys of the live rows.
    //
    // A NULL key is never stored and is never looked up. This is enforced here, in index_t, rather
    // than in any single caller, because it is an invariant of the index — not a policy of one write
    // path. Every mutation and lookup funnels through these entry points, including the two
    // rehydrate paths that reconstruct an index from disk and bypass index_engine_t entirely.
    //
    // The invariant is what SQL requires: a NULL satisfies no value comparison, and the only
    // predicates the planner routes to an index are eq/lt/lte/gt/gte (IS NULL is answered from the
    // validity mask, never from an index). So a NULL-free index is a COMPLETE index for every
    // question an index scan can be asked.
    //
    // It is also what keeps the b-tree sound. A NULL key carries logical_type::NA rather than the
    // column's type, and the concrete indexes cast every key to the column type before storing it —
    // a cast that throws for NA, inside an actor coroutine that swallows the exception. Admitting a
    // NULL key therefore corrupts the index (a truncated batch, or, when the NULL arrives first, a
    // tree of mixed NA/typed keys ordered by a comparator that is not a strict weak ordering).
    // Keeping NULLs out means the concrete indexes only ever see a key of the column's own type.
    static bool is_null_key(const value_t& key) { return key.is_null(); }

    index_t::range index_t::find(const value_t& value, core::date::timezone_offset_t local_timezone) const {
        if (is_null_key(value)) {
            return std::make_pair(cend_impl(), cend_impl()); // NULL matches nothing
        }
        return find_impl(value, local_timezone);
    }

    index_t::range index_t::lower_bound(const value_t& value, core::date::timezone_offset_t local_timezone) const {
        if (is_null_key(value)) {
            return std::make_pair(cend_impl(), cend_impl());
        }
        return lower_bound_impl(value, local_timezone);
    }

    index_t::range index_t::upper_bound(const value_t& value, core::date::timezone_offset_t local_timezone) const {
        if (is_null_key(value)) {
            return std::make_pair(cend_impl(), cend_impl());
        }
        return upper_bound_impl(value, local_timezone);
    }

    index_t::iterator index_t::cbegin() const { return cbegin_impl(); }

    index_t::iterator index_t::cend() const { return cend_impl(); }

    auto index_t::insert(value_t key, index_value_t value, core::date::timezone_offset_t local_timezone) -> void {
        if (is_null_key(key)) {
            return;
        }
        return insert_impl(key, std::move(value), local_timezone);
    }

    auto index_t::insert(value_t key, int64_t row_index, core::date::timezone_offset_t local_timezone) -> void {
        if (is_null_key(key)) {
            return;
        }
        return insert_impl(key, index_value_t(row_index), local_timezone);
    }

    auto index_t::remove(value_t key, core::date::timezone_offset_t local_timezone) -> void {
        if (is_null_key(key)) {
            return;
        }
        remove_impl(key, local_timezone);
    }

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
                                              uint64_t txn_id,
                                              core::date::timezone_offset_t local_timezone) const {
        std::pmr::vector<int64_t> result(resource_);

        // `WHERE indexed_col <op> NULL` is UNKNOWN for every row, so it selects nothing. Answering
        // it here also keeps a NULL key out of the concrete index's cast, which would throw.
        if (is_null_key(value)) {
            return result;
        }

        auto filter = [&](auto begin, auto end) {
            for (auto iter = begin; iter != end; ++iter) {
                if (index_entry_visible(*iter, start_time, txn_id)) {
                    result.push_back(iter->row_index);
                }
            }
        };

        switch (compare) {
            case expressions::compare_type::eq: {
                auto range = find(value, local_timezone);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::lt: {
                auto range = lower_bound(value, local_timezone);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::lte: {
                auto ub = upper_bound(value, local_timezone);
                filter(cbegin(), ub.first);
                break;
            }
            case expressions::compare_type::gt: {
                auto range = upper_bound(value, local_timezone);
                filter(range.first, range.second);
                break;
            }
            case expressions::compare_type::gte: {
                auto lb = lower_bound(value, local_timezone);
                filter(lb.second, cend());
                break;
            }
            case expressions::compare_type::ne: {
                auto eq_range = find(value, local_timezone);
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

    auto index_t::insert(value_t key, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t local_timezone)
        -> void {
        if (is_null_key(key)) {
            return;
        }
        insert_txn_impl(std::move(key), row_index, txn_id, local_timezone);
    }

    auto
    index_t::mark_delete(value_t key, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t local_timezone)
        -> void {
        // Symmetric with insert: a NULL key was never stored, so there is nothing to delete. The
        // skip must happen here and not below, because the disk-backed hash index records a pending
        // delete without first looking the key up — it would otherwise queue a delete for a key that
        // was never written.
        if (is_null_key(key)) {
            return;
        }
        mark_delete_impl(std::move(key), row_index, txn_id, local_timezone);
    }

    void index_t::commit_insert(uint64_t txn_id, uint64_t commit_id) { commit_insert_impl(txn_id, commit_id); }

    void index_t::commit_delete(uint64_t txn_id, uint64_t commit_id) { commit_delete_impl(txn_id, commit_id); }

    void index_t::revert_insert(uint64_t txn_id) { revert_insert_impl(txn_id); }

    void index_t::revert_delete(uint64_t txn_id) { revert_delete_impl(txn_id); }

    void index_t::cleanup_versions(uint64_t lowest_active) { cleanup_versions_impl(lowest_active); }

    index_t::pending_entries_t index_t::pending_inserts(uint64_t txn_id) const { return pending_inserts_impl(txn_id); }

    index_t::pending_entries_t index_t::pending_deletes(uint64_t txn_id) const { return pending_deletes_impl(txn_id); }

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