#include "single_field_index.hpp"

namespace components::index {

    single_field_index_t::single_field_index_t(std::pmr::memory_resource* resource,
                                               std::string name,
                                               const keys_base_storage_t& keys)
        : index_t(resource, logical_plan::index_type::single, std::move(name), keys)
        , storage_(resource) {}

    single_field_index_t::~single_field_index_t() = default;

    index_t::iterator::reference single_field_index_t::impl_t::value_ref() const { return iterator_->second; }
    index_t::iterator_t::iterator_impl_t* single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }

    bool single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        return iterator_ == dynamic_cast<const impl_t*>(other)->iterator_; //todo
    }

    bool single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        return iterator_ != dynamic_cast<const impl_t*>(other)->iterator_; //todo
    }

    index_t::iterator::iterator_impl_t* single_field_index_t::impl_t::copy() const { return new impl_t(*this); }

    single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {}

    auto single_field_index_t::insert_impl(value_t key, index_value_t value) -> void {
        storage_.insert({key, std::move(value)});
    }

    auto single_field_index_t::remove_impl(components::index::value_t key) -> void {
        storage_.erase(storage_.find(key));
    }

    index_t::range single_field_index_t::find_impl(const value_t& value) const {
        auto range = storage_.equal_range(value);
        return std::make_pair(iterator(new impl_t(range.first)), iterator(new impl_t(range.second)));
    }

    index_t::range single_field_index_t::lower_bound_impl(const value_t& value) const {
        auto it = storage_.lower_bound(value);
        return std::make_pair(cbegin(), index_t::iterator(new impl_t(it)));
    }

    index_t::range single_field_index_t::upper_bound_impl(const value_t& value) const {
        auto it = storage_.upper_bound(value);
        return std::make_pair(index_t::iterator(new impl_t(it)), cend());
    }

    index_t::iterator single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(storage_.cbegin()));
    }

    index_t::iterator single_field_index_t::cend_impl() const { return index_t::iterator(new impl_t(storage_.cend())); }

    void single_field_index_t::clean_memory_to_new_elements_impl(std::size_t) {
        storage_.clear(); //todo: cache
    }

} // namespace components::index
