#include <catch2/catch.hpp>

#include "components/index/index_engine.hpp"

using namespace components::index;

static index_value_t NULL_INDEX_VALUE{};

class dummy final : public index_t {
public:
    using storage_t = std::vector<value_t>;
    using const_iterator = storage_t::const_iterator;

    explicit dummy(std::pmr::memory_resource* resource, const std::string& name, const keys_base_storage_t& keys)
        : index_t(resource, components::logical_plan::index_type::single, name, keys) {}

private:
    void insert_impl(value_t, document_ptr) {}
    void insert_impl(value_t, index_value_t) override {}
    void insert_impl(document_ptr) override {}
    void remove_impl(value_t) override {}
    range find_impl(const value_t&) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    iterator lower_bound_impl(const query_t&) const { return iterator(new impl_t(dummy_storage_.cbegin())); }
    range lower_bound_impl(const value_t&) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    iterator upper_bound_impl(const query_t&) const { return iterator(new impl_t(dummy_storage_.cend())); }
    range upper_bound_impl(const value_t&) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    iterator cbegin_impl() const override { return iterator(new impl_t(dummy_storage_.cbegin())); }
    iterator cend_impl() const override { return iterator(new impl_t(dummy_storage_.cend())); }
    void clean_memory_to_new_elements_impl(size_t) override {}

    class impl_t final : public iterator::iterator_impl_t {
    public:
        explicit impl_t(const_iterator) {}
        iterator::reference value_ref() const override { return NULL_INDEX_VALUE; }
        iterator_t::iterator_impl_t* next() override { return nullptr; }
        bool equals(const iterator::iterator_impl_t* other) const override { return this == other; }
        bool not_equals(const iterator::iterator_impl_t* other) const override { return this != other; }
        iterator::iterator_impl_t* copy() const override { return new impl_t(*this); }
    };

    storage_t dummy_storage_;
};

TEST_CASE("base index created") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index_engine = make_index_engine(&resource);
    auto one_id = make_index<dummy>(index_engine, "dummy_one", {components::expressions::key_t{&resource, "1"}});
    auto two_id = make_index<dummy>(
        index_engine,
        "dummy_two",
        {components::expressions::key_t{&resource, "1"}, components::expressions::key_t{&resource, "2"}});
    auto two_1_id = make_index<dummy>(
        index_engine,
        "dummy_two_1",
        {components::expressions::key_t{&resource, "2"}, components::expressions::key_t{&resource, "1"}});
    REQUIRE(index_engine->size() == 3);
    REQUIRE(search_index(index_engine, one_id) != nullptr);
    REQUIRE(search_index(index_engine, two_id) != nullptr);
    REQUIRE(search_index(index_engine, two_1_id) != nullptr);
}