#include <catch2/catch.hpp>

#include "components/index/index_engine.hpp"

using namespace components::index;

class dummy final : public index_t {
public:
    explicit dummy(std::pmr::memory_resource* resource, const std::string& name, const keys_base_storage_t& keys)
        : index_t(resource, components::logical_plan::index_type::single, name, keys) {}

private:
    void insert_impl(value_t, document_ptr) {}
    void insert_impl(value_t, index_value_t) {}
    void insert_impl(document_ptr) {}
    void remove_impl(value_t) {}
    range find_impl(const value_t&) const {}
    iterator lower_bound_impl(const query_t&) const {}
    range lower_bound_impl(const value_t&) const {}
    iterator upper_bound_impl(const query_t&) const {}
    range upper_bound_impl(const value_t&) const {}
    iterator cbegin_impl() const {}
    iterator cend_impl() const {}
    void clean_memory_to_new_elements_impl(size_t) {}

    class impl_t final : public index_t::iterator::iterator_impl_t {
    public:
        index_t::iterator::reference value_ref() const override {}
        iterator_t::iterator_impl_t* next() override {}
        bool equals(const iterator_t&) const {}
        bool not_equals(const iterator_t&) const {}
    };
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