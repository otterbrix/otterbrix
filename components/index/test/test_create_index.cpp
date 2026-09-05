#include <catch2/catch_test_macros.hpp>

#include "components/index/index_engine.hpp"

using namespace components::index;

static index_value_t NULL_INDEX_VALUE{};

class dummy final : public index_t {
public:
    using storage_t = std::vector<value_t>;
    using const_iterator = storage_t::const_iterator;

    explicit dummy(std::pmr::memory_resource* resource, components::catalog::oid_t oid, const keys_base_storage_t& keys)
        : index_t(resource, components::logical_plan::index_type::single, oid, keys) {}

private:
    // The fake stands in for an ordered index (index_type::single) and hands back real
    // ranges from both bounds, so it answers yes.
    [[nodiscard]] bool supports_ordered_probe_impl() const noexcept override { return true; }
    void insert_impl(value_t, index_value_t, core::date::timezone_offset_t) override {}
    void remove_impl(value_t, core::date::timezone_offset_t) override {}
    range find_impl(const value_t&, core::date::timezone_offset_t) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    range lower_bound_impl(const value_t&, core::date::timezone_offset_t) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    range upper_bound_impl(const value_t&, core::date::timezone_offset_t) const override {
        return std::make_pair(iterator(new impl_t(dummy_storage_.cbegin())),
                              iterator(new impl_t(dummy_storage_.cend())));
    }
    iterator cbegin_impl() const override { return iterator(new impl_t(dummy_storage_.cbegin())); }
    iterator cend_impl() const override { return iterator(new impl_t(dummy_storage_.cend())); }
    void insert_txn_impl(value_t, int64_t, uint64_t, core::date::timezone_offset_t) override {}
    void mark_delete_impl(value_t, int64_t, uint64_t, core::date::timezone_offset_t) override {}
    void commit_insert_impl(uint64_t, uint64_t) override {}
    void commit_delete_impl(uint64_t, uint64_t) override {}
    void revert_insert_impl(uint64_t) override {}
    void revert_delete_impl(uint64_t) override {}
    void cleanup_versions_impl(uint64_t) override {}
    pending_entries_t pending_inserts_impl(uint64_t) const override { return pending_entries_t{resource()}; }
    pending_entries_t pending_deletes_impl(uint64_t) const override { return pending_entries_t{resource()}; }
    // The fake holds no pending state and is never read through a disk agent, so this
    // hook has nothing to fold in. Stated explicitly because index_t leaves no default
    // to inherit (see index.hpp).
    void merge_uncommitted_rows_impl(const value_t&,
                                     uint64_t,
                                     core::date::timezone_offset_t,
                                     std::pmr::vector<int64_t>&) const override {}
    void clean_memory_to_new_elements_impl(size_t) override {}

    class impl_t final : public iterator::iterator_impl_t {
    public:
        explicit impl_t(const_iterator) {}
        kind_t kind() const noexcept override { return kind_t::test_fake; }
        iterator::reference value_ref() const override { return NULL_INDEX_VALUE; }
        iterator_t::iterator_impl_t* next() override { return nullptr; }
        bool equals(const iterator::iterator_impl_t* other) const override { return this == other; }
        bool not_equals(const iterator::iterator_impl_t* other) const override { return this != other; }
        iterator::iterator_impl_t* copy() const override { return new impl_t(*this); }
    };

    storage_t dummy_storage_;
};

TEST_CASE("components::index::base_index_created") {
    auto resource = core::pmr::otterbrix_resource();
    auto index_engine = make_index_engine(&resource);
    // Indexes are identified by pg_index.indexrelid (oid), not by name.
    auto one_id = make_index<dummy>(index_engine, 101u, {components::expressions::key_t{&resource, "1"}});
    auto two_id = make_index<dummy>(
        index_engine,
        102u,
        {components::expressions::key_t{&resource, "1"}, components::expressions::key_t{&resource, "2"}});
    auto two_1_id = make_index<dummy>(
        index_engine,
        103u,
        {components::expressions::key_t{&resource, "2"}, components::expressions::key_t{&resource, "1"}});
    REQUIRE(index_engine->size() == 3);
    REQUIRE(search_index(index_engine, one_id) != nullptr);
    REQUIRE(search_index(index_engine, two_id) != nullptr);
    REQUIRE(search_index(index_engine, two_1_id) != nullptr);
}

// pg_index.indtype code alphabet: every writable index_type round-trips through its
// single-char catalog code; an unknown code decodes to no_valid (the bootstrap reader
// treats that as catalog corruption and fails LOUDLY — error log + abort — instead of
// guessing a backend); no_valid itself has no writable code (the encoder returns 0).
TEST_CASE("components::index::indtype_code_roundtrip") {
    using components::logical_plan::index_type;
    using components::logical_plan::index_type_from_indtype_code;
    using components::logical_plan::index_type_to_indtype_code;
    for (auto t : {index_type::single,
                   index_type::composite,
                   index_type::multikey,
                   index_type::hashed,
                   index_type::wildcard}) {
        const char code = index_type_to_indtype_code(t);
        REQUIRE(code != 0);
        REQUIRE(index_type_from_indtype_code(code) == t);
    }
    REQUIRE(index_type_from_indtype_code('x') == index_type::no_valid);
    REQUIRE(index_type_from_indtype_code('\0') == index_type::no_valid);
    REQUIRE(index_type_to_indtype_code(index_type::no_valid) == 0);
}
