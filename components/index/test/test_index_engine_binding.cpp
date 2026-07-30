#include <catch2/catch_test_macros.hpp>

#include <components/index/index_engine.hpp>
#include <components/vector/data_chunk.hpp>

// Characterization of the index_engine key -> column resolution rule.
//
// The rule is NAME-based and deliberately so: an index key names a column by the name the
// COLUMN carries, never by ordinal. Nothing here asserts a performance property —
// these cases exist so that a change to WHERE the resolution happens (per chunk instead of per
// row) can be proven not to change WHAT it resolves to. An ordinal-based "optimisation" fails
// every case below except the first.
//
// The four properties pinned:
//   1. a chunk carrying only SOME of the table's columns still feeds the indexes whose keys are
//      present, and only those (partial column list);
//   2. column ORDER is irrelevant — the same chunk permuted resolves identically;
//   3. a multi-key index applies only when EVERY key is present, and its stored key comes from the
//      FIRST key (single_field storage; the multi-key case is still a todo in the engine);
//   4. two columns sharing one name resolve to the FIRST match in chunk order.

using namespace components::index;
namespace types = components::types;

namespace {

    static index_value_t NULL_INDEX_VALUE{};

    // An index_t that records exactly what the engine handed it.
    class recorder_t final : public index_t {
    public:
        recorder_t(std::pmr::memory_resource* resource, const std::string& name, const keys_base_storage_t& keys)
            : index_t(resource, components::logical_plan::index_type::single, name, keys)
            , seen_(resource) {}

        const std::pmr::vector<value_t>& seen() const { return seen_; }

    private:
        using storage_t = std::vector<value_t>;
        using const_iterator = storage_t::const_iterator;

        class impl_t final : public iterator::iterator_impl_t {
        public:
            explicit impl_t(const_iterator) {}
            iterator::reference value_ref() const override { return NULL_INDEX_VALUE; }
            iterator_t::iterator_impl_t* next() override { return nullptr; }
            bool equals(const iterator::iterator_impl_t* other) const override { return this == other; }
            bool not_equals(const iterator::iterator_impl_t* other) const override { return this != other; }
            iterator::iterator_impl_t* copy() const override { return new impl_t(*this); }
        };

        void insert_impl(value_t key, index_value_t, core::date::timezone_offset_t) override {
            seen_.push_back(std::move(key));
        }
        void insert_txn_impl(value_t key, int64_t, uint64_t, core::date::timezone_offset_t) override {
            seen_.push_back(std::move(key));
        }
        void mark_delete_impl(value_t key, int64_t, uint64_t, core::date::timezone_offset_t) override {
            seen_.push_back(std::move(key));
        }

        void remove_impl(value_t, core::date::timezone_offset_t) override {}
        range find_impl(const value_t&, core::date::timezone_offset_t) const override { return empty_range(); }
        range lower_bound_impl(const value_t&, core::date::timezone_offset_t) const override { return empty_range(); }
        range upper_bound_impl(const value_t&, core::date::timezone_offset_t) const override { return empty_range(); }
        iterator cbegin_impl() const override { return iterator(new impl_t(dummy_.cbegin())); }
        iterator cend_impl() const override { return iterator(new impl_t(dummy_.cend())); }
        void commit_insert_impl(uint64_t, uint64_t) override {}
        void commit_delete_impl(uint64_t, uint64_t) override {}
        void revert_insert_impl(uint64_t) override {}
        void revert_delete_impl(uint64_t) override {}
        void cleanup_versions_impl(uint64_t) override {}
        void for_each_pending_insert_impl(uint64_t,
                                          const std::function<void(const value_t&, int64_t)>&) const override {}
        void for_each_pending_delete_impl(uint64_t,
                                          const std::function<void(const value_t&, int64_t)>&) const override {}
        void clean_memory_to_new_elements_impl(size_t) override {}

        range empty_range() const {
            return std::make_pair(iterator(new impl_t(dummy_.cbegin())), iterator(new impl_t(dummy_.cend())));
        }

        storage_t dummy_;
        std::pmr::vector<value_t> seen_;
    };

    // A one-row chunk whose columns carry the given NAMES and BIGINT values. The name is on
    // the column and not inside its type (M3-B5), which is exactly what index_engine's
    // find_column asks for.
    components::vector::data_chunk_t
    make_named_chunk(std::pmr::memory_resource* resource,
                     const std::vector<std::pair<std::string, int64_t>>& cols,
                     uint64_t capacity = 1) {
        components::vector::schema_t schema(resource);
        for (const auto& [name, _] : cols) {
            components::vector::column_schema_t record{resource};
            record.name = name;
            record.type = types::complex_logical_type{types::logical_type::BIGINT};
            schema.push_back(std::move(record));
        }
        auto chunk = components::vector::make_chunk(resource, schema, capacity);
        for (size_t i = 0; i < cols.size(); ++i) {
            chunk.set_value(i, 0, types::logical_value_t{resource, cols[i].second});
        }
        chunk.set_cardinality(1);
        return chunk;
    }

    int64_t only_key(const recorder_t* idx) {
        REQUIRE(idx->seen().size() == 1);
        return idx->seen()[0].value<int64_t>();
    }

} // namespace

TEST_CASE("components::index::engine_binding::partial_column_list") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_x = make_index<recorder_t>(engine, "ix_x", {components::expressions::key_t{&resource, "x"}});
    auto id_y = make_index<recorder_t>(engine, "ix_y", {components::expressions::key_t{&resource, "y"}});

    auto* ix = static_cast<recorder_t*>(search_index(engine, id_x));
    auto* iy = static_cast<recorder_t*>(search_index(engine, id_y));

    // The chunk carries only 'id' and 'x' — 'y' was not part of this INSERT's column list.
    auto chunk = make_named_chunk(&resource, {{"id", 1}, {"x", 42}});
    engine->insert_row(engine->bind(chunk), 0, /*storage_row=*/7, /*txn_id=*/1, core::date::timezone_offset_t{});

    REQUIRE(only_key(ix) == 42); // resolved by alias, NOT by ordinal (ordinal 0 would be 'id' = 1)
    REQUIRE(iy->seen().empty()); // 'y' absent from the chunk -> that index sees nothing
}

TEST_CASE("components::index::engine_binding::column_order_is_irrelevant") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_x = make_index<recorder_t>(engine, "ix_x", {components::expressions::key_t{&resource, "x"}});
    auto* ix = static_cast<recorder_t*>(search_index(engine, id_x));

    // Same data, 'x' last instead of first. An ordinal-based resolution would read 'id'.
    auto chunk = make_named_chunk(&resource, {{"id", 1}, {"z", 99}, {"x", 42}});
    engine->insert_row(engine->bind(chunk), 0, 7, 1, core::date::timezone_offset_t{});
    REQUIRE(only_key(ix) == 42);
}

TEST_CASE("components::index::engine_binding::multi_key_index_needs_every_key") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_xy = make_index<recorder_t>(
        engine,
        "ix_xy",
        {components::expressions::key_t{&resource, "x"}, components::expressions::key_t{&resource, "y"}});
    auto* ixy = static_cast<recorder_t*>(search_index(engine, id_xy));

    // Only one of the two keys present -> the index does not apply at all.
    auto partial = make_named_chunk(&resource, {{"id", 1}, {"x", 42}});
    engine->insert_row(engine->bind(partial), 0, 7, 1, core::date::timezone_offset_t{});
    REQUIRE(ixy->seen().empty());

    // Both keys present -> applies, and the stored key is the FIRST key's column.
    auto full = make_named_chunk(&resource, {{"id", 1}, {"y", 8}, {"x", 42}});
    engine->insert_row(engine->bind(full), 0, 7, 1, core::date::timezone_offset_t{});
    REQUIRE(only_key(ixy) == 42);
}

TEST_CASE("components::index::engine_binding::duplicate_alias_resolves_to_first_match") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_v = make_index<recorder_t>(engine, "ix_v", {components::expressions::key_t{&resource, "v"}});
    auto* iv = static_cast<recorder_t*>(search_index(engine, id_v));

    // A multi-type dynamic-schema field: two physical columns share the alias 'v'.
    // The engine takes the FIRST one in chunk order.
    auto chunk = make_named_chunk(&resource, {{"id", 1}, {"v", 10}, {"v", 20}});
    engine->insert_row(engine->bind(chunk), 0, 7, 1, core::date::timezone_offset_t{});
    REQUIRE(only_key(iv) == 10);
}

TEST_CASE("components::index::engine_binding::mark_delete_resolves_identically") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_x = make_index<recorder_t>(engine, "ix_x", {components::expressions::key_t{&resource, "x"}});
    auto id_y = make_index<recorder_t>(engine, "ix_y", {components::expressions::key_t{&resource, "y"}});
    auto* ix = static_cast<recorder_t*>(search_index(engine, id_x));
    auto* iy = static_cast<recorder_t*>(search_index(engine, id_y));

    auto chunk = make_named_chunk(&resource, {{"id", 1}, {"x", 42}});
    engine->mark_delete_row(engine->bind(chunk), 0, 7, 1, core::date::timezone_offset_t{});
    REQUIRE(only_key(ix) == 42);
    REQUIRE(iy->seen().empty());
}

TEST_CASE("components::index::engine_binding::every_row_of_a_chunk_is_resolved") {
    auto resource = core::pmr::otterbrix_resource();
    auto engine = make_index_engine(&resource);
    auto id_x = make_index<recorder_t>(engine, "ix_x", {components::expressions::key_t{&resource, "x"}});
    auto* ix = static_cast<recorder_t*>(search_index(engine, id_x));

    auto chunk = make_named_chunk(&resource, {{"id", 0}, {"x", 0}}, /*capacity=*/4);
    for (uint64_t r = 0; r < 4; ++r) {
        chunk.set_value(0, r, types::logical_value_t{&resource, static_cast<int64_t>(r)});
        chunk.set_value(1, r, types::logical_value_t{&resource, static_cast<int64_t>(100 + r)});
    }
    chunk.set_cardinality(4);

    // ONE binding, reused for every row of the chunk — the shape the callers use.
    const auto bindings = engine->bind(chunk);
    for (size_t r = 0; r < 4; ++r) {
        engine->insert_row(bindings, r, static_cast<int64_t>(r), 1, core::date::timezone_offset_t{});
    }
    REQUIRE(ix->seen().size() == 4);
    for (size_t r = 0; r < 4; ++r) {
        REQUIRE(ix->seen()[r].value<int64_t>() == static_cast<int64_t>(100 + r));
    }
}
