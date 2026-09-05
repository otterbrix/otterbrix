#include <catch2/catch_test_macros.hpp>

#include "components/index/index_engine.hpp"

using namespace components::index;

static index_value_t NULL_INDEX_VALUE{};

class dummy final : public index_t {
public:
    using storage_t = std::vector<value_t>;
    using const_iterator = storage_t::const_iterator;

    // `type` defaults to the ordered family, which is what every caller that names no
    // type wants. Naming index_type::hashed builds the fake's UNORDERED twin -- the
    // engine has to tell the two apart over one key set, so the fake has to be able to
    // be either.
    explicit dummy(std::pmr::memory_resource* resource,
                   components::catalog::oid_t oid,
                   const keys_base_storage_t& keys,
                   components::logical_plan::index_type type = components::logical_plan::index_type::single)
        : index_t(resource, type, oid, keys)
        , ordered_(type != components::logical_plan::index_type::hashed) {}

private:
    // An ordered fake hands back real ranges from both bounds, so it answers yes; the
    // hashed one answers no, exactly as the real hashed backends do.
    [[nodiscard]] bool supports_ordered_probe_impl() const noexcept override { return ordered_; }
    // The fake keeps its (empty) storage in memory and answers from it, exactly as the
    // in-memory indexes do, so no read of it travels a mailbox.
    [[nodiscard]] bool reads_through_disk_agent_impl() const noexcept override { return false; }
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
    void merge_uncommitted_rows_impl(components::expressions::compare_type,
                                     const value_t&,
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
    const bool ordered_;
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

// TWO INDEXES OVER ONE COLUMN, and dropping one of them.
//
// The pair is legal and reachable from SQL: create_index rejects a duplicate only on the
// PAIR (keys, type), so `CREATE INDEX i ON t (k)` and `CREATE INDEX j ON t USING hash (k)`
// both register (bootstrap_index_sync does not even check that much). Dropping ONE of them
// must leave the OTHER registered in every observable the engine publishes, because
// production reads all three and each one decides something different:
//
//   * size()             — the compact gate (manager_index_t::tables_without_indexes) and
//                          the repopulate driver (all_indexed_oids). Reading 0 over a live
//                          index lets a compact renumber rows underneath it AND skips the
//                          rebuild that would have repaired them;
//   * all_indexed_keys() — what the planner sees (context_storage_t::has_index_on) and what
//                          stamps DML index mirroring (stamp_table_has_indexes). Reading
//                          empty makes the surviving index invisible to reads and unfed by
//                          writes at the same time;
//   * matching(keys)     — the untyped read-path lookup.
//
// All three used to be answered out of a key-keyed map holding ONE slot per key set, which
// the second add_index could not write and the first drop_index erased on the way out.
TEST_CASE("components::index::dropping_one_index_over_a_key_keeps_its_twin") {
    using components::logical_plan::index_type;
    auto resource = core::pmr::otterbrix_resource();
    auto index_engine = make_index_engine(&resource);

    const keys_base_storage_t k{{components::expressions::key_t{&resource, "k"}}, &resource};

    auto ordered_id = make_index<dummy>(index_engine, 201u, k, index_type::single);
    auto hashed_id = make_index<dummy>(index_engine, 202u, k, index_type::hashed);
    REQUIRE(index_engine->size() == 2);

    auto* ordered = search_index(index_engine, ordered_id);
    auto* hashed = search_index(index_engine, hashed_id);
    REQUIRE(ordered != nullptr);
    REQUIRE(hashed != nullptr);
    REQUIRE(ordered != hashed);

    drop_index(index_engine, hashed);

    INFO("the ordered index is still owned by the engine, so it still counts");
    CHECK(index_engine->size() == 1);
    CHECK(index_engine->matching_relid(201u) == ordered);
    CHECK(index_engine->matching_relid(202u) == nullptr);

    INFO("an engine holding an index must not report the table as having no indexed keys");
    auto keys = index_engine->all_indexed_keys();
    REQUIRE(keys.size() == 1);
    CHECK(keys[0] == k);

    CHECK(index_engine->matching(k, index_type::single) == ordered);
    CHECK(index_engine->matching(k, index_type::hashed) == nullptr);
    CHECK(search_index(index_engine, k) == ordered);

    auto descriptions = index_engine->all_indexed_descriptions();
    REQUIRE(descriptions.size() == 1);
    CHECK(descriptions[0].type == index_type::single);
}

// all_indexed_keys() publishes a SET of key sets, not a bag. Two indexes over one column
// are two indexes (size() says so) but ONE indexed key set, and the multiplicity that
// distinguishes them — the backend type — is what all_indexed_descriptions() is for.
TEST_CASE("components::index::indexed_keys_are_a_set_not_a_bag") {
    using components::logical_plan::index_type;
    auto resource = core::pmr::otterbrix_resource();
    auto index_engine = make_index_engine(&resource);

    const keys_base_storage_t k{{components::expressions::key_t{&resource, "k"}}, &resource};
    const keys_base_storage_t j{{components::expressions::key_t{&resource, "j"}}, &resource};

    make_index<dummy>(index_engine, 301u, k, index_type::single);
    make_index<dummy>(index_engine, 302u, k, index_type::hashed);
    make_index<dummy>(index_engine, 303u, j, index_type::single);

    CHECK(index_engine->size() == 3);
    auto keys = index_engine->all_indexed_keys();
    REQUIRE(keys.size() == 2);
    CHECK(keys[0] == k);
    CHECK(keys[1] == j);
    CHECK(index_engine->all_indexed_descriptions().size() == 3);
}

// The untyped lookup's priority is DECLARED, not inherited from registration order: the
// hashed index is registered FIRST here and the ordered one must still be the answer.
// An ordered index answers all six comparison predicates; an unordered one answers eq and
// nothing else, and manager_index_t turns a range on it into an error — so handing back
// the hashed twin would fail a probe the neighbouring index could have answered.
TEST_CASE("components::index::untyped_lookup_prefers_the_ordered_index") {
    using components::logical_plan::index_type;
    auto resource = core::pmr::otterbrix_resource();
    auto index_engine = make_index_engine(&resource);

    const keys_base_storage_t k{{components::expressions::key_t{&resource, "k"}}, &resource};

    auto hashed_id = make_index<dummy>(index_engine, 401u, k, index_type::hashed);
    auto ordered_id = make_index<dummy>(index_engine, 402u, k, index_type::single);

    auto* hashed = search_index(index_engine, hashed_id);
    auto* ordered = search_index(index_engine, ordered_id);
    REQUIRE(hashed != nullptr);
    REQUIRE(ordered != nullptr);

    CHECK(search_index(index_engine, k) == ordered);

    // With the ordered one gone the hashed one is the only answer left — priority, not
    // exclusion.
    drop_index(index_engine, ordered);
    CHECK(search_index(index_engine, k) == hashed);
    CHECK(index_engine->size() == 1);
    CHECK(index_engine->all_indexed_keys().size() == 1);
}
