// WHICH ARENA A COPY LANDS ON IS NOT A FREE CHOICE.
//
// std::pmr::polymorphic_allocator::select_on_container_copy_construction() returns a
// DEFAULT-CONSTRUCTED allocator, i.e. std::pmr::get_default_resource(). So a copy of any pmr
// container made through an implicitly-defaulted copy constructor, or through a parameter taken
// BY VALUE, silently leaves the arena its owner named and lands on the process-global default.
// Move-constructing that copy afterwards then FREEZES the wrong allocator into the member for
// good, and every later move-assignment into it allocates there too.
//
// Nothing is corrupted by this — the same allocator deallocates — but the memory stops being
// visible to resource_tracer_t, i.e. to diagnostics, and rule 14 bans naming
// std::pmr::get_default_resource() precisely so that nobody allocates there. A defaulted copy
// constructor names it invisibly.
//
// The fix is NOT to make the copy constructor inherit the source's allocator. That trades an
// accounting problem for a lifetime one — see the cross-arena cases at the bottom of this file,
// where a copy wearing the source's allocator reads back 90 bytes of poison. The fix is that a
// copy which must sit on an arena NAMES it: key_t(key, resource), context_t(…, params).
//
// The measurement below does not read the code. It installs a counting resource AS the process
// default for the duration of one construction and counts what that construction takes from it;
// zero is the only passing answer. The get_allocator().resource() checks then name which arena
// the containers actually ended up on, so a failure says WHERE, not just "some allocation".

#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory_resource>
#include <new>
#include <optional>
#include <string>
#include <utility>

namespace expr = components::expressions;

namespace {

    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        std::atomic<uint64_t> allocations{0};
        std::atomic<uint64_t> bytes{0};

        void reset() noexcept {
            allocations.store(0, std::memory_order_relaxed);
            bytes.store(0, std::memory_order_relaxed);
        }

    private:
        void* do_allocate(size_t size, size_t align) override {
            allocations.fetch_add(1, std::memory_order_relaxed);
            bytes.fetch_add(size, std::memory_order_relaxed);
            return std::pmr::new_delete_resource()->allocate(size, align);
        }
        void do_deallocate(void* p, size_t size, size_t align) override {
            std::pmr::new_delete_resource()->deallocate(p, size, align);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    };

    // Immortal on purpose: it is the process default for the duration of one construction, and
    // whatever it hands out in that window may be freed long after the window closes.
    counting_resource_t& process_default_probe() {
        static counting_resource_t* probe = new counting_resource_t();
        return *probe;
    }

    struct default_resource_window_t final {
        std::pmr::memory_resource* previous;

        explicit default_resource_window_t(std::pmr::memory_resource* probe)
            : previous(std::pmr::set_default_resource(probe)) {}

        ~default_resource_window_t() { std::pmr::set_default_resource(previous); }
    };

    // Longer than any small-string buffer, so copying one is a heap allocation the probe sees
    // rather than an in-object memcpy it cannot.
    constexpr const char* long_column = "orders_customer_reference_identifier_column";
    constexpr const char* long_qualifier = "very_long_table_qualifier_that_never_fits_in_sso";
    constexpr const char* long_value = "a parameter value far too long to live inside the string object itself";

} // namespace

// key_t is copied by value all the way down the expression pipeline (pushdown_filter's
// `key_t partner = *partner_on_key;` is one of many), and each copy carries THREE pmr members:
// storage_ (a vector of pmr strings), qualifier_ and path_. Copied with no arena named, all
// three go to the process default. The trailing set_path() is the second half of the same
// defect: a move-assignment into a member already frozen on the wrong resource allocates there
// too, however carefully the caller built the vector it hands over.
//
// So the cross-arena sites name the arena — key_t(key, resource) — and that is the constructor
// measured here: the copy must take NOTHING from the process default and land, whole, on the
// arena it was given. (Which arena to pass is a separate question, answered by the cases at the
// bottom of this file; here caller and original happen to share one, so this case alone cannot
// tell "where the original was" from "where the caller asked" — it only measures the default.)
TEST_CASE("components::expressions::key_t::a copy placed on an arena takes nothing from the default") {
    core::pmr::otterbrix_resource arena;

    expr::key_t original(&arena, long_column);
    original.set_qualifier(long_qualifier);
    std::pmr::vector<size_t> original_path{&arena};
    original_path.push_back(3);
    original.set_path(std::move(original_path));

    REQUIRE(original.resource() == &arena);
    REQUIRE(original.path().get_allocator().resource() == &arena);

    // Built on the arena BEFORE the window, exactly as pushdown_filter builds its merged path.
    std::pmr::vector<size_t> merged_path{&arena};
    merged_path.push_back(7);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<expr::key_t> copy;
    {
        default_resource_window_t window{&probe};
        copy.emplace(original, &arena);
        copy->set_path(std::move(merged_path));
    }

    REQUIRE(copy.has_value());
    CHECK(copy->resource() == &arena);
    CHECK(copy->storage().get_allocator().resource() == &arena);
    REQUIRE(copy->storage().size() == 1);
    CHECK(copy->storage().front().get_allocator().resource() == &arena);
    CHECK(copy->qualifier().get_allocator().resource() == &arena);
    CHECK(copy->path().get_allocator().resource() == &arena);

    // The copy is still a copy: same name, same qualifier, and the path the caller moved in.
    CHECK(copy->as_string() == std::string(long_column));
    CHECK(std::string(copy->qualifier().c_str()) == std::string(long_qualifier));
    REQUIRE(copy->path().size() == 1);
    CHECK(copy->path().front() == 7);

    INFO("allocations taken from the process-global default resource while copying a key_t: "
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);

    // And a move afterwards must not be able to freeze anything else in: the source is already
    // on the arena, so the frozen allocator is the arena's.
    expr::key_t frozen(std::move(*copy));
    CHECK(frozen.resource() == &arena);
    CHECK(frozen.path().get_allocator().resource() == &arena);
}

// context_t took storage_parameters BY VALUE in both constructors, and storage_parameters holds
// a std::pmr::unordered_map with no copy constructor of its own. The executor hands it an
// LVALUE (`*plan_data.parameters`) once per sub-plan, so every parameterised statement copied
// its whole parameter map onto the process default and then froze it there.
TEST_CASE("components::pipeline::context_t::the parameter map keeps the arena the caller named") {
    namespace lp = components::logical_plan;

    core::pmr::otterbrix_resource arena;

    lp::storage_parameters params{&arena};
    lp::add_parameter(params, core::parameter_id_t(1), std::string(long_value));
    lp::add_parameter(params, core::parameter_id_t(2), std::int64_t(42));
    REQUIRE(params.resource() == &arena);
    REQUIRE(params.parameters.size() == 2);

    auto& probe = process_default_probe();
    probe.reset();

    std::optional<components::pipeline::context_t> ctx;
    std::optional<components::pipeline::context_t> executor_ctx;
    {
        default_resource_window_t window{&probe};
        ctx.emplace(params);
        // The shape the executor actually builds (executor.cpp, execute_sub_plan_).
        executor_ctx.emplace(components::session::session_id_t{},
                             actor_zeta::address_t::empty_address(),
                             actor_zeta::address_t::empty_address(),
                             nullptr,
                             params);
    }

    REQUIRE(ctx.has_value());
    CHECK(ctx->parameters.resource() == &arena);
    CHECK(ctx->parameters.parameters.get_allocator().resource() == &arena);
    REQUIRE(ctx->parameters.parameters.size() == 2);

    REQUIRE(executor_ctx.has_value());
    CHECK(executor_ctx->parameters.resource() == &arena);
    CHECK(executor_ctx->parameters.parameters.get_allocator().resource() == &arena);
    REQUIRE(executor_ctx->parameters.parameters.size() == 2);

    // The copy is still a copy: both parameters arrived with their values intact.
    const auto& copied = lp::get_parameter(&ctx->parameters, core::parameter_id_t(1));
    CHECK(copied.value<std::string_view>() == std::string_view(long_value));
    const auto& copied_int = lp::get_parameter(&ctx->parameters, core::parameter_id_t(2));
    CHECK(copied_int.value<std::int64_t>() == 42);

    INFO("allocations taken from the process-global default resource while building a context_t: "
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
}

// ============================================================================================
// CROSS-ARENA. Everything above builds the original and the copy on ONE arena, so "the copy
// stands where the ORIGINAL did" and "the copy stands where the CALLER asked" produce the very
// same green, and neither of them says one word about LIFETIME. key_t is copied ACROSS arena
// boundaries the whole length of the pipeline: a key built on a logical node's arena is copied
// into an operator that lives on context.resource (index_scan), into a cloned expression on the
// clone target's arena (clone_expression), into a rewritten node on an optimizer rule's arena
// (eager_aggregation). The node's arena is the SHORTER-lived one, and core/pmr.hpp's
// otterbrix_resource is a pool whose destructor releases everything it ever handed out.
//
// So the question these cases ask is not "which allocator address does the copy report" -- a
// copy wearing the SOURCE's allocator reports exactly the address the test would want to see --
// but "what does the copy still READ after the source arena is gone".
//
// The measurement: the source arena's upstream is a bump allocator over bytes THIS TEST owns
// and outlives. When the arena dies, the bytes stay addressable, and poison() paints all of
// them. A copy that kept the source's allocator then reads back poison; a copy that does not
// reads back its name.
// ============================================================================================

namespace {

    class owned_bytes_upstream_t final : public std::pmr::memory_resource {
    public:
        static constexpr size_t capacity = 1u << 20; // 1 MiB: a pool's first chunks plus slack
        static constexpr char poison_byte = 'Z';

        // Called only AFTER the arena above this upstream is destroyed. Nothing was ever handed
        // back to the system, so these bytes are still ours to paint and still addressable.
        void poison() noexcept { std::memset(buffer_, poison_byte, capacity); }

        size_t handed_out() const noexcept { return used_; }

    private:
        void* do_allocate(size_t size, size_t align) override {
            const size_t offset = (used_ + align - 1) & ~(align - 1);
            if (offset + size > capacity) {
                std::fprintf(stderr,
                             "owned_bytes_upstream_t: buffer exhausted (%zu + %zu > %zu)\n",
                             offset,
                             size,
                             capacity);
                std::abort();
            }
            used_ = offset + size;
            return buffer_ + offset;
        }
        void do_deallocate(void*, size_t, size_t) override {}
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        alignas(alignof(std::max_align_t)) char buffer_[capacity];
        size_t used_ = 0;
    };

    // How much of the name survived: the count of poison bytes a reader sees where the column
    // name should be. Reported as a NUMBER so a failure says how much was overwritten.
    size_t poison_bytes_in(const std::string& text) noexcept {
        size_t count = 0;
        for (char c : text) {
            if (c == owned_bytes_upstream_t::poison_byte) {
                ++count;
            }
        }
        return count;
    }

} // namespace

// A copy made WITHOUT naming an arena must not be bound to the source's. If it is, it is bound
// to an arena that dies first, and every later read of the name is a read of released memory.
TEST_CASE("components::expressions::key_t::a copy with no arena named outlives the source arena") {
    owned_bytes_upstream_t upstream;

    alignas(expr::key_t) unsigned char copy_storage[sizeof(expr::key_t)];
    expr::key_t* copy = nullptr;

    {
        // The logical node's arena: exactly core/pmr.hpp:29's otterbrix_resource, on bytes this
        // test owns, so that its destructor's "release everything" is observable.
        core::pmr::otterbrix_resource source{&upstream};

        expr::key_t original(&source, long_column);
        original.set_qualifier(long_qualifier);
        std::pmr::vector<size_t> original_path{&source};
        original_path.push_back(3);
        original.set_path(std::move(original_path));
        REQUIRE(original.resource() == &source);
        REQUIRE(original.storage().front().get_allocator().resource() == &source);

        // Deliberately never destroyed. In the broken form this object's allocator IS the arena
        // destroyed one line below, so running ~key_t() would fault before any CHECK could
        // report a number. The subject here is what the copy can still READ, not its teardown.
        copy = new (copy_storage) expr::key_t(original);

        INFO("copy allocator " << static_cast<const void*>(copy->resource()) << ", source arena "
                               << static_cast<const void*>(&source));
        CHECK(copy->resource() != &source);
        CHECK(copy->storage().front().get_allocator().resource() != &source);
    }

    // The source arena is gone; its bytes are ours again, and painted.
    upstream.poison();

    const std::string name = copy->as_string();
    const std::string qualifier(copy->qualifier().c_str(), copy->qualifier().size());
    INFO("bytes the source arena handed out: " << upstream.handed_out());
    INFO("name read back after the source arena died: \"" << name << "\" (" << poison_bytes_in(name)
                                                          << " poison bytes of " << name.size() << ")");
    INFO("qualifier read back: \"" << qualifier << "\" (" << poison_bytes_in(qualifier) << " poison bytes of "
                                   << qualifier.size() << ")");
    CHECK(poison_bytes_in(name) == 0);
    CHECK(poison_bytes_in(qualifier) == 0);
    CHECK(name == std::string(long_column));
    CHECK(qualifier == std::string(long_qualifier));
}

// And the form every cross-arena site uses: the copy is placed on the DESTINATION's arena — the
// operator's, the clone target's, the rewriting rule's — which is the arena the caller has
// proven outlives the copy. The source arena dies underneath it and the copy reads its name.
TEST_CASE("components::expressions::key_t::a copy placed on the destination arena outlives the source") {
    owned_bytes_upstream_t upstream;
    // The long-lived side: the operator / clone target the copy is being built for.
    core::pmr::otterbrix_resource destination;

    alignas(expr::key_t) unsigned char copy_storage[sizeof(expr::key_t)];
    expr::key_t* copy = nullptr;

    {
        core::pmr::otterbrix_resource source{&upstream};

        expr::key_t original(&source, long_column);
        original.set_qualifier(long_qualifier);
        std::pmr::vector<size_t> original_path{&source};
        original_path.push_back(3);
        original.set_path(std::move(original_path));
        original.set_side(expr::side_t::left);
        REQUIRE(original.resource() == &source);

        copy = new (copy_storage) expr::key_t(original, &destination);

        // Every pmr member, not just the one resource() reports.
        CHECK(copy->resource() == &destination);
        CHECK(copy->storage().get_allocator().resource() == &destination);
        REQUIRE(copy->storage().size() == 1);
        CHECK(copy->storage().front().get_allocator().resource() == &destination);
        CHECK(copy->qualifier().get_allocator().resource() == &destination);
        CHECK(copy->path().get_allocator().resource() == &destination);
    }

    upstream.poison();

    const std::string name = copy->as_string();
    const std::string qualifier(copy->qualifier().c_str(), copy->qualifier().size());
    INFO("bytes the source arena handed out: " << upstream.handed_out());
    INFO("name read back after the source arena died: \"" << name << "\" (" << poison_bytes_in(name)
                                                          << " poison bytes of " << name.size() << ")");
    CHECK(poison_bytes_in(name) == 0);
    CHECK(poison_bytes_in(qualifier) == 0);
    CHECK(name == std::string(long_column));
    CHECK(qualifier == std::string(long_qualifier));
    REQUIRE(copy->path().size() == 1);
    CHECK(copy->path().front() == 3);
    CHECK(copy->side() == expr::side_t::left);

    // Safe to destroy, unlike the case above: everything this copy owns is on `destination`,
    // which is still alive. That teardown is itself part of the claim.
    copy->~key_t();
}
