// select_on_container_copy_construction() returns a DEFAULT-CONSTRUCTED allocator, so an
// implicitly-defaulted copy (or a by-value parameter) of any pmr container silently lands on
// the process-global default -- invisible to resource_tracer_t, exactly what the ban on
// naming get_default_resource() is meant to prevent. A later move FREEZES that wrong allocator
// into the member for good.
//
// Not fixed by making the copy constructor inherit the source's allocator: that trades this
// accounting bug for a lifetime one (see the cross-arena cases below, where inheriting reads
// back poisoned memory once the source arena is gone). The fix is that a copy which must live
// on an arena NAMES it explicitly: key_t(key, resource), context_t(..., params).
//
// Measured, not read from the code: a counting resource installed as the process default must
// see zero allocations during construction; get_allocator().resource() checks then say WHICH
// arena a container actually landed on.

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

// key_t carries THREE pmr members (storage_, qualifier_, path_) that all go to the process
// default under a plain copy, so cross-arena sites name the arena explicitly on every copy --
// e.g. pushdown_filter's `key_t partner{*partner_on_key, resource}` -- which is the constructor
// measured here: the copy must take NOTHING from the process default. (Which arena to pass is a
// separate question, answered by the cases at the bottom of this file; here caller and original
// share one, so this case alone only measures the default.)
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

    CHECK(copy->as_string() == std::string(long_column));
    CHECK(std::string(copy->qualifier().c_str()) == std::string(long_qualifier));
    REQUIRE(copy->path().size() == 1);
    CHECK(copy->path().front() == 7);

    INFO("allocations taken from the process-global default resource while copying a key_t: "
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);

    // A move afterwards must not freeze in another allocator: the source is already on the arena.
    expr::key_t frozen(std::move(*copy));
    CHECK(frozen.resource() == &arena);
    CHECK(frozen.path().get_allocator().resource() == &arena);
}

// Regression: context_t used to take storage_parameters BY VALUE, and the executor hands it an
// lvalue (`*plan_data.parameters`) once per sub-plan, so every parameterised statement copied
// its whole parameter map onto the process default and froze it there.
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

    const auto& copied = lp::get_parameter(&ctx->parameters, core::parameter_id_t(1));
    CHECK(copied.value<std::string_view>() == std::string_view(long_value));
    const auto& copied_int = lp::get_parameter(&ctx->parameters, core::parameter_id_t(2));
    CHECK(copied_int.value<std::int64_t>() == 42);

    INFO("allocations taken from the process-global default resource while building a context_t: "
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
}

// CROSS-ARENA: everything above shares one arena between original and copy, so it cannot tell
// LIFETIME apart from accounting -- a copy wearing the source's allocator reports the "right"
// address right up until that arena dies (key_t crosses arenas throughout the pipeline: onto an
// operator's context.resource in index_scan, a clone target's in clone_expression, a rewritten
// node's in eager_aggregation, and the node's arena is the shorter-lived one).
//
// So these cases ask what the copy still READS after the source arena is gone: its upstream is
// a bump allocator over bytes this test owns and outlives, poisoned once the arena dies. A copy
// that kept the source's allocator reads back poison; one that didn't reads back its name.

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

        // Deliberately never destroyed: in the broken form its allocator IS the arena destroyed
        // below, so ~key_t() would fault before any CHECK could report a number.
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
