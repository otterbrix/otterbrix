#include <catch2/catch_test_macros.hpp>

#include <components/context/context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <memory>
#include <string>
#include <string_view>

using namespace components;

// DELETE ... USING ... RETURNING pairs every deleted row with the right-side row that matched it.
// The right side of a filtered USING source is a DICTIONARY chunk, and copying one cell out of one
// slices every index up to that row, so pairing cell by cell allocated once per cell and cost rows^2
// index entries. The pairing is one gather per column, so neither the allocations nor the bytes may
// grow with the number of deleted rows. #646.

namespace {
    constexpr uint64_t kNarrow = 64;
    constexpr uint64_t kWide = 640;

    vector::data_chunk_t
    make_chunk(std::pmr::memory_resource* resource, uint64_t rows, uint64_t width = 64, uint64_t capacity = 0) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::BIGINT);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        types.emplace_back(types::logical_type::STRING_LITERAL);
        vector::data_chunk_t chunk(resource, types, capacity == 0 ? rows : capacity);
        const std::string value(width, 'x');
        for (uint64_t row = 0; row < rows; row++) {
            chunk.set_value(0, row, static_cast<int64_t>(row));
            chunk.set_value(1, row, std::string_view{value});
            chunk.set_value(2, row, std::string_view{value});
            chunk.row_ids.data<int64_t>()[row] = static_cast<int64_t>(row);
        }
        chunk.set_cardinality(rows);
        return chunk;
    }

    // The right side holds one DICTIONARY chunk and every left row matches its first row (no
    // condition), so the number of pairings is exactly the number of deleted rows.
    struct taken_t {
        std::size_t allocations;
        std::size_t bytes;
    };

    taken_t
    taken_for(uint64_t rows, uint64_t width = 64, bool with_right = true, logical_plan::row_cap_t affected_bound = {}) {
        core::pmr::otterbrix_resource upstream;
        core::pmr::counting_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox()};

        std::pmr::vector<operators::projected_column_t> returning{&arena};
        returning.emplace_back(
            &arena,
            "*",
            expressions::expression_ptr(expressions::make_scalar_expression(&arena,
                                                                            expressions::scalar_type::star_expand,
                                                                            expressions::key_t{&arena})));
        operators::operator_delete op{&arena,
                                      log_t{},
                                      catalog::oid_t{1},
                                      table::storage_address_t{},
                                      std::pmr::string{&arena},
                                      std::move(returning),
                                      nullptr,
                                      affected_bound};

        if (with_right) {
            operators::operator_ptr right(
                new operators::read_only_operator_t(&arena, log_t{}, operators::operator_type::empty));
            operators::chunks_vector_t right_chunks{&arena};
            right_chunks.emplace_back(make_chunk(&arena, rows, width));
            vector::indexing_vector_t identity{&arena, uint64_t{0}, rows};
            right_chunks.back().slice(identity, rows);
            REQUIRE(right_chunks.back().data[1].get_vector_type() == vector::vector_type::DICTIONARY);
            right->set_output(operators::make_operator_data(&arena, std::move(right_chunks)));
            op.set_children(nullptr, right);
        }

        auto left = make_chunk(&arena, rows, width);
        operators::chunks_vector_t out{&arena};

        arena.reset();
        REQUIRE_FALSE(op.push(&ctx, std::move(left), out).contains_error());
        return {arena.allocations(), arena.allocated_bytes()};
    }
} // namespace

TEST_CASE("integration::cpp::delete_using_returning::pairing_the_matched_right_rows_does_not_allocate_per_row") {
    const auto wide = taken_for(kWide);
    const auto narrow = taken_for(kNarrow);
    INFO("rows " << kNarrow << ": " << narrow.allocations << " allocations, " << narrow.bytes << " bytes; rows "
                 << kWide << ": " << wide.allocations << " allocations, " << wide.bytes << " bytes");
    // 10x the rows: linear in the rows is about 10x, per cell about 10x allocations, quadratic about 100x bytes.
    CHECK(wide.allocations < narrow.allocations * 2);
    CHECK(wide.bytes < narrow.bytes * 20);
}

// The matched rows are staged as slices of the scan batch and of the right chunk. Their string cells are not
// copied once per staging site, so the bytes taken by push() do not grow with the width of the strings.
TEST_CASE("integration::cpp::delete_using_returning::staging_the_matched_rows_does_not_copy_strings") {
    const auto narrow = taken_for(kWide, 64);
    const auto wide = taken_for(kWide, 640);
    INFO("width 64: " << narrow.bytes << " bytes; width 640: " << wide.bytes << " bytes");
    CHECK(wide.bytes < narrow.bytes + 4096);
}

TEST_CASE("integration::cpp::delete_returning::staging_the_matched_rows_does_not_copy_strings") {
    const auto narrow = taken_for(kWide, 64, false);
    const auto wide = taken_for(kWide, 640, false);
    INFO("width 64: " << narrow.bytes << " bytes; width 640: " << wide.bytes << " bytes");
    CHECK(wide.bytes < narrow.bytes + 4096);
}

// A few matches out of many rows are gathered once into the output, so the batch is not retained by the cursor,
// and nothing else is copied: 4 rows of 2 string cells are far below one copy of the 640 scanned rows.
TEST_CASE("integration::cpp::delete_using_returning::sparse_matches_are_gathered_once") {
    const auto narrow = taken_for(kWide, 64, true, logical_plan::row_cap_t{4});
    const auto wide = taken_for(kWide, 640, true, logical_plan::row_cap_t{4});
    INFO("width 64: " << narrow.bytes << " bytes; width 640: " << wide.bytes << " bytes");
    CHECK(wide.bytes < narrow.bytes + 32768);
}

namespace {
    // Bytes handed out and not yet given back, so what an operator keeps between pushes can be read off it.
    class live_bytes_resource_t final : public std::pmr::memory_resource {
    public:
        explicit live_bytes_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

        std::size_t live() const noexcept { return live_; }

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            live_ += bytes;
            return upstream_->allocate(bytes, alignment);
        }
        void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) override {
            live_ -= bytes;
            upstream_->deallocate(ptr, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
        std::size_t live_{0};
    };

    // `pushes` scan batches of kWide rows each, of which the 3 with the smallest ids match the single USING row.
    // The operator has no RETURNING, so all it keeps is what the flush at the end needs.
    std::size_t retained_by_sparse_pushes(uint64_t pushes, uint64_t width) {
        core::pmr::otterbrix_resource upstream;
        live_bytes_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        REQUIRE(parameters.add(vector::make_scalar(&arena, int64_t{3}), vector::parameter_kind::literal) ==
                core::parameter_id_t{0});
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox()};

        expressions::key_t id{&arena, "id"};
        id.set_path(std::pmr::vector<size_t>(std::initializer_list<size_t>{0}, &arena));
        expressions::expression_ptr condition =
            expressions::make_compare_expression(&arena,
                                                 expressions::compare_type::lt,
                                                 expressions::param_storage{id},
                                                 expressions::param_storage{core::parameter_id_t{0}});
        operators::operator_delete op{&arena,
                                      log_t{},
                                      catalog::oid_t{1},
                                      table::storage_address_t{},
                                      std::pmr::string{&arena},
                                      std::pmr::vector<operators::projected_column_t>{&arena},
                                      condition};

        operators::operator_ptr right(
            new operators::read_only_operator_t(&arena, log_t{}, operators::operator_type::empty));
        operators::chunks_vector_t right_chunks{&arena};
        right_chunks.emplace_back(make_chunk(&arena, 1, width));
        right->set_output(operators::make_operator_data(&arena, std::move(right_chunks)));
        op.set_children(nullptr, right);

        operators::chunks_vector_t out{&arena};
        const auto before = arena.live();
        for (uint64_t push = 0; push < pushes; ++push) {
            auto left = make_chunk(&arena, kWide, width);
            REQUIRE_FALSE(op.push(&ctx, std::move(left), out).contains_error());
        }
        return arena.live() - before;
    }
} // namespace

// A scan batch with a few matches must not stay resident until the flush: the staged rows are copied out of it
// once the matches are a small share of the batch.
TEST_CASE("integration::cpp::delete_using::sparse_matches_do_not_keep_their_scan_batches") {
    const auto retained = retained_by_sparse_pushes(4, 640);
    INFO("retained after 4 pushes of " << kWide << " rows of 2 string cells of 640 bytes: " << retained << " bytes");
    CHECK(retained < 64 * 1024);
}

namespace {
    constexpr uint64_t kPushes = 4;

    std::unique_ptr<operators::operator_delete> star_delete(std::pmr::memory_resource* resource) {
        std::pmr::vector<operators::projected_column_t> returning{resource};
        returning.emplace_back(
            resource,
            "*",
            expressions::expression_ptr(expressions::make_scalar_expression(resource,
                                                                            expressions::scalar_type::star_expand,
                                                                            expressions::key_t{resource})));
        return std::make_unique<operators::operator_delete>(resource,
                                                            log_t{},
                                                            catalog::oid_t{1},
                                                            table::storage_address_t{},
                                                            std::pmr::string{resource},
                                                            std::move(returning));
    }

    // Plain DELETE ... RETURNING * of batches whose few rows sit in a scan-sized chunk, as a pushed-down filter
    // leaves them.
    std::size_t retained_by_filtered_batches(uint64_t rows, uint64_t capacity) {
        core::pmr::otterbrix_resource upstream;
        live_bytes_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox()};
        auto op = star_delete(&arena);
        operators::chunks_vector_t out{&arena};
        const auto before = arena.live();
        for (uint64_t push = 0; push < kPushes; ++push) {
            auto batch = make_chunk(&arena, rows, 64, capacity);
            REQUIRE_FALSE(op->push(&ctx, std::move(batch), out).contains_error());
        }
        return arena.live() - before;
    }

    std::size_t retained_by_narrow_batches() {
        core::pmr::otterbrix_resource upstream;
        live_bytes_resource_t arena{&upstream};
        logical_plan::storage_parameters parameters{&arena};
        pipeline::context_t ctx{parameters,
                                actor_zeta::address_t::empty_address(),
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox()};
        auto op = star_delete(&arena);
        operators::chunks_vector_t out{&arena};
        std::pmr::vector<types::complex_logical_type> types{&arena};
        for (int column = 0; column < 10; ++column) {
            types.emplace_back(types::logical_type::BIGINT);
        }
        const auto before = arena.live();
        for (uint64_t push = 0; push < kPushes; ++push) {
            vector::data_chunk_t batch(&arena, types, 3);
            for (uint64_t row = 0; row < 3; ++row) {
                for (uint64_t column = 0; column < 10; ++column) {
                    batch.set_value(column, row, static_cast<int64_t>(row));
                }
                batch.row_ids.data<int64_t>()[row] = static_cast<int64_t>(row);
            }
            batch.set_cardinality(3);
            REQUIRE_FALSE(op->push(&ctx, std::move(batch), out).contains_error());
        }
        return arena.live() - before;
    }
} // namespace

// The share of a batch that matched is read against the chunk's capacity: a slice keeps the whole buffer, however
// few rows it is cardinality says.
TEST_CASE("integration::cpp::delete_returning::a_few_rows_in_a_scan_sized_chunk_are_copied_out") {
    const auto retained = retained_by_filtered_batches(3, 1024);
    INFO("retained after " << kPushes << " pushes of 3 rows in a 1024-row chunk: " << retained << " bytes");
    // Two string columns take one 2 KiB heap block each; a retained 1024-row chunk is about 45 KiB per push.
    CHECK(retained < kPushes * 8192);
}

// What a slice itself costs for a narrow chunk: no payload buffer per column.
TEST_CASE("integration::cpp::delete_returning::slicing_a_narrow_chunk_is_cheap") {
    const auto retained = retained_by_narrow_batches();
    INFO("retained after " << kPushes << " pushes of 3 rows x 10 BIGINT: " << retained << " bytes");
    CHECK(retained < kPushes * 1024);
}
