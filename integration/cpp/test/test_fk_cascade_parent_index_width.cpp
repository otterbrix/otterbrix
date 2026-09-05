// The parent-side column index must be checked against the row's actual width.
//
// operator_fk_cascade_t builds its child-key lookup out of the DELETE's matched parent
// rows, addressed by fk_.parent_col_indices via `chunk.data[par_indices[j]]`. `data` is a
// std::pmr::vector and operator[] does not check its bound, so an index past the end of the
// parent row would read off the end of the chunk's column array -- a type, then a whole
// vector_t, taken from whatever follows in memory. The CHILD side of the same operator
// already carries a width guard ("too few to hold referencing column at position P"); a
// guard on one side of a pair hides what happens on the other.
//
// The two facts about par_indices that ARE checked (`absent`, and an empty list) are checked
// immediately above this loop, so this case is neither: an index that is a number and is not
// a position in the row it addresses.
//
// Not reachable from SQL: parent_col_indices is stamped by enrich against the parent's
// resolved schema, so a live plan agrees with the rows the DELETE matched. This is the floor
// under that agreement, proven by driving the operator directly.

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/catalog/fk_info.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_fk_cascade.hpp>

#include <memory_resource>
#include <string>

using namespace components;

namespace {

    // A stand-in for the DML the cascade sits on: it carries the matched OLD
    // rows in constraint_input_, which is the single canonical source
    // resolve_constraint_source walks the left_ spine to find. Nothing else about
    // it is ever driven.
    class stub_dml_t final : public operators::read_write_operator_t {
    public:
        stub_dml_t(std::pmr::memory_resource* resource, operators::operator_data_ptr rows)
            : read_write_operator_t(resource, log_t{}, operators::operator_type::insert) {
            constraint_input_ = std::move(rows);
        }
    };

    // One parent row, `columns` BIGINT columns wide.
    operators::operator_data_ptr parent_rows(std::pmr::memory_resource* resource, std::size_t columns) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        for (std::size_t i = 0; i < columns; ++i) {
            types.emplace_back(types::logical_type::BIGINT);
        }
        vector::data_chunk_t chunk{resource, types, 1};
        for (std::size_t i = 0; i < columns; ++i) {
            chunk.set_value(i, 0, int64_t{1});
        }
        chunk.set_cardinality(1);
        return operators::make_operator_data(resource, std::move(chunk));
    }

    // Drive the cascade to the point where it either refuses or reads the
    // parent column. A disk actor IS wired up (address_t compares the pointee,
    // so any non-null pointer is "not the empty address"); nothing is ever
    // enqueued on it, because the key build happens BEFORE the first send.
    bool run_cascade(std::pmr::memory_resource* resource,
                     catalog::fk_info_t fk,
                     std::size_t parent_columns,
                     std::string* err_out) {
        operators::operator_ptr cascade(new operators::operator_fk_cascade_t(resource, log_t{}, std::move(fk)));
        operators::operator_ptr source(new stub_dml_t(resource, parent_rows(resource, parent_columns)));
        cascade->set_children(source);

        pipeline::context_t ctx(logical_plan::storage_parameters{resource});
        int disk_actor_stand_in = 0;
        ctx.disk_address = actor_zeta::address_t{resource, &disk_actor_stand_in};

        auto fut = cascade->await_async_and_resume(&ctx);
        REQUIRE(fut.is_ready());
        std::move(fut).take_ready();

        if (err_out && cascade->has_error()) {
            *err_out = std::string(cascade->get_error().what);
        }
        return cascade->has_error();
    }

    catalog::fk_info_t fk_with_parent_index(std::size_t parent_index) {
        catalog::fk_info_t fk;
        fk.child_col_names = {"pid"};
        fk.parent_col_names = {"id"};
        fk.parent_col_indices = {parent_index};
        fk.child_table_oid = 42;
        fk.parent_table_oid = 43;
        fk.del_action = 'r';
        return fk;
    }

} // namespace

TEST_CASE("fk cascade: a parent column index past the end of the row is refused", "[fk_cascade]") {
    auto resource = core::pmr::otterbrix_resource();

    INFO("the matched parent rows are 2 columns wide; the constraint names column 5");
    std::string err;
    REQUIRE(run_cascade(&resource, fk_with_parent_index(5), /*parent_columns=*/2, &err));
    INFO("error: " << err);
    // The message has to say WHICH column of WHAT width could not be read.
    REQUIRE(err.find("2") != std::string::npos);
    REQUIRE(err.find("5") != std::string::npos);
}
