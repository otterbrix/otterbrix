// parent_col_indices is stamped by enrich against the parent's resolved schema, so no live SQL
// plan can drive an out-of-range index; this test pins the guard by driving the operator directly.

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

    class stub_dml_t final : public operators::read_write_operator_t {
    public:
        stub_dml_t(std::pmr::memory_resource* resource, operators::operator_data_ptr rows)
            : read_write_operator_t(resource, log_t{}, operators::operator_type::insert) {
            constraint_input_ = std::move(rows);
        }
    };

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

    bool run_cascade(std::pmr::memory_resource* resource,
                     catalog::fk_info_t fk,
                     std::size_t parent_columns,
                     std::string* err_out) {
        operators::operator_ptr cascade(new operators::operator_fk_cascade_t(resource, log_t{}, std::move(fk)));
        operators::operator_ptr source(new stub_dml_t(resource, parent_rows(resource, parent_columns)));
        cascade->set_children(source);

        int disk_actor_stand_in = 0;
        pipeline::context_t ctx(logical_plan::storage_parameters{resource},
                                actor_zeta::address_t{resource, &disk_actor_stand_in},
                                pipeline::no_mailbox(),
                                pipeline::no_mailbox());

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

// chunk.data[par_indices[j]] is a raw pmr::vector::operator[] with no bounds check; the child
// side already guards its own width, this closes the same gap on the parent side.
TEST_CASE("fk cascade: a parent column index past the end of the row is refused", "[fk_cascade]") {
    auto resource = core::pmr::otterbrix_resource();

    INFO("the matched parent rows are 2 columns wide; the constraint names column 5");
    std::string err;
    REQUIRE(run_cascade(&resource, fk_with_parent_index(5), /*parent_columns=*/2, &err));
    INFO("error: " << err);
    REQUIRE(err.find("2") != std::string::npos);
    REQUIRE(err.find("5") != std::string::npos);
}
