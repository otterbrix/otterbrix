#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>

#include <string>

// Two engines in one process share nothing, their function registries included: a UDF one of
// them registers or unregisters is invisible to the other.

using namespace test_helpers;
using namespace components::compute;

namespace {

    template<bool Even>
    core::error_t parity_exec(kernel_context&, const components::vector::data_chunk_t& in, components::vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<bool>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = (source[row] % 2 == 0) == Even;
        }
        return core::error_t::no_error();
    }

    template<bool Even>
    function_ptr make_parity(std::pmr::memory_resource* resource, const std::string& name) {
        auto fn = std::make_unique<vector_function>(name, arity::unary(), function_doc{"", "", {"arg"}, false}, 1);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::BOOLEAN)});
        auto added = fn->add_kernel(resource, vector_kernel{std::move(sig), parity_exec<Even>});
        REQUIRE_FALSE(added.contains_error());
        return fn;
    }

    configuration::config engine_config(const std::string& leaf) {
        auto config = make_test_config(integration_fixture_path("test_function_registry_per_engine/" + leaf));
        config.log.level = log_t::level::off;
        return config;
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE fdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE fdb.t (id BIGINT);")->is_success());
        REQUIRE(exec(d, "INSERT INTO fdb.t (id) VALUES (2), (4), (6), (7);")->is_success());
    }

    std::size_t rows_where(otterbrix::wrapper_dispatcher_t* d, const std::string& function) {
        auto cur = exec(d, "SELECT id FROM fdb.t WHERE " + function + "(id);");
        INFO("SELECT through " << function << ": " << (cur->is_error() ? cur->get_error().what.c_str() : "ok"));
        REQUIRE(cur->is_success());
        return cur->size();
    }

} // namespace

TEST_CASE("integration::cpp::function_registry_per_engine::a_udf_of_one_engine_is_not_seen_by_another") {
    auto a = test_make_otterbrix(engine_config("seen_a"));
    auto b = test_make_otterbrix(engine_config("seen_b"));
    seed(a->dispatcher());
    seed(b->dispatcher());

    const otterbrix::session_id_t session;
    REQUIRE_FALSE(a->dispatcher()->register_udf(session, make_parity<true>(a->dispatcher()->resource(), "a_parity"))
                      .contains_error());
    REQUIRE_FALSE(b->dispatcher()->register_udf(session, make_parity<false>(b->dispatcher()->resource(), "b_parity"))
                      .contains_error());

    CHECK(rows_where(a->dispatcher(), "a_parity") == 3);
    CHECK(rows_where(b->dispatcher(), "b_parity") == 1);
    CHECK(exec(a->dispatcher(), "SELECT id FROM fdb.t WHERE b_parity(id);")->is_error());
}

TEST_CASE("integration::cpp::function_registry_per_engine::an_unregister_in_one_engine_leaves_another_alone") {
    auto a = test_make_otterbrix(engine_config("unregister_a"));
    auto b = test_make_otterbrix(engine_config("unregister_b"));
    seed(a->dispatcher());
    seed(b->dispatcher());

    const otterbrix::session_id_t session;
    REQUIRE_FALSE(a->dispatcher()->register_udf(session, make_parity<true>(a->dispatcher()->resource(), "shared"))
                      .contains_error());
    REQUIRE_FALSE(b->dispatcher()->register_udf(session, make_parity<true>(b->dispatcher()->resource(), "shared"))
                      .contains_error());

    REQUIRE_FALSE(a->dispatcher()
                      ->unregister_udf(session, "shared", {components::types::logical_type::BIGINT})
                      .contains_error());
    CHECK(rows_where(b->dispatcher(), "shared") == 3);
}
