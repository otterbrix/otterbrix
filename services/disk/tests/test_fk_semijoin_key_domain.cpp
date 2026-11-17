// fk_hash_semijoin takes its keys in the stored columns' types: the FK operators cast them through the cast
// registry. A key of another type is a refusal, not an empty bucket: ON DELETE CASCADE/RESTRICT reads an empty
// bucket as "this parent has no children". Runs the free function directly, same as
// test_fk_scan_by_keys_semijoin.cpp.

#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <filesystem>
#include <memory_resource>
#include <string>
#include <unistd.h>
#include <vector>

using namespace components::table;
using namespace components::types;
using namespace components::vector;
using components::storage::table_storage_adapter_t;

namespace {

    void append_chunk(data_table_t& table, data_chunk_t& chunk) {
        table_append_state state(chunk.resource());
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data::committed());
    }

    std::filesystem::path fresh_otbx(const char* tag) {
        const auto path = std::filesystem::path("/tmp") / ("test_otterbrix_fk_key_domain_" + std::string(tag) + "_" +
                                                           std::to_string(::getpid()) + ".otbx");
        std::filesystem::remove(path);
        return path;
    }

} // namespace

// An INTEGER key against a BIGINT column holding the same value: a disk that converted keys itself would match it.
TEST_CASE("services::disk::fk_hash_semijoin::a_key_of_another_type_is_refused") {
    core::pmr::otterbrix_resource resource;

    std::vector<column_definition_t> cols;
    cols.emplace_back("id", logical_type::BIGINT);
    services::disk::table_storage_t ts(&resource, std::move(cols), fresh_otbx("key_type"));
    REQUIRE_FALSE(ts.construction_failed());

    {
        auto types = ts.table().copy_types();
        data_chunk_t chunk(&resource, types, 2);
        chunk.set_cardinality(2);
        chunk.set_value(0, 0, int64_t{10});
        chunk.set_value(0, 1, int64_t{20});
        append_chunk(ts.table(), chunk);
    }

    table_storage_adapter_t adapter(ts.table(), &resource);

    std::pmr::vector<complex_logical_type> ktypes{&resource};
    ktypes.emplace_back(logical_type::INTEGER);
    data_chunk_t keys(&resource, ktypes, 1);
    keys.set_cardinality(1);
    keys.set_value(0, 0, int32_t{20});

    std::pmr::vector<std::uint64_t> kidx{&resource};
    kidx.push_back(0);
    auto res_r = services::disk::fk_hash_semijoin(&resource, adapter, kidx, keys, transaction_data::committed());
    REQUIRE(res_r.has_error());
}
