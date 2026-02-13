#include "executor.hpp"

#include <components/index/single_field_index.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <services/disk/index_agent_disk.hpp>

using components::logical_plan::index_type;

using components::index::make_index;
using components::index::single_field_index_t;

using namespace components::cursor;
using namespace core::pmr;

namespace services::collection::executor {

    std::string keys_index(const components::logical_plan::keys_base_storage_t& keys) {
        std::string result;
        for (const auto& key : keys) {
            if (!result.empty()) {
                result += ",";
            }
            result += key.as_string();
        }
        return result;
    }


} // namespace services::collection::executor