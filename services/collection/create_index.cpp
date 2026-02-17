#include "executor.hpp"

#include <components/logical_plan/node_create_index.hpp>

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