#pragma once

#include <core/handler_by_id.hpp>

namespace services::collection {

    enum class route : uint64_t
    {
        create_documents,
        execute_plan,
        execute_sub_plan,
        size,
        schema,
        close_cursor,
        drop_collection,
        drop_index,
        create_documents_finish,
        execute_plan_finish,
        size_finish,
        schema_finish,
        create_index_finish,
        drop_collection_finish,
    };

    constexpr uint64_t handler_id(route type) { return handler_id(group_id_t::collection, type); }

} // namespace services::collection
