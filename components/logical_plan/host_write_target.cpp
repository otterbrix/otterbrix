#include "host_write_target.hpp"

#include "node_delete.hpp"
#include "node_insert.hpp"
#include "node_update.hpp"

namespace components::logical_plan {

    namespace {
        resolved_table_metadata_t declared_table(const node_extension_t& relation) {
            resolved_table_metadata_t table;
            table.name = std::string{relation.name()};
            table.columns.reserve(relation.columns().size());
            std::int32_t position = 0;
            for (const auto& column : relation.columns()) {
                resolved_column_metadata_t declared;
                declared.attname = std::string{column.alias()};
                declared.type = column;
                declared.attnum = position + 1;
                declared.chunk_position = position;
                table.columns.push_back(std::move(declared));
                ++position;
            }
            return table;
        }

        template<class Write>
        void bind(node_t& write, host_write_target_ptr target) {
            auto& typed = static_cast<Write&>(write);
            write.set_table_metadata(&target->metadata());
            typed.set_host_target(std::move(target));
        }
    } // namespace

    host_write_target_t::host_write_target_t(node_extension_ptr relation, extension_write_fn write)
        : relation_(std::move(relation))
        , write_(write)
        , metadata_(declared_table(*relation_)) {}

    core::error_t bind_host_write_target(std::pmr::memory_resource* resource,
                                         node_t& write,
                                         node_extension_ptr relation,
                                         extension_write_fn write_fn) {
        if (!relation || write_fn == nullptr) {
            return core::error_t{core::error_code_t::create_physical_plan_error,
                                 std::pmr::string{"a host write target needs its relation and a write function",
                                                  resource}};
        }
        host_write_target_ptr target{new host_write_target_t{std::move(relation), write_fn}};
        switch (write.type()) {
            case node_type::insert_t:
                bind<node_insert_t>(write, std::move(target));
                return core::error_t::no_error();
            case node_type::update_t:
                bind<node_update_t>(write, std::move(target));
                return core::error_t::no_error();
            case node_type::delete_t:
                bind<node_delete_t>(write, std::move(target));
                return core::error_t::no_error();
            default:
                return core::error_t{core::error_code_t::create_physical_plan_error,
                                     std::pmr::string{"only an INSERT, UPDATE or DELETE writes into a host relation",
                                                      resource}};
        }
    }

    const host_write_target_t* host_write_target(const node_t& node) noexcept {
        switch (node.type()) {
            case node_type::insert_t:
                return static_cast<const node_insert_t&>(node).host_target().get();
            case node_type::update_t:
                return static_cast<const node_update_t&>(node).host_target().get();
            case node_type::delete_t:
                return static_cast<const node_delete_t&>(node).host_target().get();
            default:
                return nullptr;
        }
    }

} // namespace components::logical_plan
