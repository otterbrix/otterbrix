#include "node_create_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       core::relname_t relname,
                                                       bool if_not_exists)
        : node_t(resource, node_type::create_collection_t)
        , relname_(std::move(static_cast<std::string&>(relname)))
        , if_not_exists_(if_not_exists)
        , relkind_(components::catalog::relkind::computed) {}

    node_create_collection_t::node_create_collection_t(std::pmr::memory_resource* resource,
                                                       core::relname_t relname,
                                                       std::vector<table::column_definition_t> column_definitions,
                                                       std::vector<table::table_constraint_t> constraints,
                                                       bool if_not_exists)
        : node_t(resource, node_type::create_collection_t)
        , relname_(std::move(static_cast<std::string&>(relname)))
        , column_definitions_(std::move(column_definitions))
        , constraints_(std::move(constraints))
        , if_not_exists_(if_not_exists)
        , relkind_(column_definitions_.empty() ? components::catalog::relkind::computed
                                               : components::catalog::relkind::regular) {}

    std::pmr::vector<types::complex_logical_type> node_create_collection_t::schema() const {
        std::pmr::vector<types::complex_logical_type> result(resource());
        result.reserve(column_definitions_.size());
        for (const auto& col : column_definitions_) {
            result.push_back(col.type());
        }
        return result;
    }

    hash_t node_create_collection_t::hash_impl() const { return 0; }

    std::string node_create_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_collection: " << relname_;
        return stream.str();
    }

    std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() {
        return column_definitions_;
    }

    const std::vector<table::column_definition_t>& node_create_collection_t::column_definitions() const {
        return column_definitions_;
    }

    const std::vector<table::table_constraint_t>& node_create_collection_t::constraints() const { return constraints_; }

    node_create_collection_ptr
    make_node_create_collection(std::pmr::memory_resource* resource, core::relname_t relname, bool if_not_exists) {
        return {new node_create_collection_t{resource, std::move(relname), if_not_exists}};
    }

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           core::relname_t relname,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool if_not_exists) {
        return {new node_create_collection_t{resource,
                                             std::move(relname),
                                             std::move(column_definitions),
                                             std::move(constraints),
                                             if_not_exists}};
    }

    core::result_wrapper_t<node_ptr> make_node_record_remote_table(std::pmr::memory_resource* resource,
                                                                   std::string server,
                                                                   std::vector<std::string> path,
                                                                   std::vector<table::column_definition_t> columns) {
        if (path.size() != 2 && path.size() != 3) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"a recorded remote table needs its canonical path inside the server: "
                                                  "schema.table or db.schema.table",
                                                  resource}};
        }
        auto node = make_node_create_collection(resource, core::relname_t{path.back()}, std::move(columns), {});
        node->set_relkind(components::catalog::relkind::foreign);
        const bool has_db = path.size() == 3;
        node->set_dbname(has_db ? std::move(path[0]) : std::string{});
        node->set_remote_slots(std::move(server), std::move(path[has_db ? 1 : 0]));
        return node_ptr{std::move(node)};
    }

} // namespace components::logical_plan
