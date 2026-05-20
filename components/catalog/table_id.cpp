#include "table_id.hpp"

#include <cassert>
#include <sstream>
#include <stdexcept>

namespace components::catalog {
    table_id::table_id(std::pmr::memory_resource* resource, std::pmr::vector<std::pmr::string> full_name)
        : namespace_parts_(std::move(full_name), resource)
        , name_(namespace_parts_.back())
        , resource_(resource) {
        namespace_parts_.erase(namespace_parts_.cend() - 1);
    }

    table_id::table_id(std::pmr::memory_resource* resource, table_namespace_t ns, std::pmr::string name)
        : namespace_parts_(std::move(ns), resource)
        , name_(std::move(name), resource)
        , resource_(resource) {}

    table_id::table_id(std::pmr::memory_resource* resource, const qualified_name_t& full_name)
        : namespace_parts_(resource)
        , name_(full_name.collection)
        , resource_(resource) {
        bool has_schema = !full_name.schema.empty();
        bool has_uid = !full_name.unique_identifier.empty();

        if (has_uid) {
            namespace_parts_.emplace_back(full_name.unique_identifier.c_str());
        }

        if (has_schema || has_uid) {
            namespace_parts_.emplace_back(full_name.schema.c_str());
        }

        namespace_parts_.emplace_back(full_name.database.c_str());
    }

    const table_namespace_t& table_id::get_namespace() const { return namespace_parts_; }

    const std::pmr::string& table_id::table_name() const { return name_; }

    std::pmr::string table_id::to_pmr_string() const {
        std::ostringstream oss;
        for (size_t i = 0; i < namespace_parts_.size(); ++i) {
            if (i > 0)
                oss << ".";
            oss << namespace_parts_[i];
        }
        oss << "." << name_;
        return std::pmr::string(oss.str(), resource_);
    }

    void table_id::set_oid(oid_t oid) {
        // OID is immutable after first assignment — programmer-error precondition.
        // Assert in debug, no-op in release if someone tries to reassign.
        assert((oid_ == INVALID_OID || oid_ == oid) && "table_id::set_oid: OID is immutable after assignment");
        if (oid_ != INVALID_OID && oid_ != oid) {
            return;
        }
        oid_ = oid;
    }
} // namespace components::catalog
