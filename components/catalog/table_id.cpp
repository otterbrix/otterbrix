#include "table_id.hpp"

namespace components::catalog {
    namespace {
        template<size_t... Idx>
        collection_full_name_t name_from_vector(const table_id& id, std::index_sequence<Idx...> idx) {
            const auto& ns = id.get_namespace();
            return collection_full_name_t{ns[ns.size() - idx.size() + Idx].c_str()..., id.table_name().c_str()};
        }

        table_namespace_t split_namespace(std::string_view str, std::pmr::memory_resource* resource, char delim = '.') {
            table_namespace_t result(resource);
            size_t start = 0;

            while (true) {
                size_t end = str.find(delim, start);
                if (end == std::string_view::npos) {
                    if (start < str.size()) {
                        result.emplace_back(str.substr(start));
                    }
                    break;
                }

                result.emplace_back(str.substr(start, end - start));
                start = end + 1;
            }

            return result;
        }
    } // namespace

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

    table_id::table_id(std::pmr::memory_resource* resource, const collection_full_name_t& full_name)
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

    bool table_id::operator==(const table_id& other) const {
        return namespace_parts_ == other.namespace_parts_ && name_ == other.name_;
    }

    table_id table_id::parse(const std::string& identifier_str, std::pmr::memory_resource* resource) {
        return {resource, split_namespace(std::string_view(identifier_str), resource)};
    }

    const table_namespace_t& table_id::get_namespace() const { return namespace_parts_; }

    const std::pmr::string& table_id::table_name() const { return name_; }

    collection_full_name_t table_id::collection_full_name() const {
        switch (namespace_parts_.size()) {
            default: // same as full.size() > 2
                return name_from_vector(*this, std::make_index_sequence<3>());
            case 2:
                return name_from_vector(*this, std::make_index_sequence<2>());
            case 1:
                return name_from_vector(*this, std::make_index_sequence<1>());
            case 0:
                return {"", name_.c_str()};
        }
    }

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

    std::string table_id::to_string() const { return std::string(to_pmr_string()); }
} // namespace components::catalog
