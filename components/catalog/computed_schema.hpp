#pragma once

#include "schema.hpp"
#include "versioned_trie/versioned_trie.hpp"

#include <components/types/types.hpp>
#include <unordered_map>

namespace components::catalog {
    class computed_schema {
    public:
        explicit computed_schema(std::pmr::memory_resource* resource);

        void append(std::pmr::string json, const types::complex_logical_type& type);
        void drop(std::pmr::string json, const types::complex_logical_type& type);
        void drop_n(std::pmr::string json, const types::complex_logical_type& type, size_t n);

        [[nodiscard]] std::vector<types::complex_logical_type> find_field_versions(const std::pmr::string& name) const;
        [[nodiscard]] types::complex_logical_type latest_types_struct() const;

    private:
        using refcounted_entry_t = std::reference_wrapper<const versioned_value<types::complex_logical_type>>;

        bool try_use_refcout(const std::pmr::string& json,
                             const types::complex_logical_type& type,
                             bool is_append,
                             size_t n = 1);

        versioned_trie<std::pmr::string, types::complex_logical_type> fields_;
        std::pmr::unordered_map<std::pmr::string, refcounted_entry_t> existing_versions_;
    };
} // namespace components::catalog
