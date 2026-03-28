#pragma once

#include <components/types/types.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace components::catalog {
    class computed_schema {
    public:
        explicit computed_schema(std::pmr::memory_resource* resource, uint64_t sparse_threshold = 0);

        // Constructor with explicit pinned columns: named columns always go to main table
        // (no threshold-based promotion for them). When pinned_columns is non-empty,
        // only pinned columns bypass sparse storage; all others remain threshold-based.
        explicit computed_schema(std::pmr::memory_resource* resource,
                                 uint64_t sparse_threshold,
                                 std::unordered_set<std::string> pinned_columns);

        // Add a (field_name, type) pair. No-op if already present.
        void append(std::pmr::string field_name, const types::complex_logical_type& type);
        void append_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        // Remove a (field_name, type) pair.
        void drop(std::pmr::string field_name, const types::complex_logical_type& type);
        void drop_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        [[nodiscard]] std::vector<types::complex_logical_type>
        find_field_versions(const std::pmr::string& field_name) const;

        // Returns logical schema:
        // - When sparse_threshold > 0: prepends _id (BIGINT), excludes still-sparse columns
        // - Otherwise: all columns in insertion order
        [[nodiscard]] types::complex_logical_type latest_types_struct() const;

        // Physical storage column name: "__field_name__TYPENUM"
        [[nodiscard]] static std::string storage_column_name(const std::string& field_name,
                                                             const types::complex_logical_type& type);

        [[nodiscard]] bool has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const;

        // --- Sparse column tracking ---

        [[nodiscard]] uint64_t sparse_threshold() const { return sparse_threshold_; }

        // Returns true if the given physical column name is still in sparse storage.
        [[nodiscard]] bool is_sparse(const std::pmr::string& phys_name) const;

        // Increment non-null count for a sparse column. Returns true if the column was promoted.
        bool increment_non_null(const std::pmr::string& phys_name, uint64_t count);

        // Returns true if any column is still sparse.
        [[nodiscard]] bool has_any_sparse() const;

        // Non-null count for a sparse column.
        [[nodiscard]] uint64_t get_non_null_count(const std::pmr::string& phys_name) const;

        struct sparse_column_info {
            std::string field_name;          // logical field name
            types::complex_logical_type type; // column type
            std::string phys_name;           // storage_column_name(field_name, type)
        };

        // Returns all (field_name, type, phys_name) pairs that are still sparse.
        [[nodiscard]] std::vector<sparse_column_info> sparse_columns() const;

        // Returns and clears the list of columns that were first seen as pinned since the last call.
        // Used by the dispatcher to call storage_add_column for newly-seen pinned columns.
        [[nodiscard]] std::vector<sparse_column_info> take_newly_pinned();

        // Sparse physical table name for a given collection + field + type.
        [[nodiscard]] static std::string sparse_table_name(const std::string& collection,
                                                           const std::string& field_name,
                                                           const types::complex_logical_type& type);

    private:
        // Marks a physical column as promoted (no longer sparse).
        void promote_to_regular(const std::pmr::string& phys_name);

        // field_name -> list of types currently present
        std::pmr::unordered_map<std::pmr::string,
                                std::pmr::vector<types::complex_logical_type>>
            fields_;

        // Preserves insertion order of (field_name, type) pairs for physical column ordering.
        std::pmr::vector<std::pair<std::pmr::string, types::complex_logical_type>> column_order_;

        // Sparse tracking (only used when sparse_threshold_ > 0)
        uint64_t sparse_threshold_{0};
        // physical_name -> non-null count
        std::pmr::unordered_map<std::pmr::string, uint64_t> non_null_counts_;
        // physical_name -> is_still_sparse
        std::pmr::unordered_map<std::pmr::string, bool> sparse_flags_;
        // Tracks the order in which columns are promoted from sparse → regular.
        // This matches the order columns are added to the physical table (via storage_add_column).
        // Used by latest_types_struct() so logical schema index == physical column index.
        std::pmr::vector<std::pair<std::pmr::string, types::complex_logical_type>> promoted_order_;
        // Reverse map: phys_name -> (field_name, type) for promote_to_regular lookup.
        std::pmr::unordered_map<std::pmr::string,
                                std::pair<std::pmr::string, types::complex_logical_type>>
            phys_to_field_type_;

        // Pinned columns: when non-empty, these field names always go to main table (never sparse).
        std::unordered_set<std::string> pinned_field_names_;
        // Columns first seen as pinned since last take_newly_pinned() call.
        std::vector<sparse_column_info> newly_pinned_;
    };
} // namespace components::catalog
