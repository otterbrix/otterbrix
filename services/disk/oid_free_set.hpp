#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <core/result_wrapper.hpp>
#include <services/wal/base.hpp>

#include <cstdint>
#include <filesystem>
#include <memory_resource>
#include <vector>

namespace services::disk {

    // [begin, end) of oids nothing names any more. A held span may still have records in the WAL: it is handed out
    // only once the journal holds nothing at or below wal_id, or replay would apply them to the oid's next owner.
    struct oid_span_t {
        components::catalog::oid_t begin;
        components::catalog::oid_t end;
        bool held;
        wal::id_t wal_id;
    };

    class oid_free_set_t {
    public:
        explicit oid_free_set_t(std::pmr::memory_resource* resource);

        // Every oid in [first, end) outside in_use (sorted, unique) is free. One found free before keeps its state;
        // one found now is held at current_wal_id.
        void rebuild(const std::pmr::vector<components::catalog::oid_t>& in_use,
                     components::catalog::oid_t first,
                     components::catalog::oid_t end,
                     wal::id_t current_wal_id);

        // A held span whose records are all older than the oldest record the journal still holds.
        void release(wal::id_t oldest_retained_wal_id);

        [[nodiscard]] bool has_released() const noexcept;

        // Takes up to count of the lowest oids that can be handed out, ascending.
        [[nodiscard]] std::pmr::vector<components::catalog::oid_t> claim(std::size_t count);

        [[nodiscard]] const std::pmr::vector<oid_span_t>& spans() const noexcept { return spans_; }
        void assign(std::pmr::vector<oid_span_t> spans);

    private:
        void coalesce_();

        std::pmr::vector<oid_span_t> spans_;
    };

    std::filesystem::path oid_free_set_path(const std::filesystem::path& db_path);

    // No file reads as an empty set.
    [[nodiscard]] core::result_wrapper_t<std::pmr::vector<oid_span_t>>
    read_oid_free_set(std::pmr::memory_resource* resource, const std::filesystem::path& db_path);

    [[nodiscard]] core::error_t persist_oid_free_set(std::pmr::memory_resource* resource,
                                                     const std::filesystem::path& db_path,
                                                     const std::pmr::vector<oid_span_t>& spans);

} // namespace services::disk
