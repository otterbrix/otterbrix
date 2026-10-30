#pragma once

#include <components/catalog/session_catalog.hpp>
#include <components/catalog/settings.hpp>

#include <memory_resource>
#include <string_view>
#include <vector>

namespace components::catalog {

    // One session's own settings over the defaults every session shares (pg_settings), as PostgreSQL 18 guc.c keeps
    // them: a value SET gives lasts until the session sets it again, one SET LOCAL gives until its transaction ends.
    // The transaction's start and each savepoint keep a copy that ROLLBACK and ROLLBACK TO return to.
    class session_settings_t {
    public:
        explicit session_settings_t(std::pmr::memory_resource* resource);

        // `canonical` is canonical_setting_value's answer; `scope` is session or local. RESET and SET ... TO DEFAULT
        // take the session's own value away; SET LOCAL ... TO DEFAULT masks it with the default until the
        // transaction ends.
        void assign(setting_scope_t scope, setting_id id, std::string_view canonical);
        void reset(setting_scope_t scope, setting_id id);
        session_catalog_t effective(const session_catalog_t& defaults) const;

        void begin_transaction();
        bool in_transaction() const noexcept { return in_transaction_; }
        void savepoint(std::string_view name);
        void release_savepoint(std::string_view name);
        void rollback_to_savepoint(std::string_view name);
        // Also the end of a statement outside a transaction block, whose SET LOCAL ends with it.
        void end_transaction(bool committed);

        // No value of its own and no transaction: nothing to keep.
        bool empty() const noexcept;

    private:
        struct layers_t {
            explicit layers_t(std::pmr::memory_resource* resource);
            // A copy would put its strings on the default resource: copy_layers keeps the destination's.
            layers_t(const layers_t&) = delete;
            layers_t& operator=(const layers_t&) = delete;
            layers_t(layers_t&&) noexcept = default;
            layers_t& operator=(layers_t&&) = delete;
            std::pmr::vector<setting_assignment_t> session;
            std::pmr::vector<setting_assignment_t> local;
        };
        static void copy_layers(const layers_t& from, layers_t& to);
        // How many savepoints there are up to the latest one named `name`, which the transaction holds.
        std::size_t savepoint_count_through(std::string_view name) const;

        struct savepoint_t {
            std::pmr::string name;
            layers_t layers;
        };

        std::pmr::memory_resource* resource_;
        layers_t current_;
        bool in_transaction_{false};
        layers_t at_begin_;
        std::pmr::vector<savepoint_t> savepoints_;
    };

} // namespace components::catalog
