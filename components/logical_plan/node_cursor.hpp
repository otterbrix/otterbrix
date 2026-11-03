#pragma once

#include "node.hpp"

#include <cstdint>
#include <limits>
#include <string>

namespace components::logical_plan {

    // FETCH ALL / MOVE ALL: every row the portal has left.
    inline constexpr uint64_t fetch_all = std::numeric_limits<uint64_t>::max();

    // DECLARE name CURSOR FOR <query>: the query is the one child. Only a cursor that reads forward inside its
    // transaction block is declared (PostgreSQL 18 portalcmds.c PerformCursorOpen).
    class node_declare_cursor_t final : public node_t {
    public:
        node_declare_cursor_t(std::pmr::memory_resource* resource, std::string name);

        const std::string& name() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
    };

    // FETCH [FORWARD] count FROM name, MOVE when it only moves. The portal is named by a SQL cursor's name, or by the
    // id a host's window left open (name empty).
    class node_fetch_t final : public node_t {
    public:
        node_fetch_t(std::pmr::memory_resource* resource, std::string name, uint64_t count, bool is_move);
        node_fetch_t(std::pmr::memory_resource* resource, uint64_t portal_id, uint64_t count);

        const std::string& name() const noexcept;
        uint64_t portal_id() const noexcept;
        void set_portal_id(uint64_t portal_id) noexcept;
        uint64_t count() const noexcept;
        bool is_move() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        uint64_t portal_id_;
        uint64_t count_;
        bool is_move_;
    };

    // CLOSE name, CLOSE ALL, or the close of a host's window by its id.
    class node_close_cursor_t final : public node_t {
    public:
        // An empty name with no id is CLOSE ALL.
        node_close_cursor_t(std::pmr::memory_resource* resource, std::string name);
        node_close_cursor_t(std::pmr::memory_resource* resource, uint64_t portal_id);

        const std::string& name() const noexcept;
        uint64_t portal_id() const noexcept;
        void set_portal_id(uint64_t portal_id) noexcept;
        bool closes_all() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        uint64_t portal_id_;
    };

    using node_declare_cursor_ptr = boost::intrusive_ptr<node_declare_cursor_t>;
    using node_fetch_ptr = boost::intrusive_ptr<node_fetch_t>;
    using node_close_cursor_ptr = boost::intrusive_ptr<node_close_cursor_t>;

    node_declare_cursor_ptr make_node_declare_cursor(std::pmr::memory_resource* resource, std::string name);
    node_fetch_ptr make_node_fetch(std::pmr::memory_resource* resource, std::string name, uint64_t count, bool is_move);
    node_fetch_ptr make_node_fetch(std::pmr::memory_resource* resource, uint64_t portal_id, uint64_t count);
    node_close_cursor_ptr make_node_close_cursor(std::pmr::memory_resource* resource, std::string name);
    node_close_cursor_ptr make_node_close_cursor(std::pmr::memory_resource* resource, uint64_t portal_id);

} // namespace components::logical_plan
