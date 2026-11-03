#include "node_cursor.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_declare_cursor_t::node_declare_cursor_t(std::pmr::memory_resource* resource, std::string name)
        : node_t(resource, node_type::declare_cursor_t)
        , name_(std::move(name)) {}

    const std::string& node_declare_cursor_t::name() const noexcept { return name_; }

    hash_t node_declare_cursor_t::hash_impl() const { return std::hash<std::string>{}(name_); }

    std::string node_declare_cursor_t::to_string_impl() const { return "$declare_cursor[" + name_ + "]"; }

    node_fetch_t::node_fetch_t(std::pmr::memory_resource* resource, std::string name, uint64_t count, bool is_move)
        : node_t(resource, node_type::fetch_t)
        , name_(std::move(name))
        , portal_id_(0)
        , count_(count)
        , is_move_(is_move) {}

    node_fetch_t::node_fetch_t(std::pmr::memory_resource* resource, uint64_t portal_id, uint64_t count)
        : node_t(resource, node_type::fetch_t)
        , portal_id_(portal_id)
        , count_(count)
        , is_move_(false) {}

    const std::string& node_fetch_t::name() const noexcept { return name_; }
    uint64_t node_fetch_t::portal_id() const noexcept { return portal_id_; }
    void node_fetch_t::set_portal_id(uint64_t portal_id) noexcept { portal_id_ = portal_id; }
    uint64_t node_fetch_t::count() const noexcept { return count_; }
    bool node_fetch_t::is_move() const noexcept { return is_move_; }

    hash_t node_fetch_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, std::hash<std::string>{}(name_));
        boost::hash_combine(hash_value, portal_id_);
        boost::hash_combine(hash_value, count_);
        boost::hash_combine(hash_value, is_move_);
        return hash_value;
    }

    std::string node_fetch_t::to_string_impl() const {
        return std::string{is_move_ ? "$move[" : "$fetch["} + (name_.empty() ? std::to_string(portal_id_) : name_) +
               ", " + std::to_string(count_) + "]";
    }

    node_close_cursor_t::node_close_cursor_t(std::pmr::memory_resource* resource, std::string name)
        : node_t(resource, node_type::close_cursor_t)
        , name_(std::move(name))
        , portal_id_(0) {}

    node_close_cursor_t::node_close_cursor_t(std::pmr::memory_resource* resource, uint64_t portal_id)
        : node_t(resource, node_type::close_cursor_t)
        , portal_id_(portal_id) {}

    const std::string& node_close_cursor_t::name() const noexcept { return name_; }
    uint64_t node_close_cursor_t::portal_id() const noexcept { return portal_id_; }
    void node_close_cursor_t::set_portal_id(uint64_t portal_id) noexcept { portal_id_ = portal_id; }
    bool node_close_cursor_t::closes_all() const noexcept { return name_.empty() && portal_id_ == 0; }

    hash_t node_close_cursor_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, std::hash<std::string>{}(name_));
        boost::hash_combine(hash_value, portal_id_);
        return hash_value;
    }

    std::string node_close_cursor_t::to_string_impl() const {
        return "$close_cursor[" +
               (closes_all()    ? std::string{"ALL"}
                : name_.empty() ? std::to_string(portal_id_)
                                : name_) +
               "]";
    }

    node_declare_cursor_ptr make_node_declare_cursor(std::pmr::memory_resource* resource, std::string name) {
        return {new node_declare_cursor_t{resource, std::move(name)}};
    }

    node_fetch_ptr
    make_node_fetch(std::pmr::memory_resource* resource, std::string name, uint64_t count, bool is_move) {
        return {new node_fetch_t{resource, std::move(name), count, is_move}};
    }

    node_fetch_ptr make_node_fetch(std::pmr::memory_resource* resource, uint64_t portal_id, uint64_t count) {
        return {new node_fetch_t{resource, portal_id, count}};
    }

    node_close_cursor_ptr make_node_close_cursor(std::pmr::memory_resource* resource, std::string name) {
        return {new node_close_cursor_t{resource, std::move(name)}};
    }

    node_close_cursor_ptr make_node_close_cursor(std::pmr::memory_resource* resource, uint64_t portal_id) {
        return {new node_close_cursor_t{resource, portal_id}};
    }

} // namespace components::logical_plan
