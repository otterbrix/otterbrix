#include "session_settings.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace components::catalog {

    namespace {
        using layer_t = std::pmr::vector<setting_assignment_t>;

        void put(layer_t& layer, setting_assignment_t assignment) {
            const auto id = assignment.id;
            auto it = std::find_if(layer.begin(), layer.end(), [id](const auto& a) { return a.id == id; });
            if (it == layer.end()) {
                layer.push_back(std::move(assignment));
                return;
            }
            *it = std::move(assignment);
        }

        void drop(layer_t& layer, setting_id id) {
            layer.erase(std::remove_if(layer.begin(), layer.end(), [id](const auto& a) { return a.id == id; }),
                        layer.end());
        }

        void copy_layer(const layer_t& from, layer_t& to) {
            auto* resource = to.get_allocator().resource();
            to.clear();
            for (const auto& assigned : from) {
                to.push_back(assigned.is_default ? setting_assignment_t::to_default(assigned.id, resource)
                                                 : setting_assignment_t{assigned.id, assigned.value, resource});
            }
        }

        void apply(session_catalog_t& settings, const layer_t& layer, const session_catalog_t& defaults) {
            auto* resource = layer.get_allocator().resource();
            for (const auto& assigned : layer) {
                const auto value = assigned.is_default ? current_setting_value(defaults, assigned.id, resource)
                                                       : std::pmr::string{assigned.value, resource};
                if (auto err = set_setting(settings, assigned.id, value, resource); err.contains_error()) {
                    std::fprintf(stderr, "session_settings: a kept value its setting refuses: %s\n", err.what.c_str());
                    assert(false && "session_settings: a kept value its setting refuses");
                    std::abort();
                }
            }
        }
    } // namespace

    session_settings_t::layers_t::layers_t(std::pmr::memory_resource* resource)
        : session(resource)
        , local(resource) {}

    void session_settings_t::copy_layers(const layers_t& from, layers_t& to) {
        copy_layer(from.session, to.session);
        copy_layer(from.local, to.local);
    }

    session_settings_t::session_settings_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , current_(resource)
        , at_begin_(resource)
        , savepoints_(resource) {}

    void session_settings_t::assign(setting_scope_t scope, setting_id id, std::string_view canonical) {
        assert(scope != setting_scope_t::system && "ALTER SYSTEM changes the shared defaults");
        if (scope == setting_scope_t::local) {
            put(current_.local, setting_assignment_t{id, canonical, resource_});
            return;
        }
        // A SET overrides a SET LOCAL of the same transaction (guc.c push_old_value).
        put(current_.session, setting_assignment_t{id, canonical, resource_});
        drop(current_.local, id);
    }

    void session_settings_t::reset(setting_scope_t scope, setting_id id) {
        assert(scope != setting_scope_t::system && "ALTER SYSTEM changes the shared defaults");
        if (scope == setting_scope_t::local) {
            put(current_.local, setting_assignment_t::to_default(id, resource_));
            return;
        }
        drop(current_.session, id);
        drop(current_.local, id);
    }

    session_catalog_t session_settings_t::effective(const session_catalog_t& defaults) const {
        session_catalog_t settings = defaults;
        apply(settings, current_.session, defaults);
        apply(settings, current_.local, defaults);
        return settings;
    }

    void session_settings_t::begin_transaction() {
        if (in_transaction_) {
            return;
        }
        in_transaction_ = true;
        copy_layers(current_, at_begin_);
        savepoints_.clear();
    }

    void session_settings_t::savepoint(std::string_view name) {
        assert(in_transaction_ && "a savepoint exists only inside a transaction block");
        savepoints_.push_back(savepoint_t{std::pmr::string{name, resource_}, layers_t{resource_}});
        copy_layers(current_, savepoints_.back().layers);
    }

    std::size_t session_settings_t::savepoint_count_through(std::string_view name) const {
        for (std::size_t count = savepoints_.size(); count > 0; --count) {
            if (savepoints_[count - 1].name == name) {
                return count;
            }
        }
        std::fprintf(stderr, "session_settings: no savepoint %.*s\n", static_cast<int>(name.size()), name.data());
        assert(false && "session_settings: no savepoint");
        std::abort();
    }

    void session_settings_t::release_savepoint(std::string_view name) {
        const auto kept = savepoint_count_through(name) - 1;
        while (savepoints_.size() > kept) {
            savepoints_.pop_back();
        }
    }

    void session_settings_t::rollback_to_savepoint(std::string_view name) {
        const auto kept = savepoint_count_through(name);
        copy_layers(savepoints_[kept - 1].layers, current_);
        while (savepoints_.size() > kept) {
            savepoints_.pop_back();
        }
    }

    void session_settings_t::end_transaction(bool committed) {
        if (in_transaction_ && !committed) {
            copy_layers(at_begin_, current_);
        }
        current_.local.clear();
        in_transaction_ = false;
        at_begin_.session.clear();
        at_begin_.local.clear();
        savepoints_.clear();
    }

    bool session_settings_t::empty() const noexcept {
        return !in_transaction_ && current_.session.empty() && current_.local.empty();
    }

} // namespace components::catalog
