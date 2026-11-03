#include "registry_changes.hpp"

namespace services::dispatcher {

    registry_changes_t registry_changes_t::copy(std::pmr::memory_resource* resource) const {
        registry_changes_t out;
        out.functions.reserve(functions.size());
        for (const auto& change : functions) {
            out.functions.push_back(function_change_t{change.uid,
                                                      change.schema_oid,
                                                      change.function ? change.function->get_copy(resource)
                                                                      : components::compute::no_function()});
        }
        out.casts = casts;
        return out;
    }

    void registry_changes_t::append(registry_changes_t more) {
        for (auto& change : more.functions) {
            functions.push_back(std::move(change));
        }
        for (auto& change : more.casts) {
            casts.push_back(std::move(change));
        }
    }

    core::error_t registry_changes_t::apply_to(components::compute::function_registry_t& registry,
                                               components::casts::cast_registry_t* cast_registry,
                                               std::pmr::memory_resource* resource) const {
        for (const auto& change : functions) {
            if (!change.function) {
                if (!registry.remove_function(change.uid)) {
                    return core::error_t{core::error_code_t::internal_error,
                                         std::pmr::string{"the registry holds no function of uid " +
                                                              std::to_string(change.uid) + " to drop",
                                                          resource}};
                }
                continue;
            }
            if (registry.get_function(change.uid) != nullptr) {
                return core::error_t{
                    core::error_code_t::internal_error,
                    std::pmr::string{"the registry already holds a function of uid " + std::to_string(change.uid),
                                     resource}};
            }
            if (auto added = registry.add_function_with_uid(change.uid, change.function->get_copy(resource));
                added.has_error()) {
                return added.error();
            }
            registry.set_schema(change.uid, change.schema_oid);
        }
        if (cast_registry == nullptr) {
            return core::error_t::no_error();
        }
        for (const auto& change : casts) {
            if (!change.entry.has_value()) {
                if (!cast_registry->remove(change.source, change.target)) {
                    return core::error_t{core::error_code_t::internal_error,
                                         std::pmr::string{"the registry holds no cast to drop", resource}};
                }
                continue;
            }
            if (auto added =
                    cast_registry->add(change.source, change.target, components::casts::cast_entry(*change.entry));
                added.contains_error()) {
                return added;
            }
        }
        return core::error_t::no_error();
    }

    void pending_registry_changes_t::add(registry_changes_t changes) { changes_.append(std::move(changes)); }

    std::size_t pending_registry_changes_t::latest_(std::string_view name) const noexcept {
        for (std::size_t i = savepoints_.size(); i > 0; --i) {
            if (savepoints_[i - 1].name == name) {
                return i - 1;
            }
        }
        return savepoints_.size();
    }

    void pending_registry_changes_t::savepoint(std::string_view name) {
        savepoints_.push_back(savepoint_t{std::string{name}, changes_.functions.size(), changes_.casts.size()});
    }

    void pending_registry_changes_t::release_savepoint(std::string_view name) {
        const auto at = latest_(name);
        if (at < savepoints_.size()) {
            savepoints_.resize(at);
        }
    }

    void pending_registry_changes_t::rollback_to_savepoint(std::string_view name) {
        const auto at = latest_(name);
        if (at == savepoints_.size()) {
            return;
        }
        const auto& kept = savepoints_[at];
        changes_.functions.erase(changes_.functions.begin() + static_cast<std::ptrdiff_t>(kept.functions),
                                 changes_.functions.end());
        changes_.casts.erase(changes_.casts.begin() + static_cast<std::ptrdiff_t>(kept.casts), changes_.casts.end());
        savepoints_.resize(at + 1);
    }

} // namespace services::dispatcher
