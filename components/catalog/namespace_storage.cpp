#include <components/catalog/namespace_storage.hpp>

namespace components::catalog {
    namespace_storage::namespace_storage(std::pmr::memory_resource* resource)
        : namespaces_(resource)
        , registered_types_(resource)
        , resource_(resource) {}

    void namespace_storage::create_namespace(const table_namespace_t& namespace_name) {
        if (namespace_exists(namespace_name)) {
            return;
        }

        if (namespace_name.size() > 1) {
            auto parent = get_parent_namespace(namespace_name);
            if (!namespace_exists(parent)) {
                create_namespace(parent);
            }
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource_);
        namespaces_.insert({path, namespace_info(resource_)});
    }

    void namespace_storage::drop_namespace(const table_namespace_t& namespace_name) {
        if (namespace_name.empty() || !namespace_exists(namespace_name)) {
            return;
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource_);
        namespaces_.erase(path);
    }

    bool namespace_storage::namespace_exists(const table_namespace_t& namespace_name) const {
        if (namespace_name.empty()) {
            return false;
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource_);
        auto it = namespaces_.find(path);
        return it != namespaces_.end();
    }

    void namespace_storage::create_type(const types::complex_logical_type& type) {
        if (type_exists(type.type_name())) {
            return;
        }

        registered_types_.emplace(type.type_name(), type);
    }

    void namespace_storage::drop_type(const std::string& type_name) {
        if (!type_exists(type_name)) {
            return;
        }

        registered_types_.erase(type_name);
    }

    bool namespace_storage::type_exists(const std::string& type_name) const {
        if (registered_types_.empty()) {
            return false;
        }

        auto it = registered_types_.find(type_name);
        return it != registered_types_.end();
    }

    void namespace_storage::create_function(const std::string& function_name, compute::registered_func_id uid) {
        auto it = registered_functions_.find(function_name);
        if (it == registered_functions_.end()) {
            registered_functions_.emplace(function_name, std::vector{std::move(uid)});
        } else {
            it->second.emplace_back(std::move(uid));
        }
    }

    void namespace_storage::drop_function(const std::string& function_name,
                                          const std::pmr::vector<types::complex_logical_type>& inputs) {
        auto it = registered_functions_.find(function_name);
        if (it == registered_functions_.end()) {
            return;
        }

        const auto& overloads = it->second;
        for (const auto& overload : overloads) {
            for (const auto& signature : overload.signatures) {
                if (signature.matches_inputs(inputs)) {
                    registered_functions_.erase(function_name);
                    return;
                }
            }
        }
    }

    bool namespace_storage::check_function_conflicts(const std::string& function_name,
                                                     const std::vector<compute::kernel_signature_t>& signatures) const {
        auto it = registered_functions_.find(function_name);
        if (it == registered_functions_.end()) {
            return true;
        }

        for (const auto& overload : it->second) {
            if (!components::compute::check_signature_conflicts(overload.signatures, signatures, registered_types_)) {
                return false;
            }
        }

        return true;
    }

    bool namespace_storage::function_name_exists(const std::string& function_name) const {
        if (registered_functions_.empty()) {
            return false;
        }

        auto it = registered_functions_.find(function_name);
        return it != registered_functions_.end();
    }

    bool namespace_storage::function_exists(const std::string& function_name,
                                            const std::pmr::vector<types::complex_logical_type>& inputs) const {
        if (registered_functions_.empty()) {
            return false;
        }

        auto it = registered_functions_.find(function_name);
        if (it == registered_functions_.end()) {
            return false;
        }

        const auto& overloads = it->second;
        for (const auto& [uid, signatures] : overloads) {
            for (const auto& signature : signatures) {
                if (signature.matches_inputs(inputs)) {
                    return true;
                }
            }
        }

        return false;
    }

    // todo: reuse list_child
    std::pmr::vector<table_namespace_t> namespace_storage::list_root_namespaces() const {
        std::pmr::vector<table_namespace_t> result(resource_);

        for (auto it = namespaces_.begin(); it != namespaces_.end(); ++it) {
            if (it->key.size() == 1) {
                auto a = it->key;
                table_namespace_t ns(a.begin(), a.end(), resource_);
                result.push_back(std::move(ns));
            }
        }

        return result;
    }

    std::pmr::vector<table_namespace_t>
    namespace_storage::list_child_namespaces(const table_namespace_t& parent) const {
        std::pmr::vector<table_namespace_t> result(resource_);
        if (!namespace_exists(parent)) {
            // use result with: no_such_namespace_exception("Parent namespace does not exist");
            return result;
        }

        table_namespace_t next(resource_);
        if (auto res = namespaces_.longest_match(parent); res.match && !res.leaf) {
            namespaces_.copy_next_key_elements(res, std::back_inserter(next));
            result.reserve(next.size());

            for (auto&& ext : next) {
                table_namespace_t extended = parent;
                extended.emplace_back(std::move(ext));
                result.emplace_back(std::move(extended));
            }
        }

        return result;
    }

    std::pmr::vector<table_namespace_t> namespace_storage::list_all_namespaces() const {
        std::pmr::vector<table_namespace_t> result(resource_);

        for (const auto& [path, info] : namespaces_) {
            table_namespace_t ns(path.begin(), path.end(), resource_);
            result.push_back(std::move(ns));
        }

        return result;
    }

    bool namespace_storage::has_child_namespaces(const table_namespace_t& namespace_name) const {
        auto match_res = namespaces_.longest_match(namespace_name);
        return !match_res.leaf;
    }

    std::pmr::vector<table_namespace_t>
    namespace_storage::get_all_descendants(const table_namespace_t& namespace_name) const {
        std::pmr::vector<table_namespace_t> result(resource_);
        if (!namespace_exists(namespace_name)) {
            // use result with: no_such_namespace_exception("Namespace does not exist");
            return result;
        }

        std::pmr::vector<table_namespace_t> stack(resource_); // recursion may overflow?

        stack.push_back(namespace_name);
        while (!stack.empty()) {
            auto current = std::move(stack.back());
            stack.pop_back();

            auto children = list_child_namespaces(current);
            for (auto&& child : children) {
                stack.push_back(child);
                result.emplace_back(std::move(child));
            }
        }

        return result;
    }

    namespace_storage::namespace_info&
    namespace_storage::get_namespace_info(const components::catalog::table_namespace_t& namespace_name) {
        if (!namespace_exists(namespace_name)) {
            throw std::logic_error("Namespace does not exist");
        }

        return namespaces_.find(namespace_name)->value;
    }

    const types::complex_logical_type& namespace_storage::get_type(const std::string& type_name) const {
        if (!type_exists(type_name)) {
            throw std::logic_error("type is not registered: " + type_name);
        }

        return registered_types_.find(type_name)->second;
    }

    std::pair<compute::function_uid, compute::kernel_signature_t>
    namespace_storage::get_function(const std::string& function_name,
                                    const std::pmr::vector<types::complex_logical_type>& inputs) const {
        if (registered_functions_.empty()) {
            throw std::logic_error("function is not registered: " + function_name);
        }

        auto it = registered_functions_.find(function_name);
        if (it == registered_functions_.end()) {
            throw std::logic_error("function is not registered: " + function_name);
        }

        const auto& overloads = it->second;
        for (const auto& [uid, signatures] : overloads) {
            for (const auto& signature : signatures) {
                if (signature.matches_inputs(inputs)) {
                    return {uid, signature};
                }
            }
        }

        throw std::logic_error("function is not registered: " + function_name);
    }

    void namespace_storage::clear() { namespaces_.clear(); }

    size_t namespace_storage::size() const { return static_cast<size_t>(namespaces_.size()); }

    table_namespace_t namespace_storage::get_parent_namespace(const table_namespace_t& namespace_name) const {
        if (namespace_name.empty()) {
            return table_namespace_t(resource_);
        }

        table_namespace_t parent(namespace_name.begin(), namespace_name.end() - 1, resource_);
        return parent;
    }
} // namespace components::catalog
