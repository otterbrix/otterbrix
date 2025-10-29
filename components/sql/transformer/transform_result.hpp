#pragma once

#include <components/document/document.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

namespace components::sql::transform {
    struct result_view {
        logical_plan::node_ptr node;
        logical_plan::parameter_node_ptr params;
    };

    struct bind_error {
        bind_error();
        explicit bind_error(std::string what);

        explicit operator bool() const;
        const std::string& what() const;

    private:
        std::string what_;
        bool is_error_;
    };

    class transform_result {
    public:
        using parameter_map_t = std::pmr::unordered_map<size_t, core::parameter_id_t>;
        using insert_location_t = std::pair<size_t, std::string>;
        using insert_map_t = std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>>;
        using insert_docs_t = std::pmr::vector<components::document::document_ptr>;

        transform_result(logical_plan::node_ptr&& node,
                         logical_plan::parameter_node_ptr&& params,
                         parameter_map_t&& param_map,
                         insert_map_t&& param_insert_map,
                         insert_docs_t&& param_insert_docs);

        template<typename T>
        transform_result& bind(size_t id, T&& value) {
            return bind(id, document::value_t(taken_params_.tape(), std::forward<T>(value)));
        }

        transform_result& bind(size_t id, document::value_t value) {
            if (last_error_) {
                return *this;
            }

            if (node_->type() == logical_plan::node_type::insert_t) {
                auto it = param_insert_map_.find(id);
                if (it == param_insert_map_.end()) {
                    last_error_ = bind_error("Parameter with id=" + std::to_string(id) + " not found");
                    return *this;
                }

                for (auto& [i, key] : it->second) {
                    param_insert_docs_.at(i)->set(key, value);
                }
            } else {
                auto it = param_map_.find(id);
                if (it == param_map_.end()) {
                    last_error_ = bind_error("Parameter with id=" + std::to_string(id) + " not found");
                    return *this;
                }

                taken_params_.parameters.insert_or_assign(it->second, std::move(value));
            }

            bound_flags_[id] = true;
            return *this;
        }

        size_t parameter_count() const;

        bool all_bound() const;

        std::variant<result_view, bind_error> finalize();

    private:
        logical_plan::node_ptr node_;
        logical_plan::parameter_node_ptr params_;
        parameter_map_t param_map_;
        insert_map_t param_insert_map_;
        insert_docs_t param_insert_docs_;

        logical_plan::storage_parameters taken_params_;
        std::pmr::unordered_map<size_t, bool> bound_flags_;
        bind_error last_error_;
    };

} // namespace components::sql::transform
