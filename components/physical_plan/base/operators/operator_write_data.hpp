#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/types.hpp>
#include <map>
#include <memory_resource>

namespace components::base::operators {

    class operator_write_data_t
        : public boost::intrusive_ref_counter<operator_write_data_t>
        , public boost::intrusive::list_base_hook<> {
        struct pair_comparator;

    public:
        using ids_t = std::pmr::vector<size_t>;
        // we need to count (name, type) entries to correctly update computed schema
        using updated_types_map_t = std::pmr::
            map<std::pair<std::pmr::string, components::types::complex_logical_type>, size_t, pair_comparator>;
        using ptr = boost::intrusive_ptr<operator_write_data_t>;

        explicit operator_write_data_t(std::pmr::memory_resource* resource)
            : resource_(resource)
            , ids_(resource)
            , updated_(resource) {}

        ptr copy() const;

        std::size_t size() const;
        ids_t& ids();
        updated_types_map_t& updated_types_map();
        void append(size_t id);

    private:
        struct pair_comparator {
            bool operator()(const std::pair<std::pmr::string, components::types::complex_logical_type>& lhs,
                            const std::pair<std::pmr::string, components::types::complex_logical_type>& rhs) const;
        };

        std::pmr::memory_resource* resource_;
        ids_t ids_;
        updated_types_map_t updated_;
    };

    using operator_write_data_ptr = operator_write_data_t::ptr;

    inline operator_write_data_ptr make_operator_write_data(std::pmr::memory_resource* resource) {
        return {new operator_write_data_t(resource)};
    }

} // namespace components::base::operators
