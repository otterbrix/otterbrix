#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/types.hpp>
#include <memory_resource>

namespace components::operators {

    class operator_write_data_t
        : public boost::intrusive_ref_counter<operator_write_data_t>
        , public boost::intrusive::list_base_hook<> {
    public:
        using ids_t = std::pmr::vector<size_t>;
        using ptr = boost::intrusive_ptr<operator_write_data_t>;

        explicit operator_write_data_t(std::pmr::memory_resource* resource)
            : ids_(resource) {}

        std::size_t size() const;
        ids_t& ids();
        void append(size_t id);

    private:
        ids_t ids_;
    };

    using operator_write_data_ptr = operator_write_data_t::ptr;

    inline operator_write_data_ptr make_operator_write_data(std::pmr::memory_resource* resource) {
        return {new operator_write_data_t(resource)};
    }

} // namespace components::operators
