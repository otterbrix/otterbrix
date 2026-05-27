#include "node_fk_cascade.hpp"

#include <sstream>

namespace components::logical_plan {

    node_fk_cascade_t::node_fk_cascade_t(std::pmr::memory_resource* resource,
                                         core::dbname_t dbname,
                                         core::relname_t relname,
                                         catalog::fk_info_t fk)
        : node_t(resource, node_type::fk_cascade_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , fk_(std::move(fk)) {}

    hash_t node_fk_cascade_t::hash_impl() const { return 0; }

    std::string node_fk_cascade_t::to_string_impl() const {
        std::ostringstream s;
        s << "$fk_cascade: " << relname_ << " child_oid=" << fk_.child_table_oid;
        return s.str();
    }

} // namespace components::logical_plan
