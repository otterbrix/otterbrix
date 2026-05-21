#include "node_fk_check.hpp"

#include <sstream>

namespace components::logical_plan {

    node_fk_check_t::node_fk_check_t(std::pmr::memory_resource* resource,
                                     core::dbname_t dbname,
                                     core::relname_t relname,
                                     catalog::fk_info_t fk)
        : node_t(resource, node_type::fk_check_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , fk_(std::move(fk)) {}

    hash_t node_fk_check_t::hash_impl() const { return 0; }

    std::string node_fk_check_t::to_string_impl() const {
        std::ostringstream s;
        s << "$fk_check: " << relname_ << " -> parent_oid=" << fk_.parent_table_oid;
        return s.str();
    }

} // namespace components::logical_plan
