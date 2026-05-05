#pragma once

#include <components/catalog/catalog_oids.hpp>

#include <string>
#include <vector>

namespace components::catalog {

    // Parse a comma-separated string of OID integers (e.g. pg_constraint.conkey / confkey).
    // Skips malformed tokens. Returns empty vector for empty input.
    std::vector<oid_t> parse_oid_csv(const std::string& s);

    // Encode a vector of OIDs as a comma-separated string — the inverse of parse_oid_csv.
    // Used when writing pg_constraint.conkey / confkey rows to pg_catalog.
    std::string encode_oid_csv(const std::vector<oid_t>& oids);

} // namespace components::catalog
