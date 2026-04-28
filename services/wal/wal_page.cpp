#include "wal_page.hpp"

#include <absl/crc/crc32c.h>

namespace services::wal {

uint32_t wal_page_header_t::compute_page_crc(const char* page_data) {
    return static_cast<uint32_t>(
        absl::ComputeCrc32c({page_data, PAGE_SIZE}));
}

} // namespace services::wal
