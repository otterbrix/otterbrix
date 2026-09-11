#include "wal_page.hpp"

#include <absl/crc/crc32c.h>

namespace services::wal {

#ifdef DEV_MODE
    namespace {
        wal_file_interposer_t* dev_wal_file_interposer_ = nullptr;
    } // namespace

    void dev_set_wal_file_interposer(wal_file_interposer_t* interposer) { dev_wal_file_interposer_ = interposer; }

    wal_file_interposer_t* dev_wal_file_interposer() { return dev_wal_file_interposer_; }
#endif

    uint32_t wal_page_header_t::compute_page_crc(const char* page_data) {
        return static_cast<uint32_t>(absl::ComputeCrc32c({page_data, PAGE_SIZE}));
    }

} // namespace services::wal
