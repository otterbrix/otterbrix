#pragma once

#include <components/index/disk_hash_storage.hpp>

namespace services::index {

    class bitcask_index_disk_t;

    // Delegates truncated hash-index key resolution to bitcask_index_disk_t.
    class bitcask_hash_key_loader_t {
    public:
        explicit bitcask_hash_key_loader_t(const bitcask_index_disk_t& bitcask);

        components::index::disk_hash_storage_t::full_key_loader_t callback() const;

        bool load(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const;
        bool load_unlocked(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const;

    private:
        const bitcask_index_disk_t* bitcask_;
    };

    components::index::disk_hash_storage_t::full_key_loader_t
    make_bitcask_hash_key_loader(const bitcask_index_disk_t& bitcask);

} // namespace services::index
