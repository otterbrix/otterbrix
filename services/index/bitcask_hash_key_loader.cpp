#include "bitcask_hash_key_loader.hpp"

#include "bitcask_index_disk.hpp"

#include <memory>

namespace services::index {

    bitcask_hash_key_loader_t::bitcask_hash_key_loader_t(const bitcask_index_disk_t& bitcask)
        : bitcask_(&bitcask) {}

    bool bitcask_hash_key_loader_t::load(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const {
        return bitcask_->load_hash_key_at(segment_id, value_offset, out_key);
    }

    bool bitcask_hash_key_loader_t::load_unlocked(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const {
        return bitcask_->load_hash_key_at_unlocked(segment_id, value_offset, out_key);
    }

    components::index::disk_hash_storage_t::full_key_loader_t bitcask_hash_key_loader_t::callback() const {
        return [this](uint32_t segment_id, uint64_t value_offset, std::string& out_key, bool lock_bitcask) {
            return lock_bitcask ? load(segment_id, value_offset, out_key)
                                : load_unlocked(segment_id, value_offset, out_key);
        };
    }

    components::index::disk_hash_storage_t::full_key_loader_t
    make_bitcask_hash_key_loader(const bitcask_index_disk_t& bitcask) {
        auto loader = std::make_shared<bitcask_hash_key_loader_t>(bitcask);
        return [loader](uint32_t segment_id, uint64_t value_offset, std::string& out_key, bool lock_bitcask) {
            return lock_bitcask ? loader->load(segment_id, value_offset, out_key)
                                : loader->load_unlocked(segment_id, value_offset, out_key);
        };
    }

} // namespace services::index
