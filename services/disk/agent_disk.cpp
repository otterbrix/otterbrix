#include "agent_disk.hpp"
#include "manager_disk.hpp"

namespace services::disk {

    using namespace core::filesystem;

    agent_disk_t::agent_disk_t(std::pmr::memory_resource* resource,
                               manager_disk_t* /*manager*/,
                               const path_t& path_db,
                               log_t& log)
        : actor_zeta::basic_actor<agent_disk_t>(resource)
        , log_(log.clone())
        , path_(path_db)
        , fs_(core::filesystem::local_file_system_t())
        , file_wal_id_(nullptr) {
        trace(log_, "agent_disk::create");
        create_directories(path_);
        file_wal_id_ = open_file(fs_,
                                 path_ / "WAL_ID",
                                 file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                 file_lock_type::NO_LOCK);
    }

    agent_disk_t::~agent_disk_t() { trace(log_, "delete agent_disk_t"); }

    auto agent_disk_t::make_type() const noexcept -> const char* { return "agent_disk"; }

    actor_zeta::behavior_t agent_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::fix_wal_id>: {
                co_await actor_zeta::dispatch(this, &agent_disk_t::fix_wal_id, msg);
                break;
            }
            default:
                break;
        }
    }

    agent_disk_t::unique_future<void> agent_disk_t::fix_wal_id(wal::id_t wal_id) {
        trace(log_, "agent_disk::fix_wal_id : {}", wal_id);
        auto id = std::to_string(wal_id);
        file_wal_id_->write(id.data(), id.size(), 0);
        file_wal_id_->truncate(static_cast<int64_t>(id.size()));
        co_return;
    }

} //namespace services::disk
