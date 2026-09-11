#include "wal.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <filesystem>
#include <limits>
#include <set>
#include <sstream>
#include <string>

namespace services::wal {

    static std::string segment_filename(const std::string& db_dir_name, uint32_t index) {
        std::ostringstream oss;
        oss << "wal_" << db_dir_name << "_";
        oss.width(6);
        oss.fill('0');
        oss << index;
        return oss.str();
    }

    wal_worker_t::wal_worker_t(std::pmr::memory_resource* resource,
                               log_t& log,
                               configuration::config_wal config,
                               components::catalog::oid_t database_oid)
        : actor_zeta::actor::basic_actor<wal_worker_t>(resource)
        , log_(log.clone())
        , config_(std::move(config))
        , database_oid_(database_oid)
        , database_dir_name_(std::to_string(static_cast<unsigned>(database_oid)))
        , database_dir_(config_.path / database_dir_name_)
        , recovery_error_(core::error_t::no_error())
        , encode_buf_(this->resource()) {
        trace(log_, "wal_worker::create for database_oid={}", static_cast<unsigned>(database_oid_));

        std::filesystem::create_directories(database_dir_);

        // A refusal here is latched, not dropped: a constructor has no caller to answer, but every handler below does.
        recovery_error_ = recover_from_disk();
        if (recovery_error_.contains_error()) {
            error(log_,
                  "wal_worker::create , db_oid={} , the journal could not be read at startup , "
                  "this worker REFUSES every write and every truncate: {}",
                  static_cast<unsigned>(database_oid_),
                  recovery_error_.what);
            return;
        }

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            recovery_error_ = writer_error;
            error(log_,
                  "wal_worker::create , db_oid={} , no segment could be opened for writing: {}",
                  static_cast<unsigned>(database_oid_),
                  recovery_error_.what);
        }
    }

    wal_worker_t::~wal_worker_t() {
        trace(log_, "wal_worker::destroy for database_oid={}", static_cast<unsigned>(database_oid_));
        // Flushed here, where there's still a logger to answer to; the destructor's last-resort flush latches its
        // refusal where nobody reads it.
        if (writer_) {
            if (auto flush_error = writer_->flush(); flush_error.contains_error()) {
                error(log_,
                      "wal_worker::destroy , db_oid={} , the final page did not reach the disk: {}",
                      static_cast<unsigned>(database_oid_),
                      flush_error.what);
            }
        }
        writer_.reset();
    }

    auto wal_worker_t::make_type() const noexcept -> const char* { return "wal_worker"; }

    actor_zeta::behavior_t wal_worker_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::load>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::truncate_before>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::truncate_before, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::current_wal_id>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::current_wal_id, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::write_physical_insert>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::write_physical_insert, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::write_physical_delete>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::write_physical_delete, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::write_physical_update>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::write_physical_update, msg);
                break;
            }
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::write_physical_grow>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::write_physical_grow, msg);
                break;
            }
            default:
                break;
        }
    }

    wal_worker_t::unique_future<wal::id_t> wal_worker_t::current_wal_id(session_id_t session) {
        trace(log_, "wal_worker::current_wal_id , session : {}", session.data());
        co_return id_.load(std::memory_order_relaxed);
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>>
    wal_worker_t::write_physical_insert(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        std::pmr::vector<components::vector::data_chunk_t> chunks,
                                        uint64_t row_start,
                                        uint64_t row_count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_,
              "wal_worker::write_physical_insert , wal_id : {} , txn : {} , rows : {}",
              wal_id,
              txn_id,
              row_count);

        encode_buf_.clear();
        // last_crc_ only takes the crc once append() accepts the record, so a refused write can't leave the chain
        // naming a record never journaled.
        const auto record_crc = encode_insert(encode_buf_,
                                              this->resource(),
                                              last_crc_,
                                              wal_id,
                                              txn_id,
                                              table_oid,
                                              chunks,
                                              row_start,
                                              row_count);

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{std::move(writer_error)};
        }
        if (auto append_error = writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);
            append_error.contains_error()) {
            error(log_,
                  "wal_worker::write_physical_insert , wal_id : {} , txn : {} , the record did NOT reach the "
                  "journal: {}",
                  wal_id,
                  txn_id,
                  append_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(append_error)};
        }
        last_crc_ = record_crc;

        co_return core::result_wrapper_t<wal::id_t>{wal_id};
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>>
    wal_worker_t::write_physical_delete(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        std::pmr::vector<int64_t> row_ids,
                                        uint64_t count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::write_physical_delete , wal_id : {} , txn : {} , count : {}", wal_id, txn_id, count);

        encode_buf_.clear();
        const auto record_crc = encode_delete(encode_buf_, last_crc_, wal_id, txn_id, table_oid, row_ids.data(), count);

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{std::move(writer_error)};
        }
        if (auto append_error = writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);
            append_error.contains_error()) {
            error(log_,
                  "wal_worker::write_physical_delete , wal_id : {} , txn : {} , the record did NOT reach the "
                  "journal: {}",
                  wal_id,
                  txn_id,
                  append_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(append_error)};
        }
        last_crc_ = record_crc;

        co_return core::result_wrapper_t<wal::id_t>{wal_id};
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>>
    wal_worker_t::write_physical_update(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        std::pmr::vector<int64_t> row_ids,
                                        std::pmr::vector<components::vector::data_chunk_t> new_chunks,
                                        uint64_t count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::write_physical_update , wal_id : {} , txn : {} , count : {}", wal_id, txn_id, count);

        encode_buf_.clear();
        const auto record_crc = encode_update(encode_buf_,
                                              this->resource(),
                                              last_crc_,
                                              wal_id,
                                              txn_id,
                                              table_oid,
                                              row_ids.data(),
                                              new_chunks,
                                              count);

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{std::move(writer_error)};
        }
        if (auto append_error = writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);
            append_error.contains_error()) {
            error(log_,
                  "wal_worker::write_physical_update , wal_id : {} , txn : {} , the record did NOT reach the "
                  "journal: {}",
                  wal_id,
                  txn_id,
                  append_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(append_error)};
        }
        last_crc_ = record_crc;

        co_return core::result_wrapper_t<wal::id_t>{wal_id};
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>>
    wal_worker_t::write_physical_add_column(session_id_t /*session*/,
                                            components::catalog::oid_t table_oid,
                                            std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                            uint64_t column_count,
                                            uint64_t txn_id,
                                            wal::id_t wal_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_,
              "wal_worker::write_physical_add_column , wal_id : {} , txn : {} , cols : {}",
              wal_id,
              txn_id,
              column_count);

        encode_buf_.clear();
        const auto record_crc =
            encode_add_column(encode_buf_, last_crc_, wal_id, txn_id, table_oid, *schema_chunk, column_count);

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{std::move(writer_error)};
        }
        if (auto append_error = writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);
            append_error.contains_error()) {
            error(log_,
                  "wal_worker::write_physical_add_column , wal_id : {} , txn : {} , the record did NOT reach the "
                  "journal: {}",
                  wal_id,
                  txn_id,
                  append_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(append_error)};
        }
        last_crc_ = record_crc;

        co_return core::result_wrapper_t<wal::id_t>{wal_id};
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>>
    wal_worker_t::write_physical_grow(session_id_t session,
                                      components::catalog::oid_t table_oid,
                                      std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                      uint64_t column_count,
                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                      uint64_t row_start,
                                      uint64_t row_count,
                                      uint64_t txn_id,
                                      wal::id_t add_column_id,
                                      wal::id_t insert_id) {
        auto added = co_await write_physical_add_column(session,
                                                        table_oid,
                                                        std::move(schema_chunk),
                                                        column_count,
                                                        txn_id,
                                                        add_column_id);
        if (added.has_error()) {
            co_return std::move(added);
        }
        co_return co_await write_physical_insert(session,
                                                 table_oid,
                                                 std::move(chunks),
                                                 row_start,
                                                 row_count,
                                                 txn_id,
                                                 insert_id);
    }

    wal_worker_t::unique_future<core::result_wrapper_t<wal::id_t>> wal_worker_t::commit_txn(session_id_t /*session*/,
                                                                                            uint64_t transaction_id,
                                                                                            wal_sync_mode sync_mode,
                                                                                            wal::id_t wal_id,
                                                                                            uint64_t commit_id) {
        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{recovery_error_};
        }
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_,
              "wal_worker::commit_txn , wal_id : {} , txn : {} , commit_id : {} , sync : {}",
              wal_id,
              transaction_id,
              commit_id,
              static_cast<int>(sync_mode));

        if (sync_mode == wal_sync_mode::OFF) {
            // OFF writes nothing, so the chain must not move either: a marker that never lands isn't the last record
            // in the journal.
            co_return core::result_wrapper_t<wal::id_t>{wal_id};
        }

        encode_buf_.clear();
        // append() accepting the marker only means buffered; the flush below is what makes it durable.
        const auto record_crc = encode_commit(encode_buf_, last_crc_, wal_id, transaction_id, commit_id);

        if (auto writer_error = ensure_writer(); writer_error.contains_error()) {
            co_return core::result_wrapper_t<wal::id_t>{std::move(writer_error)};
        }
        if (auto append_error = writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);
            append_error.contains_error()) {
            error(log_,
                  "wal_worker::commit_txn , wal_id : {} , txn : {} , the COMMIT marker did NOT reach the journal: {}",
                  wal_id,
                  transaction_id,
                  append_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(append_error)};
        }
        last_crc_ = record_crc;

        // This call is the durability claim: under FULL, a failed fsync over a returned wal_id reports a commit that
        // never reached the device; under NORMAL a refused write still isn't in the journal.
        auto sync_error = sync_mode == wal_sync_mode::FULL ? writer_->flush_and_sync() : writer_->flush();
        if (sync_error.contains_error()) {
            error(log_,
                  "wal_worker::commit_txn , wal_id : {} , txn : {} , sync : {} , the commit is NOT durable: {}",
                  wal_id,
                  transaction_id,
                  static_cast<int>(sync_mode),
                  sync_error.what);
            co_return core::result_wrapper_t<wal::id_t>{std::move(sync_error)};
        }

        co_return core::result_wrapper_t<wal::id_t>{wal_id};
    }

    // True when ids between answered_through and next_verified_lsn are missing: a hole, not a short answer.
    static bool hides_requested_id(wal::id_t answered_through, wal::id_t next_verified_lsn) noexcept {
        return next_verified_lsn > answered_through + 1;
    }

    // Contract: the whole window or a refusal. Unlike wal_reader_t (a prefix) or recover_from_disk (ignores breaks),
    // this asks whether the window is whole, since its only caller (CREATE INDEX backfill) trusts whatever it gets.
    wal_worker_t::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    wal_worker_t::load(session_id_t session, wal::id_t after_wal_id) {
        trace(log_, "wal_worker::load , session : {} , after_wal_id : {}", session.data(), after_wal_id);

        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<std::vector<record_t>>{recovery_error_};
        }

        if (writer_) {
            if (auto flush_error = writer_->flush(); flush_error.contains_error()) {
                co_return core::result_wrapper_t<std::vector<record_t>>{std::move(flush_error)};
            }
        }

        auto segments = discover_segments();

        // discover_segments() returns files in ascending name/index/id order; the walk below relies on that alone.
        std::vector<record_t> all_records;
        // Taken from records actually handed back, not a page header, since one spanning into a broken page is never
        // returned.
        wal::id_t answered_through = after_wal_id;
        // A break drops everything behind it in its own segment (STOP-A), a gap only visible in the next segment.
        bool hole_open = false;
        wal::id_t hole_low = 0;
        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(this->resource(), seg_path);
            const auto scan = reader.scan_pages();

            if (hole_open && scan.first_verified_page_lsn != 0) {
                if (hides_requested_id(hole_low, scan.first_verified_page_lsn)) {
                    error(log_,
                          "wal_worker::load , db_oid={} , the answer reaches id {} and segment '{}' resumes at "
                          "id {} , so the ids in between are on pages no reader can reach , REFUSING rather "
                          "than answering with a hole — the journal cannot show the window ({}, ...] whole",
                          static_cast<unsigned>(database_oid_),
                          hole_low,
                          seg_path.filename().string(),
                          scan.first_verified_page_lsn,
                          after_wal_id);
                    co_return core::result_wrapper_t<std::vector<record_t>>{core::error_t{
                        core::error_code_t::io_error,
                        std::pmr::string{"wal cannot show the requested window whole: the journal skips from id " +
                                             std::to_string(hole_low) + " to id " +
                                             std::to_string(scan.first_verified_page_lsn) + " at segment " +
                                             seg_path.filename().string(),
                                         this->resource()}}};
                }
                hole_open = false;
            }

            auto seg_records = reader.read_all_records(after_wal_id);
            if (seg_records.has_error()) {
                // An unreadable segment is a hole, not a short tail: later segments open fine and would hide the gap.
                error(log_,
                      "wal_worker::load , segment '{}' could not be read , refusing rather than answering "
                      "with a hole: {}",
                      seg_path.filename().string(),
                      seg_records.error().what);
                co_return core::result_wrapper_t<std::vector<record_t>>{seg_records.error()};
            }
            for (auto& r : seg_records.value()) {
                if (r.id > answered_through) {
                    answered_through = r.id;
                }
                all_records.push_back(std::move(r));
            }

            if (!scan.chain_intact) {
                if (scan.verified_pages_after_break > 0) {
                    error(log_,
                          "wal_worker::load , db_oid={} , CRC chain broken in segment '{}' at data page {} , {} "
                          "later page(s) still verify , the catchup cannot reach the committed transactions "
                          "past the break",
                          static_cast<unsigned>(database_oid_),
                          seg_path.filename().string(),
                          scan.first_broken_page,
                          scan.verified_pages_after_break);
                } else {
                    warn(log_,
                         "wal_worker::load , db_oid={} , CRC chain broken in segment '{}' at data page {} , "
                         "nothing verifies after it , the answer ends there and loses no whole page",
                         static_cast<unsigned>(database_oid_),
                         seg_path.filename().string(),
                         scan.first_broken_page);
                }

                if (scan.first_verified_lsn_after_break != 0 &&
                    hides_requested_id(answered_through, scan.first_verified_lsn_after_break)) {
                    error(log_,
                          "wal_worker::load , db_oid={} , segment '{}' answers up to id {} and still holds a "
                          "verifiable id {} past its break at data page {} , REFUSING rather than answering "
                          "with a hole — the journal cannot show the window ({}, ...] whole",
                          static_cast<unsigned>(database_oid_),
                          seg_path.filename().string(),
                          answered_through,
                          scan.first_verified_lsn_after_break,
                          scan.first_broken_page,
                          after_wal_id);
                    co_return core::result_wrapper_t<std::vector<record_t>>{core::error_t{
                        core::error_code_t::io_error,
                        std::pmr::string{"wal cannot show the requested window whole: segment " +
                                             seg_path.filename().string() + " is broken at data page " +
                                             std::to_string(scan.first_broken_page) + " and hides the ids between " +
                                             std::to_string(answered_through) + " and " +
                                             std::to_string(scan.first_verified_lsn_after_break),
                                         this->resource()}}};
                }

                // On the last segment the break is an ordinary crash-torn tail, so the hole falls out of the loop
                // still open and gets answered.
                hole_open = true;
                hole_low = answered_through;
            }
        }

        // filter_committed_records must stay the one shared filter with bootstrap replay, or a recycled txn id's
        // backfill could pass uncommitted.
        std::vector<record_t> result = filter_committed_records(std::move(all_records), nullptr);

        std::sort(result.begin(), result.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_worker::load , returning {} records", result.size());
        co_return core::result_wrapper_t<std::vector<record_t>>{std::move(result)};
    }

    // W-TORN contract: the caller must pass min(prev_checkpoint_wal_id_) across all disk tables, not the latest
    // committed wal_id, which could discard records a table still needs before its header commit.
    wal_worker_t::unique_future<core::error_t> wal_worker_t::truncate_before(session_id_t /*session*/,
                                                                             wal::id_t checkpoint_wal_id) {
        trace(log_, "wal_worker::truncate_before , checkpoint_wal_id : {}", checkpoint_wal_id);

        if (recovery_error_.contains_error()) {
            co_return recovery_error_;
        }

        auto segments = discover_segments();
        for (const auto& seg_path : segments) {
            if (writer_ && seg_path == writer_->current_segment_path()) {
                continue;
            }

            wal_page_reader_t reader(this->resource(), seg_path);

            // Could-not-read is not is-empty: page_count() == 0 for both would destroy the one segment nobody could
            // account for.
            if (!reader.is_open()) {
                error(log_,
                      "wal_worker::truncate_before , segment '{}' could not be opened , REFUSING to truncate "
                      "(an unreadable segment is not an empty one): {}",
                      seg_path.filename().string(),
                      reader.open_error().what);
                co_return reader.open_error();
            }

            size_t pc = reader.page_count();
            if (pc == 0) {
                remove_segment(seg_path);
                continue;
            }

            // The last page's page_end_lsn bounds the file only if that page still verifies; one read decides both,
            // so a second read can't swallow its own failure into a zeroed header that unlinks unconditionally.
            wal_page_header_t last_hdr{};
            if (!reader.read_verified_page_header(pc, last_hdr)) {
                // Skip this file rather than refuse the whole truncation: the other segments are still accountable.
                error(log_,
                      "wal_worker::truncate_before , the last data page of segment '{}' cannot vouch for its "
                      "own bound (unreadable or failing its checksum) , REFUSING to remove it",
                      seg_path.filename().string());
                continue;
            }
            if (last_hdr.page_end_lsn <= checkpoint_wal_id) {
                trace(log_, "wal_worker::truncate_before , removing segment : {}", seg_path.filename().string());
                remove_segment(seg_path);
            }
        }

        co_return core::error_t::no_error();
    }

    void wal_worker_t::remove_segment(const std::filesystem::path& seg_path) {
        std::error_code ec;
        if (!std::filesystem::remove(seg_path, ec) || ec) {
            warn(log_,
                 "wal_worker::truncate_before , segment '{}' could not be removed , it will be re-read on the "
                 "next startup: {}",
                 seg_path.filename().string(),
                 ec.message());
        }
    }

    // A CRC break here does not truncate anything, only bounds the high-water mark; replay stops at the break (STOP-A).
    core::error_t wal_worker_t::recover_from_disk() {
        auto segments = discover_segments();
        if (segments.empty()) {
            trace(log_,
                  "wal_worker::recover , no existing segments for db_oid={}",
                  static_cast<unsigned>(database_oid_));
            return core::error_t::no_error();
        }

        // Taken from the file names, not the scan loop, which stops at the first CRC break.
        uint32_t max_seg_index = 0;
        for (const auto& seg_path : segments) {
            uint32_t seg_idx = parse_segment_index(seg_path, database_dir_name_);
            if (seg_idx != static_cast<uint32_t>(-1) && seg_idx > max_seg_index) {
                max_seg_index = seg_idx;
            }
        }

        wal::id_t max_wal_id = 0;       // high-water mark over the files -- bounds the allocator
        wal::id_t last_readable_id = 0; // highest id that could actually be decoded
        crc32_t recovered_crc = 0;
        bool resume_segment_broken = false;

        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(this->resource(), seg_path);

            // An unopenable segment can't fold into the same branch as a CRC break: a break still lets intact pages
            // vouch for their page_end_lsn, but an unopened segment yields nothing, so max_wal_id would land below
            // ids already written.
            if (!reader.is_open()) {
                return reader.open_error();
            }

            const auto scan = reader.scan_pages();
            if (scan.highest_page_end_lsn > max_wal_id) {
                max_wal_id = scan.highest_page_end_lsn;
            }

            if (!scan.chain_intact) {
                const uint32_t seg_idx = parse_segment_index(seg_path, database_dir_name_);
                if (seg_idx == max_seg_index) {
                    resume_segment_broken = true;
                }
                if (scan.verified_pages_after_break > 0) {
                    error(log_,
                          "wal_worker::recover , db_oid={} , CRC chain broken in segment '{}' at data page {} , "
                          "{} later page(s) still verify , REPLAY STOPS AT THE BREAK and the committed "
                          "transactions after it are NOT re-applied , their ids (up to {}) stay reserved so a "
                          "restored segment can still be replayed",
                          static_cast<unsigned>(database_oid_),
                          seg_path.filename().string(),
                          scan.first_broken_page,
                          scan.verified_pages_after_break,
                          scan.highest_page_end_lsn);
                } else {
                    warn(log_,
                         "wal_worker::recover , db_oid={} , CRC chain broken in segment '{}' at data page {} , "
                         "nothing verifies after it , replay stops there and loses no whole page",
                         static_cast<unsigned>(database_oid_),
                         seg_path.filename().string(),
                         scan.first_broken_page);
                }
            }

            // No reader validates last_crc_: it's the crc of the last record actually decoded, not one past a break.
            auto records = reader.read_all_records(0);
            if (records.has_error()) {
                return records.error();
            }
            for (const auto& r : records.value()) {
                if (r.is_valid() && r.id > last_readable_id) {
                    last_readable_id = r.id;
                    recovered_crc = r.crc32;
                }
            }
        }

        id_.store(max_wal_id, std::memory_order_relaxed);
        last_crc_ = recovered_crc;

        // Appending into a broken chain lands behind the corruption point, durable but never handed back.
        const bool rotate_away = resume_segment_broken && max_seg_index != std::numeric_limits<uint32_t>::max();
        current_segment_index_ = rotate_away ? max_seg_index + 1 : max_seg_index;
        if (rotate_away) {
            warn(log_,
                 "wal_worker::recover , db_oid={} , segment {} has a broken chain , writing continues in a NEW "
                 "segment {} rather than behind the corruption point",
                 static_cast<unsigned>(database_oid_),
                 max_seg_index,
                 current_segment_index_);
        } else if (resume_segment_broken) {
            // Opening is still right even with the index space exhausted, but appends now land behind the break.
            error(log_,
                  "wal_worker::recover , db_oid={} , segment {} has a broken chain and the segment index space "
                  "is exhausted , writes continue BEHIND the corruption point and will not be readable back",
                  static_cast<unsigned>(database_oid_),
                  max_seg_index);
        }

        trace(log_,
              "wal_worker::recover , db_oid={} , max_wal_id : {} , last readable id : {} , segment_index : {}",
              static_cast<unsigned>(database_oid_),
              max_wal_id,
              last_readable_id,
              current_segment_index_);
        return core::error_t::no_error();
    }

    core::error_t wal_worker_t::ensure_writer() {
        if (writer_ && writer_->torn_tail()) {
            // A refused mid-record flush left an orphan PARTIAL_CONT span; any page appended after it reads as
            // continuation bytes, not records.
            warn(log_,
                 "wal_worker::ensure_writer , db_oid={} , segment '{}' ends in an orphan span left by a "
                 "refused write , writing continues in a NEW segment",
                 static_cast<unsigned>(database_oid_),
                 writer_->current_segment_path().filename().string());
            writer_.reset();
            ++current_segment_index_;
        }
        if (writer_) {
            auto seg = writer_->current_segment_path();
            std::error_code ec;
            auto sz = std::filesystem::file_size(seg, ec);
            if (!ec && sz >= config_.max_segment_size) {
                // Rotating away from a page that didn't reach disk loses it outright.
                if (auto flush_error = writer_->flush(); flush_error.contains_error()) {
                    return flush_error;
                }
                writer_.reset();
                ++current_segment_index_;
            } else {
                return core::error_t::no_error();
            }
        }

        auto path = segment_path(current_segment_index_);
        writer_ = std::make_unique<wal_page_writer_t>(this->resource(),
                                                      path,
                                                      database_dir_name_,
                                                      current_segment_index_,
                                                      config_.max_segment_size);
        if (writer_->open_error().contains_error()) {
            auto open_error = writer_->open_error();
            // Dropped so a later call retries the open instead of appending into a handle that isn't there.
            writer_.reset();
            return open_error;
        }
        return core::error_t::no_error();
    }

    std::filesystem::path wal_worker_t::segment_path(uint32_t seg_index) const {
        return database_dir_ / segment_filename(database_dir_name_, seg_index);
    }

    std::vector<std::filesystem::path> wal_worker_t::discover_segments() const {
        std::vector<std::filesystem::path> result;

        if (!std::filesystem::exists(database_dir_)) {
            return result;
        }

        std::string prefix = "wal_" + database_dir_name_ + "_";

        for (const auto& entry : std::filesystem::directory_iterator(database_dir_)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            auto fname = entry.path().filename().string();
            if (fname.size() >= prefix.size() && fname.compare(0, prefix.size(), prefix) == 0) {
                result.push_back(entry.path());
            }
        }

        // Sorted lexicographically, which works because the suffix is zero-padded.
        std::sort(result.begin(), result.end());
        return result;
    }

    uint32_t wal_worker_t::parse_segment_index(const std::filesystem::path& path, const std::string& db_dir_name) {
        auto fname = path.filename().string();
        std::string prefix = "wal_" + db_dir_name + "_";
        if (fname.size() <= prefix.size() || fname.compare(0, prefix.size(), prefix) != 0) {
            return static_cast<uint32_t>(-1);
        }
        // from_chars over the whole suffix, not std::stoul+catch, which half-parsed foreign names like "12abc" as 12.
        const std::string_view suffix{fname.data() + prefix.size(), fname.size() - prefix.size()};
        uint32_t index = 0;
        const auto [ptr, ec] = std::from_chars(suffix.data(), suffix.data() + suffix.size(), index);
        if (ec != std::errc{} || ptr != suffix.data() + suffix.size()) {
            return static_cast<uint32_t>(-1);
        }
        return index;
    }

} // namespace services::wal
