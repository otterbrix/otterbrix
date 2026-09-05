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

    // -----------------------------------------------------------------------
    // Segment file naming
    //
    //   wal_<database_oid>_000000
    //   wal_<database_oid>_000001
    //   ...
    //
    // database_oid is rendered as decimal (e.g. "4" for main_database).
    // -----------------------------------------------------------------------

    static std::string segment_filename(const std::string& db_dir_name, uint32_t index) {
        // Format: wal_<db>_NNNNNN
        std::ostringstream oss;
        oss << "wal_" << db_dir_name << "_";
        oss.width(6);
        oss.fill('0');
        oss << index;
        return oss.str();
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

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

        // Ensure the database WAL directory exists.
        std::filesystem::create_directories(database_dir_);

        // Recover state from existing segment files on disk. A refusal is LATCHED, not
        // dropped: an actor constructor has no caller to answer, but every handler below
        // does, and each of them refuses while it is set. See recover_from_disk().
        recovery_error_ = recover_from_disk();
        if (recovery_error_.contains_error()) {
            error(log_,
                  "wal_worker::create , db_oid={} , the journal could not be read at startup , "
                  "this worker REFUSES every write and every truncate: {}",
                  static_cast<unsigned>(database_oid_),
                  recovery_error_.what);
            return;
        }

        // Open a writer for the current (or first) segment.
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
        // Flush HERE, where there is still a logger to answer to. ~wal_page_writer_t keeps a
        // last-resort flush, but it can only latch its refusal into a member nobody reads —
        // so the durability-carrying flush is this one.
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

    // -----------------------------------------------------------------------
    // behavior -- message dispatch
    // -----------------------------------------------------------------------

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
            case actor_zeta::msg_id<wal_worker_t, &wal_worker_t::write_physical_add_column>: {
                co_await actor_zeta::dispatch(this, &wal_worker_t::write_physical_add_column, msg);
                break;
            }
            default:
                break;
        }
    }

    // -----------------------------------------------------------------------
    // current_wal_id
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<wal::id_t> wal_worker_t::current_wal_id(session_id_t session) {
        trace(log_, "wal_worker::current_wal_id , session : {}", session.data());
        co_return id_.load(std::memory_order_relaxed);
    }

    // -----------------------------------------------------------------------
    // write_physical_insert
    // -----------------------------------------------------------------------

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
        // THE CHAIN MOVES WHEN THE JOURNAL DOES. encode_* answers the crc THIS record will
        // carry; last_crc_ takes it only after append() accepts the record. Advancing it at
        // encode time left the chain naming a record that a refused write never put in the
        // journal — and recover_from_disk() re-derives the chain from the last DECODABLE
        // record, so a restart disagreed with the running process about where the chain is.
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

    // -----------------------------------------------------------------------
    // write_physical_delete
    // -----------------------------------------------------------------------

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
        // Chain discipline: see write_physical_insert — the crc is committed to last_crc_
        // only after append() accepts the record.
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

    // -----------------------------------------------------------------------
    // write_physical_update
    // -----------------------------------------------------------------------

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
        // Chain discipline: see write_physical_insert.
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

    // -----------------------------------------------------------------------
    // write_physical_add_column
    // -----------------------------------------------------------------------

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
        // Chain discipline: see write_physical_insert.
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

    // -----------------------------------------------------------------------
    // commit_txn
    // -----------------------------------------------------------------------

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
            // OFF mode writes nothing, so the chain must not move either: last_crc_ names the
            // last record IN the journal, and a marker that never lands is not one. This used
            // to encode the marker and advance the chain "for continuity" — continuity with a
            // phantom, and a restart (which re-derives the chain from the last decodable
            // record) would never have agreed with it.
            co_return core::result_wrapper_t<wal::id_t>{wal_id};
        }

        encode_buf_.clear();
        // Chain discipline: see write_physical_insert. The chain takes the marker's crc once
        // append() accepts it — buffered is "in the journal" (the same claim every DML write
        // above makes); DURABLE is the flush below, whose refusal fails the commit without
        // un-writing the marker: the page stays buffered and the next flush retries it.
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

        // THE DURABILITY CLAIM IS THIS CALL, so its answer must not be dropped. Under
        // wal_sync_mode::FULL, returning the wal_id over a failed fsync reports a durable
        // commit over a page that never reached the device. Under NORMAL the promise is weaker
        // (buffered page, no fsync), but a REFUSED write is still a record that is not in the
        // journal, so both legs travel the wrapper.
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

    // Is the interval between what the answer reaches and the next id the journal can still vouch
    // for NON-EMPTY? `answered_through` is the highest id the reply is known to be complete up to
    // (it starts at the caller's after_wal_id and rises with every record handed back);
    // `next_verified_lsn` is the first id past the damage that some page still vouches for. If the
    // second is more than one above the first, the ids in between exist, were asked about, and are
    // not in the reply: a HOLE, not a short answer.
    //
    // One condition applied to every segment the same way. A break entirely BELOW after_wal_id is
    // read straight through — a deliberate difference from wal_reader_t, not an oversight: replay
    // asks "what may I apply from the beginning", so any break bounds it, while the catchup asks
    // about one WINDOW and old damage outside that window hides nothing from it.
    static bool hides_requested_id(wal::id_t answered_through, wal::id_t next_verified_lsn) noexcept {
        return next_verified_lsn > answered_through + 1;
    }

    // -----------------------------------------------------------------------
    // load
    //
    // CONTRACT: THE WHOLE WINDOW (after_wal_id, high-water] OR A REFUSAL. Partial success is not
    // in it. This is a THIRD question about a damaged journal, and the tree already answers the
    // other two differently on purpose:
    //   * wal_reader_t (replay) asks "what may I APPLY?" — a prefix, stopping at the first break,
    //     because applying across a hole puts later updates onto row versions never restored;
    //   * recover_from_disk (the id allocator) asks "where do I RESUME?" — a high-water mark over
    //     the FILES, ignoring breaks entirely, because a page past a break still vouches for the
    //     ids it carries;
    //   * this asks "is the window WHOLE?" — and its only caller, the CREATE INDEX catchup
    //     (operator_create_index_backfill.cpp), turns whatever it gets into index entries and then
    //     declares the index valid. An answer missing a range makes the published index answer with
    //     a SUBSET — silently, and for the life of the index. So this question is binary.
    //
    // Without the hole check, load concatenates the STOP-A prefix of segment k with the WHOLE of
    // segment k+1 and reports success — a range with a hole in it, handed to the one caller that
    // cannot survive one, while wal_reader_t logs the same damage at error level.
    //
    // Two passes: read all records from all segments, collect the committed txn_ids from COMMIT
    // records, then return only physical records whose txn_id is committed (or txn_id == 0), plus
    // the COMMIT markers themselves.
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<core::result_wrapper_t<std::vector<record_t>>>
    wal_worker_t::load(session_id_t session, wal::id_t after_wal_id) {
        trace(log_, "wal_worker::load , session : {} , after_wal_id : {}", session.data(), after_wal_id);

        if (recovery_error_.contains_error()) {
            co_return core::result_wrapper_t<std::vector<record_t>>{recovery_error_};
        }

        // Flush current writer so all data is on disk. A refusal here means the newest
        // records are NOT on disk, so anything read below would be an incomplete answer
        // presented as a complete one.
        if (writer_) {
            if (auto flush_error = writer_->flush(); flush_error.contains_error()) {
                co_return core::result_wrapper_t<std::vector<record_t>>{std::move(flush_error)};
            }
        }

        auto segments = discover_segments();

        // Pass 1: read all raw records from all segments.
        //
        // discover_segments() returns the files in ascending name order, which is ascending
        // segment index, which is ascending id — the walk below relies on that and on
        // nothing else.
        std::vector<record_t> all_records;
        // The highest id the reply is complete up to. Taken from the records actually HANDED
        // BACK rather than from a page header: a page's page_end_lsn counts a record that
        // merely STARTS on it, and a record spanning into a broken page is one read_all_records
        // never returns. Starting it at after_wal_id folds in "was this id even asked for".
        wal::id_t answered_through = after_wal_id;
        // A break drops everything behind it in ITS OWN segment (STOP-A), so it opens a gap
        // whose far edge is only visible in the NEXT segment. Carried across iterations.
        bool hole_open = false;
        wal::id_t hole_low = 0;
        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(this->resource(), seg_path);
            const auto scan = reader.scan_pages();

            // Close a gap left open by an earlier segment. If this segment's first
            // still-verifiable id sits more than one above where the answer reached, the ids
            // in between were skipped rather than cut off.
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
                // An unreadable segment is a HOLE in the id range, not a short tail: the
                // segments that follow it open fine and would be handed back as if the gap
                // were not there. The only honest answer is the refusal.
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
                // SAY IT, at the level the damage deserves — a crash-torn tail and committed
                // transactions stranded past the break are not the same event and must not
                // share a log line (the same split wal_reader_t and recover_from_disk make).
                // Silence here makes a catchup over a damaged journal indistinguishable from
                // a clean one.
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

                // Damage inside THIS segment that the segment itself can prove hid ids.
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
                                             std::to_string(scan.first_broken_page) +
                                             " and hides the ids between " + std::to_string(answered_through) +
                                             " and " + std::to_string(scan.first_verified_lsn_after_break),
                                         this->resource()}}};
                }

                // Nothing in this segment settles it. A LATER segment may — or may not:
                // when this is the last segment, the break is the ordinary crash-torn tail,
                // nothing sits beyond it, and there is nothing to refuse about. That case
                // deliberately falls out of the loop with the hole still open and is
                // answered, because banning CREATE INDEX after every crash would be a
                // failure on a path that cannot be repaired from inside.
                hole_open = true;
                hole_low = answered_through;
            }
        }

        // Pass 2: keep records belonging to committed transactions — the SHARED,
        // wal-id-ordered filter (filter_committed_records, wal.hpp), the same one the
        // bootstrap replay applies. It has to stay shared: an open-coded unordered set
        // membership test here, or a second copy of it in wal_reader.cpp, lets through the
        // CREATE INDEX backfill records of a recycled txn id that never committed in THIS
        // incarnation.
        std::vector<record_t> result = filter_committed_records(std::move(all_records), nullptr);

        // Sort by wal_id ascending.
        std::sort(result.begin(), result.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_worker::load , returning {} records", result.size());
        co_return core::result_wrapper_t<std::vector<record_t>>{std::move(result)};
    }

    // -----------------------------------------------------------------------
    // truncate_before
    //
    // Delete segment files where the highest wal_id in the file is
    // <= checkpoint_wal_id.
    //
    // W-TORN contract: the caller (manager_dispatcher_t after checkpoint_all)
    // must pass min(prev_checkpoint_wal_id_) across all DISK tables — NOT the
    // latest committed wal_id. The latest wal_id would discard records still
    // needed if a table's next checkpoint round dies before its header commit:
    // the two-slot root then reopens the SUPERSEDED root at next startup, whose
    // WAL floor is the prev id.
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<core::error_t> wal_worker_t::truncate_before(session_id_t /*session*/,
                                                                             wal::id_t checkpoint_wal_id) {
        trace(log_, "wal_worker::truncate_before , checkpoint_wal_id : {}", checkpoint_wal_id);

        if (recovery_error_.contains_error()) {
            co_return recovery_error_;
        }

        auto segments = discover_segments();
        for (const auto& seg_path : segments) {
            // Do not delete the segment that the writer is currently using.
            if (writer_ && seg_path == writer_->current_segment_path()) {
                continue;
            }

            // Read page headers to find the maximum wal_id in this segment.
            wal_page_reader_t reader(this->resource(), seg_path);

            // "COULD NOT READ" IS NOT "IS EMPTY". Collapsing both into page_count() == 0 makes
            // the branch below unlink the file for either — so the one segment whose contents
            // nobody could account for is the one that gets destroyed. REFUSE:
            // the segment stays on disk, a later round can read it once the cause is gone,
            // and replaying an already-checkpointed segment is idempotent (each table skips
            // records at or below its own .otbx.wal_id), so keeping it costs nothing.
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
                // Genuinely empty segment (opened, holds no data page) -- safe to remove.
                remove_segment(seg_path);
                continue;
            }

            // THE HIGHEST wal_id IN THE FILE IS THE LAST DATA PAGE'S page_end_lsn — ids are
            // appended in ascending order — BUT THAT FIELD ONLY MEANS SOMETHING IF THE PAGE STILL
            // VERIFIES. It sits inside the region the page CRC covers, so a corrupt page's copy of
            // it is precisely what the checksum failed to vouch for, and a low value there is what
            // this branch unlinks the file for: one flipped byte deleted a segment full of records
            // ABOVE the checkpoint. Same family as the refusal above — "unreadable is not empty".
            //
            // Checking THIS page rather than the whole chain is deliberate: a segment with an
            // earlier broken page but a verifying last page is fully bounded by that page, and if
            // the bound is at or below the checkpoint the file is entirely superseded by storage —
            // removing it also un-pins replay, which would otherwise stop at that break on every
            // startup for a segment nobody needs. ONE READ decides both halves: splitting it into
            // verify_page_checksum(pc) and then read_page_header(pc) — a SECOND read of the same
            // page — swallows that read's failure into a ZEROED header, and page_end_lsn == 0 is
            // <= every checkpoint id, so a page that reads once and fails once unlinks a segment
            // whose records sit ABOVE the checkpoint.
            wal_page_header_t last_hdr{};
            if (!reader.read_verified_page_header(pc, last_hdr)) {
                // SKIP THIS FILE, do not refuse the whole truncation: unlike the unopenable
                // case above, the other segments are still perfectly accountable, so they can
                // be reclaimed correctly. Keeping this one costs disk and nothing else —
                // replay is filtered per table by the checkpoint wal_id sidecar, so re-reading
                // it changes nothing.
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

    // A segment that survives its unlink is harmless: replay is filtered per table by the
    // checkpoint wal_id sidecar, so re-reading it changes nothing. Say so anyway rather than
    // discarding the answer.
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

    // -----------------------------------------------------------------------
    // recover_from_disk
    //
    // On startup, scan existing segment files to:
    //   1. Find the highest wal_id ALREADY ON DISK (so nothing is ever issued twice).
    //   2. Pick the segment the writer resumes into.
    //   3. Recover last_crc_ for chain continuity.
    //
    // A CRC BREAK ANSWERS ONE OF THOSE QUESTIONS AND NOT THE OTHERS. Logging "truncating at
    // corruption point" and then `break`ing out of the loop without truncating anything folds all
    // three answers into "whatever the replay scan managed to read":
    //   - discover_segments() sorts ascending, so every LATER segment goes unread and
    //     current_segment_index_ stays on the broken one — the writer then reopens it and appends
    //     BEHIND the corruption point, where no reader in the tree can reach;
    //   - read_all_records() stops at the first broken page, so ids living in the pages after it
    //     are invisible and the allocator resumes below them. The next write reissues them behind
    //     the break, so the NEXT startup reads the same short prefix and issues the same ids again.
    //
    // So this reads the remaining segments for the high-water mark ALONE and replays not a byte of
    // them: it writes nothing and deletes nothing, the engine opens, no id is handed out twice, and
    // a repaired segment replays in full on the next start. Actually truncating the tail would
    // destroy COMMITTED transactions irreversibly, at startup, before anyone has looked at them.
    // Refusing to start — as this file does for a segment that will not OPEN — does not carry over:
    // an unopenable segment yields nothing, so the id space cannot be bounded at all, whereas a
    // torn trailing page is the ORDINARY outcome of a crash and refusing on it would make every
    // crash a database that will not open.
    //
    // WHAT IS STILL LOST: replay stops at the break (wal_reader_t, STOP-A) because applying a range
    // with a HOLE in it is worse than applying a shorter one, so committed transactions recorded
    // after the break are NOT re-applied while the corruption stands. Their ids are now reserved
    // though, so repairing or restoring the segment makes them replayable instead of finding them
    // overwritten. That case is logged at error level below; a torn tail with nothing behind it is
    // the benign one and only warns.
    // -----------------------------------------------------------------------

    core::error_t wal_worker_t::recover_from_disk() {
        auto segments = discover_segments();
        if (segments.empty()) {
            trace(log_,
                  "wal_worker::recover , no existing segments for db_oid={}",
                  static_cast<unsigned>(database_oid_));
            return core::error_t::no_error();
        }

        // THE HIGHEST SEGMENT INDEX COMES FROM THE FILE NAMES, not from the scan loop below:
        // that loop stops at the first CRC break, so a break in segment 000000 would leave
        // this at 0 while 000001.. sat in the same directory. A name needs no checksum to be
        // trusted.
        uint32_t max_seg_index = 0;
        for (const auto& seg_path : segments) {
            uint32_t seg_idx = parse_segment_index(seg_path, database_dir_name_);
            if (seg_idx != static_cast<uint32_t>(-1) && seg_idx > max_seg_index) {
                max_seg_index = seg_idx;
            }
        }

        wal::id_t max_wal_id = 0;      // high-water mark over the FILES -- bounds the allocator
        wal::id_t last_readable_id = 0; // highest id that could actually be DECODED
        crc32_t recovered_crc = 0;
        bool resume_segment_broken = false;

        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(this->resource(), seg_path);

            // A segment that will not OPEN is a different failure from a segment whose CRC
            // chain breaks, and it must not be folded into the same "read what you can and
            // carry on" branch. A CRC break still lets every intact page vouch for its own
            // page_end_lsn, so max_wal_id below is a true high-water mark of what is on disk;
            // an unopened segment yields nothing, so max_wal_id would land BELOW ids that are
            // already written, and next_wal_id() would then hand them out a second time.
            if (!reader.is_open()) {
                return reader.open_error();
            }

            // (1) THE ALLOCATOR BOUND. Every data page whose checksum verifies is trustworthy
            //     about its own page_end_lsn, including pages sitting past a break.
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

            // (2) THE CRC CHAIN LINK. last_crc_ is the last_crc32 stamped into the next
            //     record; no reader validates it, so the honest value is the crc of the last
            //     record that could actually be decoded, not of one inferred past a break.
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

        // (3) WHERE THE WRITER RESUMES. Appending into a segment whose chain is broken puts
        //     the new record behind the corruption point, where read_all_records stops — the
        //     journal would accept it, report it durable, and never hand it back. Start a
        //     fresh segment instead: that creates a file and destroys none.
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
            // The segment index space is exhausted, so there is no fresh segment to move to.
            // Opening is still the right answer — refusing here would brick a database over a
            // counter — but the consequence has to be stated: appends land behind the break
            // and no reader will reach them.
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

    // -----------------------------------------------------------------------
    // ensure_writer
    //
    // Create the page writer if it doesn't exist. If the current segment
    // exceeds the configured max size, rotate to a new segment.
    // -----------------------------------------------------------------------

    core::error_t wal_worker_t::ensure_writer() {
        if (writer_ && writer_->torn_tail()) {
            // A refused mid-record flush left an ORPHAN PARTIAL_CONT span at the end of this
            // segment. Any page appended after it would be read as that span's continuation
            // bytes, not as records — the same "append behind the corruption point" hazard
            // recover_from_disk rotates away from at startup. The writer's buffer is already
            // empty (append discarded the continuation page), so nothing buffered is lost by
            // rotating here.
            warn(log_,
                 "wal_worker::ensure_writer , db_oid={} , segment '{}' ends in an orphan span left by a "
                 "refused write , writing continues in a NEW segment",
                 static_cast<unsigned>(database_oid_),
                 writer_->current_segment_path().filename().string());
            writer_.reset();
            ++current_segment_index_;
        }
        if (writer_) {
            // Check if the current segment file has exceeded max size.
            auto seg = writer_->current_segment_path();
            std::error_code ec;
            auto sz = std::filesystem::file_size(seg, ec);
            if (!ec && sz >= config_.max_segment_size) {
                // Flush + close current writer, open a new segment. Rotating away from a page
                // that did NOT reach the disk loses it outright: the next writer starts at
                // offset 0 of a different file and nothing ever revisits this one.
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
            // Drop the unusable writer so a later call retries the open instead of appending
            // into a handle that is not there.
            writer_.reset();
            return open_error;
        }
        return core::error_t::no_error();
    }

    // -----------------------------------------------------------------------
    // Segment file helpers
    // -----------------------------------------------------------------------

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

        // Sort by segment index (lexicographic on the zero-padded suffix works).
        std::sort(result.begin(), result.end());
        return result;
    }

    uint32_t wal_worker_t::parse_segment_index(const std::filesystem::path& path, const std::string& db_dir_name) {
        auto fname = path.filename().string();
        std::string prefix = "wal_" + db_dir_name + "_";
        if (fname.size() <= prefix.size() || fname.compare(0, prefix.size(), prefix) != 0) {
            return static_cast<uint32_t>(-1);
        }
        // from_chars over the WHOLE suffix — the std::stoul-under-catch(...) that stood
        // here both used exceptions as control flow and HALF-PARSED foreign names:
        // "000012.bak" and "12abc" answered 12, so a stray neighbour of the journal took
        // part in the max-segment-index arithmetic that decides where the next segment
        // is written. The engine writes digits and nothing else; anything else refuses.
        const std::string_view suffix{fname.data() + prefix.size(), fname.size() - prefix.size()};
        uint32_t index = 0;
        const auto [ptr, ec] = std::from_chars(suffix.data(), suffix.data() + suffix.size(), index);
        if (ec != std::errc{} || ptr != suffix.data() + suffix.size()) {
            return static_cast<uint32_t>(-1);
        }
        return index;
    }

} // namespace services::wal
