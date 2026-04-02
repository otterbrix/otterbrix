#include "wal.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <set>
#include <sstream>

namespace services::wal {

    // -----------------------------------------------------------------------
    // Segment file naming
    //
    //   wal_<database>_000000
    //   wal_<database>_000001
    //   ...
    // -----------------------------------------------------------------------

    static std::string segment_filename(const std::string& db_name, uint32_t index) {
        // Format: wal_<db>_NNNNNN
        std::ostringstream oss;
        oss << "wal_" << db_name << "_";
        oss.width(6);
        oss.fill('0');
        oss << index;
        return oss.str();
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    wal_worker_t::wal_worker_t(std::pmr::memory_resource* resource,
                               manager_wal_replicate_t* /*manager*/,
                               log_t& log,
                               configuration::config_wal config,
                               const std::string& database_name)
        : actor_zeta::actor::basic_actor<wal_worker_t>(resource)
        , log_(log.clone())
        , config_(std::move(config))
        , database_name_(database_name)
        , database_dir_(config_.path / database_name_)
        , encode_buf_(this->resource()) {
        trace(log_, "wal_worker::create for database '{}'", database_name_);

        // Ensure the database WAL directory exists.
        std::filesystem::create_directories(database_dir_);

        // Recover state from existing segment files on disk.
        recover_from_disk();

        // Open a writer for the current (or first) segment.
        ensure_writer();
    }

    wal_worker_t::~wal_worker_t() {
        trace(log_, "wal_worker::destroy for database '{}'", database_name_);
        // The writer flushes on destruction.
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

    wal_worker_t::unique_future<wal::id_t>
    wal_worker_t::write_physical_insert(session_id_t /*session*/,
                                        std::string database,
                                        std::string collection,
                                        std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                                        uint64_t row_start,
                                        uint64_t row_count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::write_physical_insert , wal_id : {} , txn : {} , rows : {}",
              wal_id, txn_id, row_count);

        encode_buf_.clear();
        last_crc_ = encode_insert(encode_buf_, this->resource(),
                                  last_crc_, wal_id, txn_id,
                                  database, collection,
                                  *data_chunk, row_start, row_count);

        ensure_writer();
        writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);

        co_return wal_id;
    }

    // -----------------------------------------------------------------------
    // write_physical_delete
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<wal::id_t>
    wal_worker_t::write_physical_delete(session_id_t /*session*/,
                                        std::string database,
                                        std::string collection,
                                        std::pmr::vector<int64_t> row_ids,
                                        uint64_t count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::write_physical_delete , wal_id : {} , txn : {} , count : {}",
              wal_id, txn_id, count);

        encode_buf_.clear();
        last_crc_ = encode_delete(encode_buf_,
                                  last_crc_, wal_id, txn_id,
                                  database, collection,
                                  row_ids.data(), count);

        ensure_writer();
        writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);

        co_return wal_id;
    }

    // -----------------------------------------------------------------------
    // write_physical_update
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<wal::id_t>
    wal_worker_t::write_physical_update(session_id_t /*session*/,
                                        std::string database,
                                        std::string collection,
                                        std::pmr::vector<int64_t> row_ids,
                                        std::unique_ptr<components::vector::data_chunk_t> new_data,
                                        uint64_t count,
                                        uint64_t txn_id,
                                        wal::id_t wal_id) {
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::write_physical_update , wal_id : {} , txn : {} , count : {}",
              wal_id, txn_id, count);

        encode_buf_.clear();
        last_crc_ = encode_update(encode_buf_, this->resource(),
                                  last_crc_, wal_id, txn_id,
                                  database, collection,
                                  row_ids.data(), *new_data, count);

        ensure_writer();
        writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);

        co_return wal_id;
    }

    // -----------------------------------------------------------------------
    // commit_txn
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<wal::id_t>
    wal_worker_t::commit_txn(session_id_t /*session*/,
                             uint64_t transaction_id,
                             wal_sync_mode sync_mode,
                             wal::id_t wal_id) {
        id_.store(wal_id, std::memory_order_relaxed);

        trace(log_, "wal_worker::commit_txn , wal_id : {} , txn : {} , sync : {}",
              wal_id, transaction_id, static_cast<int>(sync_mode));

        if (sync_mode == wal_sync_mode::OFF) {
            // OFF mode: encode the commit marker but do not write to disk.
            // Update last_crc_ for chain continuity in case the mode changes later.
            encode_buf_.clear();
            last_crc_ = encode_commit(encode_buf_, last_crc_, wal_id, transaction_id);
            co_return wal_id;
        }

        // Encode commit marker.
        encode_buf_.clear();
        last_crc_ = encode_commit(encode_buf_, last_crc_, wal_id, transaction_id);

        ensure_writer();
        writer_->append(encode_buf_.data(), encode_buf_.size(), wal_id);

        if (sync_mode == wal_sync_mode::FULL) {
            writer_->flush_and_sync();
        } else {
            // NORMAL: flush buffered page to disk, but no fsync.
            writer_->flush();
        }

        co_return wal_id;
    }

    // -----------------------------------------------------------------------
    // load
    //
    // Two-pass approach:
    //   1. Read all records from all segment files.
    //   2. Collect committed txn_ids from COMMIT records.
    //   3. Return only physical records whose txn_id is in the committed set
    //      (or txn_id == 0), plus COMMIT markers themselves.
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<std::vector<record_t>>
    wal_worker_t::load(session_id_t session, wal::id_t after_wal_id) {
        trace(log_, "wal_worker::load , session : {} , after_wal_id : {}",
              session.data(), after_wal_id);

        // Flush current writer so all data is on disk.
        if (writer_) {
            writer_->flush();
        }

        auto segments = discover_segments();

        // Pass 1: read all raw records from all segments.
        std::vector<record_t> all_records;
        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(seg_path);
            auto seg_records = reader.read_all_records(after_wal_id);
            for (auto& r : seg_records) {
                all_records.push_back(std::move(r));
            }
        }

        // Pass 2: collect committed transaction IDs.
        std::set<uint64_t> committed_txns;
        for (const auto& r : all_records) {
            if (r.is_commit_marker() && r.is_valid()) {
                committed_txns.insert(r.transaction_id);
            }
        }

        // Pass 3: filter -- keep records belonging to committed transactions.
        std::vector<record_t> result;
        result.reserve(all_records.size());
        for (auto& r : all_records) {
            if (!r.is_valid()) {
                continue;
            }
            if (r.transaction_id == 0 ||
                committed_txns.count(r.transaction_id) > 0) {
                result.push_back(std::move(r));
            }
        }

        // Sort by wal_id ascending.
        std::sort(result.begin(), result.end(),
                  [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_worker::load , returning {} records", result.size());
        co_return result;
    }

    // -----------------------------------------------------------------------
    // truncate_before
    //
    // Delete segment files where the highest wal_id in the file is
    // <= checkpoint_wal_id.
    // -----------------------------------------------------------------------

    wal_worker_t::unique_future<void>
    wal_worker_t::truncate_before(session_id_t /*session*/, wal::id_t checkpoint_wal_id) {
        trace(log_, "wal_worker::truncate_before , checkpoint_wal_id : {}", checkpoint_wal_id);

        auto segments = discover_segments();
        for (const auto& seg_path : segments) {
            // Do not delete the segment that the writer is currently using.
            if (writer_ && seg_path == writer_->current_segment_path()) {
                continue;
            }

            // Read page headers to find the maximum wal_id in this segment.
            wal_page_reader_t reader(seg_path);
            size_t pc = reader.page_count();
            if (pc == 0) {
                // Empty segment -- safe to remove.
                std::filesystem::remove(seg_path);
                continue;
            }

            // The last data page's page_end_lsn is the highest wal_id.
            auto last_hdr = reader.read_page_header(pc); // last data page index
            if (last_hdr.page_end_lsn <= checkpoint_wal_id) {
                trace(log_, "wal_worker::truncate_before , removing segment : {}",
                      seg_path.filename().string());
                std::filesystem::remove(seg_path);
            }
        }

        co_return;
    }

    // -----------------------------------------------------------------------
    // recover_from_disk
    //
    // On startup, scan existing segment files to:
    //   1. Find the highest wal_id already written (so we continue from there).
    //   2. Verify CRC chain integrity; on first corruption, log and truncate.
    //   3. Recover last_crc_ for chain continuity.
    // -----------------------------------------------------------------------

    void wal_worker_t::recover_from_disk() {
        auto segments = discover_segments();
        if (segments.empty()) {
            trace(log_, "wal_worker::recover , no existing segments for '{}'", database_name_);
            return;
        }

        wal::id_t max_wal_id = 0;
        crc32_t recovered_crc = 0;
        uint32_t max_seg_index = 0;

        for (const auto& seg_path : segments) {
            uint32_t seg_idx = parse_segment_index(seg_path, database_name_);
            if (seg_idx != static_cast<uint32_t>(-1) && seg_idx > max_seg_index) {
                max_seg_index = seg_idx;
            }

            wal_page_reader_t reader(seg_path);

            // Verify page checksums (W-CRC). On corruption, log and stop reading
            // further segments (W-CORRUPT STOP-A).
            if (!reader.verify_chain()) {
                warn(log_, "wal_worker::recover , CRC chain broken in segment '{}' , "
                           "truncating at corruption point",
                     seg_path.filename().string());

                // Read whatever valid records exist before the corruption.
                auto records = reader.read_all_records(0);
                for (const auto& r : records) {
                    if (r.is_valid() && r.id > max_wal_id) {
                        max_wal_id = r.id;
                        recovered_crc = r.crc32;
                    }
                }
                // Stop processing further segments -- corruption means anything
                // after this point is unreliable.
                break;
            }

            // All pages valid -- read records to find max wal_id and last CRC.
            auto records = reader.read_all_records(0);
            for (const auto& r : records) {
                if (r.is_valid() && r.id > max_wal_id) {
                    max_wal_id = r.id;
                    recovered_crc = r.crc32;
                }
            }
        }

        id_.store(max_wal_id, std::memory_order_relaxed);
        last_crc_ = recovered_crc;
        current_segment_index_ = max_seg_index;

        trace(log_, "wal_worker::recover , database '{}' , max_wal_id : {} , segment_index : {}",
              database_name_, max_wal_id, current_segment_index_);
    }

    // -----------------------------------------------------------------------
    // ensure_writer
    //
    // Create the page writer if it doesn't exist. If the current segment
    // exceeds the configured max size, rotate to a new segment.
    // -----------------------------------------------------------------------

    void wal_worker_t::ensure_writer() {
        if (writer_) {
            // Check if the current segment file has exceeded max size.
            auto seg = writer_->current_segment_path();
            std::error_code ec;
            auto sz = std::filesystem::file_size(seg, ec);
            if (!ec && sz >= config_.max_segment_size) {
                // Flush + close current writer, open a new segment.
                writer_->flush();
                writer_.reset();
                ++current_segment_index_;
            } else {
                return;
            }
        }

        auto path = segment_path(current_segment_index_);
        writer_ = std::make_unique<wal_page_writer_t>(
            path,
            database_name_,
            current_segment_index_,
            config_.max_segment_size);
    }

    // -----------------------------------------------------------------------
    // Segment file helpers
    // -----------------------------------------------------------------------

    std::filesystem::path wal_worker_t::segment_path(uint32_t seg_index) const {
        return database_dir_ / segment_filename(database_name_, seg_index);
    }

    std::vector<std::filesystem::path> wal_worker_t::discover_segments() const {
        std::vector<std::filesystem::path> result;

        if (!std::filesystem::exists(database_dir_)) {
            return result;
        }

        std::string prefix = "wal_" + database_name_ + "_";

        for (const auto& entry : std::filesystem::directory_iterator(database_dir_)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            auto fname = entry.path().filename().string();
            if (fname.size() >= prefix.size() &&
                fname.compare(0, prefix.size(), prefix) == 0) {
                result.push_back(entry.path());
            }
        }

        // Sort by segment index (lexicographic on the zero-padded suffix works).
        std::sort(result.begin(), result.end());
        return result;
    }

    uint32_t wal_worker_t::parse_segment_index(const std::filesystem::path& path,
                                                const std::string& db_name) {
        auto fname = path.filename().string();
        std::string prefix = "wal_" + db_name + "_";
        if (fname.size() <= prefix.size() ||
            fname.compare(0, prefix.size(), prefix) != 0) {
            return static_cast<uint32_t>(-1);
        }
        auto suffix = fname.substr(prefix.size());
        try {
            return static_cast<uint32_t>(std::stoul(suffix));
        } catch (...) {
            return static_cast<uint32_t>(-1);
        }
    }

} // namespace services::wal
