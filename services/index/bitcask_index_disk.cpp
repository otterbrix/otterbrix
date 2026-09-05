#include "bitcask_index_disk.hpp"

#include "absl/crc/crc32c.h"
#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace services::index {

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::move_files;
    using core::filesystem::open_file;
    using core::filesystem::remove_directory;
    using core::filesystem::remove_file;

#ifdef DEV_MODE
    namespace {
        bitcask_file_interposer_t* dev_bitcask_file_interposer_ = nullptr;
    } // namespace

    void dev_set_bitcask_file_interposer(bitcask_file_interposer_t* interposer) {
        dev_bitcask_file_interposer_ = interposer;
    }

    bitcask_file_interposer_t* dev_bitcask_file_interposer() { return dev_bitcask_file_interposer_; }
#endif

    namespace {
        // EVERY handle this store opens goes through here, so the DEV_MODE seam is armed in
        // one place instead of at each of the seven open sites. In a release build this is
        // core::filesystem::open_file and nothing else.
        std::unique_ptr<core::filesystem::file_handle_t> open_bitcask_file(
            core::filesystem::local_file_system_t& fs,
            const std::filesystem::path& path,
            file_flags flags,
            file_lock_type lock) {
            auto handle = open_file(fs, path, flags, lock);
#ifdef DEV_MODE
            if (auto* interposer = dev_bitcask_file_interposer(); interposer != nullptr) {
                handle = interposer->wrap(path, std::move(handle));
            }
#endif
            return handle;
        }

        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key,
                                                              core::date::timezone_offset_t session_tz) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT: {
                    // Signed-integer widening can not fail for the types this switch admits;
                    // still, never assert-then-value() (a failed cast in Release would deref
                    // an empty optional). A non-widenable key keeps its native representation
                    // — identical to the default branch, and self-consistent between insert
                    // and find (both normalize the same way).
                    auto casted = key.cast_as(complex_logical_type(logical_type::BIGINT), session_tz);
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT: {
                    auto casted = key.cast_as(complex_logical_type(logical_type::UBIGINT), session_tz);
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                default:
                    return key;
            }
        }

        constexpr const char* segment_prefix = "bitcask.";
        constexpr const char* segment_suffix = ".data";
        constexpr const char* current_segment_file = "CURRENT";
        constexpr const char* merge_manifest_file = "bitcask.merge";
        constexpr const char* hash_index_file = "hash_index.bin";
        constexpr const char* txn_log_file = "bitcask.txn.log";
        constexpr const char* txn_applied_file = "bitcask.txn.applied";
        constexpr unsigned segment_id_width = 6;
        constexpr uint32_t txn_magic = 0x314E5854; // TXN1

        struct record_header_t {
            uint32_t crc;
            uint8_t kind;
            uint64_t payload_size;
            uint64_t timestamp;
        };

        struct txn_frame_header_t {
            uint32_t magic;
            uint32_t crc;
            uint64_t txn_id;
            uint8_t op_kind; // 1=insert, 2=delete(row)
            uint64_t payload_size;
        };

        std::pmr::string serialize_payload(std::pmr::memory_resource* resource,
                                           const services::index::bitcask_index_disk_t::value_t& key,
                                           const std::pmr::vector<size_t>& rows) {
            std::pmr::string out(resource);
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint32_t>(out, static_cast<uint32_t>(rows.size()));
            for (auto row : rows) {
                components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row));
            }
            return out;
        }

        void deserialize_payload(std::pmr::memory_resource* resource,
                                 const std::pmr::string& payload,
                                 services::index::bitcask_index_disk_t::value_t& key,
                                 std::pmr::vector<size_t>& rows) {
            size_t pos = 0;
            key = components::index::codec::read_logical_value(resource, payload, pos);
            const auto n = components::index::codec::read_le<uint32_t>(payload, pos);
            rows.clear();
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                rows.emplace_back(static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos)));
            }
        }

        std::filesystem::path segment_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            std::ostringstream oss;
            oss << segment_prefix << std::setw(segment_id_width) << std::setfill('0') << segment_id << segment_suffix;
            return directory / oss.str();
        }

        std::filesystem::path merge_temp_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            return segment_file_path(directory, segment_id).string() + ".merge";
        }

        std::filesystem::path current_segment_path(const std::filesystem::path& directory) {
            return directory / current_segment_file;
        }

        bool parse_segment_id(const std::filesystem::path& path, uint64_t& segment_id) {
            const auto filename = path.filename().string();
            const std::string_view filename_sv{filename};
            constexpr std::string_view prefix = segment_prefix;
            constexpr std::string_view suffix = segment_suffix;
            if (!filename_sv.starts_with(prefix) || !filename_sv.ends_with(suffix)) {
                return false;
            }
            const std::string_view digits =
                filename_sv.substr(prefix.size(), filename_sv.size() - prefix.size() - suffix.size());
            if (digits.empty()) {
                return false;
            }
            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), segment_id);
            return ec == std::errc() && ptr == digits.data() + digits.size();
        }

        bool read_current_segment_id(const std::filesystem::path& directory, uint64_t& segment_id) {
            std::ifstream input(current_segment_path(directory));
            if (!input.good()) {
                return false;
            }
            input >> segment_id;
            return !input.fail();
        }

        std::filesystem::path merge_manifest_path(const std::filesystem::path& directory) {
            return directory / merge_manifest_file;
        }

        bool read_merge_manifest(const std::filesystem::path& directory,
                                 uint64_t& merged_segment_id,
                                 std::vector<uint64_t>& removed_segment_ids) {
            std::ifstream input(merge_manifest_path(directory));
            if (!input.good()) {
                return false;
            }
            std::size_t removed_count = 0;
            input >> merged_segment_id >> removed_count;
            if (input.fail()) {
                return false;
            }
            removed_segment_ids.clear();
            removed_segment_ids.reserve(removed_count);
            for (std::size_t i = 0; i < removed_count; ++i) {
                uint64_t removed_id = 0;
                input >> removed_id;
                if (input.fail()) {
                    removed_segment_ids.clear();
                    return false;
                }
                removed_segment_ids.push_back(removed_id);
            }
            return true;
        }

        void remove_merge_manifest(const std::filesystem::path& directory) {
            std::error_code ec;
            std::filesystem::remove(merge_manifest_path(directory), ec);
        }

        // Durably publish a sidecar: fsync temp, then rename over target.
        // rename(2) atomically replaces an existing target on POSIX, so never
        // unlink the target first — that leaves a crash window with no file.
        [[nodiscard]] bool publish_replacement_file(core::filesystem::local_file_system_t& fs,
                                                    const std::filesystem::path& temp_path,
                                                    const std::filesystem::path& target_path) {
            auto temp_file =
                open_bitcask_file(fs, temp_path, file_flags::READ | file_flags::WRITE, file_lock_type::NO_LOCK);
            if (!temp_file || !temp_file->sync()) {
                return false;
            }
            temp_file.reset();
            if (move_files(fs, temp_path, target_path)) {
                return true;
            }
#if defined(_WIN32)
            std::error_code ec;
            std::filesystem::remove(target_path, ec);
            return move_files(fs, temp_path, target_path);
#else
            return false;
#endif
        }

        // FALSE means the manifest is NOT on disk. It used to abort on each of the three
        // steps below; the manifest is written from inside merge_immutable_segments, which
        // has an error channel, and a merge that cannot record what it is about to do must
        // stop before it publishes anything -- not take the process with it.
        [[nodiscard]] bool write_merge_manifest(core::filesystem::local_file_system_t& fs,
                                                const std::filesystem::path& directory,
                                                uint64_t merged_segment_id,
                                                const std::vector<uint64_t>& removed_segment_ids) {
            const auto manifest_path = merge_manifest_path(directory);
            const auto temp_path = manifest_path.string() + ".tmp";
            {
                std::ofstream output(temp_path, std::ios::trunc);
                if (!output.good()) {
                    return false;
                }
                output << merged_segment_id << ' ' << removed_segment_ids.size();
                for (const auto removed_id : removed_segment_ids) {
                    output << ' ' << removed_id;
                }
                output << '\n';
                output.flush();
                if (!output.good()) {
                    remove_file(fs, temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, manifest_path)) {
                return true;
            }
            // The publish left the temp behind; nothing reads it, and leaving it would have
            // the next attempt write over a file it did not create.
            remove_file(fs, temp_path);
            return false;
        }

        // FALSE means CURRENT still names the previous segment. Its one caller is
        // open_active_segment, which runs on every start and every rotation and now reports
        // as a value: a read-only directory used to cost the engine its process here.
        //
        // THIS IS WHERE OPENING THE INDEX BECOMES A WRITE TO ITS DIRECTORY: a temp file is
        // created next to CURRENT and renamed over it, both of which need `w` on the
        // directory. It has been so from long before the keydir was made a derived structure,
        // so the unlinks the rebuild added spend a permission this line already required --
        // see the contract above bitcask_index_disk_t::open().
        [[nodiscard]] bool write_current_segment_id(core::filesystem::local_file_system_t& fs,
                                                    const std::filesystem::path& directory,
                                                    uint64_t segment_id) {
            const auto current_path = current_segment_path(directory);
            const auto temp_path = current_path.string() + ".tmp";
            {
                std::ofstream output(temp_path, std::ios::trunc);
                if (!output.good()) {
                    return false;
                }
                output << segment_id;
                output.flush();
                if (!output.good()) {
                    remove_file(fs, temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, current_path)) {
                return true;
            }
            remove_file(fs, temp_path);
            return false;
        }

        // FALSE means the record did NOT reach the file -- the header short, the payload
        // short, or the write refused outright. Both writes used to be issued and dropped,
        // so a full device produced a snapshot that exists only in the keydir: the statement
        // reported success, the key kept pointing at an offset holding nothing, and the next
        // read of it answered empty.
        [[nodiscard]] bool write_record(core::filesystem::file_handle_t& file,
                                        uint8_t kind,
                                        uint64_t timestamp,
                                        const std::pmr::string& payload) {
            record_header_t header{0, kind, static_cast<uint64_t>(payload.size()), timestamp};

            absl::crc32c_t crc = absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
            if (!payload.empty()) {
                crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
            }
            header.crc = static_cast<uint32_t>(crc);

            if (file.write(&header, sizeof(header)) != static_cast<int64_t>(sizeof(header))) {
                return false;
            }
            if (!payload.empty() &&
                file.write(const_cast<char*>(payload.data()), payload.size()) !=
                    static_cast<int64_t>(payload.size())) {
                return false;
            }
            return true;
        }
    } // namespace

    // NO I/O HAPPENS HERE. Every field is set and nothing is touched on disk, which is
    // what lets this ctor run inside bitcask_index_agent_t's member initializer list: the
    // agent holds the store BY VALUE (it cannot be moved in -- the deleted copy ctor
    // suppresses the implicit move), so construction has to be the step that cannot fail
    // and open() has to be the step that can.
    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_txn_ids,
                                               deferred_open_t)
        : resource_(resource)
        , flush_threshold_(flush_threshold)
        , path_(path)
        , hash_index_file_path_(path_ / hash_index_file)
        , fs_(core::filesystem::local_file_system_t())
        , segment_record_limit_(segment_record_limit)
        , committed_txn_ids_(committed_txn_ids.begin(), committed_txn_ids.end(), resource) {}

    // THE WHOLE OPEN, as a value. The keydir opens here and the reason it could not is
    // this function's RETURN rather than a flag on the object: nothing after the failing
    // step has run, so the owner drops the half-built store and hands the reason on
    // instead of publishing an index over storage that is not there.
    //
    // OPENING THIS INDEX IS A WRITE TO ITS DIRECTORY. That is a contract, not a side effect,
    // and it is written here because it has been misread as new: the directory is ENUMERATED
    // (collect_segments) and CURRENT is republished in it through a temp file plus a rename
    // (open_active_segment -> write_current_segment_id), on every single open, since long
    // before the keydir became a derived structure. The permission set is therefore r+w+x on
    // the index directory and rw on its files, and disk_hash_table_t::reset_storage's unlinks
    // do not widen it -- they spend `w` that this path already spends two calls later. What
    // changed with the rebuild rule is only WHERE the refusal is met first, and what changed
    // with the error channel is that it is met as a VALUE: write_current_segment_id used to
    // end the process on it.
    //
    // THERE IS NO READ-ONLY MODE for this index, and a directory that cannot be written to
    // has no open at all -- which is the honest answer, because an index that cannot record
    // which segment is active cannot be written to safely afterwards.
    //
    // AND THE DIRECTORY BELONGS TO EXACTLY ONE PROCESS. Nothing here interlocks with a second
    // opener: the wipe, the replay and the CURRENT publication would interleave with another
    // process's segment appends with no lock between them.
    core::error_t bitcask_index_disk_t::open() {
        RETURN_IF_ERROR(initialize_storage());
        if (auto open_result = open_hash_index(); open_result.contains_error()) {
            return open_result;
        }
        RETURN_IF_ERROR(load_from_disk());
        if (crc_failure_) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"bitcask: CRC mismatch during recovery", resource_}};
        }
        RETURN_IF_ERROR(open_active_segment());
        RETURN_IF_ERROR(recover_txn_log());
        // Recovery applies its frames through insert()/remove(), which are void and park
        // what they could not write in pending_write_error_. open() is the first caller
        // that can report them, so it drains the parking slot here rather than leaving a
        // recovery failure to surface on some later statement's flush.
        return force_flush();
    }

    // Construct-and-open, for the backend tests. It aborts on exactly the failures open()
    // reports as values, because a constructor has no channel to report them on (rule 2
    // forbids the exception that would be the alternative).
    //
    // THIS ABORT IS LEGITIMATE AND STAYS, and the reason is that nothing in production can
    // reach it: the only callers of this overload are services/index/tests
    // (test_bitcask_index_disk.cpp, stress_test_index.cpp). Production builds the store
    // through the deferred ctor above plus open(), inside bitcask_index_agent_t's member
    // initializer list, precisely so an environmental failure costs the INDEX its
    // registration and never the ENGINE its start.
    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_txn_ids)
        : bitcask_index_disk_t(path,
                               resource,
                               flush_threshold,
                               segment_record_limit,
                               std::move(committed_txn_ids),
                               deferred_open_t{}) {
        if (open().contains_error()) {
            assert(false && "bitcask I/O failure: the construct-and-open ctor could not open the store");
            std::abort();
        }
    }

    // The keydir file, opened by the store that owns it. Reached through open() rather
    // than through a constructor, because the failures it reports -- an unopenable path,
    // an unreadable or incompatible header -- are environmental: they must cost the index
    // its registration, never the engine its start (integration test
    // test_index_bootstrap_failure), and only a function that RETURNS can say so.
    core::error_t bitcask_index_disk_t::io_failure(std::string_view message) const {
        return core::error_t{core::error_code_t::index_create_fail,
                             std::pmr::string{message.data(), message.size(), resource_}};
    }

    core::error_t bitcask_index_disk_t::open_hash_index() {
        auto storage =
            disk_hash_table_t::create(hash_index_file_path_, disk_hash_table_t::default_bucket_count, resource());
        if (storage.has_error()) {
            return storage.error();
        }
        hash_index_ = std::move(storage.value());
        return core::error_t::no_error();
    }

    bitcask_index_disk_t::~bitcask_index_disk_t() {
        if (!hash_index_) {
            // The keydir never opened (see open_hash_index) or drop() released it, so
            // there is nothing to flush. Nothing to unhook either: the key loader travels
            // with each call instead of being installed on the table.
            return;
        }
        // LAST CHANCE, NOT THE ONLY ONE, and that is why the value is dropped here by name.
        // Every write door of this store ends in a force_flush a caller reads: the agent
        // runs one at the end of commit_inserts/commit_deletes (publish_buckets) and one
        // per force_flush message, and apply_txn_* return their own. So by the time this
        // runs, either the flush already succeeded (dirty_ is false and this is a no-op) or
        // its failure was already reported to the statement that owned it. A destructor has
        // no channel of its own -- rule 2 forbids the exception that would be the
        // alternative -- and this store holds no logger to write to.
        auto ignored_flush_error = force_flush();
    }

    // THE KEY, OR THE REASON IT COULD NOT BE READ. read_rows_at's bool is the record's
    // KIND, and the kind is not the question here: read_rows_at fills *out_key BEFORE it
    // classifies the record, so a tombstone carries the same whole key a value record does.
    // Whether the key still holds rows is decided by read_rows_at's own three-way answer at
    // the four places that ask it -- find(), current_rows(), load_entries() and the merge --
    // and duplicating that decision here is exactly what folded a read failure into "this
    // entry is not your key".
    //
    // read.value() is deliberately not consulted. Nothing today can point a keydir entry at
    // a tombstone (put() reaches the keydir from three places only -- recovery's
    // record_kind_t::value branch, append_snapshot, and the merge-journal replay -- and each
    // stores the offset of a VALUE record; append_tombstone never calls put), but the answer
    // is chosen so that a fourth put could not turn this back into a silent wrong answer.
    core::result_wrapper_t<std::pmr::string> bitcask_index_disk_t::load_hash_key_at(uint32_t segment_id,
                                                                                    uint64_t value_offset) const {
        row_ids_t rows(resource());
        value_t key(resource(), nullptr);
        auto read = read_rows_at(segment_id, value_offset, rows, &key);
        if (read.has_error()) {
            return read.error();
        }
        // ON THIS STORE'S RESOURCE (rule 8). The encoder underneath still builds a
        // std::string -- codec::encode_disk_hash_key is shared with callers that have no
        // resource to hand it -- so the crossing happens here, once, on a path that runs
        // only for a key longer than disk_hash_table_t::inline_key_limit. It is not a hot
        // path and it is not claimed to be one: a profile of the heaviest case in the suite
        // (the randomized stress) shows this function called ZERO times, because encoded
        // integer keys are nine bytes against a limit of sixty-four.
        const auto key_bytes = key_bytes_for_hash(key);
        return std::pmr::string(key_bytes.data(), key_bytes.size(), resource());
    }

    void bitcask_index_disk_t::set_bulk_mode(bool enabled) {
        if (enabled) {
            if (!bulk_mode_ && hash_index_) {
                bulk_prev_rehash_suppressed_ = hash_index_->set_auto_rehash_suppressed(true);
                bulk_rehash_guard_active_ = true;
            }
            bulk_mode_ = true;
            return;
        }

        if (!enabled && bulk_mode_ && hash_index_ && bulk_rehash_guard_active_) {
            hash_index_->set_auto_rehash_suppressed(bulk_prev_rehash_suppressed_);
            bulk_rehash_guard_active_ = false;
            if (!bulk_prev_rehash_suppressed_) {
                // set_bulk_mode is void and its callers are handlers that have already
                // written; a keydir that could not finish growing is handed to the next
                // force_flush rather than dropped here.
                note_write_error(hash_index_->trigger_rehash_if_needed());
            }
        }
        bulk_mode_ = enabled;
    }

    core::error_t bitcask_index_disk_t::initialize_storage() {
        if (std::filesystem::exists(path_)) {
            return core::error_t::no_error();
        }
        std::error_code ec;
        std::filesystem::create_directories(path_, ec);
        if (ec) {
            return io_failure("bitcask: the index directory " + path_.string() + " could not be created: " +
                              ec.message());
        }
        return core::error_t::no_error();
    }

    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key) const {
        auto normalized = normalize_hash_key(key, core::date::timezone_offset_t{});
        return components::index::codec::encode_disk_hash_key(normalized);
    }

    void bitcask_index_disk_t::apply_merge_recovery_cleanup() {
        uint64_t merged_segment_id = 0;
        std::vector<uint64_t> removed_segment_ids;
        if (!read_merge_manifest(path_, merged_segment_id, removed_segment_ids)) {
            return;
        }

        if (!std::filesystem::exists(segment_file_path(path_, merged_segment_id))) {
            remove_merge_manifest(path_);
            return;
        }

        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            if (!std::filesystem::exists(removed_path)) {
                continue;
            }
            remove_file(fs_, removed_path);
        }
    }

    core::error_t bitcask_index_disk_t::load_from_disk() {
        const bool prev_rehash_suppressed = hash_index_->set_auto_rehash_suppressed(true);
        struct restore_rehash_state_t {
            disk_hash_table_t* table{nullptr};
            bool prev{false};
            ~restore_rehash_state_t() {
                if (table) {
                    table->set_auto_rehash_suppressed(prev);
                }
            }
        } restore_rehash_state{hash_index_.get(), prev_rehash_suppressed};

        apply_merge_recovery_cleanup();

        // THE KEYDIR IS A DERIVED STRUCTURE AND THIS FUNCTION IS ITS ONLY AUTHOR. Wiping it
        // here makes that true instead of merely intended: the three places that put entries
        // into it (this replay, append_snapshot, the merge-journal replay) all record the
        // offset of a VALUE record of a segment, so there is nothing in it that the segments
        // below do not say again.
        //
        // What it buys is that every entry erase_all_refs_for_key meets during the replay was
        // put there by THIS call, from a record THIS call has just read through its own open
        // descriptor. A loader refusal on this path can therefore no longer mean "the segment
        // this entry names was unlinked three restarts ago" -- the shape a killed merge leaves
        // behind, and one no live index could ever repair, because the failure travels out of
        // open() and the store that owns the repair door is never constructed. It can now only
        // mean "the device is refusing right now", where refusing is exactly right and the
        // next open clears it along with the fault.
        //
        // UNCONDITIONAL, and in particular BEFORE the early return on an empty segment set: an
        // index whose segments have all gone would otherwise keep a full stale keydir and go
        // on answering find() out of it.
        RETURN_IF_ERROR(hash_index_->reset_storage());

        VALUE_OR_RETURN(auto segments, collect_segments());
        if (segments.empty()) {
            active_segment_id_ = regular_segment_id_start_;
            next_segment_id_ = regular_segment_id_start_ + 1;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
            restore_rehash_state.table = nullptr;
            hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
            if (!prev_rehash_suppressed) {
                RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
            }
            return core::error_t::no_error();
        }

        for (auto& segment : segments) {
            auto f = open_bitcask_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                // A SEGMENT THAT WILL NOT OPEN USED TO STOP THE ENGINE. This is the rebuild
                // loop: the keydir it is filling IS the index, and a segment it cannot read
                // is an index missing every key that segment holds. That has to cost the
                // INDEX its registration -- open() reports it and
                // bitcask_index_agent_t::create drops the half-built agent -- never the
                // process its life (integration test test_index_bootstrap_failure).
                return io_failure("bitcask: segment " + segment.path.string() + " could not be opened for recovery");
            }
            const auto file_size = f->file_size();
            uint64_t offset = 0;
            while (offset + sizeof(record_header_t) <= file_size) {
                record_header_t header{};
                // THE LOOP CONDITION ALREADY PROVED THE HEADER IS INSIDE THE FILE, so a
                // short read here is a device refusing, not a torn tail -- and a rebuild
                // that stops on one leaves every later key of this segment out of the index
                // for the whole uptime, silently. The truncated-tail case keeps its break
                // three lines below, where the SIZE says the record is not all there.
                if (!f->read(&header, sizeof(header), offset)) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " refused a record header during recovery");
                }

                const auto payload_offset = offset + sizeof(record_header_t);
                if (payload_offset + header.payload_size > file_size) {
                    // Truncated tail: the record was never fully written (a crash mid-append).
                    // Everything before it is intact, and there is nothing after it.
                    break;
                }

                std::pmr::string payload(resource());
                payload.resize(static_cast<size_t>(header.payload_size));
                if (header.payload_size != 0 &&
                    !f->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                    // Same as the header above: the size check says these bytes exist.
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " refused a record payload during recovery");
                }
                absl::crc32c_t calc = absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                                            sizeof(header) - sizeof(header.crc)));
                if (!payload.empty()) {
                    calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
                }
                if (static_cast<uint32_t>(calc) != header.crc) {
                    // Segment corruption: flag and return rather than abort, so
                    // open() can report a core::error_t. It checks this flag post-load;
                    // the construct-and-open ctor aborts on what open() then returns.
                    crc_failure_ = true;
                    return core::error_t::no_error();
                }
                value_t key(resource(), nullptr);
                row_ids_t rows(resource());
                deserialize_payload(resource(), payload, key, rows);
                const auto key_bytes = key_bytes_for_hash(key);
                if (static_cast<record_kind_t>(header.kind) == record_kind_t::tombstone) {
                    RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
                } else if (static_cast<record_kind_t>(header.kind) == record_kind_t::value) {
                    // A REBUILD THAT COULD NOT REPLAY A RECORD MUST NOT OPEN. The keydir
                    // this loop is filling IS the index; finishing the loop over a record
                    // that did not land would publish an index that is missing rows the
                    // segments hold, and open() would report success over it.
                    RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
                    RETURN_IF_ERROR(hash_index_->put(key_bytes,
                                                     rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                                     static_cast<uint32_t>(segment.id),
                                                     payload_offset));
                } else {
                    // The CRC above already matched, so this is a WELL-FORMED record of a
                    // kind this build does not know -- a foreign or newer format, not a torn
                    // tail. Stopping quietly here dropped the rest of the segment from the
                    // index without a word.
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a record of an unknown kind");
                }
                next_timestamp_ = std::max(next_timestamp_, header.timestamp);
                ++segment.record_count;
                offset = payload_offset + header.payload_size;
            }
        }

        uint64_t configured_active_segment_id = 0;
        const bool has_configured_active_segment = read_current_segment_id(path_, configured_active_segment_id);

        const auto active_it = std::find_if(segments.begin(), segments.end(), [&](const auto& segment) {
            return has_configured_active_segment && segment.id == configured_active_segment_id;
        });
        const auto& active_segment = active_it == segments.end() ? segments.back() : *active_it;
        active_segment_id_ = active_segment.id;
        next_segment_id_ = segments.back().id + 1;
        active_segment_records_ = active_segment.record_count;
        active_data_file_path_ = active_segment.path;

        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
        }
        return core::error_t::no_error();
    }

    // EVERY std::filesystem CALL HERE IS THE std::error_code OVERLOAD. The throwing ones
    // that used to sit in this body are exceptions escaping the open path on a
    // -fno-exceptions build (rule 2), and the outcome they produced when they did not throw
    // was worse: a directory the process may not read came back as "no segments", which
    // load_from_disk now takes as the whole truth about this index.
    core::result_wrapper_t<std::pmr::vector<bitcask_index_disk_t::segment_info_t>>
    bitcask_index_disk_t::collect_segments() const {
        // ON THIS STORE'S RESOURCE (rule 8). The list is built on every open, every merge and
        // every clear() and is as long as the index has segments, so it is exactly the kind of
        // allocation that belongs in the owner's pool rather than in the process-wide heap.
        std::pmr::vector<segment_info_t> segments(resource());
        const auto listing_failure = [this](const std::error_code& code) {
            return io_failure("bitcask: the index directory " + path_.string() + " could not be listed: " +
                              code.message());
        };

        std::error_code ec;
        const bool directory_present = std::filesystem::exists(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        if (!directory_present) {
            // THE ONE LEGITIMATE EMPTY ANSWER: nothing has been written here yet. It is kept
            // apart from the refusals above and below precisely because the rebuild acts on
            // it -- an index with no segments is an index with no keys, and that is true.
            return segments;
        }
        const bool is_directory = std::filesystem::is_directory(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        if (!is_directory) {
            // A non-directory in the index's place is not "no segments yet", it is a layout
            // this store cannot be running on. Answering with the empty list would hand the
            // rebuild a licence to wipe the keydir over it.
            return io_failure("bitcask: the index path " + path_.string() + " is not a directory");
        }

        auto entry = std::filesystem::directory_iterator(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        const auto end = std::filesystem::directory_iterator();
        while (entry != end) {
            const bool regular_file = entry->is_regular_file(ec);
            if (ec) {
                return listing_failure(ec);
            }
            if (regular_file) {
                uint64_t segment_id = 0;
                if (parse_segment_id(entry->path(), segment_id)) {
                    segments.push_back(segment_info_t{segment_id, entry->path(), 0});
                }
            }
            entry.increment(ec);
            if (ec) {
                return listing_failure(ec);
            }
        }

        std::sort(segments.begin(), segments.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
        return segments;
    }

    core::error_t bitcask_index_disk_t::open_active_segment() {
        if (active_data_file_path_.empty()) {
            active_segment_id_ = active_segment_id_ == 0 ? allocate_next_segment_id() : active_segment_id_;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        }

        file_ = open_bitcask_file(fs_,
                                  active_data_file_path_,
                                  file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                  file_lock_type::NO_LOCK);
        if (!file_) {
            return io_failure("bitcask: active segment " + active_data_file_path_.string() +
                              " could not be opened");
        }
        if (!file_->seek(file_->file_size())) {
            return io_failure("bitcask: active segment " + active_data_file_path_.string() +
                              " could not be positioned at its end");
        }
        if (!write_current_segment_id(fs_, path_, active_segment_id_)) {
            // CURRENT still names the previous segment. Refusing here rather than writing
            // into a segment nothing points at: load_from_disk picks the active segment by
            // this file, so an append that landed in an unnamed one would be replayed in the
            // wrong order after a restart.
            return io_failure("bitcask: the CURRENT segment pointer could not be published");
        }
        return core::error_t::no_error();
    }

    uint64_t bitcask_index_disk_t::allocate_next_segment_id() { return next_segment_id_++; }

    core::error_t bitcask_index_disk_t::rotate_active_segment() {
        // THE OLD SEGMENT MUST BE ON THE DEVICE BEFORE THE HANDLE GOES. Nothing reopens it
        // for writing, so a flush that refused here is a flush that never happens.
        RETURN_IF_ERROR(sync_if_dirty());
        file_.reset();
        active_segment_id_ = allocate_next_segment_id();
        active_segment_records_ = 0;
        active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        RETURN_IF_ERROR(open_active_segment());
        // RECORD the debt, do not pay it here. Paying it here would put a whole-keydir
        // compaction in the middle of one record append, and a statement large enough to
        // fill N segments would pay it N times. bitcask_index_agent_t pays it once, at the
        // end of the write handler this rotation happened inside.
        merge_pending_ = true;
        return core::error_t::no_error();
    }

    void bitcask_index_disk_t::merge_pending_segments() {
        if (!merge_pending_) {
            return;
        }
        // Cleared FIRST: the merge below either compacts what the rotations left or finds
        // nothing to compact, and either way the debt is settled. Clearing it afterwards
        // would re-run the whole scan on the next call for every early return inside.
        merge_pending_ = false;
        // merge_pending_segments is void and the agent calls it at the end of a write
        // handler it is already inside; a merge that could not finish rides out on
        // force_flush like every other write failure this store cannot report in place.
        auto merge_error = merge_immutable_segments();
        if (merge_error.contains_error()) {
            // THE DEBT IS STILL OWED. A refused merge publishes nothing and unlinks nothing,
            // so the rotated segments it was going to compact are all still there -- and
            // dropping the flag would mean nothing ever compacts them again until the next
            // rotation happens to set it. The agent only calls this once per write handler,
            // so a permanently failing merge costs one attempt per statement, not a spin.
            merge_pending_ = true;
        }
        note_write_error(std::move(merge_error));
    }

    core::error_t bitcask_index_disk_t::rotate_active_segment_if_needed() {
        if (bulk_mode_) {
            return core::error_t::no_error();
        }
        if (active_segment_records_ >= segment_record_limit_) {
            return rotate_active_segment();
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<bool> bitcask_index_disk_t::read_rows_at(uint32_t segment_id,
                                                                    uint64_t value_offset,
                                                                    row_ids_t& rows,
                                                                    value_t* out_key) const {
        const auto segment_path = segment_file_path(path_, segment_id);
        auto f = open_bitcask_file(fs_, segment_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!f) {
            return io_failure("bitcask: segment " + segment_path.string() + " could not be opened for reading");
        }
        record_header_t header{};
        std::pmr::string payload(resource());
        if (value_offset < sizeof(record_header_t)) {
            // A keydir entry points PAST its record header, so an offset smaller than one
            // header is an entry that cannot describe a record at all.
            return io_failure("bitcask: keydir entry points inside the record header of " + segment_path.string());
        }
        const auto header_offset = value_offset - sizeof(record_header_t);
        if (!f->read(&header, sizeof(header), header_offset)) {
            return io_failure("bitcask: record header at " + std::to_string(header_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
        payload.resize(static_cast<size_t>(header.payload_size));
        if (header.payload_size != 0 && !f->read(payload.data(), header.payload_size, value_offset)) {
            return io_failure("bitcask: record payload at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
        absl::crc32c_t calc = absl::ComputeCrc32c(
            absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
        if (!payload.empty()) {
            calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
        }
        if (static_cast<uint32_t>(calc) != header.crc) {
            return io_failure("bitcask: CRC mismatch on the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string());
        }
        value_t key(resource(), nullptr);
        deserialize_payload(resource(), payload, key, rows);
        if (out_key) {
            *out_key = value_t(resource(), key);
        }
        // The ONE legal false: a tombstone. `rows` is the empty list the record carries.
        return static_cast<record_kind_t>(header.kind) == record_kind_t::value;
    }

    core::result_wrapper_t<bitcask_index_disk_t::row_ids_t>
    bitcask_index_disk_t::current_rows(const value_t& key) const {
        const auto key_bytes = key_bytes_for_hash(key);
        VALUE_OR_RETURN(auto ref, hash_index_->get(key_bytes, key_loader()));
        if (!ref.has_value()) {
            return row_ids_t(resource());
        }
        row_ids_t rows(resource());
        // THE EMPTY LIST USED TO MEAN BOTH THINGS HERE, and this is the read every write
        // door of this store builds its next snapshot from: insert() appends to what this
        // answers and append_snapshot REPLACES the key's whole row list with the result. So
        // an unreadable record came back as "this key has no rows" and one ordinary INSERT
        // erased every row_id the key already had, permanently. Refuse instead; a tombstone
        // still legitimately answers with the empty list below.
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return row_ids_t(resource());
        }
        return rows;
    }

    // THE LOOP'S EXIT CONDITION USED TO BE AMBIGUOUS. erase answered false both for "no
    // ref left for this key" and for "the chain ran out from under me", and this loop read
    // the second as the first -- so a key whose refs spilled past an unreadable page kept
    // the refs behind that page and the snapshot written next pointed at only some of them.
    core::error_t bitcask_index_disk_t::erase_all_refs_for_key(std::string_view key_bytes) {
        while (true) {
            VALUE_OR_RETURN(const bool erased, hash_index_->erase(key_bytes, key_loader()));
            if (!erased) {
                return core::error_t::no_error();
            }
        }
    }

    core::error_t bitcask_index_disk_t::append_snapshot(const value_t& key, const row_ids_t& rows) {
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        // NO SEGMENT MEANS NO APPEND, said as a value. This is not a defensive nullptr check
        // on an invariant that holds: rotate_active_segment drops the old handle BEFORE it
        // opens the new one and zeroes active_segment_records_ on the way, so a rotation
        // whose open() refused -- a full volume, a directory that will not take a new file --
        // leaves the store with no handle AND with a record count that will not ask for
        // another rotation. The very next ordinary INSERT then walked past
        // rotate_active_segment_if_needed straight into this dereference and took the
        // process with it, one statement after an environmental refusal that had been
        // reported correctly.
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, rows);
        const auto offset = file_->seek_position();
        if (!write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload)) {
            // BEFORE THE KEYDIR IS TOUCHED, deliberately. The two steps below erase the
            // key's existing refs and point it at this record; running them over a record
            // that is not on disk would make the key unfindable while the statement went on
            // to report success.
            return io_failure("bitcask: the snapshot record could not be written to " +
                              active_data_file_path_.string());
        }
        const auto key_bytes = key_bytes_for_hash(key);
        if (auto erase_error = erase_all_refs_for_key(key_bytes); erase_error.contains_error()) {
            ++active_segment_records_;
            return erase_error;
        }
        if (auto put_error = hash_index_->put(key_bytes,
                                              rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                              static_cast<uint32_t>(active_segment_id_),
                                              offset + sizeof(record_header_t));
            put_error.contains_error()) {
            ++active_segment_records_;
            return put_error;
        }
        ++active_segment_records_;
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::append_tombstone(const value_t& key) {
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        // Same reason as append_snapshot's, and the same road in: a refused rotation leaves
        // no handle, and write_record dereferences one.
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, row_ids_t(resource()));
        if (!write_record(*file_, static_cast<uint8_t>(record_kind_t::tombstone), ++next_timestamp_, payload)) {
            // Same order as append_snapshot: the refs stay until the tombstone is durable,
            // otherwise a restart replays the key as still present while this call reported
            // it removed.
            return io_failure("bitcask: the tombstone record could not be written to " +
                              active_data_file_path_.string());
        }
        const auto key_bytes = key_bytes_for_hash(key);
        auto erase_error = erase_all_refs_for_key(key_bytes);
        ++active_segment_records_;
        return erase_error;
    }

    std::filesystem::path bitcask_index_disk_t::txn_log_file_path() const { return path_ / txn_log_file; }

    std::filesystem::path bitcask_index_disk_t::txn_applied_file_path() const { return path_ / txn_applied_file; }

    uint64_t bitcask_index_disk_t::read_applied_log_offset() const {
        std::ifstream in(txn_applied_file_path());
        uint64_t offset = 0;
        if (!in.good()) {
            return 0;
        }
        in >> offset;
        return in.fail() ? 0 : offset;
    }

    core::error_t bitcask_index_disk_t::write_applied_log_offset(uint64_t offset) const {
        const auto applied_path = txn_applied_file_path();
        const auto temp_path = applied_path.string() + ".tmp";
        {
            std::ofstream out(temp_path, std::ios::trunc);
            if (!out.good()) {
                // M3.5: a failed sidecar open is a recoverable IO failure now —
                // surface it instead of aborting the process.
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar open failed", resource()}};
            }
            out << offset;
            out.flush();
            if (!out.good()) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar flush failed", resource()}};
            }
        }
        if (!publish_replacement_file(fs_, temp_path, applied_path)) {
            remove_file(fs_, temp_path);
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"bitcask: applied-offset sidecar publish failed", resource()}};
        }
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::append_txn_record(uint64_t txn_id,
                                                          uint8_t op_kind,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        std::pmr::string payload(resource());
        components::index::codec::append_le<uint32_t>(payload, static_cast<uint32_t>(values.size()));
        for (const auto& [key, row_id] : values) {
            components::index::codec::append_logical_value(payload, key);
            components::index::codec::append_le<uint64_t>(payload, static_cast<uint64_t>(row_id));
        }

        txn_frame_header_t header{};
        header.magic = txn_magic;
        header.txn_id = txn_id;
        header.op_kind = op_kind;
        header.payload_size = static_cast<uint64_t>(payload.size());

        absl::crc32c_t crc =
            absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.txn_id),
                                                  sizeof(header) - sizeof(header.magic) - sizeof(header.crc)));
        if (!payload.empty()) {
            crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
        }
        header.crc = static_cast<uint32_t>(crc);

        if (!txn_log_file_) {
            txn_log_file_ = open_bitcask_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                // M3.5: recoverable IO failure — surface, do not abort.
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed", resource()}};
            }
        }
        // THE COMMENT ABOVE recover_txn_log SAYS THESE FRAMES ARE FSYNC'D DURABLE BEFORE
        // THE WAL COMMIT MARKER. All three calls used to be issued and dropped, so the
        // sentence was false exactly when it mattered: an ENOSPC or a refused fsync on
        // COMMIT produced no_error, the manager wrote the commit marker over a frame that
        // is not on the device, and the transaction's index entries were gone after a crash
        // with nothing anywhere reporting it.
        const auto frame_offset = txn_log_file_->file_size();
        if (!txn_log_file_->seek(frame_offset)) {
            return io_failure("bitcask: the txn log could not be positioned for an append");
        }
        if (txn_log_file_->write(&header, sizeof(header)) != static_cast<int64_t>(sizeof(header))) {
            return io_failure("bitcask: the txn-log frame header could not be written");
        }
        if (!payload.empty() && txn_log_file_->write(payload.data(), payload.size()) !=
                                    static_cast<int64_t>(payload.size())) {
            return io_failure("bitcask: the txn-log frame payload could not be written");
        }
        if (!txn_log_file_->sync()) {
            return io_failure("bitcask: the txn-log frame could not be made durable");
        }
        return core::error_t::no_error();
    }

    // Replay the index txn log, gated by the WAL committed-txn set (M1.1).
    //
    // Invariant: index txn-log frames are fsync'd durable BEFORE the WAL commit
    // marker is written. A crash inside that window leaves durable index frames
    // for a transaction whose WAL replay rejects (no COMMIT marker). Replaying
    // such a frame would resurrect an uncommitted transaction's index entries
    // (phantom entries). The gate therefore APPLIES a frame only when its txn_id
    // is in committed_txn_ids_; every frame (applied or skipped) still advances
    // write_applied_log_offset(frame_end) so the log is consumed monotonically.
    // There is no txn_id==0 frame class — both writers are guarded txn_id!=0.
    core::error_t bitcask_index_disk_t::recover_txn_log() {
        const auto log_path = txn_log_file_path();
        if (!std::filesystem::exists(log_path)) {
            return core::error_t::no_error();
        }

        // READ THROUGH THE SAME DOOR THE SEGMENTS USE. This was an std::ifstream, whose
        // "could not open" was a silent `return` -- and no test could stage that failure,
        // because an ifstream reaches the path directly and nothing in this build can refuse
        // it. Positional reads off a file_handle_t give the loop the same shape
        // load_from_disk already has (the SIZE decides where the log ends, so a truncated
        // tail is a length question rather than a stream-state one) and put the log behind
        // the same DEV_MODE seam as every other file this store opens.
        const uint64_t applied_offset = read_applied_log_offset();
        auto in = open_bitcask_file(fs_, log_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!in) {
            // THE FILE IS THERE AND WILL NOT OPEN. This used to return as if the log were
            // empty, which silently dropped every committed frame of the last window from
            // the index for the whole uptime -- while a CORRUPT frame below took the process
            // down. One policy now: recovery refuses, open() hands it up, the index does not
            // register and the engine lives.
            return io_failure("bitcask: the txn log exists and could not be opened for recovery");
        }
        const uint64_t log_size = in->file_size();
        uint64_t frame_offset = applied_offset;

        while (frame_offset + sizeof(txn_frame_header_t) <= log_size) {
            txn_frame_header_t header{};
            if (!in->read(&header, sizeof(header), frame_offset)) {
                return io_failure("bitcask: the txn log refused a frame header during recovery");
            }
            if (header.magic != txn_magic) {
                return io_failure("bitcask: the txn log holds a frame with a bad magic");
            }
            const uint64_t payload_offset = frame_offset + sizeof(txn_frame_header_t);
            if (payload_offset + header.payload_size > log_size) {
                // Truncated tail: the frame was never fully written. Everything before it
                // is intact and has already been applied.
                break;
            }
            std::pmr::string payload(resource());
            payload.resize(static_cast<size_t>(header.payload_size));
            if (header.payload_size != 0 &&
                !in->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                return io_failure("bitcask: the txn log refused a frame payload during recovery");
            }

            absl::crc32c_t calc =
                absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.txn_id),
                                                      sizeof(header) - sizeof(header.magic) - sizeof(header.crc)));
            if (!payload.empty()) {
                calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
            }
            if (static_cast<uint32_t>(calc) != header.crc) {
                return io_failure("bitcask: CRC mismatch on a txn-log frame during recovery");
            }

            // Gate: apply only frames of committed transactions. A frame whose
            // txn_id never committed (its WAL commit marker did not land) is
            // skipped to avoid phantom index entries. op_kind is still validated
            // for every frame so a corrupt log still refuses.
            const bool committed = committed_txn_ids_.count(header.txn_id) > 0;
            if (header.op_kind != 1 && header.op_kind != 2) {
                return io_failure("bitcask: the txn log holds a frame with an unknown op kind");
            }
            if (committed) {
                size_t pos = 0;
                const auto count = components::index::codec::read_le<uint32_t>(payload, pos);
                for (uint32_t i = 0; i < count; ++i) {
                    auto key = components::index::codec::read_logical_value(resource(), payload, pos);
                    const auto row_id = static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos));
                    if (header.op_kind == 1) {
                        insert(key, row_id);
                    } else {
                        remove(key, row_id);
                    }
                }
                RETURN_IF_ERROR(sync_if_dirty());
            }
            // Every frame — applied or skipped — advances the applied offset so
            // the log is consumed monotonically and never re-replayed. A sidecar that
            // cannot be rewritten would re-replay this frame on the next open, so recovery
            // refuses here rather than continuing; open() carries the reason out (this used
            // to abort the process for want of a channel that now exists).
            const uint64_t frame_end_offset = payload_offset + header.payload_size;
            RETURN_IF_ERROR(write_applied_log_offset(frame_end_offset));
            frame_offset = frame_end_offset;
        }
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::apply_txn_inserts(uint64_t txn_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // M3.5: a txn-log append/open/sidecar IO failure is recoverable — return
        // it so the manager turns it into an index-side abort. The durable index
        // frame is written BEFORE the data segments are touched, so bailing here
        // leaves the data segments untouched and the frame is re-evaluated by the
        // recover gate on the next open (gated on the WAL commit marker).
        if (auto err = append_txn_record(txn_id, 1, values); err.contains_error()) {
            return err;
        }
        if (!txn_log_file_) {
            txn_log_file_ = open_bitcask_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed (inserts)", resource()}};
            }
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            VALUE_OR_RETURN(auto rows, current_rows(key));
            if (std::find(rows.begin(), rows.end(), row_id) != rows.end()) {
                continue;
            }
            rows.emplace_back(row_id);
            if (auto err = append_snapshot(key, rows); err.contains_error()) {
                return err;
            }
            mark_operation_dirty();
        }
        RETURN_IF_ERROR(sync_if_dirty());
        return write_applied_log_offset(applied_offset);
    }

    core::error_t bitcask_index_disk_t::apply_txn_deletes(uint64_t txn_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // M3.5: mirror of apply_txn_inserts — IO failure becomes a returned error
        // rather than a process abort. Same frame-before-segments ordering.
        if (auto err = append_txn_record(txn_id, 2, values); err.contains_error()) {
            return err;
        }
        if (!txn_log_file_) {
            txn_log_file_ = open_bitcask_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed (deletes)", resource()}};
            }
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            VALUE_OR_RETURN(auto rows, current_rows(key));
            if (rows.empty()) {
                continue;
            }
            const auto original_size = rows.size();
            rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            if (rows.size() == original_size) {
                continue;
            }
            if (rows.empty()) {
                RETURN_IF_ERROR(append_tombstone(key));
            } else {
                RETURN_IF_ERROR(append_snapshot(key, rows));
            }
            mark_operation_dirty();
        }
        RETURN_IF_ERROR(sync_if_dirty());
        return write_applied_log_offset(applied_offset);
    }

    void bitcask_index_disk_t::insert(const value_t& key, size_t value) {
        auto rows = current_rows(key);
        if (rows.has_error()) {
            // The dedup read below decides whether this row is already indexed, so a read
            // that could not finish cannot be treated as "not there": that would append a
            // snapshot built from a PARTIAL row list and drop the rows it could not see.
            note_write_error(rows.error());
            return;
        }
        auto& row_ids = rows.value();
        if (std::find(row_ids.begin(), row_ids.end(), value) != row_ids.end()) {
            return;
        }
        row_ids.emplace_back(value);
        note_write_error(append_snapshot(key, row_ids));
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        // bitcask's insert IS its bulk insert, exactly as its remove is already its bulk
        // remove (see remove_bulk_unchecked below). The per-operation work a bulk path
        // exists to skip is the btree's O(items-per-key) find() scan; bitcask has none —
        // current_rows is one hash lookup plus one record read — and flush_if_needed
        // already returns early while bulk mode is engaged.
        //
        // This used to append a snapshot holding ONLY `value`, on the stated assumption
        // that the caller's keys are unique. They are not. A non-unique index is the
        // ordinary case, and append_snapshot REPLACES a key's entire row list, so every
        // rebuild feed — repopulate_table, and the txn-0 bulk leg of
        // the index agent's insert_many — collapsed each repeated key down to whichever
        // row happened to be written last. CHECKPOINT and VACUUM both drive that feed, so
        // a hashed index silently lost its duplicates at the first checkpoint and every
        // restart afterwards answered from the reduced list.
        //
        // The loss was invisible until reads started going through find(): the keydir
        // that answered them before keeps one entry per key anyway, so it reported the
        // same single row either way.
        insert(key, value);
    }

    void bitcask_index_disk_t::remove(value_t key) {
        auto ref = hash_index_->get(key_bytes_for_hash(key), key_loader());
        if (ref.has_error()) {
            note_write_error(ref.error());
            return;
        }
        if (!ref.value().has_value()) {
            return;
        }
        note_write_error(append_tombstone(key));
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::remove(const value_t& key, size_t row_id) {
        auto read = current_rows(key);
        if (read.has_error()) {
            // Same reason as insert(): an unfinished read here would look like "the key
            // holds no such row" and skip a removal that is owed.
            note_write_error(read.error());
            return;
        }
        auto& rows = read.value();
        if (rows.empty()) {
            return;
        }
        const auto original_size = rows.size();
        rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
        if (rows.size() == original_size) {
            return;
        }

        if (rows.empty()) {
            note_write_error(append_tombstone(key));
        } else {
            note_write_error(append_snapshot(key, rows));
        }
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::remove_bulk_unchecked(const value_t& key, size_t row_id) {
        // bitcask's remove is an O(1) hash lookup + snapshot rewrite and already skips
        // the per-op flush while bulk mode is engaged (flush_if_needed checks bulk_mode_),
        // so the bulk remove IS the normal remove — there is no per-key find()-scan to
        // avoid here (that is the btree backend's problem).
        remove(key, row_id);
    }

    void bitcask_index_disk_t::flush_if_needed() {
        if (bulk_mode_) {
            return;
        }
        if (should_flush()) {
            // Its callers (insert/remove) are void; the refusal is parked and the next
            // force_flush hands it over, which is the same road every other write failure
            // on those doors takes.
            note_write_error(sync_if_dirty());
        }
    }

    core::error_t bitcask_index_disk_t::force_flush() {
        // A FAILED FLUSH IS THE ANSWER, not a footnote. The checkpoint reads this value and
        // trims the WAL behind it, so reporting no_error over an fsync that refused would
        // cut the log in front of an index that never reached the device.
        auto flush_error = sync_if_dirty();
        // Hand over anything the void-returning write paths could not report themselves, once.
        auto pending = pending_write_error_;
        pending_write_error_ = core::error_t::no_error();
        // The flush that just refused is the newer, more specific fact; a parked error from
        // an earlier statement is not lost either -- it stays parked for the next call.
        if (flush_error.contains_error()) {
            note_write_error(std::move(pending));
            return flush_error;
        }
        return pending;
    }

    void bitcask_index_disk_t::note_write_error(core::error_t err) {
        if (err.contains_error() && !pending_write_error_.contains_error()) {
            pending_write_error_ = std::move(err);
        }
    }

    core::error_t bitcask_index_disk_t::sync_if_dirty() {
        if (!is_dirty() || !file_) {
            return core::error_t::no_error();
        }
        // THE DIRTY FLAG STAYS SET ON A REFUSAL. Both fsyncs used to be issued and dropped
        // and the flag cleared regardless, so the next flush found "nothing to write" over
        // data that never left the page cache -- and force_flush() answered no_error, which
        // is what the checkpoint reads before it trims the WAL.
        if (!file_->sync()) {
            return io_failure("bitcask: the active segment could not be made durable");
        }
        // The keydir is the other half of the same answer: a segment on the device whose
        // keydir entry is not makes the key unfindable just the same.
        RETURN_IF_ERROR(hash_index_->sync());
        reset_flush_state();
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::load_entries(entries_t& entries) const {
        // for_each's callback answers nothing, so the first record this walk cannot read is
        // remembered here and returned once the walk is over. A rebuild fed from PART of an
        // index is a rebuild that drops rows without saying so -- which is exactly what the
        // keydir walk already refuses for, and the record read is the other half of it.
        core::error_t read_error = core::error_t::no_error();
        auto walk_error = hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            if (read_error.contains_error()) {
                return;
            }
            row_ids_t rows(resource());
            value_t key(resource(), nullptr);
            auto read = read_rows_at(ref.log_file_id, ref.log_offset, rows, &key);
            if (read.has_error()) {
                read_error = read.error();
                return;
            }
            if (!read.value()) {
                return; // tombstone: this key legitimately contributes no entries
            }
            for (auto row : rows) {
                entries.emplace_back(value_t(resource(), key), row);
            }
        });
        if (walk_error.contains_error()) {
            return walk_error;
        }
        return read_error;
    }

    core::error_t bitcask_index_disk_t::find(const value_t& value, result& res) const {
        VALUE_OR_RETURN(auto ref, hash_index_->get(key_bytes_for_hash(value), key_loader()));
        if (!ref.has_value()) {
            return core::error_t::no_error();
        }
        row_ids_t rows(resource());
        // "Could not read the record" used to leave res untouched and answer no_error, i.e.
        // it reached the reader as "this key has no rows" -- the same silent subset the
        // keydir walk above already refuses to produce.
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return core::error_t::no_error();
        }
        res.reserve(res.size() + rows.size());
        res.insert(res.end(), rows.begin(), rows.end());
        return core::error_t::no_error();
    }

    // scan_range IS GONE FROM HERE, and the absence is the change. It existed only
    // because the erased base declared it pure, and its whole body was an abort: a hashed
    // store has no ordering to scan. The refusal it stood for now lives one level up, in
    // bitcask_index_agent_t::read_rows, which is the only caller that could ever ask and
    // is the only place that can answer with core::error_t instead of a signal.

    core::error_t bitcask_index_disk_t::merge_immutable_segments() {
        std::vector<segment_info_t> immutable_segments;
        std::vector<uint64_t> removed_segment_ids;
        std::vector<disk_hash_table_t::value_ref_t> refs;
        bool built = false;

        const uint64_t frontier_segment_id = active_segment_id_;
        VALUE_OR_RETURN(const auto segments, collect_segments());
        for (const auto& seg : segments) {
            if (seg.id < frontier_segment_id) {
                immutable_segments.push_back(seg);
            }
        }
        if (immutable_segments.empty()) {
            return core::error_t::no_error();
        }
        // THE MERGED OUTPUT ALTERNATES BETWEEN THE TWO RESERVED IDS, 1 AND 0.
        //
        // It used to be `front().id - 1`, which is right exactly twice: the first merge
        // takes {2,...} and writes 1, the second takes {1,3,...} and writes 0 -- and the
        // THIRD takes {0,...}, so `0 - 1` wrapped to 2^64-1. That produced a segment file
        // named for the wrapped id while the keydir recorded its low 32 bits
        // (0xFFFFFFFF), so every relocated key pointed at a file name that does not
        // exist and find() answered EMPTY for the whole merged set. Three rotations is
        // roughly 3 * segment_record_limit index writes, i.e. ordinary traffic.
        //
        // The lowest immutable segment is either the PREVIOUS merged output (0 or 1) or,
        // on the first merge, a regular segment (>= 2). Flipping the reserved bit in the
        // first case and taking 1 in the second keeps the output below every regular id
        // -- which is what makes merged data replay before rotated data -- and it can
        // never collide with the segment it just read, because the merge that wrote the
        // previous output removed the other reserved id.
        const uint64_t merged_segment_id =
            immutable_segments.front().id < regular_segment_id_start_ ? immutable_segments.front().id ^ 1u : 1u;
        for (const auto& seg : immutable_segments) {
            // The merged output is published by renaming over its own path, so a segment
            // that IS the output must not also be unlinked afterwards.
            if (seg.id != merged_segment_id) {
                removed_segment_ids.push_back(seg.id);
            }
        }
        // A LOCAL, not bulk_prev_rehash_suppressed_. This used to park the flag on that
        // MEMBER because the function released the store's lock in the middle and the
        // value had to survive the gap -- and the member belongs to set_bulk_mode, which
        // restores the keydir's rehash setting from it when the bulk window closes. A
        // merge that overwrote it left the window restoring the merge's value instead of
        // the one bulk mode captured, i.e. auto-rehash suppressed for good. There is no
        // gap any more, so there is no reason to leave the value on the object.
        const bool prev_rehash_suppressed = hash_index_->set_auto_rehash_suppressed(true);
        // RESTORED BY SCOPE EXIT, not by hand at each way out. Every refusal added below
        // returns straight from the middle of this function, and a merge that left
        // auto-rehash suppressed would leave it suppressed for the life of the store -- the
        // same shape load_from_disk already uses, and the reason it uses it.
        struct restore_rehash_state_t {
            disk_hash_table_t* table{nullptr};
            bool prev{false};
            ~restore_rehash_state_t() {
                if (table) {
                    table->set_auto_rehash_suppressed(prev);
                }
            }
        } restore_rehash_state{hash_index_.get(), prev_rehash_suppressed};
        // THE REF LIST DECIDES WHICH SEGMENTS GET DELETED at the end of this function. A
        // walk that stopped early would build a SHORT list, relocate only the keys on it,
        // and then unlink the segments holding the keys it never saw.
        RETURN_IF_ERROR(hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            if (ref.log_file_id < static_cast<uint32_t>(frontier_segment_id)) {
                refs.push_back(ref);
            }
        }));
        if (refs.empty()) {
            return core::error_t::no_error();
        }
        const auto merged_path = segment_file_path(path_, merged_segment_id);
        const auto temp_path = merge_temp_file_path(path_, merged_segment_id);
        const auto meta_temp_path = std::filesystem::path(temp_path.string() + ".meta");
        remove_file(fs_, temp_path);
        remove_file(fs_, meta_temp_path);

        // NOTHING IS PUBLISHED UNTIL EVERYTHING SUCCEEDED, so every refusal below drops the
        // two temp files and leaves the directory exactly as it found it: no manifest, no
        // merged segment, and -- the part that used to be unconditional -- no unlinked
        // sources. The table keeps answering from the segments it was answering from, and
        // the debt merge_pending_segments re-arms makes the next attempt run again.
        const auto abandon_merge = [&](std::unique_ptr<core::filesystem::file_handle_t>& merged,
                                       std::unique_ptr<core::filesystem::file_handle_t>& meta,
                                       core::error_t reason) {
            merged.reset();
            meta.reset();
            remove_file(fs_, temp_path);
            remove_file(fs_, meta_temp_path);
            return reason;
        };

        auto merged_file = open_bitcask_file(fs_,
                                             temp_path,
                                             file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                             file_lock_type::NO_LOCK);
        if (!merged_file) {
            return io_failure("bitcask: the merge output " + temp_path.string() + " could not be opened");
        }
        auto meta_file = open_bitcask_file(fs_,
                                           meta_temp_path,
                                           file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                           file_lock_type::NO_LOCK);
        if (!meta_file) {
            return abandon_merge(merged_file,
                                 meta_file,
                                 io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                                            " could not be opened"));
        }

        uint64_t meta_records = 0;
        for (const auto& ref : refs) {
            row_ids_t rows(resource());
            value_t key(resource(), nullptr);
            auto read = read_rows_at(ref.log_file_id, ref.log_offset, rows, &key);
            if (read.has_error()) {
                // A RECORD THAT WOULD NOT READ USED TO BE SKIPPED -- and the segment holding
                // it was unlinked a few lines further down anyway, so the key was gone from
                // the index and from the disk at once. The whole merge stops instead.
                return abandon_merge(merged_file, meta_file, read.error());
            }
            if (!read.value()) {
                // A tombstone carries no rows, so there is nothing to relocate: dropping it
                // from the merged output IS the compaction.
                continue;
            }
            const auto key_bytes = key_bytes_for_hash(key);
            auto payload = serialize_payload(resource(), key, rows);
            const auto offset = merged_file->seek_position();
            if (!write_record(*merged_file, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload)) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: a relocated record could not be written to " +
                                                temp_path.string()));
            }

            uint32_t key_size = static_cast<uint32_t>(key_bytes.size());
            int64_t row_value = rows.empty() ? -1 : static_cast<int64_t>(rows.back());
            uint32_t old_log_file_id = ref.log_file_id;
            uint64_t old_log_offset = ref.log_offset;
            uint64_t new_log_offset = offset + sizeof(record_header_t);
            const auto meta_write = [&](const void* data, uint64_t size) {
                return meta_file->write(const_cast<void*>(data), size) == static_cast<int64_t>(size);
            };
            if (!meta_write(&key_size, sizeof(key_size)) ||
                (key_size != 0 && !meta_write(key_bytes.data(), key_size)) ||
                !meta_write(&old_log_file_id, sizeof(old_log_file_id)) ||
                !meta_write(&old_log_offset, sizeof(old_log_offset)) ||
                !meta_write(&row_value, sizeof(row_value)) ||
                !meta_write(&new_log_offset, sizeof(new_log_offset))) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge journal entry could not be written to " +
                                                meta_temp_path.string()));
            }
            ++meta_records;
        }

        if (meta_records != 0) {
            if (!merged_file->sync() || !meta_file->sync()) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge output could not be made durable"));
            }
            merged_file.reset();
            meta_file.reset();
            if (!write_merge_manifest(fs_, path_, merged_segment_id, removed_segment_ids)) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge manifest could not be published"));
            }
            if (!move_files(fs_, temp_path, merged_path)) {
                remove_merge_manifest(path_);
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merged segment could not be published as " +
                                                merged_path.string()));
            }
            built = true;
        } else {
            merged_file.reset();
            meta_file.reset();
            remove_file(fs_, temp_path);
            remove_file(fs_, meta_temp_path);
        }

        if (!built) {
            restore_rehash_state.table = nullptr;
            hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
            if (!prev_rehash_suppressed) {
                RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
            }
            return core::error_t::no_error();
        }

        // PAST THIS LINE THE MANIFEST IS ON DISK and names the merged segment, so a refusal
        // below is no longer a lost merge: apply_merge_recovery_cleanup finishes it on the
        // next open, and load_from_disk rebuilds the keydir from every segment anyway. What
        // must NOT happen is unlinking the sources over a half-applied relocation, which is
        // why each step below returns instead of falling through to the removal loop.
        meta_file = open_bitcask_file(fs_, meta_temp_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!meta_file) {
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() + " could not be reopened");
        }
        uint64_t meta_offset = 0;
        const uint64_t meta_size = meta_file->file_size();
        while (meta_offset < meta_size) {
            // A BARE BREAK HERE MEANT "the rest of the relocations never happened", and the
            // loop below then unlinked the sources those un-relocated keys still point at.
            // The journal was written by this same call and fsync'd, so a short read is a
            // device refusing, never a legitimate end -- meta_size is the end.
            uint32_t key_size = 0;
            if (!meta_file->read(&key_size, sizeof(key_size), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(key_size);

            std::string key_bytes;
            key_bytes.resize(key_size);
            if (key_size != 0 && !meta_file->read(key_bytes.data(), key_size, meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += key_size;

            uint32_t old_log_file_id = 0;
            uint64_t old_log_offset = 0;
            int64_t row_value = 0;
            uint64_t new_log_offset = 0;
            if (!meta_file->read(&old_log_file_id, sizeof(old_log_file_id), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(old_log_file_id);
            if (!meta_file->read(&old_log_offset, sizeof(old_log_offset), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(old_log_offset);
            if (!meta_file->read(&row_value, sizeof(row_value), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(row_value);
            if (!meta_file->read(&new_log_offset, sizeof(new_log_offset), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(new_log_offset);

            VALUE_OR_RETURN(auto current, hash_index_->get(key_bytes, key_loader()));
            if (!current.has_value()) {
                continue;
            }
            if (current->log_file_id != old_log_file_id || current->log_offset != old_log_offset) {
                continue;
            }
            RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
            RETURN_IF_ERROR(
                hash_index_->put(key_bytes, row_value, static_cast<uint32_t>(merged_segment_id), new_log_offset));
        }
        meta_file.reset();
        remove_file(fs_, meta_temp_path);
        // THE RELOCATED KEYDIR REACHES THE DEVICE BEFORE THE SOURCES ARE UNLINKED. A refusal
        // here leaves every source segment in place and the manifest on disk, so the next
        // open replays them and finishes the job.
        RETURN_IF_ERROR(hash_index_->sync());
        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            remove_file(fs_, removed_path);
        }
        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
        }
        return core::error_t::no_error();
    }

    // WIPING THIS INDEX IS A STEP THAT CAN REFUSE, and the reason it could not is this
    // function's RETURN. It used to be void, so the only thing it could do with a failure
    // was park it in pending_write_error_ -- where the READ path never looks. Between a
    // clear() whose rebuild refused and the next force_flush, find() answered "this key has
    // no rows" over segments that were all still on the device: a silent wrong answer, on
    // the door the runtime repopulate takes on every CHECKPOINT.
    //
    // Wipe all stored data IN PLACE, keeping the instance alive and writable
    // (re-initialized empty). Unlike drop(), the directory and a fresh active segment
    // survive so the subsequent txn_id==0 re-inserts (direct, non-txn-log path) repopulate
    // cleanly.
    //
    // THERE IS NOTHING TO DRAIN. A merge used to be a task on this store's own thread, so
    // this had to stop that thread before unlinking the segments the task was about to read.
    // A merge is now the owner's own work, run from inside one of its handlers, and this IS
    // one of its handlers -- no merge can be running and none can start. What survives the
    // wipe is the DEBT, and it is dropped below with the segments it names.
    core::error_t bitcask_index_disk_t::clear() {
        // THE LISTING COMES FIRST, ABOVE EVERY HANDLE AND EVERY UNLINK, and this is the one
        // early return in the whole function. collect_segments is const and walks nothing but
        // std::filesystem, so lifting it costs nothing and buys the only clean refusal this
        // door has: the store is left EXACTLY as it was -- handles open, keydir intact,
        // segments intact -- so the caller's failed statement is the end of it and the next
        // clear() starts from a consistent index. The old body parked the failure here and
        // went on to unlink CURRENT, the txn log and the sidecar, and then to wipe the keydir
        // through load_from_disk, which is how a directory that merely could not be READ
        // ended up as a live store with an empty keydir over full segments.
        VALUE_OR_RETURN(auto segments, collect_segments());

        // Close every open handle before unlinking so stale inodes are not held.
        file_.reset();
        txn_log_file_.reset();

        // BELOW THIS LINE THERE ARE NO EARLY RETURNS, and that is not stylistic. Every step
        // from here is a mutation, and bailing out between two of them leaves a store with no
        // active segment and no keydir -- the shape drop() produces, on an object the caller
        // is entitled to keep using. So the steps all run, the FIRST reason wins, and the
        // caller gets it at the end.
        core::error_t first_error = core::error_t::no_error();
        const auto record = [&first_error](core::error_t err) {
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        };
        // std::filesystem::remove's error_code overload, NOT remove_file's bool, and the
        // difference is the whole point: remove_file answers false both for "the device would
        // not unlink it" and for "there was nothing there", and the second is the wipe's goal
        // reached. The ec overload separates them, and it keeps exceptions off this path
        // (rule 2). Every one of these results used to be dropped, so a clear() that could
        // not remove a segment reported success -- and load_from_disk then honestly replayed
        // the survivor back into the keydir, handing find() rows that clear() had promised
        // were gone.
        const auto unlink_artifact = [&](const std::filesystem::path& artifact) {
            std::error_code ec;
            std::filesystem::remove(artifact, ec);
            if (ec) {
                record(io_failure("bitcask: " + artifact.string() + " could not be removed by clear(): " +
                                  ec.message()));
            }
        };

        // Remove all on-disk bitcask artifacts: data segments, the CURRENT pointer, the txn
        // log and its applied-offset sidecar. hash_index.bin is NOT among them -- the rebuild
        // below unlinks and re-creates it through disk_hash_table_t::reset_storage, and the
        // TABLE OBJECT survives that (the unique_ptr is never replaced), which is what
        // clear_keeps_shared_hash_storage pins. The comment that used to stand here said the
        // file was cleared in place; that stopped being true when the wipe moved to
        // reset_storage, and the property that actually matters -- the store's handle on the
        // keydir staying valid -- is a property of the OBJECT, not of the inode.
        for (const auto& segment : segments) {
            unlink_artifact(segment.path);
        }
        unlink_artifact(current_segment_path(path_));
        unlink_artifact(txn_log_file_path());
        unlink_artifact(txn_applied_file_path());
        unlink_artifact(merge_manifest_path(path_));

        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_ = regular_segment_id_start_;
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        bulk_mode_ = false;
        // The rotations that owed a merge owed it over segments that no longer exist.
        merge_pending_ = false;

        // Recreate the backing exactly as the ctor does, but over the now-empty directory.
        record(initialize_storage());
        if (!hash_index_) {
            // THIS ABORT IS LEGITIMATE AND STAYS. It is not an I/O outcome -- no device is
            // involved -- it is a broken invariant: clear() keeps the index alive and
            // writable, so it is only ever called on a live one; the store is released
            // exactly once, by drop(), and the agent refuses to clear after that. Reaching
            // here means that guard was bypassed, and every line below would work on a null
            // keydir. Turning it into a reported error would carry on over one.
            assert(false && "bitcask_index_disk_t::clear: the store was released by drop()");
            std::abort();
        }
        // THE WIPE IS load_from_disk's, not a second one here. The keydir has exactly one
        // author now (disk_hash_table_t::reset_storage, called from load_from_disk below), so
        // this door no longer opens its own. What it DOES still owe is the rehash-suppression
        // flag: disk_hash_table_t::clear() used to clear it as a side effect, and bulk_mode_
        // is reset by hand above for the same reason -- a store coming out of clear() is not
        // in bulk mode and must not be left with auto-rehash pinned off.
        hash_index_->set_auto_rehash_suppressed(false);
        bool storage_is_open = true;
        if (auto load_error = load_from_disk(); load_error.contains_error()) {
            record(std::move(load_error));
            storage_is_open = false;
        } else if (auto segment_error = open_active_segment(); segment_error.contains_error()) {
            record(std::move(segment_error));
            storage_is_open = false;
        }
        if (!storage_is_open) {
            // THE REBUILD DID NOT FINISH, so what the keydir holds no longer describes what
            // is on the device, and the segments it was built from have just been unlinked.
            // Closing it is what makes every later read and every later write REFUSE instead
            // of answering out of it -- see disk_hash_table_t::close_storage for why that
            // needs no flag and no per-door guard. It is loud, it is not fatal, and the next
            // successful clear() re-opens the table and the index serves again.
            //
            // A refusal from one of the unlinks above does NOT come here: that store is
            // consistent with its disk, it simply holds more than clear() promised to leave,
            // and the caller has been told so by value.
            hash_index_->close_storage();
        }
        // NOT ONE note_write_error ON THIS PATH. The parking slot exists for the void write
        // doors that have nowhere else to put a failure; this door has a return value, and
        // parking here is what made the failure invisible to find() in the first place.
        //
        // committed_txn_ids_ is intentionally left as-is: the txn log it gated
        // is gone, and txn_id==0 re-inserts take the direct path (no gate).
        return first_error;
    }
    void bitcask_index_disk_t::drop() {
        // No drain here either, for the reason clear() states: a merge only ever runs
        // inside one of the owner's handlers, and this is one of them.
        merge_pending_ = false;
        if (is_dirty() && file_) {
            // The files are unlinked below, so nothing here can be acted on and nothing
            // downstream will ever look for it -- but the answers are still read rather than
            // discarded, and the state stays honest if the sync refused.
            const bool segment_synced = file_->sync();
            const bool keydir_synced = hash_index_ ? !hash_index_->sync().contains_error() : true;
            if (segment_synced && keydir_synced) {
                reset_flush_state();
            }
        }
        file_.reset();
        txn_log_file_.reset();
        hash_index_.reset();
        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_ = regular_segment_id_start_;
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        remove_directory(fs_, path_);
    }
} // namespace services::index
