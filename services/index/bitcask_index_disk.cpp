#include "bitcask_index_disk.hpp"

#include "absl/crc/crc32c.h"
#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <cstdio>
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

#ifdef DEV_MODE
    namespace {
        bitcask_file_interposer_t* dev_bitcask_file_interposer_ = nullptr;
        std::atomic<uint64_t> g_bitcask_rotated_segment_opens{0};
    } // namespace

    uint64_t bitcask_rotated_segment_opens() noexcept {
        return g_bitcask_rotated_segment_opens.load(std::memory_order_relaxed);
    }
    void reset_bitcask_rotated_segment_opens() noexcept {
        g_bitcask_rotated_segment_opens.store(0, std::memory_order_relaxed);
    }

    void dev_set_bitcask_file_interposer(bitcask_file_interposer_t* interposer) {
        dev_bitcask_file_interposer_ = interposer;
    }

    bitcask_file_interposer_t* dev_bitcask_file_interposer() { return dev_bitcask_file_interposer_; }
#endif

    namespace {
        // EVERY handle this store opens goes through here, so the DEV_MODE seam is armed in
        // one place instead of at each of the seven open sites. In a release build this is
        // core::filesystem::open_file and nothing else.
        // WHY A FILE WOULD NOT OPEN, kept from the one call that knows.
        // core::filesystem::open_file answers nullptr and nothing else, so every refusal in
        // this file said "could not be opened" and stopped -- the SAME SENTENCE for a file
        // that is not there, for a permission, and for a descriptor table that is full. The
        // last two are transient and the first is not, and nothing in the message let an
        // operator tell them apart: a refusal that costs an index its registration read as
        // undiagnosable, which is exactly the shape a load-dependent failure takes when it
        // finally happens on someone else's machine.
        //
        // Captured AT THE SEAM because that is where it is still true: open_file returns
        // straight off the failed open(2), so errno is that call's, and anything run in
        // between -- the interposer lookup below included -- is entitled to clobber it.
        // THREAD_LOCAL rather than static: one store per index agent, one agent per thread of
        // the executor pool, and a diagnostic is not a reason to share a byte between them.
        thread_local int last_open_errno = 0;

        std::string open_refusal_reason() {
            return last_open_errno == 0 ? std::string{"no reason reported"}
                                        : std::string{std::strerror(last_open_errno)};
        }

        std::unique_ptr<core::filesystem::file_handle_t> open_bitcask_file(
            core::filesystem::local_file_system_t& fs,
            const std::filesystem::path& path,
            file_flags flags,
            file_lock_type lock) {
            auto handle = open_file(fs, path, flags, lock);
            last_open_errno = handle == nullptr ? errno : 0;
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

        // ANSWERS WHETHER THE RECORD WAS READ, and that answer is load-bearing rather than
        // tidy. The payload is [key][uint32 count][uint64 row ids]: `pos` walks it, and the
        // key codec leaves `pos` UNMOVED when it refuses. The count was then read from the
        // KEY'S OWN BYTES and every row id after it from wherever that landed -- so a refused
        // key did not produce "no rows", it produced a list of INVENTED row ids, silently, on
        // the path that opens the database. Both callers refuse the whole operation now.
        [[nodiscard]] bool deserialize_payload(std::pmr::memory_resource* resource,
                                               const std::pmr::string& payload,
                                               services::index::bitcask_index_disk_t::value_t& key,
                                               std::pmr::vector<size_t>& rows) {
            size_t pos = 0;
            bool ok = true;
            key = components::index::codec::read_logical_value(resource, payload, pos, &ok);
            const auto n = components::index::codec::read_le<uint32_t>(payload, pos, &ok);
            if (!ok) {
                return false;
            }
            rows.clear();
            // The count is four stored bytes; one flipped high bit claims four billion rows
            // out of a record that holds room for a handful. Sized against what is actually
            // left of the payload -- eight bytes per row id -- before it is trusted to
            // reserve, so a corrupt count cannot ask the allocator for 32GB.
            if (n > (payload.size() - pos) / sizeof(uint64_t)) {
                return false;
            }
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                rows.emplace_back(
                    static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos, &ok)));
            }
            return ok;
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

        // FOUR ANSWERS, NOT TWO, and the three that used to be one `false` are opposite facts
        // about the store.
        //
        //   absent     -- nothing has written this sidecar yet. The ordinary state of a fresh
        //                 directory, and the ONE answer a caller may quietly act on.
        //   unopenable -- the file IS there and the open was refused: a permission, a
        //                 descriptor limit, a device. TRANSIENT -- it clears by itself, and
        //                 the next open finds the sidecar exactly where it was.
        //   damaged    -- the file is there, it opened, and its bytes will not parse. This one
        //                 does NOT clear by itself.
        //   ok         -- the value came out.
        //
        // While `unopenable` and `absent` were the same answer, a directory the process could
        // not read for a moment was silently taken for a directory that had never been written
        // to -- and the callers below then substituted a value of their own for the one they
        // could not read.
        enum class sidecar_state_t
        {
            ok,
            absent,
            unopenable,
            damaged
        };

        sidecar_state_t read_sidecar_uint64(const std::filesystem::path& file, uint64_t& out) {
            std::error_code ec;
            const bool present = std::filesystem::exists(file, ec);
            if (ec) {
                // exists() itself refusing is a directory this process cannot look into, which
                // is the transient class, not "there is no such file".
                return sidecar_state_t::unopenable;
            }
            if (!present) {
                return sidecar_state_t::absent;
            }
            std::ifstream input(file);
            if (!input.good()) {
                return sidecar_state_t::unopenable;
            }
            input >> out;
            return input.fail() ? sidecar_state_t::damaged : sidecar_state_t::ok;
        }

        sidecar_state_t read_current_segment_id(const std::filesystem::path& directory, uint64_t& segment_id) {
            return read_sidecar_uint64(current_segment_path(directory), segment_id);
        }

        std::filesystem::path merge_manifest_path(const std::filesystem::path& directory) {
            return directory / merge_manifest_file;
        }

        sidecar_state_t read_merge_manifest(const std::filesystem::path& directory,
                                            uint64_t& merged_segment_id,
                                            std::vector<uint64_t>& removed_segment_ids) {
            const auto manifest_path_value = merge_manifest_path(directory);
            std::error_code ec;
            const bool present = std::filesystem::exists(manifest_path_value, ec);
            if (ec) {
                return sidecar_state_t::unopenable;
            }
            if (!present) {
                return sidecar_state_t::absent;
            }
            std::ifstream input(manifest_path_value);
            if (!input.good()) {
                // THE FILE IS THERE AND WOULD NOT OPEN, which is not the same fact as "no merge
                // was interrupted here" and must not be answered with it: the caller unlinks
                // segments on the strength of this file, so "no manifest" is a licence and this
                // is a refusal.
                return sidecar_state_t::unopenable;
            }
            std::size_t removed_count = 0;
            input >> merged_segment_id >> removed_count;
            if (input.fail()) {
                return sidecar_state_t::damaged;
            }
            removed_segment_ids.clear();
            // NO reserve() ON A NUMBER THAT CAME OFF THE DISK. `7 999999999999999999` parses
            // without a hitch -- eighteen digits fit a size_t, so no failbit -- and
            // reserve(1e18) asks the allocator for eight exabytes and throws std::bad_alloc,
            // on the path that opens a database, with nothing in this build to catch it
            // (rule 2). The loop below is bounded by the FILE instead: it reads ids until the
            // stream runs out, which for a lying count is one iteration past the real ones.
            // The list a merge names is a handful of segments, so the growth this gives up is
            // a few reallocations of a vector that never gets long.
            for (std::size_t i = 0; i < removed_count; ++i) {
                uint64_t removed_id = 0;
                input >> removed_id;
                if (input.fail()) {
                    removed_segment_ids.clear();
                    return sidecar_state_t::damaged;
                }
                removed_segment_ids.push_back(removed_id);
            }
            return sidecar_state_t::ok;
        }

        // TRUE MEANS THE PATH IS GONE FROM THE DIRECTORY. std::filesystem::remove's
        // error_code overload rather than remove_file's bool, for the reason clear() states
        // where it does the same: remove_file answers false both for "the device would not
        // unlink it" and for "there was nothing there", and only the first is a failure --
        // the second is the goal already reached. The ec overload separates them and keeps
        // exceptions off this path (rule 2).
        [[nodiscard]] bool unlink_if_present(const std::filesystem::path& artifact, std::error_code& ec) {
            ec.clear();
            std::filesystem::remove(artifact, ec);
            return !ec;
        }

        // FALSE MEANS THE RECORD OF THE MERGE IS STILL ON DISK. Both callers act on that: the
        // one that finished a merge must not leave a manifest naming segments it has already
        // unlinked (the next merge would overwrite it, and with it the only note saying an
        // unlink is still owed), and the one that could not publish must not leave a manifest
        // naming a merged segment that does not exist.
        [[nodiscard]] bool remove_merge_manifest(const std::filesystem::path& directory, std::error_code& ec) {
            return unlink_if_present(merge_manifest_path(directory), ec);
        }

        // LOUD, NOT FATAL, and this is the one shape in the store where that is the whole
        // answer. The two sidecar writers below drop their temp on a path that has ALREADY
        // decided to return false, so the removal cannot change what they answer -- and unlike
        // the merge temps, a survivor here cannot corrupt anything either: both temps are
        // reopened with std::ios::trunc by the next attempt and nothing else in the store ever
        // reads them. Turning it into a refusal would fail an open over a stray byte in a file
        // nobody reads; dropping it in silence, which is what used to happen, leaves an
        // operator with a directory quietly filling up and no line anywhere saying why. So it
        // is SAID, on the same channel the construct-and-open ctor uses, and the caller's own
        // failure carries on being the answer.
        void report_undroppable_temp(const std::filesystem::path& temp_path) {
            std::error_code ec;
            if (unlink_if_present(temp_path, ec)) {
                return;
            }
            std::fprintf(stderr,
                         "bitcask: the temp file %s was left behind by a failed publish and could not be removed: "
                         "%s\n",
                         temp_path.string().c_str(),
                         ec.message().c_str());
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
                    report_undroppable_temp(temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, manifest_path)) {
                return true;
            }
            // The publish left the temp behind; nothing reads it, and leaving it would have
            // the next attempt write over a file it did not create.
            report_undroppable_temp(temp_path);
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
                    report_undroppable_temp(temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, current_path)) {
                return true;
            }
            report_undroppable_temp(temp_path);
            return false;
        }

        // NOT-COMPLETE means the record did NOT reach the file -- the header short, the
        // payload short, or the write refused outright. Both writes used to be issued and
        // dropped, so a full device produced a snapshot that exists only in the keydir: the
        // statement reported success, the key kept pointing at an offset holding nothing, and
        // the next read of it answered empty.
        //
        // IT REPORTS HOW MUCH LANDED, not just whether it finished. A record is TWO writes,
        // so a refusal can leave anything from nothing to a whole header plus part of a
        // payload sitting at the end of the segment; a bool collapsed all of those into one
        // answer and left the caller -- which is the only code that knows where the record
        // began -- unable to tell a stump from an untouched file. That is the same loss the
        // filesystem layer under it used to commit (see core::filesystem::write_result_t),
        // and re-committing it one level up would make the fix below it pointless.
        [[nodiscard]] core::filesystem::write_result_t write_record(core::filesystem::file_handle_t& file,
                                                                    uint8_t kind,
                                                                    uint64_t timestamp,
                                                                    const std::pmr::string& payload) {
            // VALUE-INITIALIZED FIRST, THEN FILLED IN, and the difference is three bytes that
            // used to be neither.
            //
            // record_header_t is {uint32, uint8, uint64, uint64}, which the ABI lays out with
            // THREE PADDING BYTES at offsets 5-7. `record_header_t header{a, b, c, d}` is
            // AGGREGATE initialization, and aggregate initialization says nothing about
            // padding -- while `record_header_t header{}` value-initializes, which zeroes the
            // whole object, padding included. Those three bytes sit INSIDE the range the CRC
            // below covers and inside the 24 bytes written to the device, so the store used
            // to hash and ship three bytes of whatever the stack held. Reading them is
            // undefined behaviour, they are three bytes of this process's memory written into
            // a database file, and the CRC of a record was not a function of the record. This
            // is what the txn frame header two doors down has always done (txn_frame_header_t
            // header{}, then the fields), and the seven padding bytes IT carries are the
            // reason it matters more than it looks.
            //
            // THE ON-DISK HASH DOES NOT CHANGE. Range and seed are untouched, and both sides
            // compute the CRC over the bytes AS THEY SIT IN THE FILE -- the writer over the
            // image it is about to write, the reader over the image it just read -- so every
            // record written before this change still verifies, byte for byte. What changes
            // is only that the three bytes now have a value this store chose.
            record_header_t header{};
            header.kind = kind;
            header.payload_size = static_cast<uint64_t>(payload.size());
            header.timestamp = timestamp;

            absl::crc32c_t crc = absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
            if (!payload.empty()) {
                crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
            }
            header.crc = static_cast<uint32_t>(crc);

            const auto header_write = file.write(&header, sizeof(header));
            if (!header_write.complete) {
                return core::filesystem::write_result_t::refused(header_write.bytes_written);
            }
            if (payload.empty()) {
                return core::filesystem::write_result_t::done(header_write.bytes_written);
            }
            const auto payload_write = file.write(const_cast<char*>(payload.data()), payload.size());
            const uint64_t landed = header_write.bytes_written + payload_write.bytes_written;
            if (!payload_write.complete) {
                return core::filesystem::write_result_t::refused(landed);
            }
            return core::filesystem::write_result_t::done(landed);
        }

        // WHAT A CALLER OWES A STUMP. A record that half-landed is not a failure the segment
        // can simply forget: the next append asks the descriptor where it is, gets a position
        // PAST the stump, and writes a well-formed record after it -- so the stump stops being
        // a tail and becomes a frame in the middle of the stream. Every reader of that file
        // then walks into it: the segment scan reads the stump's bytes as a record header and
        // refuses on the CRC, the txn log reads them as a frame header and refuses on the
        // magic, and in both cases what is refused is the WHOLE file rather than the four
        // bytes that are actually broken. An index that will not open is not an acceptable
        // answer to one write that ran out of device, so the stump goes away here, at the only
        // moment anything still knows where it began.
        //
        // Truncating cannot rescue the record -- it is gone either way, and the caller still
        // reports the refusal. It only keeps the refusal LOCAL. All three steps are checked
        // because a truncate, an fsync or a re-seek that does not take leaves the descriptor
        // pointing past the end of a file that is now shorter, and silently writing the next
        // record into a hole would be worse than the stump.
        //
        // THE REPAIR IS MADE AS DURABLE AS THE DAMAGE IT UNDOES, which is what the fsync is
        // for and why its failure is a failure of the repair. A short count IS "these bytes
        // reached the device"; the truncate that removes them is a metadata change that sits
        // in the cache until something syncs it. Without this line the window between the
        // refusal and the next force_flush is one in which a crash leaves the stump on disk
        // and nothing anywhere recording that it should not be there -- i.e. the repair would
        // be strictly less durable than what it repairs, which is no repair.
        [[nodiscard]] bool discard_partial_record(core::filesystem::file_handle_t& file,
                                                  const core::filesystem::write_result_t& result,
                                                  uint64_t record_offset) {
            if (!result.partial()) {
                return true;
            }
            if (!file.truncate(static_cast<int64_t>(record_offset))) {
                return false;
            }
            if (!file.sync()) {
                return false;
            }
            return file.seek(record_offset);
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
    // registration and never the ENGINE its start. RE-VERIFIED against the whole tree after
    // the merge-cleanup refusals below were added: bitcask_index_agent.cpp is still the only
    // non-test construction site and still passes deferred_open_t.
    //
    // WHAT IT MEANS FOR A CASE THAT STAGES A REFUSAL: every failure open() can report -- an
    // unreadable segment or txn log, a damaged merge manifest, a source the cleanup cannot
    // unlink -- arrives here as a process abort, because a constructor has no channel and
    // rule 2 forbids the exception. A case that WANTS the refusal builds the store with
    // deferred_open_t and reads open()'s value; a case that wants a working store uses this
    // one and an abort is the correct, loud answer to a fixture that did not set up.
    //
    // AND IT SAYS WHY. The message used to live in an assert, i.e. nowhere in a Release
    // build, so the failure was an exit code and a fixture nobody could tell apart from the
    // next one.
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
        if (const auto open_error = open(); open_error.contains_error()) {
            std::fprintf(stderr,
                         "bitcask: the construct-and-open ctor could not open %s: %s\n",
                         path.string().c_str(),
                         open_error.what.c_str());
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
        bool key_hashable = true;
        const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
        if (!key_hashable) {
            return io_failure("bitcask: a stored key has no hash encoding in this build");
        }
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

    // `ok` is passed by the three callers whose `key` came OFF THE DISK -- the rebuild loop,
    // load_hash_key_at and the merge relocation. The other five are handed a key the query
    // built for a column the CREATE INDEX gate vetted, which is the invariant the encoder's
    // remaining Debug assert stands on. The flag is only ever set to false.
    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key, bool* ok) const {
        auto normalized = normalize_hash_key(key, core::date::timezone_offset_t{});
        return components::index::codec::encode_disk_hash_key(normalized, ok);
    }

    // FINISHING AN INTERRUPTED MERGE IS A STEP THAT CAN REFUSE, and this used to be a `void`
    // with three ways to give up in silence: a manifest it could not read, an unlink whose
    // answer it dropped, and a finished merge whose manifest it never removed. All three end
    // in the same place, because load_from_disk replays EVERY segment it finds: a source the
    // merge already rewrote is replayed straight back into the keydir, and the keys the merge
    // DROPPED -- the deletes, which is what a compaction is for -- come back live with it.
    // That is a wrong answer produced on the path that opens the database.
    //
    // There is a channel now. It costs the INDEX its registration and never the ENGINE its
    // process (open() -> bitcask_index_agent_t::create), and every refusal below leaves the
    // manifest exactly where it is, so the next open retries the same work.
    //
    // ONE OF THOSE RETRIES NEVER SUCCEEDS, and it is named as such where it is raised: a
    // manifest whose BYTES will not parse is a permanent refusal, because the two numbers it
    // held are the only statement of which segments this merge consumed and there is no safe
    // guess. Every other refusal here -- a manifest that would not open, a segment that would
    // not unlink -- is transient and clears with the condition that caused it.
    core::error_t bitcask_index_disk_t::apply_merge_recovery_cleanup() {
        const auto manifest_path = merge_manifest_path(path_);
        std::error_code ec;

        uint64_t merged_segment_id = 0;
        std::vector<uint64_t> removed_segment_ids;
        switch (read_merge_manifest(path_, merged_segment_id, removed_segment_ids)) {
            case sidecar_state_t::absent:
                // NO MERGE WAS INTERRUPTED HERE. The ordinary case, and the one legitimate
                // silent answer -- kept apart from the two refusals below, which are about a
                // manifest that IS there.
                return core::error_t::no_error();
            case sidecar_state_t::unopenable:
                // THE FILE IS THERE AND THE OPEN WAS REFUSED. Said apart from the damaged case
                // because it is a DIFFERENT FACT with a different life: a permission, a
                // descriptor limit or a device, every one of which clears by itself and leaves
                // the manifest byte-for-byte where it was. The open refuses for this uptime and
                // the next one, with the condition gone, finishes the merge -- so an operator
                // reading this line looks at the environment, not at the data.
                return io_failure("bitcask: the merge manifest " + manifest_path.string() +
                                  " is present and could not be opened; the index is not registered while that "
                                  "lasts, and the next open retries it unchanged");
            case sidecar_state_t::damaged:
                // THE FILE OPENED AND ITS BYTES WILL NOT PARSE, and unlike the case above this
                // one does NOT clear by itself: it is a PERMANENT refusal, deliberately.
                //
                // The manifest is published through a temp file plus a rename
                // (write_merge_manifest -> publish_replacement_file), so it is never
                // half-written by a crash -- bytes that will not parse are damaged bytes. And
                // WHICH segments the merge was about is exactly what those bytes held. Neither
                // way out without them is safe: replaying every segment resurrects the keys the
                // merge dropped (its deletes come back live), and dropping the merged segment
                // instead assumes no source has been unlinked yet, which is the one thing the
                // manifest existed to say. So the store refuses rather than guess, and it says
                // what clears it -- the index is DERIVED data, so re-creating it costs nothing
                // but the build.
                return io_failure("bitcask: the merge manifest " + manifest_path.string() +
                                  " is present and its bytes could not be read as a manifest; this does not clear by "
                                  "itself -- drop and re-create the index, or remove its directory, to rebuild it "
                                  "from the table");
            case sidecar_state_t::ok:
                break;
        }

        const bool merged_present = std::filesystem::exists(segment_file_path(path_, merged_segment_id), ec);
        if (ec) {
            return io_failure("bitcask: the merged segment named by " + manifest_path.string() +
                              " could not be looked up: " + ec.message());
        }
        if (!merged_present) {
            // The merge never published its output, so the sources it names are the whole
            // truth about this index and every one of them stays. Only the record of the
            // attempt goes.
            if (!remove_merge_manifest(path_, ec)) {
                return io_failure("bitcask: the manifest of a merge that published nothing could not be removed from " +
                                  manifest_path.string() + ": " + ec.message());
            }
            return core::error_t::no_error();
        }

        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            if (!unlink_if_present(removed_path, ec)) {
                // A SOURCE THAT SURVIVES IS NOT AN UNTIDY DIRECTORY, it is the resurrection
                // above. The manifest stays where it is -- it is the only thing that says this
                // unlink is still owed -- and the next open runs exactly this loop again.
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + ec.message());
            }
        }
        // EVERY SOURCE IS GONE, so the merge is finished and the record of it is the last
        // thing left to remove. Leaving it was not harmless: the next merge writes its own
        // manifest over this one, so a source THIS merge failed to unlink would stop being
        // named by anything and the retry above would never learn it was owed.
        if (!remove_merge_manifest(path_, ec)) {
            return io_failure("bitcask: the manifest of a finished merge could not be removed from " +
                              manifest_path.string() + ": " + ec.message());
        }
        return core::error_t::no_error();
    }

    void bitcask_index_disk_t::drop_cached_rotated_segment_(uint64_t segment_id) const noexcept {
        for (auto it = rotated_read_cache_.begin(); it != rotated_read_cache_.end(); ++it) {
            if (it->segment_id == segment_id) {
                rotated_read_cache_.erase(it);
                return;
            }
        }
    }

    core::error_t bitcask_index_disk_t::load_from_disk() {
        // EVERY LOAD RE-DERIVES, so state derived by the previous one dies here: the
        // rotated-read handles (the files behind them may be about to change or go), and
        // crc_failure_ (wave #332) -- the flag outliving its load made the state live
        // longer than its cause, and a later reload's verdict would have been polluted by
        // the previous one's.
        invalidate_rotated_read_cache_();
        crc_failure_ = false;
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

        RETURN_IF_ERROR(apply_merge_recovery_cleanup());

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
        // NOTHING WALKED, so nothing to say about where the records end. Set on every road out
        // of this function, so open_active_segment can never act on what a previous open left.
        active_segment_clean_end_ = no_tail_to_trim;
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

        // WHICH SEGMENT IS THE ACTIVE ONE IS DECIDED BEFORE THE WALK, not after it, because
        // the walk needs the answer: a tail this build cannot read is REPAIRABLE in the one
        // file that is still being appended to and is plain damage in every other (see the
        // CRC arm below). The rule itself is unchanged -- CURRENT names it, and the newest
        // segment stands in when CURRENT names nothing that is here.
        uint64_t configured_active_segment_id = 0;
        switch (read_current_segment_id(path_, configured_active_segment_id)) {
            case sidecar_state_t::absent:
                // A DIRECTORY WITH SEGMENTS AND NO CURRENT: the layout that predates the
                // pointer, or a wipe that got as far as CURRENT. The newest segment is the
                // documented stand-in, and this is the one state in which substituting it is
                // an answer rather than a guess.
                configured_active_segment_id = segments.back().id;
                break;
            case sidecar_state_t::unopenable:
                // CURRENT IS THERE AND WOULD NOT OPEN. Standing the newest segment in for it
                // used to happen right here, in silence: if CURRENT named an OLDER segment,
                // every append of this uptime landed in a segment CURRENT does not name, and
                // the next open -- with the descriptor limit or the permission gone -- replayed
                // them in the wrong order. Transient, so the refusal costs this uptime only.
                return io_failure("bitcask: the CURRENT segment pointer " + current_segment_path(path_).string() +
                                  " is present and could not be opened; the index is not registered while that lasts, "
                                  "and the next open reads it unchanged");
            case sidecar_state_t::damaged:
                return io_failure("bitcask: the CURRENT segment pointer " + current_segment_path(path_).string() +
                                  " is present and does not hold a segment id; this does not clear by itself -- drop "
                                  "and re-create the index, or remove its directory, to rebuild it from the table");
            case sidecar_state_t::ok:
                break;
        }
        const auto active_it = std::find_if(segments.begin(), segments.end(), [&](const auto& segment) {
            return segment.id == configured_active_segment_id;
        });
        // CURRENT NAMING A SEGMENT THAT IS NOT HERE is the documented stand-in case too: the
        // file was read, so this is not a guess over an unread one. The INDEX is what is kept,
        // not the iterator -- the walk below writes through `segments` and must not be reading
        // a position taken before it.
        const size_t active_segment_index =
            active_it == segments.end() ? segments.size() - 1 : static_cast<size_t>(active_it - segments.begin());
        const uint64_t active_segment_id = segments[active_segment_index].id;

        for (auto& segment : segments) {
            auto f = open_bitcask_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                // A SEGMENT THAT WILL NOT OPEN USED TO STOP THE ENGINE. This is the rebuild
                // loop: the keydir it is filling IS the index, and a segment it cannot read
                // is an index missing every key that segment holds. That has to cost the
                // INDEX its registration -- open() reports it and
                // bitcask_index_agent_t::create drops the half-built agent -- never the
                // process its life (integration test test_index_bootstrap_failure).
                return io_failure("bitcask: segment " + segment.path.string() +
                                  " could not be opened for recovery: " + open_refusal_reason());
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
                // SUBTRACTION, NOT ADDITION, and the guard only works this way round. Both
                // sides are uint64 and payload_size comes OFF THE DISK, so
                // `payload_offset + payload_size` WRAPS for a size near UINT64_MAX and lands
                // under file_size -- the check passes, and the resize below asks the allocator
                // for sixteen exabytes and throws std::bad_alloc on the path that opens a
                // database, with nothing in this build to catch it (rule 2). The loop condition
                // has already proved payload_offset <= file_size, so the difference cannot
                // underflow and the comparison is exact for every value the header can hold.
                if (header.payload_size > file_size - payload_offset) {
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
                    // THE BYTES HERE ARE NOT THE BYTES THAT WERE WRITTEN, and that is all this
                    // check can say: the length that would point at the NEXT record sits inside
                    // the range the CRC covers, so from `offset` on this file holds nothing any
                    // reader of this build can walk. Where it happened decides what that costs.
                    if (segment.id == active_segment_id) {
                        // THE ACTIVE SEGMENT: this is the tail of the write path, and the tail
                        // is repairable. A crash inside write_record can leave a record that
                        // reached the device WHOLE and wrong -- the header landed, the payload
                        // did not, or a sector rotted under it -- which is the same accident as
                        // the short tail three arms up and was the only one of the two treated
                        // as fatal. It used to set crc_failure_, and open() then refused BEFORE
                        // open_active_segment ran, so the cut that would have healed it never
                        // happened and the index was unopenable for good: one bad byte in the
                        // newest record cost the whole index, permanently, with a full-scan
                        // query plan as the only way the engine could still answer.
                        //
                        // Now it ends the walk exactly as the short tail does. `offset` becomes
                        // this segment's scan_end, open_active_segment truncates the file there
                        // and fsyncs, and the store opens without the unreadable tail. What is
                        // dropped is the records a crash could have torn; the ones a
                        // TRANSACTION committed are in the txn log and its replay puts them
                        // back. LOUD, because the drop is silent otherwise and nothing else in
                        // the process will ever mention it.
                        std::fprintf(stderr,
                                     "bitcask: %s holds a record at offset %llu whose CRC does not match; the "
                                     "active segment's unreadable tail (%llu bytes) is being cut and the index "
                                     "opens without it\n",
                                     segment.path.string().c_str(),
                                     static_cast<unsigned long long>(offset),
                                     static_cast<unsigned long long>(file_size - offset));
                        break;
                    }
                    // A ROTATED SEGMENT: nothing has appended to this file since the day it was
                    // rotated, so this is not a torn write, it is a damaged file -- and the
                    // records after the damage are records find() would silently stop
                    // answering. Flag and return rather than abort, so open() can report a
                    // core::error_t; the construct-and-open ctor aborts on what open() returns.
                    crc_failure_ = true;
                    return core::error_t::no_error();
                }
                value_t key(resource(), nullptr);
                row_ids_t rows(resource());
                // THE CRC ABOVE MATCHED, SO THIS IS NOT A TORN TAIL. A record whose payload
                // still will not decode is a record this build cannot represent -- a foreign
                // or newer key encoding -- and a rebuild that walked past it would publish a
                // keydir missing rows the segments hold, with open() reporting success over
                // it. Same answer as the unknown-kind arm below.
                if (!deserialize_payload(resource(), payload, key, rows)) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a record whose key could not be decoded");
                }
                // AND THE ENCODER CAN REFUSE THE VALUE THE DECODER JUST PRODUCED. This is the
                // one encode call on the path that OPENS a database, which is why
                // codec::encode_disk_hash_key reports instead of aborting: a tag this build
                // has no hash encoding for used to take the process down here, in release
                // builds too, leaving the database unopenable rather than merely refusing.
                bool key_hashable = true;
                const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
                if (!key_hashable) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a key this build has no hash encoding for");
                }
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
            // WHERE THIS SEGMENT'S RECORDS REALLY END. Both ways out of the loop above leave
            // `offset` at the first byte the walk could not read as a record: the truncated-tail
            // break, and the condition itself when fewer than a header remain. Equal to the file
            // size on a segment that ends cleanly, which is the ordinary case and asks for
            // nothing.
            segment.scan_end = offset;
        }

        // RESOLVED BEFORE THE WALK (see above), and read back here so the two can never
        // disagree: the walk decided a repairable tail from the same segment this line
        // publishes.
        const auto& active_segment = segments[active_segment_index];
        active_segment_id_ = active_segment.id;
        next_segment_id_ = segments.back().id + 1;
        active_segment_records_ = active_segment.record_count;
        active_data_file_path_ = active_segment.path;
        // THE ONE SEGMENT ANYTHING WILL BE APPENDED TO IS THE ONE WHOSE TAIL MATTERS. A stump
        // at the end of a rotated segment stays a tail for ever -- nothing opens those for
        // writing -- so the replay's `break` in front of it is right and permanent there.
        active_segment_clean_end_ = active_segment.scan_end;

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
                              " could not be opened: " + open_refusal_reason());
        }
        // THE TAIL THE REPLAY COULD NOT READ GOES BEFORE ANYTHING IS APPENDED AFTER IT.
        //
        // This is the crash half of discard_partial_record: the write path undoes its own
        // stump because it is still running, and a power cut inside write_record leaves the
        // identical stump with nobody left to undo it. Cutting it here is free of cost and of
        // risk -- load_from_disk has just walked this file and told us its records end at
        // active_segment_clean_end_, and no byte after that point is reachable by any reader
        // this build has. Leaving it is what is expensive: the seek below would put the next
        // record BEHIND the stump, and from then on the replay stops at it and silently drops
        // everything written after the crash.
        //
        // The cut is FSYNC'D for the reason the write-side repair is: an unsynced truncate is
        // less durable than the bytes it removes, so a second crash would find the stump back.
        //
        // THE MEASUREMENT IS SPENT ONLY ONCE THE CUT HAS HAPPENED, the same discipline the txn
        // log's copy of this repair keeps and for the same reason: clearing it before the
        // truncate means a refused truncate destroys the one record of where the records end,
        // and a retry -- the rotation that calls this again, or an open() the owner retries --
        // walks past the stump with nothing left to tell it there is one. The handle goes back
        // on a refusal so the retry re-enters this block from the top.
        if (const auto clean_end = active_segment_clean_end_;
            clean_end != no_tail_to_trim && file_->file_size() > clean_end) {
            if (!file_->truncate(static_cast<int64_t>(clean_end)) || !file_->sync()) {
                file_.reset();
                return io_failure("bitcask: the unreadable tail of active segment " +
                                  active_data_file_path_.string() + " could not be removed");
            }
        }
        // CONSUMED. The next open_active_segment (a rotation's brand-new file, a clear()'s
        // rebuild) must not act on a measurement taken of a different file.
        active_segment_clean_end_ = no_tail_to_trim;
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

    core::error_t bitcask_index_disk_t::merge_pending_segments() {
        if (!merge_pending_) {
            return core::error_t::no_error();
        }
        // Cleared FIRST: the merge below either compacts what the rotations left or finds
        // nothing to compact, and either way the debt is settled. Clearing it afterwards
        // would re-run the whole scan on the next call for every early return inside.
        merge_pending_ = false;
        auto merge_error = merge_immutable_segments();
        if (merge_error.contains_error()) {
            // THE DEBT IS STILL OWED, and it is owed whichever kind of refusal this was.
            //
            // A merge that gives up BEFORE its manifest is published leaves the directory as it
            // found it -- no merged segment, no manifest, every source still there -- so the
            // rotated segments it was going to compact are all still waiting. A merge that
            // gives up AFTER the manifest is published (a source it could not unlink, a journal
            // it could not remove, a keydir sync the device refused) has published a merged
            // segment and left the manifest naming what is still owed: the next OPEN finishes
            // that half through apply_merge_recovery_cleanup, and re-arming the flag here is
            // what makes THIS uptime try again rather than wait for a restart.
            //
            // Either way, dropping the flag would mean nothing compacts these segments again
            // until the next rotation happens to set it. The agent only calls this once per
            // write handler, so a permanently failing merge costs one attempt per statement,
            // not a spin.
            merge_pending_ = true;
        }
        // RETURNED, not parked (wave #305): note_write_error used to hold this for the
        // NEXT force_flush, which mis-attributed the refusal to a later round's flush.
        // The agent's pay_merge_debt folds it into the reply of the handler that ran the
        // merge, which is the round the failure belongs to.
        return merge_error;
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
        // THE ACTIVE SEGMENT IS READ THROUGH THE DESCRIPTOR THIS STORE ALREADY HOLDS FOR IT.
        //
        // This function is the random-access read behind every find(), behind the snapshot
        // every write door builds, and behind every merge relocation -- and it used to ask the
        // operating system for a BRAND-NEW DESCRIPTOR on each one, for a file the store has
        // had open since it started. A run of sixty-four thousand reads is sixty-four thousand
        // open/close pairs, and every one of them is a chance to meet a refusal that has
        // nothing to do with this index: a descriptor table exhausted SYSTEM-WIDE by some
        // other process is a transient the store converts into an index that stops answering,
        // and the query planner then has no index where a moment ago it had one.
        //
        // That is not a hypothetical shape. It is what the long randomized case
        // (stress_test_index.cpp) meets under a parallel suite -- a find refusing on
        // bitcask.000002.data, a file nothing had deleted and the store itself had open --
        // roughly once in ten runs, never in isolation, and never on the same iteration twice.
        //
        // IT IS THE SAME BYTES EITHER WAY. Both reads are POSITIONAL, so they compile to
        // pread(2) and do not touch the descriptor's offset that the sequential append path
        // relies on; file_size() is fstat(2) and does not either; and a second descriptor on
        // the same inode saw the identical page cache, so nothing about what is read changes.
        // What goes is one syscall pair per read and the failure mode that came with it.
        //
        // ROTATED segments are still opened per read: nothing keeps them open, and there can
        // be arbitrarily many of them. This closes the hot path, not the whole class.
        core::filesystem::file_handle_t* f = nullptr;
        if (file_ && static_cast<uint64_t>(segment_id) == active_segment_id_) {
            f = file_.get();
        } else {
            // ROTATED segments read through the held-descriptor LRU (wave #327): a
            // rotated file never changes, so a held handle answers the same bytes a fresh
            // open would -- minus the syscall pair and minus the failure mode of a
            // process-wide descriptor table exhausted by a neighbour.
            for (auto& lease : rotated_read_cache_) {
                if (lease.segment_id == segment_id) {
                    lease.last_used = ++rotated_read_tick_;
                    f = lease.handle.get();
                    break;
                }
            }
            if (f == nullptr) {
                auto opened_segment =
                    open_bitcask_file(fs_, segment_path, file_flags::READ, file_lock_type::NO_LOCK);
                if (!opened_segment) {
                    return io_failure("bitcask: segment " + segment_path.string() +
                                      " could not be opened for reading: " + open_refusal_reason());
                }
#ifdef DEV_MODE
                g_bitcask_rotated_segment_opens.fetch_add(1, std::memory_order_relaxed);
#endif
                f = opened_segment.get();
                if (rotated_read_cache_.size() >= rotated_read_cache_capacity_) {
                    auto victim = rotated_read_cache_.begin();
                    for (auto it = rotated_read_cache_.begin(); it != rotated_read_cache_.end(); ++it) {
                        if (it->last_used < victim->last_used) {
                            victim = it;
                        }
                    }
                    rotated_read_cache_.erase(victim);
                }
                rotated_read_cache_.push_back(
                    rotated_segment_lease_t{static_cast<uint64_t>(segment_id),
                                            std::move(opened_segment),
                                            ++rotated_read_tick_});
            }
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
            drop_cached_rotated_segment_(static_cast<uint64_t>(segment_id));
            return io_failure("bitcask: record header at " + std::to_string(header_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
        // THE THIRD READ DOOR, AND THE ONE THAT HAD NO GUARD AT ALL. The two recovery walks
        // bound their payload against the file they are walking; this one -- the random-access
        // read every find(), every snapshot and every merge relocation goes through -- resized
        // straight to a length that came OFF THE DISK. A header holding UINT64_MAX asks the
        // allocator for sixteen exabytes and throws std::bad_alloc, which nothing in this build
        // catches (rule 2). Written as a SUBTRACTION for the reason the other two now are: the
        // addition wraps, and a wrapped check is not a check.
        const auto segment_size = f->file_size();
        if (value_offset > segment_size || header.payload_size > segment_size - value_offset) {
            return io_failure("bitcask: the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " claims a payload of " +
                              std::to_string(header.payload_size) + " bytes, which runs past the end of the segment");
        }
        payload.resize(static_cast<size_t>(header.payload_size));
        if (header.payload_size != 0 && !f->read(payload.data(), header.payload_size, value_offset)) {
            drop_cached_rotated_segment_(static_cast<uint64_t>(segment_id));
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
        // The CRC matched, so the bytes are the ones that were written. A payload that still
        // will not decode leaves `rows` holding invented row ids (see deserialize_payload),
        // and this is the read every write door of this store builds its next snapshot from.
        if (!deserialize_payload(resource(), payload, key, rows)) {
            return io_failure("bitcask: the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " could not be decoded");
        }
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
        // BEFORE THE ROTATION, because a rotation would open a fresh segment and hand this
        // store a clean file to append to -- which is exactly the "carry on over the stump"
        // this refusal exists to prevent, just with the stump left behind in a segment the
        // replay still walks.
        RETURN_IF_ERROR(refuse_if_sealed());
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
        const auto record_write =
            write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);
        if (!record_write.complete) {
            // BEFORE THE KEYDIR IS TOUCHED, deliberately. The two steps below erase the
            // key's existing refs and point it at this record; running them over a record
            // that is not on disk would make the key unfindable while the statement went on
            // to report success.
            //
            // `offset` is where this record began, and it is the only place that knows: the
            // stump goes back to it so the refusal stays this statement's and does not become
            // the whole segment's on the next open (see discard_partial_record).
            //
            // A REPAIR THAT REFUSED CLOSES THE STORE (seal_writes). The stump is still at the
            // end of the segment and the descriptor is still past it, so the next append would
            // land behind it and turn it into the interior frame the replay cannot pass.
            if (!discard_partial_record(*file_, record_write, offset)) {
                return seal_writes("bitcask: a partly written snapshot record could not be discarded from " +
                                   active_data_file_path_.string());
            }
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
        // Same door and the same reason as append_snapshot's: a tombstone is a record too.
        RETURN_IF_ERROR(refuse_if_sealed());
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        // Same reason as append_snapshot's, and the same road in: a refused rotation leaves
        // no handle, and write_record dereferences one.
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, row_ids_t(resource()));
        // The record's own start, for the same reason append_snapshot reads it: a stump can
        // only be undone by the code that knows where it began.
        const auto offset = file_->seek_position();
        const auto record_write =
            write_record(*file_, static_cast<uint8_t>(record_kind_t::tombstone), ++next_timestamp_, payload);
        if (!record_write.complete) {
            // Same order as append_snapshot: the refs stay until the tombstone is durable,
            // otherwise a restart replays the key as still present while this call reported
            // it removed.
            // Closes the store for the same reason append_snapshot's does.
            if (!discard_partial_record(*file_, record_write, offset)) {
                return seal_writes("bitcask: a partly written tombstone record could not be discarded from " +
                                   active_data_file_path_.string());
            }
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

    // ZERO USED TO MEAN BOTH THINGS, and they are opposite. "No sidecar has been written yet"
    // is a fresh log whose frames all still need applying, and zero is exactly right for it.
    // "The sidecar is there and would not open" is a log whose frames have ALREADY been
    // applied up to some point nobody can now read -- and answering zero replays every one of
    // them again, on the path that opens the database, over a keydir the segments have already
    // been replayed into. The value is a RESULT now, and the caller refuses on the difference
    // instead of substituting a number.
    core::result_wrapper_t<uint64_t> bitcask_index_disk_t::read_applied_log_offset() const {
        const auto applied_path = txn_applied_file_path();
        uint64_t offset = 0;
        switch (read_sidecar_uint64(applied_path, offset)) {
            case sidecar_state_t::absent:
                // NOTHING HAS BEEN APPLIED YET. The ordinary state of a log that has never been
                // recovered from, and the one answer that is a fact rather than a stand-in.
                return uint64_t{0};
            case sidecar_state_t::unopenable:
                return io_failure("bitcask: the applied-offset sidecar " + applied_path.string() +
                                  " is present and could not be opened; the index is not registered while that "
                                  "lasts, and the next open reads it unchanged");
            case sidecar_state_t::damaged:
                return io_failure("bitcask: the applied-offset sidecar " + applied_path.string() +
                                  " is present and does not hold an offset; this does not clear by itself -- drop "
                                  "and re-create the index, or remove its directory, to rebuild it from the table");
            case sidecar_state_t::ok:
                break;
        }
        return offset;
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
            // THE PUBLISH ALREADY FAILED, so this function refuses either way and the removal
            // cannot change that -- but the answer is stated instead of dropped. A survivor is
            // harmless on its own: every writer of this temp opens it with std::ios::trunc, so
            // the next attempt overwrites it rather than appending to it, and nothing else in
            // the store ever reads it. So it is said in the SAME refusal rather than turned
            // into a second one -- loud, not fatal.
            std::error_code cleanup_ec;
            if (!unlink_if_present(temp_path, cleanup_ec)) {
                return io_failure("bitcask: the applied-offset sidecar could not be published as " +
                                  applied_path.string() + ", and its temp " + temp_path +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            return io_failure("bitcask: the applied-offset sidecar could not be published as " +
                              applied_path.string());
        }
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::append_txn_record(uint64_t txn_id,
                                                          uint8_t op_kind,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // The third write door, sealed for the same reason as the two segment doors: the
        // stump this one can leave is in the txn log, and an interior frame there costs
        // recovery every committed frame in the file rather than one segment's tail.
        RETURN_IF_ERROR(refuse_if_sealed());
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
            // THE TAIL RECOVERY COULD NOT READ GOES BEFORE ANYTHING IS APPENDED AFTER IT.
            //
            // The same repair open_active_segment makes for the active segment, made here for
            // the same reason and at the same moment: recovery has just walked this file and
            // said its frames end at txn_log_clean_end_, no byte past that point is reachable
            // by any reader this build has, and nothing has been appended yet -- so the cut
            // costs nothing. Leaving the bytes is what costs: the append below seeks to
            // file_size(), which is PAST them, and the stump becomes an interior frame that
            // takes the whole log down on the next open.
            //
            // It belongs in this lazy open rather than in open(): this is the only door that
            // creates the handle, so putting it anywhere else would leave the one path that
            // reopens the log after a discard_partial_record repairing itself back over the
            // stump. FSYNC'D for the reason the segment cut is: an unsynced truncate is less
            // durable than the bytes it removes, so a second crash would find the stump back.
            //
            // AND THE MEASUREMENT IS SPENT ONLY ONCE THE CUT HAS HAPPENED. Clearing it first
            // disarmed the repair on its own failure: the truncate refuses, this returns, and
            // the ONLY record of where the frames end is already gone -- while the handle it
            // just opened is not, so the `if (!txn_log_file_)` above never runs again and the
            // very next append seeks to file_size(), lands behind the stump and turns it into
            // an interior frame. That is precisely the unrecoverable log this repair was
            // written to prevent, reached BY the repair. So: the cut first, the reset after,
            // and on a refusal the handle goes back so the next attempt re-enters this block
            // with the measurement still in hand.
            if (const auto clean_end = txn_log_clean_end_;
                clean_end != no_tail_to_trim && txn_log_file_->file_size() > clean_end) {
                if (!txn_log_file_->truncate(static_cast<int64_t>(clean_end)) || !txn_log_file_->sync()) {
                    txn_log_file_.reset();
                    return io_failure("bitcask: the unreadable tail of the txn log " + txn_log_file_path().string() +
                                      " could not be removed");
                }
            }
            // CONSUMED, and only here. The file this measurement was taken of is now cut to
            // it, so the next lazy open -- a brand-new log after a clear() -- must not act on
            // it.
            txn_log_clean_end_ = no_tail_to_trim;
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
        // THE FRAME IS ONE UNIT AND ITS STUMP IS THIS FUNCTION'S TO REMOVE. recover_txn_log
        // tolerates a short tail -- its loop simply stops when fewer than a header's worth of
        // bytes remain -- but only while the stump IS the tail. The very next frame appended
        // after a refusal seeks to file_size(), lands past the stump, and turns it into an
        // interior frame whose magic recovery then refuses, taking every committed frame in
        // the log down with it. frame_offset is where this frame began, so the two writes are
        // undone together back to it.
        const auto header_write = txn_log_file_->write(&header, sizeof(header));
        if (!header_write.complete) {
            // AND THE SEAL COVERS THIS PATH TOO, though the handle is a different one. Dropping
            // txn_log_file_ would not do: this function REOPENS it lazily and then seeks to
            // file_size(), so a dropped handle repairs itself straight back over the stump.
            if (!discard_partial_record(*txn_log_file_, header_write, frame_offset)) {
                return seal_writes("bitcask: a partly written txn-log frame header could not be discarded");
            }
            return io_failure("bitcask: the txn-log frame header could not be written");
        }
        if (!payload.empty()) {
            const auto payload_write = txn_log_file_->write(payload.data(), payload.size());
            if (!payload_write.complete) {
                if (!discard_partial_record(*txn_log_file_,
                                            core::filesystem::write_result_t::refused(header_write.bytes_written +
                                                                                      payload_write.bytes_written),
                                            frame_offset)) {
                    return seal_writes("bitcask: a partly written txn-log frame payload could not be discarded");
                }
                return io_failure("bitcask: the txn-log frame payload could not be written");
            }
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
        // NOTHING WALKED YET, so nothing to say about where the frames end -- set on every
        // road out of this function, exactly as load_from_disk does for the active segment,
        // so the lazy open in append_txn_record can never act on what a previous open left.
        txn_log_clean_end_ = no_tail_to_trim;
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
        VALUE_OR_RETURN(const uint64_t applied_offset, read_applied_log_offset());
        auto in = open_bitcask_file(fs_, log_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!in) {
            // THE FILE IS THERE AND WILL NOT OPEN. This used to return as if the log were
            // empty, which silently dropped every committed frame of the last window from
            // the index for the whole uptime -- while a CORRUPT frame below took the process
            // down. One policy now: recovery refuses, open() hands it up, the index does not
            // register and the engine lives.
            return io_failure("bitcask: the txn log exists and could not be opened for recovery: " +
                              open_refusal_reason());
        }
        const uint64_t log_size = in->file_size();
        uint64_t frame_offset = applied_offset;

        while (frame_offset + sizeof(txn_frame_header_t) <= log_size) {
            txn_frame_header_t header{};
            if (!in->read(&header, sizeof(header), frame_offset)) {
                return io_failure("bitcask: the txn log refused a frame header during recovery");
            }
            if (header.magic != txn_magic) {
                // THE FRAMES END HERE, and this is the one file in the store where that is the
                // whole of it: the txn log is appended to and nothing else, so there is no
                // "interior" to distinguish -- everything past a header this build cannot
                // recognise is unreachable, and the magic is not inside the range the CRC
                // covers, so garbage bytes reach this arm before the checksum ever sees them.
                //
                // It used to REFUSE, and the refusal was permanent: recover_txn_log runs on
                // every open, so a single stump that a later append had walked past cost the
                // index every committed frame in the log -- for good. Now it ends the walk the
                // way the truncated tail three lines down does, the lazy open in
                // append_txn_record cuts the file at frame_offset, and the next open finds a
                // log that reads clean.
                std::fprintf(stderr,
                             "bitcask: %s holds no readable frame at offset %llu (bad magic); the log's unreadable "
                             "tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
            }
            const uint64_t payload_offset = frame_offset + sizeof(txn_frame_header_t);
            // SUBTRACTION, for the reason load_from_disk's guard is written that way: both
            // sides are uint64 and payload_size came off the disk, so the addition WRAPS for a
            // size near UINT64_MAX and slips under log_size -- and the resize below then asks
            // the allocator for sixteen exabytes and throws std::bad_alloc on the path that
            // opens a database (rule 2). The loop condition has already proved
            // payload_offset <= log_size, so the difference cannot underflow.
            if (header.payload_size > log_size - payload_offset) {
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
                // THE BYTES ARE NOT THE BYTES THAT WERE WRITTEN, so the length that would point
                // at the next frame cannot be trusted either and the readable frames end here.
                // Same arm as the bad magic above and for the same reason -- the log is the one
                // file with no interior -- and the same repair: the walk stops, the lazy open
                // in append_txn_record cuts at frame_offset, the next open reads clean. This
                // used to be a permanent refusal that cost the index every committed frame in
                // the log rather than the ones behind the damage.
                std::fprintf(stderr,
                             "bitcask: %s holds a frame at offset %llu whose CRC does not match; the log's "
                             "unreadable tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
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
                // THE CRC OVER THIS FRAME ALREADY MATCHED, so a decode refusal here is a
                // frame this build cannot represent -- and replaying it half-way is the worst
                // of the three options. The key codec leaves `pos` unmoved when it refuses, so
                // the row id behind it was read from the KEY's own bytes and handed straight
                // to insert()/remove(): a recovery that invented index entries, on the path
                // that opens the database, without a word. Refuse the open instead.
                bool frame_ok = true;
                const auto count = components::index::codec::read_le<uint32_t>(payload, pos, &frame_ok);
                for (uint32_t i = 0; i < count && frame_ok; ++i) {
                    auto key = components::index::codec::read_logical_value(resource(), payload, pos, &frame_ok);
                    const auto row_id =
                        static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos, &frame_ok));
                    if (!frame_ok) {
                        break;
                    }
                    if (header.op_kind == 1) {
                        insert(key, row_id);
                    } else {
                        remove(key, row_id);
                    }
                }
                if (!frame_ok) {
                    return io_failure("bitcask: a committed txn-log frame could not be decoded during recovery");
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
        // WHERE THIS LOG'S FRAMES REALLY END. Both ways out of the loop leave `frame_offset`
        // at the first byte the walk could not read as a frame: the truncated-tail break, and
        // the condition itself when fewer than a header remain. Equal to the file size on a
        // log that ends cleanly, which is the ordinary case and asks for nothing.
        txn_log_clean_end_ = frame_offset;
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
        // A SECOND LAZY OPEN HERE WOULD BE A DOOR MISSING HALF OF THE FIRST ONE'S JOB.
        //
        // append_txn_record above owns the only lazy open of this log, and that open does one
        // thing more than a plain open_bitcask_file: it cuts the tail recovery could not read,
        // BEFORE anything is appended behind it. This block used to repeat the open WITHOUT
        // the cut -- so on any path that reached it, the append below would land past a stump
        // and turn it into an interior frame, which is exactly the unrecoverable log the cut
        // exists to prevent.
        //
        // It is unreachable, and it stays that way by construction: append_txn_record returns
        // on every road that leaves the handle closed, and this line is past its success. So
        // it is a CHECKED invariant now rather than a second, divergent implementation of it.
        if (!txn_log_file_) {
            return io_failure("bitcask: the txn log is closed after a frame this store reported as written");
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
        // The same checked invariant as apply_txn_inserts', for the reason stated there: there
        // is exactly ONE lazy open of this log, and it is the one that cuts the tail.
        if (!txn_log_file_) {
            return io_failure("bitcask: the txn log is closed after a frame this store reported as written");
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

    core::error_t bitcask_index_disk_t::seal_writes(std::string_view reason) {
        writes_sealed_ = true;
        return io_failure(reason);
    }

    core::error_t bitcask_index_disk_t::refuse_if_sealed() const {
        if (!writes_sealed_) {
            return core::error_t::no_error();
        }
        // The reason the seal was set is already on its way to the caller of the statement
        // that set it; this is the message every LATER write gets, and it says the thing that
        // matters to them -- that there is a record on the device that nothing may be written
        // behind. FLUSHING is deliberately not sealed: everything appended before the stump is
        // real, and force_flush is how its durability (and this refusal) reaches the caller.
        return io_failure("bitcask: " + path_.string() +
                          " is not taking writes: a partly written record could not be discarded from the active "
                          "file, and every append after it would land behind a record no reader can pass");
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
        // The merge unlinks its sources and republishes the directory; a held rotated
        // handle would keep an unlinked inode alive and, worse, could answer a segment id
        // the directory has re-derived. Dropped up front, before anything changes shape.
        invalidate_rotated_read_cache_();
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
        // A STALE TEMP THAT SURVIVES THIS IS NOT UNTIDINESS, IT IS THE OUTPUT. Both files are
        // opened below with FILE_CREATE, which is O_CREAT and NOT O_TRUNC, and both are written
        // from position zero -- so whatever an abandoned earlier attempt left BEYOND the bytes
        // this one writes stays in the file and is published with it. On the segment that is a
        // merged segment with a garbage tail, which the next open's replay reads as a record
        // and rejects on CRC; on the journal it is extra relocation entries the replay loop
        // below walks (its bound is the FILE SIZE) and applies to the keydir. Both answers used
        // to be dropped.
        std::error_code stale_temp_ec;
        if (!unlink_if_present(temp_path, stale_temp_ec)) {
            return io_failure("bitcask: the merge output " + temp_path.string() +
                              " was left behind by an earlier attempt and could not be removed: " +
                              stale_temp_ec.message());
        }
        if (!unlink_if_present(meta_temp_path, stale_temp_ec)) {
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                              " was left behind by an earlier attempt and could not be removed: " +
                              stale_temp_ec.message());
        }

        // NOTHING IS PUBLISHED UNTIL EVERYTHING SUCCEEDED, so every refusal that goes through
        // HERE drops the two temp files and leaves the directory exactly as it found it: no
        // manifest, no merged segment, and -- the part that used to be unconditional -- no
        // unlinked sources. The table keeps answering from the segments it was answering from,
        // and the debt merge_pending_segments re-arms makes the next attempt run again.
        //
        // THE REFUSALS PAST THE MANIFEST DO NOT GO THROUGH HERE AND DO NOT LEAVE THE DIRECTORY
        // AS THEY FOUND IT -- that is the point of publishing the manifest, and the paragraph
        // above `meta_file = open_bitcask_file(...)` further down says what they leave and who
        // finishes it. Two of them can also leave the manifest itself behind: a failed
        // move_files whose manifest removal ALSO refuses, and every refusal in the tail of this
        // function. Both are recoverable by the next open, neither is silent.
        //
        // AND THE UNLINKS ARE READ. A temp this lambda could not remove is the stale temp the
        // NEXT attempt refuses on (see the two unlink_if_present calls above), so dropping the
        // answer moved the report one merge into the future and put it on the wrong file.
        const auto abandon_merge = [&](std::unique_ptr<core::filesystem::file_handle_t>& merged,
                                       std::unique_ptr<core::filesystem::file_handle_t>& meta,
                                       core::error_t reason) {
            merged.reset();
            meta.reset();
            std::error_code cleanup_ec;
            const std::string first_reason{std::string_view{reason.what}};
            if (!unlink_if_present(temp_path, cleanup_ec)) {
                return io_failure(first_reason + "; the merge output " + temp_path.string() +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            if (!unlink_if_present(meta_temp_path, cleanup_ec)) {
                return io_failure(first_reason + "; the merge journal " + meta_temp_path.string() +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            return reason;
        };

        auto merged_file = open_bitcask_file(fs_,
                                             temp_path,
                                             file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                             file_lock_type::NO_LOCK);
        if (!merged_file) {
            return io_failure("bitcask: the merge output " + temp_path.string() + " could not be opened: " +
                              open_refusal_reason());
        }
        auto meta_file = open_bitcask_file(fs_,
                                           meta_temp_path,
                                           file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                           file_lock_type::NO_LOCK);
        if (!meta_file) {
            return abandon_merge(merged_file,
                                 meta_file,
                                 io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                                            " could not be opened: " + open_refusal_reason()));
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
            // The key came back out of read_rows_at, so it was DECODED OFF THIS DISK: the
            // same reason the rebuild loop passes the flag applies here, and a merge is not
            // allowed to take the process down either.
            bool key_hashable = true;
            const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
            if (!key_hashable) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: a relocated key has no hash encoding in this build"));
            }
            auto payload = serialize_payload(resource(), key, rows);
            const auto offset = merged_file->seek_position();
            if (!write_record(*merged_file, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload)
                     .complete) {
                // NO STUMP TO DISCARD HERE: abandon_merge deletes the temp segment and the
                // temp journal outright, so a half-written record dies with the file it is in
                // and never becomes anything's tail. The live segments are untouched.
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
            // Same as the relocation above: the journal is a TEMP file abandon_merge unlinks,
            // so a stump here needs no undoing -- only reporting.
            const auto meta_write = [&](const void* data, uint64_t size) {
                return meta_file->write(const_cast<void*>(data), size).complete;
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
                // THE MANIFEST GOES BACK OFF THE DISK because nothing was published: it names
                // a merged segment that does not exist, and the sources it names must all
                // stay. Leaving it is survivable -- the next open's cleanup finds the merged
                // segment missing and removes it there -- but the answer is READ rather than
                // dropped, and when the removal is the second thing to refuse it is said in
                // the same breath as the first.
                std::error_code manifest_ec;
                if (!remove_merge_manifest(path_, manifest_ec)) {
                    return abandon_merge(merged_file,
                                         meta_file,
                                         io_failure("bitcask: the merged segment could not be published as " +
                                                    merged_path.string() +
                                                    ", and the merge manifest could not be removed either: " +
                                                    manifest_ec.message()));
                }
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merged segment could not be published as " +
                                                merged_path.string()));
            }
            built = true;
        } else {
            // NOTHING WAS WORTH MERGING (every ref below the frontier was a tombstone), so the
            // two temps go and the directory is left as it was found. THE ANSWERS ARE READ:
            // both files are opened by the NEXT attempt with FILE_CREATE, which does not
            // truncate, so a survivor is bytes that attempt would publish (see the stale-temp
            // refusals at the top). Refusing here rather than there puts the report on the
            // merge that actually left the file, and costs only a compaction the debt re-arms.
            merged_file.reset();
            meta_file.reset();
            std::error_code empty_merge_ec;
            if (!unlink_if_present(temp_path, empty_merge_ec)) {
                return io_failure("bitcask: the merge output " + temp_path.string() +
                                  " of a merge that produced nothing could not be removed: " +
                                  empty_merge_ec.message());
            }
            if (!unlink_if_present(meta_temp_path, empty_merge_ec)) {
                return io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                                  " of a merge that produced nothing could not be removed: " +
                                  empty_merge_ec.message());
            }
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
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() + " could not be reopened: " +
                              open_refusal_reason());
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
        // THE JOURNAL HAS BEEN FULLY REPLAYED, so it goes -- and the answer is read, which it
        // was not. A survivor is the stale journal the next merge opens with FILE_CREATE and
        // writes into from position zero without truncating, so its leftover tail becomes
        // relocation entries that replay walks (its bound is the file size) and applies to the
        // keydir. Refusing here is cheap: the sources are still in place, the manifest is still
        // on disk naming them, and the next open finishes exactly this merge.
        std::error_code journal_ec;
        if (!unlink_if_present(meta_temp_path, journal_ec)) {
            return io_failure("bitcask: the replayed merge journal " + meta_temp_path.string() +
                              " could not be removed: " + journal_ec.message());
        }
        // THE RELOCATED KEYDIR REACHES THE DEVICE BEFORE THE SOURCES ARE UNLINKED. A refusal
        // here leaves every source segment in place and the manifest on disk, so the next
        // open replays them and finishes the job.
        RETURN_IF_ERROR(hash_index_->sync());
        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            std::error_code removal_ec;
            if (!unlink_if_present(removed_path, removal_ec)) {
                // THE ANSWER USED TO BE DROPPED, and a source that survives its own merge is
                // not an untidy directory: load_from_disk replays every segment it finds, so
                // the next open replays this one back into the keydir -- with the keys this
                // merge DROPPED, the deletes it was compacting away, coming back live. The
                // manifest is still on disk and still names this segment, so refusing here
                // hands the retry to apply_merge_recovery_cleanup on the next open, and
                // merge_pending_segments re-arms the debt for this uptime.
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + removal_ec.message());
            }
        }
        // AND THE RECORD OF THE MERGE GOES WITH IT, which this path never did. Every finished
        // merge left its manifest behind, and the next merge overwrote it -- so a source some
        // earlier merge had failed to unlink stopped being named by anything and no cleanup
        // could ever learn it was owed. The order is deliberate: sources first, manifest last,
        // so a crash between them leaves the manifest saying exactly what is left to do.
        std::error_code manifest_ec;
        if (!remove_merge_manifest(path_, manifest_ec)) {
            return io_failure("bitcask: the manifest of the merge into " + merged_path.string() +
                              " could not be removed: " + manifest_ec.message());
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

        // Close every open handle before unlinking so stale inodes are not held --
        // including the rotated-read leases, whose files are about to go.
        invalidate_rotated_read_cache_();
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
        // THROUGH THE SAME DOOR THE MERGE CLEANUP USES (unlink_if_present): remove_file's
        // bool answers false both for "the device would not unlink it" and for "there was
        // nothing there", and the second is the wipe's goal reached, so the ec overload is
        // the one that can tell them apart -- and it keeps exceptions off this path (rule 2).
        // Every one of these results used to be dropped, so a clear() that could not remove a
        // segment reported success -- and load_from_disk then honestly replayed the survivor
        // back into the keydir, handing find() rows that clear() had promised were gone.
        const auto unlink_artifact = [&](const std::filesystem::path& artifact) {
            std::error_code ec;
            if (!unlink_if_present(artifact, ec)) {
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
        // THE MEASUREMENT WENT WITH THE FILE IT WAS ABOUT. The txn log has just been unlinked
        // by the loop above, so the next append_txn_record creates a brand-new one -- and a
        // clean end taken of the old file must not be applied to it. (load_from_disk below
        // does the same for the segment's, on its own way through.)
        txn_log_clean_end_ = no_tail_to_trim;
        bulk_mode_ = false;
        // The rotations that owed a merge owed it over segments that no longer exist.
        merge_pending_ = false;
        // AND THE SEAL GOES WITH THE FILE IT WAS ABOUT. The segment (or the txn log) whose
        // stump could not be discarded has just been unlinked by the loop above, so there is
        // no longer anything for a later append to land behind. This is the repair door
        // seal_writes names: without it a store that met one refused truncate would never take
        // a write again, for the rest of the process, over a file that no longer exists.
        writes_sealed_ = false;

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
        invalidate_rotated_read_cache_();
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
        // AND THE ANSWER IS READ. drop() is void -- its caller has already let go of the index
        // -- so there is no channel to refuse on, but a directory that survives a drop is not
        // an untidy /tmp: a CREATE INDEX that lands on the same name opens THIS directory, and
        // load_from_disk replays every segment still in it, so the dropped index's rows come
        // back as the new one's. Loud on the one channel a void door has.
        if (!remove_directory(fs_, path_)) {
            std::fprintf(stderr,
                         "bitcask: the index directory %s survived drop(); an index re-created under this name "
                         "will replay what is still in it\n",
                         path_.string().c_str());
        }
    }
} // namespace services::index
