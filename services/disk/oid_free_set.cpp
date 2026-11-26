#include "oid_free_set.hpp"
#include "oid_reservation.hpp"

#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <cstring>

namespace services::disk {

    namespace {

        using components::catalog::oid_t;

        std::uint32_t raw(oid_t oid) noexcept { return static_cast<std::uint32_t>(oid); }

        // begin, end, held, wal_id
        constexpr std::size_t span_record_size = 3 * sizeof(std::uint32_t) + sizeof(std::uint64_t);

        core::error_t free_set_error(std::pmr::memory_resource* resource,
                                     core::error_code_t code,
                                     const std::filesystem::path& path,
                                     const char* reason) {
            std::pmr::string what{"oid free set ", resource};
            what += path.string();
            what += ": ";
            what += reason;
            return core::error_t{code, std::move(what)};
        }

    } // namespace

    oid_free_set_t::oid_free_set_t(std::pmr::memory_resource* resource)
        : spans_(resource) {}

    void
    oid_free_set_t::rebuild(const std::pmr::vector<oid_t>& in_use, oid_t first, oid_t end, wal::id_t current_wal_id) {
        std::pmr::vector<oid_span_t> next(spans_.get_allocator().resource());
        auto previous = spans_.begin();
        auto emit = [&](std::uint32_t from, std::uint32_t to) {
            std::uint32_t cursor = from;
            while (cursor < to) {
                while (previous != spans_.end() && raw(previous->end) <= cursor) {
                    ++previous;
                }
                if (previous == spans_.end() || raw(previous->begin) >= to) {
                    next.push_back({oid_t{cursor}, oid_t{to}, true, current_wal_id});
                    return;
                }
                if (raw(previous->begin) > cursor) {
                    next.push_back({oid_t{cursor}, previous->begin, true, current_wal_id});
                    cursor = raw(previous->begin);
                }
                const std::uint32_t stop = std::min(raw(previous->end), to);
                next.push_back({oid_t{cursor}, oid_t{stop}, previous->held, previous->wal_id});
                cursor = stop;
            }
        };

        std::uint32_t cursor = raw(first);
        for (const auto oid : in_use) {
            if (raw(oid) < cursor) {
                continue;
            }
            if (raw(oid) >= raw(end)) {
                break;
            }
            if (raw(oid) > cursor) {
                emit(cursor, raw(oid));
            }
            cursor = raw(oid) + 1;
        }
        if (cursor < raw(end)) {
            emit(cursor, raw(end));
        }
        spans_ = std::move(next);
        coalesce_();
    }

    void oid_free_set_t::release(wal::id_t oldest_retained_wal_id) {
        for (auto& span : spans_) {
            if (span.held && span.wal_id < oldest_retained_wal_id) {
                span.held = false;
                span.wal_id = 0;
            }
        }
        coalesce_();
    }

    bool oid_free_set_t::has_released() const noexcept {
        return std::any_of(spans_.begin(), spans_.end(), [](const oid_span_t& span) { return !span.held; });
    }

    std::pmr::vector<oid_t> oid_free_set_t::claim(std::size_t count) {
        std::pmr::vector<oid_t> taken(spans_.get_allocator().resource());
        for (auto& span : spans_) {
            if (taken.size() == count) {
                break;
            }
            if (span.held) {
                continue;
            }
            while (span.begin < span.end && taken.size() < count) {
                taken.push_back(span.begin);
                span.begin = oid_t{raw(span.begin) + 1};
            }
        }
        spans_.erase(
            std::remove_if(spans_.begin(), spans_.end(), [](const oid_span_t& span) { return span.begin == span.end; }),
            spans_.end());
        return taken;
    }

    void oid_free_set_t::assign(std::pmr::vector<oid_span_t> spans) {
        spans_ = std::move(spans);
        coalesce_();
    }

    void oid_free_set_t::coalesce_() {
        std::size_t kept = 0;
        for (std::size_t i = 0; i < spans_.size(); ++i) {
            if (kept != 0) {
                auto& last = spans_[kept - 1];
                const auto& span = spans_[i];
                if (last.end == span.begin && last.held == span.held && last.wal_id == span.wal_id) {
                    last.end = span.end;
                    continue;
                }
            }
            spans_[kept++] = spans_[i];
        }
        spans_.resize(kept);
    }

    std::filesystem::path oid_free_set_path(const std::filesystem::path& db_path) { return db_path / "oid_free_set"; }

    core::result_wrapper_t<std::pmr::vector<oid_span_t>> read_oid_free_set(std::pmr::memory_resource* resource,
                                                                           const std::filesystem::path& db_path) {
        std::pmr::vector<oid_span_t> spans(resource);
        core::filesystem::local_file_system_t fs;
        const auto path = oid_free_set_path(db_path);
        if (!core::filesystem::file_exists(fs, path)) {
            return spans;
        }
        auto file = core::filesystem::open_file(fs, path, core::filesystem::file_flags::READ);
        if (file == nullptr) {
            return free_set_error(resource, core::error_code_t::io_error, path, "exists but could not be opened");
        }
        const auto size = static_cast<std::size_t>(file->file_size());
        std::pmr::vector<char> bytes(size, resource);
        if (size != 0 &&
            core::filesystem::read(fs, *file, bytes.data(), static_cast<int64_t>(size)) != static_cast<int64_t>(size)) {
            return free_set_error(resource, core::error_code_t::io_error, path, "could not be read whole");
        }
        std::uint64_t count = 0;
        if (size < sizeof(count)) {
            return free_set_error(resource, core::error_code_t::data_corruption, path, "has no span count");
        }
        std::memcpy(&count, bytes.data(), sizeof(count));
        if (size != sizeof(count) + count * span_record_size) {
            return free_set_error(resource,
                                  core::error_code_t::data_corruption,
                                  path,
                                  "does not hold the spans its count names");
        }
        spans.reserve(count);
        const char* cursor = bytes.data() + sizeof(count);
        for (std::uint64_t i = 0; i < count; ++i) {
            std::uint32_t begin = 0;
            std::uint32_t end = 0;
            std::uint32_t held = 0;
            std::uint64_t wal_id = 0;
            std::memcpy(&begin, cursor, sizeof(begin));
            cursor += sizeof(begin);
            std::memcpy(&end, cursor, sizeof(end));
            cursor += sizeof(end);
            std::memcpy(&held, cursor, sizeof(held));
            cursor += sizeof(held);
            std::memcpy(&wal_id, cursor, sizeof(wal_id));
            cursor += sizeof(wal_id);
            if (begin >= end || held > 1 || (!spans.empty() && begin < raw(spans.back().end))) {
                return free_set_error(resource,
                                      core::error_code_t::data_corruption,
                                      path,
                                      "holds a span that is empty, unordered or overlapping");
            }
            spans.push_back({oid_t{begin}, oid_t{end}, held == 1, wal_id});
        }
        return spans;
    }

    core::error_t persist_oid_free_set(std::pmr::memory_resource* resource,
                                       const std::filesystem::path& db_path,
                                       const std::pmr::vector<oid_span_t>& spans) {
        const std::uint64_t count = spans.size();
        std::pmr::vector<char> bytes(sizeof(count) + count * span_record_size, resource);
        char* cursor = bytes.data();
        std::memcpy(cursor, &count, sizeof(count));
        cursor += sizeof(count);
        for (const auto& span : spans) {
            const std::uint32_t begin = raw(span.begin);
            const std::uint32_t end = raw(span.end);
            const std::uint32_t held = span.held ? 1 : 0;
            std::memcpy(cursor, &begin, sizeof(begin));
            cursor += sizeof(begin);
            std::memcpy(cursor, &end, sizeof(end));
            cursor += sizeof(end);
            std::memcpy(cursor, &held, sizeof(held));
            cursor += sizeof(held);
            std::memcpy(cursor, &span.wal_id, sizeof(span.wal_id));
            cursor += sizeof(span.wal_id);
        }
        return replace_file_durably(resource, oid_free_set_path(db_path), bytes.data(), bytes.size(), "oid free set");
    }

} // namespace services::disk
