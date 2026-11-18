#include "time_zone.hpp"

#include "timezones.hpp"

#include <absl/time/civil_time.h>
#include <absl/time/clock.h>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>
#include <unordered_map>

namespace core::date {

    namespace {

        constexpr int64_t micros_per_second = 1'000'000;
        // 2000-01-01 00:00:00 UTC, the epoch timestamp_t / timestamptz_t count from, in unix seconds.
        constexpr int64_t pg_epoch_unix_seconds = 946'684'800;
        const absl::CivilSecond civil_epoch{2000, 1, 1, 0, 0, 0};

        std::string lowered(std::string_view text) {
            std::string out(text);
            for (auto& symbol : out) {
                symbol = static_cast<char>(std::tolower(static_cast<unsigned char>(symbol)));
            }
            return out;
        }

        struct split_micros_t {
            int64_t seconds;
            int64_t fraction;
        };

        split_micros_t split(int64_t micros) noexcept {
            int64_t seconds = micros / micros_per_second;
            int64_t fraction = micros % micros_per_second;
            if (fraction < 0) {
                seconds -= 1;
                fraction += micros_per_second;
            }
            return {seconds, fraction};
        }

        absl::Time instant_of(timestamptz_t value) noexcept {
            const auto [seconds, fraction] = split(value.value.count());
            return absl::FromUnixSeconds(seconds + pg_epoch_unix_seconds) + absl::Microseconds(fraction);
        }

        // The zoneinfo tree as lowercase name -> name as the tree spells it, read once. TZDIR, as absl reads it, or
        // /usr/share/zoneinfo. "localtime" is the host's own zone, which no statement names.
        const std::unordered_map<std::string, std::string>& zone_names() {
            static const std::unordered_map<std::string, std::string> names = [] {
                std::unordered_map<std::string, std::string> found;
                const char* tzdir = std::getenv("TZDIR");
                const std::filesystem::path root{tzdir != nullptr && *tzdir != '\0' ? tzdir : "/usr/share/zoneinfo"};
                std::error_code error;
                std::filesystem::recursive_directory_iterator it{root, error};
                if (error) {
                    return found;
                }
                const std::filesystem::recursive_directory_iterator end{};
                for (; it != end; it.increment(error)) {
                    if (error) {
                        break;
                    }
                    if (!it->is_regular_file(error) || error) {
                        error.clear();
                        continue;
                    }
                    // Lexically, so a link ("UTC" -> "Etc/UTC") is indexed under its own name.
                    const auto relative = it->path().lexically_relative(root).generic_string();
                    if (relative.empty() || relative == "localtime") {
                        continue;
                    }
                    found.emplace(lowered(relative), relative);
                }
                return found;
            }();
            return names;
        }

    } // namespace

    time_zone_t time_zone_t::fixed(timezone_offset_t offset) noexcept {
        time_zone_t zone;
        zone.offset_ = offset;
        return zone;
    }

    timezone_offset_t time_zone_t::offset_at(timestamptz_t instant) const {
        if (fixed_) {
            return offset_;
        }
        return timezone_offset_t{zone_.At(instant_of(instant)).offset};
    }

    timezone_offset_t time_zone_t::offset_of_local(timestamp_t local) const {
        if (fixed_) {
            return offset_;
        }
        const auto [seconds, fraction] = split(local.value.count());
        const absl::CivilSecond civil = civil_epoch + seconds;
        const auto info = zone_.At(civil);
        const absl::Time instant = info.kind == absl::TimeZone::TimeInfo::REPEATED ? info.post : info.pre;
        return timezone_offset_t{static_cast<int32_t>(civil - absl::ToCivilSecond(instant, absl::UTCTimeZone()))};
    }

    timezone_offset_t time_zone_t::current_offset() const {
        if (fixed_) {
            return offset_;
        }
        return timezone_offset_t{zone_.At(absl::Now()).offset};
    }

    timestamp_t time_zone_t::to_local(timestamptz_t instant) const {
        const auto offset = std::chrono::duration_cast<microseconds>(offset_at(instant));
        return timestamp_t{instant.value + offset};
    }

    timestamptz_t time_zone_t::to_utc(timestamp_t local) const {
        const auto offset = std::chrono::duration_cast<microseconds>(offset_of_local(local));
        return timestamptz_t{local.value - offset};
    }

    bool named_time_zone(std::string_view name, time_zone_t* zone) {
        const auto& names = zone_names();
        const auto found = names.find(lowered(name));
        if (found == names.end()) {
            return false;
        }
        absl::TimeZone loaded;
        if (!absl::LoadTimeZone(found->second, &loaded)) {
            return false;
        }
        time_zone_t result;
        result.fixed_ = false;
        result.zone_ = loaded;
        *zone = result;
        return true;
    }

    bool setting_time_zone(std::string_view name, time_zone_t* zone) {
        const auto lower = lowered(name);
        timezone_offset_t offset{};
        if (fixed_offset_of(lower, &offset)) {
            *zone = time_zone_t::fixed(offset);
            return true;
        }
        if (named_time_zone(lower, zone)) {
            return true;
        }
        if (abbreviation_offset(lower, &offset)) {
            *zone = time_zone_t::fixed(offset);
            return true;
        }
        return false;
    }

    bool conversion_time_zone(std::string_view name, time_zone_t* zone) {
        const auto lower = lowered(name);
        timezone_offset_t offset{};
        if (fixed_offset_of(lower, &offset) || abbreviation_offset(lower, &offset)) {
            *zone = time_zone_t::fixed(offset);
            return true;
        }
        return named_time_zone(lower, zone);
    }

} // namespace core::date
