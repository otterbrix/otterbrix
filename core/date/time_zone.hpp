#pragma once

#include "date_types.hpp"

#include <absl/time/time.h>

#include <string_view>

namespace core::date {

    // A time zone: a fixed offset, or a zone of the IANA database with its daylight saving time rules. A value; a
    // copy shares the loaded rules, which are immutable.
    class time_zone_t {
    public:
        // UTC.
        time_zone_t() = default;
        static time_zone_t fixed(timezone_offset_t offset) noexcept;

        // Seconds east of UTC in effect at `instant`.
        [[nodiscard]] timezone_offset_t offset_at(timestamptz_t instant) const;
        // Seconds east of UTC the wall time `local` reads in. A time a spring-forward skips reads in the offset before
        // the transition, one a fall-back repeats in the later of its two instants (PostgreSQL 18, B.3).
        [[nodiscard]] timezone_offset_t offset_of_local(timestamp_t local) const;
        // Seconds east of UTC in effect now.
        [[nodiscard]] timezone_offset_t current_offset() const;

        [[nodiscard]] timestamp_t to_local(timestamptz_t instant) const;
        [[nodiscard]] timestamptz_t to_utc(timestamp_t local) const;

    private:
        friend bool named_time_zone(std::string_view name, time_zone_t* zone);

        bool fixed_{true};
        timezone_offset_t offset_{};
        absl::TimeZone zone_;
    };

    // A zone of the IANA database, its name matched in any case ("Europe/Berlin", "europe/berlin"); false when the
    // database has no such zone or the host has no database.
    bool named_time_zone(std::string_view name, time_zone_t* zone);

    // The zone the TimeZone setting names: a fixed offset ("+03:00", "utc-5"), a zone of the IANA database, or an
    // abbreviation; false when it names none.
    bool setting_time_zone(std::string_view name, time_zone_t* zone);

    // The zone timezone(zone, value) / AT TIME ZONE names: a fixed offset, an abbreviation, or a zone of the IANA
    // database, the abbreviation first (PostgreSQL 18 DecodeTimezoneName); false when it names none.
    bool conversion_time_zone(std::string_view name, time_zone_t* zone);

} // namespace core::date
