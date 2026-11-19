#include "timezones.hpp"

#include <cmath>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <utility>

namespace core::date {

    namespace {

        // Abbreviations, lowercase, as seconds east of UTC. An abbreviation is one fixed offset, daylight saving
        // ones included (CEST is +02 whatever the date), following PostgreSQL's default pg_timezone_abbrevs table;
        // ambiguous ones use the PostgreSQL-documented interpretation. Grouped by offset.
        const std::unordered_map<std::string_view, int32_t> ABBREVIATIONS = {
            // UTC-11
            {"nut", -39600},
            {"sst", -39600}, // Samoa Standard Time

            // UTC-10
            {"ckt", -36000},
            {"hast", -36000},
            {"hst", -36000},
            {"taht", -36000},

            // UTC-09:30
            {"mart", -34200},

            // UTC-9
            {"akst", -32400},
            {"gamt", -32400},
            {"hny", -32400},

            // UTC-8
            {"akdt", -28800},
            {"hadt", -28800},
            {"hay", -28800},
            {"pst", -28800},

            // UTC-7
            {"mst", -25200},
            {"hnr", -25200},
            {"pdt", -25200},

            // UTC-6
            {"cst", -21600}, // PostgreSQL default: US Central Standard
            {"east", -21600},
            {"galt", -21600},
            {"hnc", -21600},
            {"mdt", -21600},

            // UTC-5
            {"cot", -18000},
            {"easst", -18000},
            {"ect", -18000},
            {"est", -18000},
            {"hne", -18000},
            {"pet", -18000},

            // UTC-4
            {"adt", -14400},
            {"amt", -14400}, // Amazon Standard Time
            {"ast", -14400},
            {"bot", -14400},
            {"cdt", -14400}, // Central Daylight (US)
            {"clt", -14400},
            {"edt", -14400},
            {"gyt", -14400},
            {"hna", -14400},
            {"pyt", -14400},
            {"vet", -14400},

            // UTC-3:30
            {"hnt", -12600},
            {"nst", -12600},

            // UTC-2:30
            {"ndt", -9000},
            {"hat", -9000},

            // UTC-3
            {"art", -10800},
            {"brt", -10800},
            {"clst", -10800},
            {"fkt", -10800},
            {"fkst", -10800},
            {"gft", -10800},
            {"pmst", -10800},
            {"pyst", -10800},
            {"rott", -10800},
            {"srt", -10800},
            {"uyt", -10800},
            {"wgt", -10800},

            // UTC-2
            {"brst", -7200},
            {"fnt", -7200},
            {"pmdt", -7200},
            {"uyst", -7200},
            {"wgst", -7200},

            // UTC-1
            {"azot", -3600},
            {"azost", -3600},
            {"cvt", -3600},
            {"egt", -3600},

            // UTC+0
            {"egst", 0},
            {"ut", 0},
            {"z", 0},

            // UTC+1
            {"bst", 3600},
            {"cet", 3600},
            {"met", 3600},
            {"wat", 3600},
            {"west", 3600},

            // UTC+2
            {"cat", 7200},
            {"cest", 7200},
            {"eet", 7200},
            {"idt", 7200}, // Israel Daylight
            {"kalt", 7200},
            {"mest", 7200},
            {"sast", 7200},
            {"wast", 7200},

            // UTC+3
            {"eat", 10800},
            {"eest", 10800},
            {"fet", 10800},
            {"msk", 10800},
            {"syot", 10800},

            // UTC+3:30
            {"irst", 12600},

            // UTC+4
            {"azt", 14400},
            {"get", 14400},
            {"gst", 14400}, // Gulf Standard Time
            {"gest", 14400},
            {"irdt", 14400},
            {"msd", 14400},
            {"mut", 14400},
            {"ret", 14400},
            {"samt", 14400},
            {"sct", 14400},

            // UTC+4:30
            {"aft", 16200},

            // UTC+5
            {"mawt", 18000},
            {"mvt", 18000},
            {"orat", 18000},
            {"pkt", 18000},
            {"tft", 18000},
            {"tjt", 18000},
            {"tmt", 18000},
            {"uzt", 18000},
            {"yekt", 18000},

            // UTC+5:30
            {"ist", 19800}, // Indian Standard Time (PostgreSQL default)
            {"slst", 19800},

            // UTC+5:45
            {"npt", 20700},

            // UTC+6
            {"almt", 21600},
            {"btt", 21600},
            {"iot", 21600},
            {"kgt", 21600},
            {"omst", 21600},
            {"qyzt", 21600},
            {"vost", 21600},
            {"xjt", 21600},

            // UTC+6:30
            {"cct", 23400}, // Cocos Islands
            {"mmt", 23400},

            // UTC+7
            {"cxt", 25200},
            {"davt", 25200},
            {"hovt", 25200},
            {"ict", 25200},
            {"krat", 25200},
            {"novt", 25200},
            {"tha", 25200},
            {"wib", 25200},

            // UTC+8
            {"awst", 28800},
            {"bnt", 28800},
            {"hkt", 28800},
            {"hovst", 28800},
            {"irkt", 28800},
            {"myt", 28800},
            {"pht", 28800},
            {"sgt", 28800},
            {"ulat", 28800},
            {"wita", 28800},
            {"wst", 28800}, // Western Australia Standard

            // UTC+8:45
            {"acwst", 31500},
            {"cwst", 31500},

            // UTC+9
            {"awdt", 32400},
            {"jst", 32400},
            {"kst", 32400},
            {"pwt", 32400},
            {"tlt", 32400},
            {"ulast", 32400},
            {"wit", 32400},
            {"yakt", 32400},

            // UTC+9:30
            {"acst", 34200},
            {"cast", 34200},

            // UTC+10
            {"aest", 36000},
            {"chst", 36000},
            {"chut", 36000},
            {"ddut", 36000},
            {"pgt", 36000},
            {"vlat", 36000},

            // UTC+10:30
            {"lhst", 37800},

            // UTC+11
            {"aedt", 39600},
            {"lhdt", 39600},
            {"magt", 39600},
            {"nct", 39600},
            {"nft", 39600},
            {"pont", 39600},
            {"sakt", 39600},
            {"sbt", 39600},
            {"vut", 39600},

            // UTC+12
            {"anat", 43200},
            {"fjt", 43200},
            {"gilt", 43200},
            {"magst", 43200},
            {"mht", 43200},
            {"nrt", 43200},
            {"nzst", 43200},
            {"pett", 43200},
            {"petst", 43200},
            {"tvt", 43200},
            {"wft", 43200},

            // UTC+12:45
            {"chast", 45900},

            // UTC+13
            {"fjst", 46800},
            {"nzdt", 46800},
            {"phot", 46800},
            {"tkt", 46800},
            {"tot", 46800},

            // UTC+13:45
            {"chadt", 49500},

            // UTC+14
            {"lint", 50400},
            {"tost", 50400},
        };

    } // namespace

    bool fixed_offset_of(std::string_view name, timezone_offset_t* offset) {
        if (name == "utc" || name == "gmt") {
            *offset = timezone_offset_t{0};
            return true;
        }

        std::string_view rest = name;
        if (rest.size() >= 3 && (rest.substr(0, 3) == "utc" || rest.substr(0, 3) == "gmt")) {
            rest = rest.substr(3);
        }

        if (rest.empty() || (rest[0] != '+' && rest[0] != '-')) {
            return false;
        }

        const int32_t sign = (rest[0] == '+') ? 1 : -1;
        rest = rest.substr(1);
        if (rest.empty()) {
            return false;
        }

        const auto colon = rest.find(':');
        const std::string_view hours_sv = (colon != std::string_view::npos) ? rest.substr(0, colon) : rest;
        const std::string_view mins_sv = (colon != std::string_view::npos) ? rest.substr(colon + 1) : "";
        if (hours_sv.empty() || (colon != std::string_view::npos && mins_sv.empty())) {
            return false;
        }

        int32_t hours = 0;
        for (const char c : hours_sv) {
            if (c < '0' || c > '9' || hours > 99) {
                return false;
            }
            hours = hours * 10 + (c - '0');
        }

        int32_t minutes = 0;
        for (const char c : mins_sv) {
            if (c < '0' || c > '9' || minutes > 99) {
                return false;
            }
            minutes = minutes * 10 + (c - '0');
        }

        if (hours > 14 || minutes > 59) {
            return false;
        }

        *offset = timezone_offset_t{sign * (hours * 3600 + minutes * 60)};
        return true;
    }

    bool abbreviation_offset(std::string_view name, timezone_offset_t* offset) {
        const auto it = ABBREVIATIONS.find(name);
        if (it == ABBREVIATIONS.end()) {
            return false;
        }
        *offset = timezone_offset_t{it->second};
        return true;
    }

    std::string format_timezone(timezone_offset_t offset) {
        if (offset.count() == 0) {
            return "UTC";
        }

        int32_t abs_offset = std::abs(offset.count());
        int32_t hours = abs_offset / 3600;
        int32_t minutes = (abs_offset % 3600) / 60;
        char buf[16];
        std::snprintf(buf, sizeof(buf), "UTC%c%02d:%02d", offset.count() > 0 ? '+' : '-', hours, minutes);
        return buf;
    }

} // namespace core::date
