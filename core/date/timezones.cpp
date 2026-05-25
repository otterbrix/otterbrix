#include "timezones.hpp"

#include <cmath>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <utility>

namespace core::date {

    namespace {

        // All keys are lowercase. Values are seconds east of UTC.
        // Standard (non-DST) offsets are used for named zones.
        // Entries are grouped by UTC offset for readability.
        //
        // Note: Etc/GMT+N uses POSIX sign convention — Etc/GMT+N = UTC-N.
        // Abbreviations follow PostgreSQL's default pg_timezone_abbrevs table;
        // ambiguous ones use the PostgreSQL-documented interpretation.
        const std::unordered_map<std::string_view, int32_t> TIMEZONE_MAP = {

            // UTC-12
            {"etc/gmt+12", -43200},
            {"pacific/apia", -43200}, // pre-2011 Samoa; kept for compat
            {"pacific/baker_island", -43200},

            // UTC-11
            {"etc/gmt+11", -39600},
            {"pacific/midway", -39600},
            {"pacific/niue", -39600},
            {"pacific/pago_pago", -39600},
            {"us/samoa", -39600},
            // abbreviations
            {"nut", -39600},
            {"sst", -39600}, // Samoa Standard Time

            // UTC-10
            {"america/adak", -36000},
            {"etc/gmt+10", -36000},
            {"pacific/honolulu", -36000},
            {"pacific/johnston", -36000},
            {"pacific/rarotonga", -36000},
            {"pacific/tahiti", -36000},
            {"us/aleutian", -36000},
            {"us/hawaii", -36000},
            // abbreviations
            {"ckt", -36000},
            {"hast", -36000},
            {"hst", -36000},
            {"taht", -36000},

            // UTC-9:30
            {"pacific/marquesas", -34200},
            // abbreviations
            {"mart", -34200},

            // UTC-9
            {"america/anchorage", -32400},
            {"america/juneau", -32400},
            {"america/metlakatla", -32400},
            {"america/nome", -32400},
            {"america/sitka", -32400},
            {"america/yakutat", -32400},
            {"etc/gmt+9", -32400},
            {"pacific/gambier", -32400},
            {"us/alaska", -32400},
            // abbreviations
            {"akst", -32400},
            {"gamt", -32400},
            {"hny", -32400},

            // UTC-8
            {"america/los_angeles", -28800},
            {"america/tijuana", -28800},
            {"america/vancouver", -28800},
            {"etc/gmt+8", -28800},
            {"pacific/pitcairn", -28800},
            {"us/pacific", -28800},
            // abbreviations
            {"akdt", -28800},
            {"hadt", -28800},
            {"hay", -28800},
            {"pst", -28800},

            // UTC-7
            {"america/boise", -25200},
            {"america/cambridge_bay", -25200},
            {"america/chihuahua", -25200},
            {"america/creston", -25200},
            {"america/dawson", -25200},
            {"america/dawson_creek", -25200},
            {"america/denver", -25200},
            {"america/edmonton", -25200},
            {"america/fort_nelson", -25200},
            {"america/hermosillo", -25200},
            {"america/inuvik", -25200},
            {"america/mazatlan", -25200},
            {"america/ojinaga", -25200},
            {"america/phoenix", -25200},
            {"america/whitehorse", -25200},
            {"america/yellowknife", -25200},
            {"canada/mountain", -25200},
            {"canada/yukon", -25200},
            {"etc/gmt+7", -25200},
            {"mexico/bajasur", -25200},
            {"navajo", -25200},
            {"us/arizona", -25200},
            {"us/mountain", -25200},
            // abbreviations
            {"mst", -25200},
            {"hnr", -25200},
            {"pdt", -25200},

            // UTC-6
            {"america/bahia_banderas", -21600},
            {"america/belize", -21600},
            {"america/chicago", -21600},
            {"america/costa_rica", -21600},
            {"america/el_salvador", -21600},
            {"america/guatemala", -21600},
            {"america/indiana/knox", -21600},
            {"america/indiana/tell_city", -21600},
            {"america/managua", -21600},
            {"america/matamoros", -21600},
            {"america/menominee", -21600},
            {"america/merida", -21600},
            {"america/monterrey", -21600},
            {"america/mexico_city", -21600},
            {"america/north_dakota/beulah", -21600},
            {"america/north_dakota/center", -21600},
            {"america/north_dakota/new_salem", -21600},
            {"america/rainy_river", -21600},
            {"america/rankin_inlet", -21600},
            {"america/regina", -21600},
            {"america/resolute", -21600},
            {"america/swift_current", -21600},
            {"america/tegucigalpa", -21600},
            {"america/winnipeg", -21600},
            {"canada/central", -21600},
            {"chile/easterisland", -21600},
            {"etc/gmt+6", -21600},
            {"mexico/general", -21600},
            {"pacific/easter", -21600},
            {"pacific/galapagos", -21600},
            {"us/central", -21600},
            {"us/indiana-starke", -21600},
            // abbreviations
            {"cst", -21600}, // PostgreSQL default: US Central Standard
            {"east", -21600},
            {"galt", -21600},
            {"hnc", -21600},
            {"mdt", -21600},

            // UTC-5
            {"america/atikokan", -18000},
            {"america/bogota", -18000},
            {"america/cancun", -18000},
            {"america/cayman", -18000},
            {"america/detroit", -18000},
            {"america/eirunepe", -18000},
            {"america/grand_turk", -18000},
            {"america/guayaquil", -18000},
            {"america/havana", -18000},
            {"america/indiana/indianapolis", -18000},
            {"america/indiana/marengo", -18000},
            {"america/indiana/petersburg", -18000},
            {"america/indiana/vevay", -18000},
            {"america/indiana/vincennes", -18000},
            {"america/indiana/winamac", -18000},
            {"america/iqaluit", -18000},
            {"america/jamaica", -18000},
            {"america/kentucky/louisville", -18000},
            {"america/kentucky/monticello", -18000},
            {"america/lima", -18000},
            {"america/nassau", -18000},
            {"america/new_york", -18000},
            {"america/nipigon", -18000},
            {"america/panama", -18000},
            {"america/pangnirtung", -18000},
            {"america/port-au-prince", -18000},
            {"america/rio_branco", -18000},
            {"america/thunder_bay", -18000},
            {"america/toronto", -18000},
            {"brazil/acre", -18000},
            {"canada/eastern", -18000},
            {"cuba", -18000},
            {"etc/gmt+5", -18000},
            {"us/east-indiana", -18000},
            {"us/eastern", -18000},
            {"us/michigan", -18000},
            // abbreviations
            {"cot", -18000},
            {"easst", -18000},
            {"ect", -18000},
            {"est", -18000},
            {"hne", -18000},
            {"pet", -18000},

            // UTC-4
            {"america/anguilla", -14400},
            {"america/antigua", -14400},
            {"america/aruba", -14400},
            {"america/asuncion", -14400},
            {"america/barbados", -14400},
            {"america/blanc-sablon", -14400},
            {"america/boa_vista", -14400},
            {"america/campo_grande", -14400},
            {"america/caracas", -14400},
            {"america/cuiaba", -14400},
            {"america/curacao", -14400},
            {"america/dominica", -14400},
            {"america/glace_bay", -14400},
            {"america/goose_bay", -14400},
            {"america/grenada", -14400},
            {"america/guadeloupe", -14400},
            {"america/guyana", -14400},
            {"america/halifax", -14400},
            {"america/kralendijk", -14400},
            {"america/la_paz", -14400},
            {"america/lower_princes", -14400},
            {"america/manaus", -14400},
            {"america/marigot", -14400},
            {"america/martinique", -14400},
            {"america/moncton", -14400},
            {"america/montserrat", -14400},
            {"america/porto_velho", -14400},
            {"america/puerto_rico", -14400},
            {"america/santiago", -14400},
            {"america/santo_domingo", -14400},
            {"america/st_barthelemy", -14400},
            {"america/st_kitts", -14400},
            {"america/st_lucia", -14400},
            {"america/st_thomas", -14400},
            {"america/st_vincent", -14400},
            {"america/thule", -14400},
            {"america/tortola", -14400},
            {"atlantic/bermuda", -14400},
            {"brazil/west", -14400},
            {"canada/atlantic", -14400},
            {"chile/continental", -14400},
            {"etc/gmt+4", -14400},
            // abbreviations
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
            {"america/st_johns", -12600},
            {"canada/newfoundland", -12600},
            // abbreviations
            {"hnt", -12600},
            {"nst", -12600},

            // UTC-2:30
            // abbreviations
            {"ndt", -9000},
            {"hat", -9000},

            // UTC-3
            {"america/araguaina", -10800},
            {"america/argentina/buenos_aires", -10800},
            {"america/argentina/catamarca", -10800},
            {"america/argentina/cordoba", -10800},
            {"america/argentina/jujuy", -10800},
            {"america/argentina/la_rioja", -10800},
            {"america/argentina/mendoza", -10800},
            {"america/argentina/rio_gallegos", -10800},
            {"america/argentina/salta", -10800},
            {"america/argentina/san_juan", -10800},
            {"america/argentina/san_luis", -10800},
            {"america/argentina/tucuman", -10800},
            {"america/argentina/ushuaia", -10800},
            {"america/bahia", -10800},
            {"america/belem", -10800},
            {"america/cayenne", -10800},
            {"america/fortaleza", -10800},
            {"america/godthab", -10800},
            {"america/maceio", -10800},
            {"america/miquelon", -10800},
            {"america/montevideo", -10800},
            {"america/nuuk", -10800},
            {"america/paramaribo", -10800},
            {"america/punta_arenas", -10800},
            {"america/recife", -10800},
            {"america/rosario", -10800},
            {"america/santarem", -10800},
            {"america/sao_paulo", -10800},
            {"antarctica/palmer", -10800},
            {"antarctica/rothera", -10800},
            {"atlantic/stanley", -10800},
            {"brazil/east", -10800},
            {"etc/gmt+3", -10800},
            // abbreviations
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
            {"america/noronha", -7200},
            {"atlantic/south_georgia", -7200},
            {"brazil/denoronha", -7200},
            {"etc/gmt+2", -7200},
            // abbreviations
            {"brst", -7200},
            {"fnt", -7200},
            {"pmdt", -7200},
            {"uyst", -7200},
            {"wgst", -7200},

            // UTC-1
            {"america/scoresbysund", -3600},
            {"atlantic/azores", -3600},
            {"atlantic/cape_verde", -3600},
            {"etc/gmt+1", -3600},
            // abbreviations
            {"azot", -3600},
            {"azost", -3600},
            {"cvt", -3600},
            {"egt", -3600},

            // UTC+0
            {"africa/abidjan", 0},
            {"africa/accra", 0},
            {"africa/bamako", 0},
            {"africa/banjul", 0},
            {"africa/bissau", 0},
            {"africa/conakry", 0},
            {"africa/dakar", 0},
            {"africa/freetown", 0},
            {"africa/lome", 0},
            {"africa/monrovia", 0},
            {"africa/nouakchott", 0},
            {"africa/ouagadougou", 0},
            {"africa/sao_tome", 0},
            {"america/danmarkshavn", 0},
            {"antarctica/troll", 0},
            {"atlantic/canary", 0},
            {"atlantic/faroe", 0},
            {"atlantic/faeroe", 0},
            {"atlantic/madeira", 0},
            {"atlantic/reykjavik", 0},
            {"atlantic/st_helena", 0},
            {"etc/gmt", 0},
            {"etc/gmt+0", 0},
            {"etc/gmt-0", 0},
            {"etc/gmt0", 0},
            {"etc/utc", 0},
            {"etc/universal", 0},
            {"etc/zulu", 0},
            {"europe/dublin", 0},
            {"europe/guernsey", 0},
            {"europe/isle_of_man", 0},
            {"europe/jersey", 0},
            {"europe/lisbon", 0},
            {"europe/london", 0},
            {"eire", 0},
            {"gb", 0},
            {"gb-eire", 0},
            {"gmt", 0},
            {"gmt0", 0},
            {"greenwich", 0},
            {"iceland", 0},
            {"portugal", 0},
            {"uct", 0},
            {"universal", 0},
            {"utc", 0},
            {"wet", 0},
            {"zulu", 0},
            // abbreviations
            {"egst", 0},
            {"ut", 0},
            {"z", 0},

            // UTC+1
            {"africa/algiers", 3600},
            {"africa/casablanca", 3600},
            {"africa/ceuta", 3600},
            {"africa/douala", 3600},
            {"africa/el_aaiun", 3600},
            {"africa/kinshasa", 3600},
            {"africa/lagos", 3600},
            {"africa/libreville", 3600},
            {"africa/luanda", 3600},
            {"africa/malabo", 3600},
            {"africa/ndjamena", 3600},
            {"africa/niamey", 3600},
            {"africa/porto-novo", 3600},
            {"africa/tunis", 3600},
            {"arctic/longyearbyen", 3600},
            {"etc/gmt-1", 3600},
            {"europe/amsterdam", 3600},
            {"europe/andorra", 3600},
            {"europe/belgrade", 3600},
            {"europe/berlin", 3600},
            {"europe/bratislava", 3600},
            {"europe/brussels", 3600},
            {"europe/budapest", 3600},
            {"europe/busingen", 3600},
            {"europe/copenhagen", 3600},
            {"europe/gibraltar", 3600},
            {"europe/ljubljana", 3600},
            {"europe/luxembourg", 3600},
            {"europe/madrid", 3600},
            {"europe/malta", 3600},
            {"europe/monaco", 3600},
            {"europe/oslo", 3600},
            {"europe/paris", 3600},
            {"europe/podgorica", 3600},
            {"europe/prague", 3600},
            {"europe/rome", 3600},
            {"europe/san_marino", 3600},
            {"europe/sarajevo", 3600},
            {"europe/skopje", 3600},
            {"europe/stockholm", 3600},
            {"europe/tirane", 3600},
            {"europe/vaduz", 3600},
            {"europe/vatican", 3600},
            {"europe/vienna", 3600},
            {"europe/warsaw", 3600},
            {"europe/zagreb", 3600},
            {"europe/zurich", 3600},
            {"poland", 3600},
            // abbreviations
            {"bst", 3600},
            {"cet", 3600},
            {"met", 3600},
            {"wat", 3600},
            {"west", 3600},

            // UTC+2
            {"africa/blantyre", 7200},
            {"africa/bujumbura", 7200},
            {"africa/cairo", 7200},
            {"africa/gaborone", 7200},
            {"africa/harare", 7200},
            {"africa/johannesburg", 7200},
            {"africa/khartoum", 7200},
            {"africa/kigali", 7200},
            {"africa/lubumbashi", 7200},
            {"africa/lusaka", 7200},
            {"africa/maputo", 7200},
            {"africa/maseru", 7200},
            {"africa/mbabane", 7200},
            {"africa/tripoli", 7200},
            {"africa/windhoek", 7200},
            {"asia/beirut", 7200},
            {"asia/famagusta", 7200},
            {"asia/gaza", 7200},
            {"asia/hebron", 7200},
            {"asia/jerusalem", 7200},
            {"asia/nicosia", 7200},
            {"asia/tel_aviv", 7200},
            {"egypt", 7200},
            {"etc/gmt-2", 7200},
            {"europe/athens", 7200},
            {"europe/bucharest", 7200},
            {"europe/chisinau", 7200},
            {"europe/helsinki", 7200},
            {"europe/kaliningrad", 7200},
            {"europe/kiev", 7200},
            {"europe/kyiv", 7200},
            {"europe/mariehamn", 7200},
            {"europe/nicosia", 7200},
            {"europe/riga", 7200},
            {"europe/sofia", 7200},
            {"europe/tallinn", 7200},
            {"europe/uzhgorod", 7200},
            {"europe/vilnius", 7200},
            {"europe/zaporozhye", 7200},
            {"israel", 7200},
            {"libya", 7200},
            // abbreviations
            {"cat", 7200},
            {"cest", 7200},
            {"eet", 7200},
            {"idt", 7200}, // Israel Daylight -- DST but included for compat
            {"kalt", 7200},
            {"mest", 7200},
            {"sast", 7200},
            {"wast", 7200},

            // UTC+3
            {"africa/addis_ababa", 10800},
            {"africa/asmara", 10800},
            {"africa/dar_es_salaam", 10800},
            {"africa/djibouti", 10800},
            {"africa/juba", 10800},
            {"africa/kampala", 10800},
            {"africa/mogadishu", 10800},
            {"africa/nairobi", 10800},
            {"antarctica/syowa", 10800},
            {"asia/aden", 10800},
            {"asia/amman", 10800}, // Jordan: permanent UTC+3 since 2022
            {"asia/baghdad", 10800},
            {"asia/bahrain", 10800},
            {"asia/damascus", 10800},
            {"asia/kuwait", 10800},
            {"asia/qatar", 10800},
            {"asia/riyadh", 10800},
            {"etc/gmt-3", 10800},
            {"europe/istanbul", 10800},
            {"europe/kirov", 10800},
            {"europe/minsk", 10800},
            {"europe/moscow", 10800},
            {"europe/simferopol", 10800},
            {"europe/volgograd", 10800},
            {"indian/antananarivo", 10800},
            {"indian/comoro", 10800},
            {"indian/mayotte", 10800},
            {"turkey", 10800},
            {"w-su", 10800},
            // abbreviations
            {"eat", 10800},
            {"eest", 10800},
            {"fet", 10800},
            {"msk", 10800},
            {"syot", 10800},

            // UTC+3:30
            {"asia/tehran", 12600},
            {"iran", 12600},
            // abbreviations
            {"irst", 12600},

            // UTC+4
            {"asia/baku", 14400},
            {"asia/dubai", 14400},
            {"asia/muscat", 14400},
            {"asia/tbilisi", 14400},
            {"asia/yerevan", 14400},
            {"etc/gmt-4", 14400},
            {"europe/astrakhan", 14400},
            {"europe/samara", 14400},
            {"europe/saratov", 14400},
            {"europe/ulyanovsk", 14400},
            {"indian/mahe", 14400},
            {"indian/mauritius", 14400},
            {"indian/reunion", 14400},
            // abbreviations
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
            {"asia/kabul", 16200},
            // abbreviations
            {"aft", 16200},

            // UTC+5
            {"antarctica/mawson", 18000},
            {"asia/aqtau", 18000},
            {"asia/aqtobe", 18000},
            {"asia/ashgabat", 18000},
            {"asia/ashkhabad", 18000},
            {"asia/atyrau", 18000},
            {"asia/dushanbe", 18000},
            {"asia/karachi", 18000},
            {"asia/oral", 18000},
            {"asia/qyzylorda", 18000},
            {"asia/samarkand", 18000},
            {"asia/tashkent", 18000},
            {"asia/yekaterinburg", 18000},
            {"etc/gmt-5", 18000},
            {"indian/kerguelen", 18000},
            {"indian/maldives", 18000},
            // abbreviations
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
            {"asia/calcutta", 19800},
            {"asia/colombo", 19800},
            {"asia/kolkata", 19800},
            // abbreviations
            {"ist", 19800}, // Indian Standard Time (PostgreSQL default)
            {"slst", 19800},

            // UTC+5:45
            {"asia/kathmandu", 20700},
            {"asia/katmandu", 20700},
            // abbreviations
            {"npt", 20700},

            // UTC+6
            {"antarctica/vostok", 21600},
            {"asia/almaty", 21600},
            {"asia/bishkek", 21600},
            {"asia/dhaka", 21600},
            {"asia/omsk", 21600},
            {"asia/qostanay", 21600},
            {"asia/thimphu", 21600},
            {"asia/thimbu", 21600},
            {"asia/urumqi", 21600},
            {"etc/gmt-6", 21600},
            {"indian/chagos", 21600},
            // abbreviations
            {"almt", 21600},
            {"btt", 21600},
            {"iot", 21600},
            {"kgt", 21600},
            {"omst", 21600},
            {"qyzt", 21600},
            {"vost", 21600},
            {"xjt", 21600},

            // UTC+6:30
            {"asia/rangoon", 23400},
            {"asia/yangon", 23400},
            {"indian/cocos", 23400},
            // abbreviations
            {"cct", 23400}, // Cocos Islands
            {"mmt", 23400},

            // UTC+7
            {"antarctica/davis", 25200},
            {"asia/bangkok", 25200},
            {"asia/barnaul", 25200},
            {"asia/ho_chi_minh", 25200},
            {"asia/hovd", 25200},
            {"asia/jakarta", 25200},
            {"asia/krasnoyarsk", 25200},
            {"asia/novokuznetsk", 25200},
            {"asia/novosibirsk", 25200},
            {"asia/phnom_penh", 25200},
            {"asia/pontianak", 25200},
            {"asia/saigon", 25200},
            {"asia/tomsk", 25200},
            {"asia/vientiane", 25200},
            {"etc/gmt-7", 25200},
            {"indian/christmas", 25200},
            // abbreviations
            {"cxt", 25200},
            {"davt", 25200},
            {"hovt", 25200},
            {"ict", 25200},
            {"krat", 25200},
            {"novt", 25200},
            {"tha", 25200},
            {"wib", 25200},

            // UTC+8
            {"asia/brunei", 28800},
            {"asia/choibalsan", 28800},
            {"asia/chongqing", 28800},
            {"asia/harbin", 28800},
            {"asia/hong_kong", 28800},
            {"asia/irkutsk", 28800},
            {"asia/kashgar", 28800},
            {"asia/kuala_lumpur", 28800},
            {"asia/kuching", 28800},
            {"asia/macao", 28800},
            {"asia/macau", 28800},
            {"asia/makassar", 28800},
            {"asia/manila", 28800},
            {"asia/shanghai", 28800},
            {"asia/singapore", 28800},
            {"asia/taipei", 28800},
            {"asia/ujung_pandang", 28800},
            {"asia/ulaanbaatar", 28800},
            {"asia/ulan_bator", 28800},
            {"australia/perth", 28800},
            {"australia/west", 28800},
            {"etc/gmt-8", 28800},
            {"hongkong", 28800},
            {"prc", 28800},
            {"roc", 28800},
            {"singapore", 28800},
            // abbreviations
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
            {"australia/eucla", 31500},
            // abbreviations
            {"acwst", 31500},
            {"cwst", 31500},

            // UTC+9
            {"asia/chita", 32400},
            {"asia/dili", 32400},
            {"asia/jayapura", 32400},
            {"asia/khandyga", 32400},
            {"asia/pyongyang", 32400},
            {"asia/seoul", 32400},
            {"asia/tokyo", 32400},
            {"asia/yakutsk", 32400},
            {"etc/gmt-9", 32400},
            {"japan", 32400},
            {"pacific/palau", 32400},
            {"rok", 32400},
            // abbreviations
            {"awdt", 32400},
            {"jst", 32400},
            {"kst", 32400},
            {"pwt", 32400},
            {"tlt", 32400},
            {"ulast", 32400},
            {"wit", 32400},
            {"yakt", 32400},

            // UTC+9:30
            {"australia/adelaide", 34200},
            {"australia/broken_hill", 34200},
            {"australia/darwin", 34200},
            {"australia/north", 34200},
            {"australia/south", 34200},
            {"australia/yancowinna", 34200},
            // abbreviations
            {"acst", 34200},
            {"cast", 34200},

            // UTC+10
            {"antarctica/dumontdurville", 36000},
            {"asia/ust-nera", 36000},
            {"asia/vladivostok", 36000},
            {"australia/act", 36000},
            {"australia/brisbane", 36000},
            {"australia/canberra", 36000},
            {"australia/hobart", 36000},
            {"australia/lindeman", 36000},
            {"australia/melbourne", 36000},
            {"australia/nsw", 36000},
            {"australia/queensland", 36000},
            {"australia/sydney", 36000},
            {"australia/tasmania", 36000},
            {"australia/victoria", 36000},
            {"etc/gmt-10", 36000},
            {"pacific/chuuk", 36000},
            {"pacific/guam", 36000},
            {"pacific/port_moresby", 36000},
            {"pacific/saipan", 36000},
            // abbreviations
            {"aest", 36000},
            {"chst", 36000},
            {"chut", 36000},
            {"ddut", 36000},
            {"pgt", 36000},
            {"vlat", 36000},

            // UTC+10:30
            {"australia/lhi", 37800},
            {"australia/lord_howe", 37800},
            // abbreviations
            {"lhst", 37800},

            // UTC+11
            {"antarctica/macquarie", 39600},
            {"asia/magadan", 39600},
            {"asia/sakhalin", 39600},
            {"asia/srednekolymsk", 39600},
            {"etc/gmt-11", 39600},
            {"pacific/bougainville", 39600},
            {"pacific/efate", 39600},
            {"pacific/guadalcanal", 39600},
            {"pacific/kosrae", 39600},
            {"pacific/norfolk", 39600},
            {"pacific/noumea", 39600},
            {"pacific/pohnpei", 39600},
            // abbreviations
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
            {"antarctica/mcmurdo", 43200},
            {"asia/anadyr", 43200},
            {"asia/kamchatka", 43200},
            {"etc/gmt-12", 43200},
            {"kwajalein", 43200},
            {"nz", 43200},
            {"pacific/auckland", 43200},
            {"pacific/fiji", 43200},
            {"pacific/funafuti", 43200},
            {"pacific/kwajalein", 43200},
            {"pacific/majuro", 43200},
            {"pacific/nauru", 43200},
            {"pacific/tarawa", 43200},
            {"pacific/wake", 43200},
            {"pacific/wallis", 43200},
            // abbreviations
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
            {"nz-chat", 45900},
            {"pacific/chatham", 45900},
            // abbreviations
            {"chast", 45900},

            // UTC+13
            {"etc/gmt-13", 46800},
            {"pacific/apia", 46800}, // Samoa changed sides of date line in 2011
            {"pacific/enderbury", 46800},
            {"pacific/fakaofo", 46800},
            {"pacific/kanton", 46800},
            {"pacific/tongatapu", 46800},
            // abbreviations
            {"fjst", 46800},
            {"nzdt", 46800},
            {"phot", 46800},
            {"tkt", 46800},
            {"tot", 46800},

            // UTC+13:45
            // abbreviations
            {"chadt", 49500},

            // UTC+14
            {"etc/gmt-14", 50400},
            {"pacific/kiritimati", 50400},
            // abbreviations
            {"lint", 50400},
            {"tost", 50400},
        };

        // Parses "utc", "gmt", "utc±HH", "utc±HH:MM", "gmt±HH:MM", "±HH", "±HH:MM".
        // Input is assumed to be lowercase.
        std::optional<timezone_offset_t> parse_fixed_offset(std::string_view s) {
            if (s == "utc" || s == "gmt") {
                return timezone_offset_t{0};
            }

            std::string_view rest = s;
            if (rest.size() >= 3 && (rest.substr(0, 3) == "utc" || rest.substr(0, 3) == "gmt")) {
                rest = rest.substr(3);
            }

            if (rest.empty() || (rest[0] != '+' && rest[0] != '-')) {
                return std::nullopt;
            }

            int32_t sign = (rest[0] == '+') ? 1 : -1;
            rest = rest.substr(1);
            if (rest.empty()) {
                return std::nullopt;
            }

            auto colon = rest.find(':');
            std::string_view hours_sv = (colon != std::string_view::npos) ? rest.substr(0, colon) : rest;
            std::string_view mins_sv = (colon != std::string_view::npos) ? rest.substr(colon + 1) : "";

            int32_t hours = 0;
            for (char c : hours_sv) {
                if (c < '0' || c > '9') {
                    return std::nullopt;
                }
                hours = hours * 10 + (c - '0');
            }

            int32_t minutes = 0;
            for (char c : mins_sv) {
                if (c < '0' || c > '9') {
                    return std::nullopt;
                }
                minutes = minutes * 10 + (c - '0');
            }

            if (hours > 14 || minutes > 59) {
                return std::nullopt;
            }

            return timezone_offset_t{sign * (hours * 3600 + minutes * 60)};
        }

    } // namespace

    std::optional<timezone_offset_t> timezone_to_offset(std::string_view name) {
        if (!name.empty() &&
            (name[0] == '+' || name[0] == '-' || name.substr(0, 3) == "utc" || name.substr(0, 3) == "gmt")) {
            if (auto parsed = parse_fixed_offset(name)) {
                return parsed;
            }
        }

        auto it = TIMEZONE_MAP.find(name);
        if (it != TIMEZONE_MAP.end()) {
            return timezone_offset_t{it->second};
        }

        return std::nullopt;
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
