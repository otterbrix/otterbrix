#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
SCALE="0.01"
SUITES=()
BUILD_DIR="/tmp/benchmark-build"
CLICKBENCH_ROWS=0

# --- Helpers ---

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Download benchmark data for otterbrix SQL benchmarks.

Options:
  --suite=NAME          Download specific suite (ssb|tpch|job|clickbench).
                        Can be repeated. Default: all suites.
  --scale=N             Scale factor for TPC-H/SSB (default: 0.01)
  --data-dir=DIR        Where to put data (default: benchmark/data)
  --clickbench-rows=N   Limit ClickBench to first N rows (0=full, default: 0)
  --help                Show this help
EOF
    exit 0
}

log() {
    echo "==> $*"
}

err() {
    echo "ERROR: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || err "Required command not found: $1"
}

# Prepend a header line to a file (in-place)
prepend_header() {
    local file="$1"
    local header="$2"
    local tmp
    tmp="$(mktemp)"
    echo "$header" > "$tmp"
    cat "$file" >> "$tmp"
    mv "$tmp" "$file"
}

# --- SSB ---

download_ssb() {
    log "Downloading SSB data (scale=$SCALE)..."
    mkdir -p "$DATA_DIR/ssb"
    mkdir -p "$BUILD_DIR"

    local ssb_dir="$BUILD_DIR/ssb-dbgen"
    if [ ! -d "$ssb_dir" ]; then
        git clone https://github.com/eyalroz/ssb-dbgen.git "$ssb_dir"
    fi

    cd "$ssb_dir"
    cmake -B build 2>/dev/null
    cmake --build build -j4

    log "Generating SSB tables (scale=$SCALE)..."
    cd build
    ./dbgen -s "$SCALE" -f

    # SSB .tbl files: pipe-delimited, no headers
    # Tables: customer.tbl, date.tbl, lineorder.tbl, part.tbl, supplier.tbl
    # (bash 3.2 compatible: case-based lookup instead of associative arrays)
    ssb_header_for() {
        case "$1" in
            customer.tbl)  echo "c_custkey|c_name|c_address|c_city|c_nation|c_region|c_phone|c_mktsegment" ;;
            date.tbl)      echo "d_datekey|d_date|d_dayofweek|d_month|d_year|d_yearmonthnum|d_yearmonth|d_daynuminweek|d_daynuminmonth|d_daynuminyear|d_monthnuminyear|d_weeknuminyear|d_sellingseason|d_lastdayinweekfl|d_lastdayinmonthfl|d_holidayfl|d_weekdayfl" ;;
            lineorder.tbl) echo "lo_orderkey|lo_linenumber|lo_custkey|lo_partkey|lo_suppkey|lo_orderdate|lo_orderpriority|lo_shippriority|lo_quantity|lo_extendedprice|lo_ordtotalprice|lo_discount|lo_revenue|lo_supplycost|lo_tax|lo_commitdate|lo_shipmode" ;;
            part.tbl)      echo "p_partkey|p_name|p_mfgr|p_category|p_brand1|p_color|p_type|p_size|p_container" ;;
            supplier.tbl)  echo "s_suppkey|s_name|s_address|s_city|s_nation|s_region|s_phone" ;;
        esac
    }

    local gen_dir="$ssb_dir/build"
    for tbl in customer.tbl date.tbl lineorder.tbl part.tbl supplier.tbl; do
        if [ -f "$gen_dir/$tbl" ]; then
            cp "$gen_dir/$tbl" "$DATA_DIR/ssb/$tbl"
            prepend_header "$DATA_DIR/ssb/$tbl" "$(ssb_header_for "$tbl")"
            log "  $tbl -> $DATA_DIR/ssb/$tbl"
        else
            echo "  WARNING: $tbl not found"
        fi
    done

    log "SSB data ready in $DATA_DIR/ssb/"
}

# --- TPC-H ---

download_tpch() {
    log "Downloading TPC-H data (scale=$SCALE)..."
    mkdir -p "$DATA_DIR/tpch"
    mkdir -p "$BUILD_DIR"

    local tpch_dir="$BUILD_DIR/tpch-dbgen"
    if [ ! -d "$tpch_dir" ]; then
        git clone https://github.com/electrum/tpch-dbgen.git "$tpch_dir"
    fi

    cd "$tpch_dir"
    # Old C code needs relaxed warnings on modern compilers
    make clean 2>/dev/null || true
    make CC="gcc -Wno-implicit-int -Wno-implicit-function-declaration" -j4

    log "Generating TPC-H tables (scale=$SCALE)..."
    ./dbgen -s "$SCALE" -f

    # TPC-H .tbl files: pipe-delimited, no headers
    # (bash 3.2 compatible: case-based lookup instead of associative arrays)
    tpch_header_for() {
        case "$1" in
            customer.tbl) echo "c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment" ;;
            orders.tbl)   echo "o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderdate|o_orderpriority|o_clerk|o_shippriority|o_comment" ;;
            lineitem.tbl) echo "l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment" ;;
            part.tbl)     echo "p_partkey|p_name|p_mfgr|p_brand|p_type|p_size|p_container|p_retailprice|p_comment" ;;
            partsupp.tbl) echo "ps_partkey|ps_suppkey|ps_availqty|ps_supplycost|ps_comment" ;;
            supplier.tbl) echo "s_suppkey|s_name|s_address|s_nationkey|s_phone|s_acctbal|s_comment" ;;
            nation.tbl)   echo "n_nationkey|n_name|n_regionkey|n_comment" ;;
            region.tbl)   echo "r_regionkey|r_name|r_comment" ;;
        esac
    }

    for tbl in customer.tbl orders.tbl lineitem.tbl part.tbl partsupp.tbl supplier.tbl nation.tbl region.tbl; do
        if [ -f "$tpch_dir/$tbl" ]; then
            cp "$tpch_dir/$tbl" "$DATA_DIR/tpch/$tbl"
            prepend_header "$DATA_DIR/tpch/$tbl" "$(tpch_header_for "$tbl")"
            log "  $tbl -> $DATA_DIR/tpch/$tbl"
        else
            echo "  WARNING: $tbl not found"
        fi
    done

    log "TPC-H data ready in $DATA_DIR/tpch/"
}

# --- JOB (IMDB) ---

download_job() {
    log "Downloading JOB/IMDB data..."
    mkdir -p "$DATA_DIR/job"
    mkdir -p "$BUILD_DIR"

    local archive="$BUILD_DIR/imdb.tgz"
    if [ ! -f "$archive" ]; then
        log "Downloading imdb.tgz (~1.2GB)..."
        curl -L -o "$archive" "http://homepages.cwi.nl/~boncz/job/imdb.tgz"
    fi

    log "Extracting IMDB data..."
    tar xzf "$archive" -C "$DATA_DIR/job/"

    # IMDB CSV files: comma-delimited, no headers
    # Prepend headers for all 21 tables
    # (bash 3.2 compatible: case-based lookup instead of associative arrays)
    job_header_for() {
        case "$1" in
            aka_name.csv)        echo "id,person_id,name,imdb_index,name_pcode_cf,name_pcode_nf,surname_pcode,md5sum" ;;
            aka_title.csv)       echo "id,movie_id,title,imdb_index,kind_id,production_year,phonetic_code,episode_of_id,season_nr,episode_nr,note,md5sum" ;;
            cast_info.csv)       echo "id,person_id,movie_id,person_role_id,note,nr_order,role_id" ;;
            char_name.csv)       echo "id,name,imdb_index,imdb_id,name_pcode_nf,surname_pcode,md5sum" ;;
            comp_cast_type.csv)  echo "id,kind" ;;
            company_name.csv)    echo "id,name,country_code,imdb_id,name_pcode_nf,name_pcode_sf,md5sum" ;;
            company_type.csv)    echo "id,kind" ;;
            complete_cast.csv)   echo "id,movie_id,subject_id,status_id" ;;
            info_type.csv)       echo "id,info" ;;
            keyword.csv)         echo "id,keyword,phonetic_code" ;;
            kind_type.csv)       echo "id,kind" ;;
            link_type.csv)       echo "id,link" ;;
            movie_companies.csv) echo "id,movie_id,company_id,company_type_id,note" ;;
            movie_info.csv)      echo "id,movie_id,info_type_id,info,note" ;;
            movie_info_idx.csv)  echo "id,movie_id,info_type_id,info,note" ;;
            movie_keyword.csv)   echo "id,movie_id,keyword_id" ;;
            movie_link.csv)      echo "id,movie_id,linked_movie_id,link_type_id" ;;
            name.csv)            echo "id,name,imdb_index,imdb_id,gender,name_pcode_cf,name_pcode_nf,surname_pcode,md5sum" ;;
            person_info.csv)     echo "id,person_id,info_type_id,info,note" ;;
            role_type.csv)       echo "id,role" ;;
            title.csv)           echo "id,title,imdb_index,kind_id,production_year,imdb_id,phonetic_code,episode_of_id,season_nr,episode_nr,series_years,md5sum" ;;
        esac
    }

    for csv in aka_name.csv aka_title.csv cast_info.csv char_name.csv comp_cast_type.csv \
               company_name.csv company_type.csv complete_cast.csv info_type.csv keyword.csv \
               kind_type.csv link_type.csv movie_companies.csv movie_info.csv movie_info_idx.csv \
               movie_keyword.csv movie_link.csv name.csv person_info.csv role_type.csv title.csv; do
        local filepath="$DATA_DIR/job/$csv"
        if [ -f "$filepath" ]; then
            prepend_header "$filepath" "$(job_header_for "$csv")"
            log "  $csv (header added)"
        else
            echo "  WARNING: $csv not found"
        fi
    done

    log "JOB/IMDB data ready in $DATA_DIR/job/"
}

# --- ClickBench ---

download_clickbench() {
    log "Downloading ClickBench data..."
    mkdir -p "$DATA_DIR/clickbench"

    local gz_file="$DATA_DIR/clickbench/hits.csv.gz"
    local csv_file="$DATA_DIR/clickbench/hits.csv"

    if [ ! -f "$csv_file" ] && [ ! -f "$gz_file" ]; then
        log "Downloading hits.csv.gz (~10GB compressed)..."
        curl -L -o "$gz_file" \
            "https://datasets.clickhouse.com/hits_compatible/hits.csv.gz"
    fi

    if [ -f "$gz_file" ] && [ ! -f "$csv_file" ]; then
        log "Decompressing hits.csv.gz..."
        gunzip "$gz_file"
    fi

    # ClickBench hits.csv: tab-delimited, no header
    # Convert to pipe-delimited for consistency with the CSV loader
    local header="WatchID|JavaEnable|Title|GoodEvent|EventTime|EventDate|CounterID|ClientIP|RegionID|UserID|CounterClass|OS|UserAgent|URL|Referer|IsRefresh|RefererCategoryID|RefererRegionID|URLCategoryID|URLRegionID|ResolutionWidth|ResolutionHeight|ResolutionDepth|FlashMajor|FlashMinor|FlashMinor2|NetMajor|NetMinor|UserAgentMajor|UserAgentMinor|CookieEnable|JavascriptEnable|IsMobile|MobilePhone|MobilePhoneModel|Params|IPNetworkID|TraficSourceID|SearchEngineID|SearchPhrase|AdvEngineID|IsArtifical|WindowClientWidth|WindowClientHeight|ClientTimeZone|ClientEventTime|SilverlightVersion1|SilverlightVersion2|SilverlightVersion3|SilverlightVersion4|PageCharset|CodeVersion|IsLink|IsDownload|IsNotBounce|FUniqID|OriginalURL|HID|IsOldCounter|IsEvent|IsParameter|DontCountHits|WithHash|HitColor|LocalEventTime|Age|Sex|Income|Interests|Robotness|RemoteIP|WindowName|OpenerName|HistoryLength|BrowserLanguage|BrowserCountry|SocialNetwork|SocialAction|HTTPError|SendTiming|DNSTiming|ConnectTiming|ResponseStartTiming|ResponseEndTiming|FetchTiming|SocialSourceNetworkID|SocialSourcePage|ParamPrice|ParamOrderID|ParamCurrency|ParamCurrencyID|OpenstatServiceName|OpenstatCampaignID|OpenstatAdID|OpenstatSourceID|UTMSource|UTMedium|UTMCampaign|UTMContent|UTMTerm|FromTag|HasGCLID|RefererHash|URLHash|CLID"

    log "Converting hits.csv from TSV to pipe-delimited..."
    local tmp
    tmp="$(mktemp)"
    echo "$header" > "$tmp"

    if [ "$CLICKBENCH_ROWS" -gt 0 ]; then
        log "Limiting ClickBench to first $CLICKBENCH_ROWS rows..."
        head -n "$CLICKBENCH_ROWS" "$csv_file" | tr '\t' '|' >> "$tmp"
    else
        tr '\t' '|' < "$csv_file" >> "$tmp"
    fi

    mv "$tmp" "$csv_file"

    log "ClickBench data ready in $DATA_DIR/clickbench/"
}

# --- Argument parsing ---

while [[ $# -gt 0 ]]; do
    case "$1" in
        --suite=*)
            SUITES+=("${1#*=}")
            ;;
        --scale=*)
            SCALE="${1#*=}"
            ;;
        --data-dir=*)
            DATA_DIR="${1#*=}"
            ;;
        --clickbench-rows=*)
            CLICKBENCH_ROWS="${1#*=}"
            ;;
        --help)
            usage
            ;;
        *)
            err "Unknown option: $1 (use --help)"
            ;;
    esac
    shift
done

# Default: all suites
if [ ${#SUITES[@]} -eq 0 ]; then
    SUITES=(ssb tpch job clickbench)
fi

# --- Validate ---

require_cmd git
require_cmd make
require_cmd curl

log "Data directory: $DATA_DIR"
log "Suites: ${SUITES[*]}"
log "Scale factor: $SCALE"
mkdir -p "$DATA_DIR"

# --- Run ---

for suite in "${SUITES[@]}"; do
    case "$suite" in
        ssb)        download_ssb ;;
        tpch)       download_tpch ;;
        job)        download_job ;;
        clickbench) download_clickbench ;;
        *)          err "Unknown suite: $suite" ;;
    esac
done

log "Done! Data is in $DATA_DIR/"
