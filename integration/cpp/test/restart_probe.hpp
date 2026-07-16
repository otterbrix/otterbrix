#pragma once

// ===========================================================================
// RESTART-CONSISTENCY DIFFERENTIAL HARNESS
//
// The invariant under test (RC):
//
//   For any statement sequence S = S1 . S2, any restart flavor f, and any
//   storage mode m, the observable trace of
//
//       run(S1) ; terminate(f) ; reopen ; run(S2)
//
//   restricted to S2 must equal the observable trace of run(S1 . S2) with no
//   restart at all, over the committed prefix of S1.
//
// The harness realizes that with THREE runs of an identical probe list:
//
//   before   -- fresh database, run setup, run probes at key_pre
//   after    -- reopen that same data directory, run probes at key_post
//   control  -- a SEPARATE database, ONE process, NO restart: setup, probes at
//               key_pre, then probes at key_post again
//
// The control is what makes the other two mean anything. `before` and `after`
// are not symmetric -- `after` starts from `before`'s end state -- so a probe
// can differ across the restart for reasons that have nothing to do with the
// restart (residue a cleanup failed to remove, an ordering effect, a value that
// is simply not deterministic). The control run reproduces exactly that
// asymmetry with the restart REMOVED. Anything that moves in the control is a
// defect of the probe, and is reported as INCONCLUSIVE rather than as a
// violation of the invariant.
//
// Five further properties are load-bearing:
//
//   1. PROBES MUST WRITE, not only read. A row inserted before the restart
//      carries its DEFAULT in the WAL/checkpoint payload, so it reads back
//      correctly no matter how badly the schema was restored. The loss is only
//      observable on a write issued AFTER the restart. Read-only persistence
//      tests are structurally blind to it. A probe that writes must also CLEAN
//      UP, or its row inflates every later unkeyed read in the next phase.
//
//   2. `after == before` IS NOT ENOUGH. It is satisfied by being equally broken
//      on both sides. A read may therefore carry an `expect`: an absolute oracle
//      checked against the PRE-restart observation. A mismatch is BASELINE_WRONG
//      -- the value was already wrong before any restart, so its survival proves
//      nothing.
//
//   3. A GROUP WHOSE SETUP FAILED TESTED NOTHING. Setup runs pre-restart only,
//      so a CREATE/INSERT that silently ERRs leaves both phases agreeing on the
//      same empty table -- and the group passes. Any setup statement that
//      returns an error fails the group, unless it is listed in
//      known_broken_setup (which keeps the engine gap reviewed instead of
//      silent).
//
//   4. "THE DATABASE DID NOT REOPEN" IS AN OBSERVATION, NOT A CRASH. Each phase
//      runs in a forked child. A fatal signal, an exception out of the
//      constructor, or a HANG (the child arms an alarm) is recorded and diffed
//      like any other result.
//
//   5. A CRASH RESTART MUST BE A REAL CRASH. ~base_otterbrix_t runs a full
//      CHECKPOINT on every clean shutdown, so a "disk, no explicit CHECKPOINT"
//      run is NOT a no-checkpoint control. The crash flavor _exit()s from inside
//      the live scope, so no destructor -- and therefore no checkpoint -- runs.
// ===========================================================================

#include "test_config.hpp"

#include <spdlog/spdlog.h>

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace restart_rc {

    // ---------------------------------------------------------------------
    // Storage mode and restart flavor
    // ---------------------------------------------------------------------

    enum class storage_mode
    {
        in_memory,      // default table storage; durability via WAL replay only
        disk,           // WITH (storage = 'disk'); checkpoint only on clean shutdown
        disk_checkpoint // WITH (storage = 'disk') + an explicit CHECKPOINT after setup
    };

    enum class restart_flavor
    {
        clean, // scopes unwind: ~base_otterbrix_t runs a full checkpoint
        crash  // _exit() from inside the live scope: no destructor, no checkpoint
    };

    inline const char* to_string(storage_mode m) {
        switch (m) {
            case storage_mode::in_memory:
                return "mem";
            case storage_mode::disk:
                return "disk";
            case storage_mode::disk_checkpoint:
                return "diskck";
        }
        return "?";
    }

    inline const char* to_string(restart_flavor f) { return f == restart_flavor::clean ? "clean" : "crash"; }

    // The CREATE TABLE suffix that selects the storage mode.
    inline std::string storage_clause(storage_mode m) {
        return m == storage_mode::in_memory ? std::string{} : std::string{" WITH (storage = 'disk')"};
    }

    // ---------------------------------------------------------------------
    // Probes and groups
    // ---------------------------------------------------------------------

    struct read_t {
        std::string name;
        std::string sql;
        // Optional ABSOLUTE oracle for the PRE-restart observation, written in
        // normalized form ("$K+0", never the literal key). Empty means the read is
        // differential only. Without an oracle, `after == before` certifies a value
        // that was already wrong before the restart.
        std::string expect;

        read_t(std::string read_name, std::string read_sql, std::string expected = {})
            : name(std::move(read_name))
            , sql(std::move(read_sql))
            , expect(std::move(expected)) {}
    };

    struct probe_t {
        std::string name;                // prefixes every observation this probe makes
        std::vector<std::string> writes; // run first; each one is itself observed
        std::vector<read_t> reads;       // the observations that are compared
        // Statements that undo `writes`, so the table is back in its setup state when
        // the next probe runs. A probe that writes MUST clean up after itself, or its
        // row is still there in the next phase and every later unkeyed read (a COUNT,
        // a full scan) sees one extra row -- a divergence the harness manufactured.
        std::vector<std::string> cleanup;
    };

    struct group_t {
        std::string name;
        // Session-scoped statements (SET TIMEZONE, ...). Re-issued at the top of EVERY
        // phase, because a session does not survive a restart -- only what the setting
        // persisted into the catalog does. Putting these in `setup` would test nothing:
        // the control run would keep them in-process while a real reopen loses them.
        std::vector<std::string> session_setup;
        // DDL+DML, run once, pre-restart, on a fresh db. Each statement runs on its own
        // session (autocommit). An entry prefixed "@txn " runs on ONE session shared by
        // every @txn entry of the group — transactions are keyed by session id, so this
        // is how a BEGIN/.../ROLLBACK or BEGIN/.../COMMIT sequence is expressed.
        std::vector<std::string> setup;
        std::vector<probe_t> probes;    // run identically in every phase
        // Setup SQL this engine cannot execute today. Listed here, a failure is a known
        // gap rather than a silent pass; not listed, it fails the group.
        std::vector<std::string> known_broken_setup;
    };

    // ---------------------------------------------------------------------
    // Canonicalization
    //
    // A row renders as its values AND their reported types, so losing a column
    // type across the restart (BLOB -> UNKNOWN) diverges exactly like losing a
    // value does. NULL renders distinctly from any value, so a validity bitmap
    // that comes back all-valid diverges too.
    // ---------------------------------------------------------------------

    inline std::string type_tag(components::types::logical_type t) {
        using lt = components::types::logical_type;
        switch (t) {
            case lt::NA:
                return "NA";
            case lt::BOOLEAN:
                return "BOOL";
            case lt::TINYINT:
                return "I8";
            case lt::SMALLINT:
                return "I16";
            case lt::INTEGER:
                return "I32";
            case lt::BIGINT:
                return "I64";
            case lt::HUGEINT:
                return "I128";
            case lt::UTINYINT:
                return "U8";
            case lt::USMALLINT:
                return "U16";
            case lt::UINTEGER:
                return "U32";
            case lt::UBIGINT:
                return "U64";
            case lt::UHUGEINT:
                return "U128";
            case lt::FLOAT:
                return "F32";
            case lt::DOUBLE:
                return "F64";
            case lt::DECIMAL:
                return "DEC";
            case lt::DATE:
                return "DATE";
            case lt::TIME:
                return "TIME";
            case lt::TIME_TZ:
                return "TIMETZ";
            case lt::TIMESTAMP:
                return "TS";
            case lt::TIMESTAMP_TZ:
                return "TSTZ";
            case lt::INTERVAL:
                return "IVL";
            case lt::BLOB:
                return "BLOB";
            case lt::BIT:
                return "BIT";
            case lt::UUID:
                return "UUID";
            case lt::STRING_LITERAL:
                return "STR";
            case lt::INTEGER_LITERAL:
                return "ILIT";
            case lt::STRUCT:
                return "STRUCT";
            case lt::LIST:
                return "LIST";
            case lt::MAP:
                return "MAP";
            case lt::ARRAY:
                return "ARRAY";
            case lt::ENUM:
                return "ENUM";
            case lt::UNKNOWN:
                return "UNKNOWN";
            case lt::INVALID:
                return "INVALID";
            default:
                break;
        }
        return "T" + std::to_string(static_cast<int>(t));
    }

    inline std::string to_string_i128(components::types::int128_t v) {
        if (v == 0) {
            return "0";
        }
        const bool negative = v < 0;
        auto magnitude = static_cast<components::types::uint128_t>(negative ? -v : v);
        std::string digits;
        while (magnitude != 0) {
            digits += static_cast<char>('0' + static_cast<int>(magnitude % 10));
            magnitude /= 10;
        }
        if (negative) {
            digits += '-';
        }
        std::reverse(digits.begin(), digits.end());
        return digits;
    }

    // Render one cell as <TYPE>:<value>, or <TYPE>:NULL. Dispatches only on the
    // bare logical_type enum, never on the type's extension -- a restart can leave
    // the extension type-confused (set_alias on a null extension allocates a
    // GENERIC one), so a blind downcast here would fault inside the test harness.
    //
    // `declared` is the column's type as the cursor reports it. It is what tags a
    // NULL cell: the value of a NULL row carries no type of its own (it comes back
    // as NA), so without the declared type a column that decayed to UNKNOWN across
    // the restart would render identically to one that did not.
    inline std::string render_value(const components::types::logical_value_t& v,
                                    const components::types::logical_type* declared = nullptr) {
        using lt = components::types::logical_type;
        const auto t = v.type().type();
        const std::string tag = type_tag(declared != nullptr && t == lt::NA ? *declared : t);
        if (v.is_null()) {
            return tag + ":NULL";
        }
        std::ostringstream os;
        os << tag << ':';
        switch (t) {
            case lt::BOOLEAN:
                os << (v.value<bool>() ? "true" : "false");
                break;
            case lt::TINYINT:
                os << static_cast<int64_t>(v.value<int8_t>());
                break;
            case lt::SMALLINT:
                os << static_cast<int64_t>(v.value<int16_t>());
                break;
            case lt::INTEGER:
                os << static_cast<int64_t>(v.value<int32_t>());
                break;
            case lt::BIGINT:
            case lt::INTEGER_LITERAL:
                os << v.value<int64_t>();
                break;
            case lt::HUGEINT:
                os << to_string_i128(v.value<components::types::int128_t>());
                break;
            case lt::DECIMAL: {
                // The scale lives in the type extension, which a v1 .otbx reload
                // corrupts -- so read it only if it is there, and never downcast blind.
                // The raw scaled payload alone would make a lost scale invisible.
                os << v.value<int64_t>() << '@';
                const auto* extension = v.type().extension();
                if (extension != nullptr) {
                    os << static_cast<int>(
                        static_cast<const components::types::decimal_logical_type_extension*>(extension)->scale());
                } else {
                    os << '?';
                }
                break;
            }
            case lt::UTINYINT:
                os << static_cast<uint64_t>(v.value<uint8_t>());
                break;
            case lt::USMALLINT:
                os << static_cast<uint64_t>(v.value<uint16_t>());
                break;
            case lt::UINTEGER:
                os << static_cast<uint64_t>(v.value<uint32_t>());
                break;
            case lt::UBIGINT:
                os << v.value<uint64_t>();
                break;
            case lt::FLOAT:
                os << std::to_string(v.value<float>());
                break;
            case lt::DOUBLE:
                os << std::to_string(v.value<double>());
                break;
            case lt::STRING_LITERAL:
            case lt::BLOB:
                os << '"' << v.value<std::string_view>() << '"';
                break;
            case lt::DATE:
                os << v.value<core::date::date_t>().value;
                break;
            case lt::TIMESTAMP:
                os << v.value<core::date::timestamp_t>().value;
                break;
            case lt::TIMESTAMP_TZ:
                os << v.value<core::date::timestamptz_t>().value;
                break;
            case lt::TIME:
                os << v.value<core::date::time_t>().value;
                break;
            case lt::ARRAY:
            case lt::LIST:
            case lt::STRUCT:
            case lt::MAP: {
                // The element COUNT is part of the render, so a lost ARRAY fixed size
                // diverges; the recursion renders each element's type, so a lost child
                // type diverges too. ENUM is deliberately NOT routed here: it stores a
                // scalar, and children() is null-guarded but not type-guarded.
                const auto& children = v.children();
                os << '{' << children.size() << ':';
                for (std::size_t i = 0; i < children.size(); ++i) {
                    if (i != 0) {
                        os << ',';
                    }
                    os << render_value(children[i]);
                }
                os << '}';
                break;
            }
            default:
                os << '?';
                break;
        }
        return os.str();
    }

    inline std::string sanitize(std::string s) {
        for (auto& c : s) {
            if (c == '\n' || c == '\r' || c == '\t') {
                c = ' ';
            }
        }
        if (s.size() > 800) {
            s.resize(800);
            s += "...";
        }
        return s;
    }

    // Run one statement and canonicalize its outcome.
    //
    //   ERR|<message>                         a failing statement (text compared verbatim)
    //   OK|rows=N|cols=M|<row>|<row>|...      a succeeding one, rows sorted so that
    //                                         physical order -- which legitimately differs
    //                                         between a WAL-replayed and an .otbx-loaded
    //                                         table -- is not compared
    //
    // `session` lets several statements share one session, which is how a transaction
    // is expressed (transactions are keyed by session id). Passing nullptr mints a
    // fresh session, i.e. autocommit.
    inline std::string observe_raw(otterbrix::wrapper_dispatcher_t* dispatcher,
                                   const std::string& sql,
                                   const otterbrix::session_id_t* session = nullptr) {
        otterbrix::session_id_t own_session;
        auto cur = dispatcher->execute_sql(session != nullptr ? *session : own_session, sql);
        if (!cur) {
            return "ERR|<null cursor>";
        }
        if (cur->is_error()) {
            return "ERR|" + sanitize(std::string(cur->get_error().what.c_str()));
        }
        const std::size_t rows = cur->size();
        const std::size_t cols = cur->column_count();
        const auto& column_types = cur->type_data();
        std::vector<std::string> rendered;
        rendered.reserve(rows);
        for (std::size_t r = 0; r < rows; ++r) {
            std::string line;
            for (std::size_t c = 0; c < cols; ++c) {
                if (c != 0) {
                    line += ',';
                }
                const components::types::logical_type declared =
                    c < column_types.size() ? column_types[c].type() : components::types::logical_type::NA;
                line += render_value(cur->value(c, r), &declared);
            }
            rendered.push_back(std::move(line));
        }
        std::sort(rendered.begin(), rendered.end());
        std::ostringstream os;
        os << "OK|rows=" << rows << "|cols=" << cols;
        for (const auto& row : rendered) {
            os << "|[" << row << ']';
        }
        return sanitize(os.str());
    }

    // ---------------------------------------------------------------------
    // Phase keys
    //
    // The two phases write different rows on purpose, so a post-restart write
    // cannot collide with the row its pre-restart twin left behind. The key is
    // then folded back out of the observation, so a probe may freely SELECT it.
    // ---------------------------------------------------------------------

    constexpr int key_window = 100; // a probe may use $K .. $K+99
    constexpr int key_pre = 700100;
    constexpr int key_post = 800100; // NOT key_pre + key_window: adjacent windows would
                                     // make a probe's `$K + 99` collide with the other
                                     // phase's own key on the shared data directory.
    static_assert(key_post - key_pre >= key_window, "the two key windows must not overlap");

    inline std::string replace_all(std::string s, std::string_view token, const std::string& with) {
        for (std::size_t pos = s.find(token); pos != std::string::npos; pos = s.find(token, pos + with.size())) {
            s.replace(pos, token.size(), with);
        }
        return s;
    }

    // $K -> the phase-unique key; $S -> the storage clause for this mode. A group is
    // therefore written once and runs unchanged in every storage mode.
    inline std::string expand(const std::string& sql, int key, storage_mode mode) {
        return replace_all(replace_all(sql, "$S", storage_clause(mode)), "$K", std::to_string(key));
    }

    // Fold BOTH phases' key windows, in BOTH phases, matching only WHOLE digit runs.
    //
    //   both windows: a data value that coincidentally lands in a key window (a
    //     timestamp's raw epoch, a bigint literal) is then folded IDENTICALLY on both
    //     sides and cancels out, instead of being folded in one phase only -- which
    //     would manufacture a divergence.
    //   whole runs:   1700100 must not become 1$K+0. Substring replacement would.
    inline std::string normalize_keys(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        std::size_t i = 0;
        while (i < s.size()) {
            if (std::isdigit(static_cast<unsigned char>(s[i])) == 0) {
                out += s[i++];
                continue;
            }
            std::size_t j = i;
            while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j])) != 0) {
                ++j;
            }
            const std::string run = s.substr(i, j - i);
            bool folded = false;
            if (run.size() <= 18) {
                const long long value = std::stoll(run);
                for (const int base : {key_pre, key_post}) {
                    if (value >= base && value < base + key_window) {
                        out += "$K+" + std::to_string(value - base);
                        folded = true;
                        break;
                    }
                }
            }
            if (!folded) {
                out += run;
            }
            i = j;
        }
        return out;
    }

    inline std::string observe(otterbrix::wrapper_dispatcher_t* dispatcher,
                               const std::string& sql,
                               int key,
                               storage_mode mode,
                               const otterbrix::session_id_t* session = nullptr) {
        return normalize_keys(observe_raw(dispatcher, expand(sql, key, mode), session));
    }

    // Where a group's data directory lives. Deliberately NOT under /tmp: on many
    // machines that is a small tmpfs shared with other work, and a battery that fills
    // it produces phantom "database did not reopen" results -- and takes unrelated
    // processes down with it.
    inline std::filesystem::path data_root() {
        if (const char* env = std::getenv("RC_DATA_ROOT")) {
            return std::filesystem::path(env);
        }
        return std::filesystem::path("/var/tmp/otterbrix/restart_consistency");
    }

    // A hung reopen is one of the four ways this database fails to come back, and
    // without a deadline it wedges the whole suite instead of reporting. Generous by
    // design: a timeout firing on an honest-but-slow phase would be reported as a
    // database that did not reopen, i.e. a fabricated bug.
    inline unsigned phase_timeout_seconds() {
        if (const char* env = std::getenv("RC_PHASE_TIMEOUT")) {
            const long v = std::strtol(env, nullptr, 10);
            if (v > 0) {
                return static_cast<unsigned>(v);
            }
        }
        return 180;
    }

    // ---------------------------------------------------------------------
    // Running a phase in a child process
    // ---------------------------------------------------------------------

    struct phase_result {
        bool opened{false};
        std::string failure;         // set when `opened` is false
        std::string shutdown_signal; // opened and answered, then died on the way out
        std::vector<std::pair<std::string, std::string>> observations;
    };

    namespace detail {

        // fork() preserves signal dispositions. Catch2 keeps SIGSEGV/SIGBUS/SIGABRT
        // engaged for the whole test body, so without this a child that crashes -- the
        // DESIGNED outcome for a group whose database will not reopen -- runs Catch2's
        // fatal-signal handler and writes a spurious failure report for the PARENT's
        // test case onto the shared output stream.
        inline void detach_from_catch2_signal_handlers() {
            for (const int sig : {SIGINT, SIGILL, SIGFPE, SIGSEGV, SIGBUS, SIGTERM, SIGABRT}) {
                ::signal(sig, SIG_DFL);
            }
            stack_t disable{};
            disable.ss_flags = SS_DISABLE;
            ::sigaltstack(&disable, nullptr);
        }

        // The parent must be single-threaded at fork(). The logger leaves spdlog's
        // periodic flush thread running in any process that has built a test_spaces --
        // i.e. this Catch2 parent, as soon as any earlier test case in the binary did --
        // and nothing ever shuts it down. Forking while that thread holds the sink mutex
        // gives the child a locked mutex with no owner, and the child hangs forever.
        // Setting a zero interval destroys the worker here, in the parent, while the
        // thread it belongs to still exists to be joined.
        inline void quiesce_parent_before_fork() {
            spdlog::flush_every(std::chrono::seconds(0));
            std::cout.flush();
            std::cerr.flush();
            std::fflush(nullptr); // the child must not re-emit the parent's buffered stdio
        }

        // Streaming writer: each observation is flushed as it is produced, so a child
        // that dies inside a probe -- or inside the shutdown checkpoint, after answering
        // everything -- still leaves behind everything it managed to collect.
        class result_sink {
        public:
            explicit result_sink(const std::string& path)
                : out_(path, std::ios::trunc) {}

            void emit(const std::string& name, const std::string& value) {
                out_ << name << '\t' << value << '\n';
                out_.flush();
            }

            void fail(const std::string& what) {
                out_ << '\t' << "__OPEN_FAILED__\t" << sanitize(what) << '\n';
                out_.flush();
            }

        private:
            std::ofstream out_;
        };

        // Run every probe once, at `key`, emitting each observation as it is produced.
        inline void run_probes(otterbrix::wrapper_dispatcher_t* dispatcher,
                               const group_t& group,
                               int key,
                               storage_mode mode,
                               result_sink& sink) {
            for (const auto& p : group.probes) {
                for (std::size_t i = 0; i < p.writes.size(); ++i) {
                    sink.emit(p.name + ".write" + std::to_string(i), observe(dispatcher, p.writes[i], key, mode));
                }
                for (const auto& r : p.reads) {
                    sink.emit(p.name + '.' + r.name, observe(dispatcher, r.sql, key, mode));
                }
                for (std::size_t i = 0; i < p.cleanup.size(); ++i) {
                    sink.emit(p.name + ".cleanup" + std::to_string(i), observe(dispatcher, p.cleanup[i], key, mode));
                }
            }
        }

        inline void run_session_setup(otterbrix::wrapper_dispatcher_t* dispatcher,
                                      const group_t& group,
                                      int key,
                                      storage_mode mode,
                                      result_sink& sink) {
            for (const auto& sql : group.session_setup) {
                sink.emit("session: " + sanitize(expand(sql, key, mode)), observe(dispatcher, sql, key, mode));
            }
        }

        inline void run_setup(otterbrix::wrapper_dispatcher_t* dispatcher,
                              const group_t& group,
                              int key,
                              storage_mode mode,
                              result_sink& sink) {
            const otterbrix::session_id_t txn_session; // shared by every "@txn " entry
            for (const auto& sql : group.setup) {
                constexpr std::string_view txn_prefix = "@txn ";
                if (sql.rfind(txn_prefix, 0) == 0) {
                    const auto stripped = sql.substr(txn_prefix.size());
                    sink.emit("setup: " + sanitize(expand(stripped, key, mode)),
                              observe(dispatcher, stripped, key, mode, &txn_session));
                } else {
                    sink.emit("setup: " + sanitize(expand(sql, key, mode)), observe(dispatcher, sql, key, mode));
                }
            }
            if (mode == storage_mode::disk_checkpoint) {
                sink.emit("setup: CHECKPOINT", observe(dispatcher, "CHECKPOINT;", key, mode));
            }
        }

        // One phase of the restart run. Never returns.
        [[noreturn]] inline void run_phase_child(const group_t& group,
                                                 const configuration::config& config,
                                                 storage_mode mode,
                                                 restart_flavor flavor,
                                                 bool is_setup_phase,
                                                 const std::string& out_path) {
            detach_from_catch2_signal_handlers();
            ::alarm(phase_timeout_seconds());
            result_sink sink(out_path);
            try {
                // Everything that talks to the database lives in this scope. On a clean
                // restart the scope exits normally and ~base_otterbrix_t runs its
                // checkpoint; on a crash restart we _exit() while still inside it, so no
                // destructor runs at all.
                {
                    test_spaces space(config);
                    auto* dispatcher = space.dispatcher();
                    const int key = is_setup_phase ? key_pre : key_post;

                    run_session_setup(dispatcher, group, key, mode, sink);
                    if (is_setup_phase) {
                        run_setup(dispatcher, group, key, mode, sink);
                    }
                    run_probes(dispatcher, group, key, mode, sink);

                    if (is_setup_phase && flavor == restart_flavor::crash) {
                        // No unwinding: no checkpoint, no clean WAL seal. This is the state
                        // a kill -9 or a power loss leaves behind.
                        _exit(0);
                    }
                }
            } catch (const std::exception& e) {
                sink.fail(std::string("exception: ") + e.what());
                _exit(0);
            } catch (...) {
                sink.fail("exception: <unknown>");
                _exit(0);
            }
            _exit(0);
        }

        // THE CONTROL: one process, one database, NO restart. Setup, probes at key_pre,
        // then the same probes again at key_post. Any observation that moves between the
        // two runs moves without a restart being involved at all -- it is a defect of the
        // probe (residue, ordering, non-determinism), not of the database. Without this,
        // every such artifact is indistinguishable from a restart bug.
        [[noreturn]] inline void run_control_child(const group_t& group,
                                                   const configuration::config& config,
                                                   storage_mode mode,
                                                   const std::string& out_first,
                                                   const std::string& out_second) {
            detach_from_catch2_signal_handlers();
            ::alarm(phase_timeout_seconds());
            try {
                test_spaces space(config);
                auto* dispatcher = space.dispatcher();
                {
                    result_sink sink(out_first);
                    run_session_setup(dispatcher, group, key_pre, mode, sink);
                    run_setup(dispatcher, group, key_pre, mode, sink);
                    run_probes(dispatcher, group, key_pre, mode, sink);
                }
                {
                    result_sink sink(out_second);
                    run_session_setup(dispatcher, group, key_post, mode, sink);
                    run_probes(dispatcher, group, key_post, mode, sink);
                }
            } catch (const std::exception& e) {
                result_sink(out_second).fail(std::string("exception: ") + e.what());
                _exit(0);
            } catch (...) {
                result_sink(out_second).fail("exception: <unknown>");
                _exit(0);
            }
            _exit(0);
        }

        inline phase_result read_results(const std::string& path, int wait_status) {
            phase_result result;
            std::ifstream in(path);
            const bool had_file = static_cast<bool>(in);
            if (in) {
                std::string line;
                while (std::getline(in, line)) {
                    const auto tab = line.find('\t');
                    if (tab == std::string::npos) {
                        continue;
                    }
                    const std::string name = line.substr(0, tab);
                    const std::string value = line.substr(tab + 1);
                    if (name.empty() && value.rfind("__OPEN_FAILED__\t", 0) == 0) {
                        result.failure = value.substr(std::strlen("__OPEN_FAILED__\t"));
                        return result;
                    }
                    result.observations.emplace_back(name, value);
                }
            }
            if (WIFSIGNALED(wait_status)) {
                const int sig = WTERMSIG(wait_status);
                const std::string described =
                    sig == SIGALRM ? "timeout: phase did not finish in " + std::to_string(phase_timeout_seconds()) +
                                         "s (hung)"
                                   : std::string("signal: ") + strsignal(sig);
                if (result.observations.empty()) {
                    result.failure = described; // died during open
                    return result;
                }
                // It DID open and answer every probe, and then died on the way out. Keep
                // the evidence; report the crash separately.
                result.opened = true;
                result.shutdown_signal = described;
                return result;
            }
            if (result.observations.empty() && !had_file) {
                result.failure = "no results file (child produced nothing)";
                return result;
            }
            result.opened = true;
            return result;
        }

        inline int wait_for(pid_t pid) {
            int status = 0;
            while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
            }
            return status;
        }

        inline phase_result run_phase(const group_t& group,
                                      const configuration::config& config,
                                      storage_mode mode,
                                      restart_flavor flavor,
                                      bool is_setup_phase,
                                      const std::string& out_path) {
            std::remove(out_path.c_str());
            quiesce_parent_before_fork();
            const pid_t pid = ::fork();
            if (pid == 0) {
                run_phase_child(group, config, mode, flavor, is_setup_phase, out_path);
            }
            if (pid < 0) {
                phase_result result;
                result.failure = std::string("fork failed: ") + std::strerror(errno);
                return result;
            }
            return read_results(out_path, wait_for(pid));
        }

        struct control_result {
            bool opened{false};
            std::string failure;
            std::set<std::string> dirty; // observations that move with NO restart at all
        };

        // Cached per (group, mode) and reused across both flavors: the control does not
        // depend on how the database was terminated.
        inline const control_result& run_control(const group_t& group, storage_mode mode) {
            static std::map<std::string, control_result> cache;
            const std::string cache_key = group.name + "/" + to_string(mode);
            if (const auto it = cache.find(cache_key); it != cache.end()) {
                return it->second;
            }
            // Its OWN data directory: running it in the restart run's directory would
            // corrupt the very state under test.
            const auto dir = data_root() / "control" / group.name / to_string(mode);
            auto config = test_create_config(dir);
            test_clear_directory(config);
            const std::string out_first = (dir / "rc_control_1.tsv").string();
            const std::string out_second = (dir / "rc_control_2.tsv").string();
            std::remove(out_first.c_str());
            std::remove(out_second.c_str());

            control_result result;
            quiesce_parent_before_fork();
            const pid_t pid = ::fork();
            if (pid == 0) {
                run_control_child(group, config, mode, out_first, out_second);
            }
            if (pid < 0) {
                result.failure = std::string("fork failed: ") + std::strerror(errno);
                return cache.emplace(cache_key, std::move(result)).first->second;
            }
            const int status = wait_for(pid);
            const auto first = read_results(out_first, 0);
            const auto second = read_results(out_second, status);
            if (!first.opened || !second.opened) {
                // A control that dies cannot vindicate anything. The whole cell becomes
                // inconclusive rather than a violation of the invariant.
                result.failure = second.opened ? first.failure : second.failure;
                return cache.emplace(cache_key, std::move(result)).first->second;
            }
            std::map<std::string, std::string> second_by_name;
            for (const auto& [name, value] : second.observations) {
                second_by_name.emplace(name, value);
            }
            for (const auto& [name, value] : first.observations) {
                if (name.rfind("setup: ", 0) == 0 || name.rfind("session: ", 0) == 0) {
                    continue;
                }
                const auto it = second_by_name.find(name);
                if (it == second_by_name.end() || it->second != value) {
                    result.dirty.insert(name);
                }
            }
            result.opened = true;
            return cache.emplace(cache_key, std::move(result)).first->second;
        }

    } // namespace detail

    // ---------------------------------------------------------------------
    // The verdict
    // ---------------------------------------------------------------------

    struct divergence_t {
        std::string probe;
        std::string before;
        std::string after;
    };

    struct rc_report {
        bool reopened{false};
        std::string reopen_failure;
        std::string shutdown_failure;
        std::string inconclusive_cell;                                  // the control itself failed
        std::vector<std::pair<std::string, std::string>> setup_failures; // this group tested nothing
        std::vector<divergence_t> baseline_wrong;                        // wrong BEFORE any restart
        std::vector<divergence_t> divergences;                           // the actual violations
        std::vector<divergence_t> inconclusive;                          // move without a restart too
        std::size_t compared{0};

        bool consistent() const {
            return reopened && inconclusive_cell.empty() && shutdown_failure.empty() && setup_failures.empty() &&
                   baseline_wrong.empty() && divergences.empty();
        }

        std::string describe() const {
            std::ostringstream os;
            if (!inconclusive_cell.empty()) {
                os << "INCONCLUSIVE (the no-restart control failed): " << inconclusive_cell << '\n';
                return os.str();
            }
            if (!setup_failures.empty()) {
                os << setup_failures.size() << " SETUP STATEMENT(S) FAILED -- this group tested nothing:\n";
                for (const auto& [name, value] : setup_failures) {
                    os << "  " << name << "\n    " << value << '\n';
                }
            }
            if (!reopened) {
                os << "DATABASE DID NOT REOPEN: " << reopen_failure << '\n';
                return os.str();
            }
            if (!shutdown_failure.empty()) {
                os << "CRASHED DURING SHUTDOWN: " << shutdown_failure << '\n';
            }
            for (const auto& d : baseline_wrong) {
                os << "BASELINE WRONG (already incorrect BEFORE any restart, so its survival proves nothing):\n  "
                   << d.probe << "\n    expected: " << d.before << "\n    actual  : " << d.after << '\n';
            }
            os << divergences.size() << " of " << compared << " probes diverged across the restart:\n";
            for (const auto& d : divergences) {
                os << "  " << d.probe << "\n    before: " << d.before << "\n    after : " << d.after << '\n';
            }
            if (!inconclusive.empty()) {
                os << inconclusive.size()
                   << " probe(s) are NOT RESTART-CLEAN (they move with no restart at all -- a harness defect, "
                      "not a database bug):\n";
                for (const auto& d : inconclusive) {
                    os << "  " << d.probe << "\n    run1: " << d.before << "\n    run2: " << d.after << '\n';
                }
            }
            return os.str();
        }
    };

    // Run one group under one (mode, flavor) and report every divergence.
    inline rc_report check_group(const group_t& group, storage_mode mode, restart_flavor flavor) {
        rc_report report;

        const auto& control = detail::run_control(group, mode);
        if (!control.opened) {
            report.inconclusive_cell = control.failure;
            return report;
        }

        const auto dir = data_root() / group.name / to_string(mode) / to_string(flavor);
        // A full battery writes several gigabytes. Everything a reader needs is already
        // in describe(), which Catch2 prints, so keep the directory only on request.
        struct wipe_on_exit {
            std::filesystem::path dir;
            ~wipe_on_exit() {
                if (std::getenv("RC_KEEP") == nullptr) {
                    std::error_code ec;
                    std::filesystem::remove_all(dir, ec);
                }
            }
        } wipe{dir};

        auto config = test_create_config(dir);
        test_clear_directory(config);
        const std::string out_pre = (dir / "rc_pre.tsv").string();
        const std::string out_post = (dir / "rc_post.tsv").string();

        const auto before = detail::run_phase(group, config, mode, flavor, /*is_setup_phase=*/true, out_pre);
        if (!before.opened) {
            // The pre-restart phase failed. That is a bug, but not a RESTART bug.
            report.reopen_failure = "pre-restart phase failed: " + before.failure;
            return report;
        }

        // A group whose setup silently errored leaves both phases agreeing on the same
        // broken state, and would otherwise pass while testing nothing.
        std::set<std::string> allowed_broken;
        for (const auto& sql : group.known_broken_setup) {
            allowed_broken.insert(sanitize(expand(sql, key_pre, mode)));
        }
        for (const auto& [name, value] : before.observations) {
            const bool is_setup = name.rfind("setup: ", 0) == 0 || name.rfind("session: ", 0) == 0;
            if (!is_setup || value.rfind("ERR|", 0) != 0) {
                continue;
            }
            const auto colon = name.find(' ');
            const std::string sql = colon == std::string::npos ? name : name.substr(colon + 1);
            if (allowed_broken.count(sql) == 0) {
                report.setup_failures.emplace_back(name, value);
            }
        }

        const auto after = detail::run_phase(group, config, mode, flavor, /*is_setup_phase=*/false, out_post);
        if (!after.opened) {
            report.reopen_failure = after.failure;
            return report;
        }
        report.reopened = true;
        report.shutdown_failure = after.shutdown_signal;
        if (!before.shutdown_signal.empty()) {
            report.shutdown_failure += " (the pre-restart phase also crashed during shutdown: " +
                                       before.shutdown_signal + ")";
        }

        // The absolute oracles, checked against the PRE-restart observation.
        std::map<std::string, std::string> expected;
        for (const auto& p : group.probes) {
            for (const auto& r : p.reads) {
                if (!r.expect.empty()) {
                    expected.emplace(p.name + '.' + r.name, r.expect);
                }
            }
        }

        std::map<std::string, std::string> after_by_name;
        for (const auto& [name, value] : after.observations) {
            after_by_name.emplace(name, value);
        }
        for (const auto& [name, value] : before.observations) {
            if (name.rfind("setup: ", 0) == 0 || name.rfind("session: ", 0) == 0) {
                continue; // setup has no post-restart twin
            }
            if (const auto e = expected.find(name); e != expected.end() && e->second != value) {
                report.baseline_wrong.push_back({name, e->second, value});
            }
            ++report.compared;
            const auto it = after_by_name.find(name);
            const std::string after_value =
                it == after_by_name.end() ? std::string("<probe missing after restart>") : it->second;
            if (after_value == value) {
                continue;
            }
            // Report, never drop: a real restart bug hiding behind a dirty probe must
            // still be visible -- as a harness defect rather than as a green.
            if (control.dirty.count(name) != 0) {
                report.inconclusive.push_back({name, value, after_value});
            } else {
                report.divergences.push_back({name, value, after_value});
            }
        }
        return report;
    }

} // namespace restart_rc
