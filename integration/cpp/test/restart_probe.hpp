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
// The harness realizes that by running an identical PROBE list twice against
// the same data directory -- once before the restart and once after -- and
// diffing the two observation vectors probe by probe.
//
// Four properties of this design are load-bearing; each of them was learned by
// getting it wrong first:
//
//   1. PROBES MUST WRITE, not only read.
//      A row inserted before the restart carries its DEFAULT in the WAL/
//      checkpoint payload, so it reads back correctly no matter how badly the
//      schema was restored. The loss is only observable on a write issued
//      AFTER the restart. Read-only persistence tests are structurally blind
//      to it. Every probe may therefore carry `writes`, which run in both
//      phases, keyed by a phase-unique $K so the two phases cannot collide.
//
//   2. EVERY GROUP RUNS IN ITS OWN PROCESS, ON ITS OWN DATA DIRECTORY.
//      Several restart bugs make the database permanently unopenable. In a
//      shared process one poisoned group takes the whole battery down with it
//      and every other result is lost.
//
//   3. "THE DATABASE DID NOT REOPEN" IS AN OBSERVATION, NOT A CRASH.
//      The reopen happens in a forked child. If it dies of SIGSEGV/SIGBUS, or
//      throws out of the constructor, the parent records RESTART_FAIL(...) and
//      diffs it like any other observation.
//
//   4. A CRASH RESTART MUST BE A REAL CRASH.
//      ~base_otterbrix_t runs a full CHECKPOINT on every clean shutdown, so a
//      "disk, no explicit CHECKPOINT" run is NOT a no-checkpoint control. The
//      crash flavor calls _exit() from inside the live scope, so no destructor
//      -- and therefore no checkpoint -- ever runs.
//
// An observation is canonicalized to a single line, so a divergence in row
// count, row content, NULL-ness, reported column type, or error text all show
// up as the same kind of string inequality.
// ===========================================================================

#include "test_config.hpp"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
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

    inline const char* to_string(restart_flavor f) {
        return f == restart_flavor::clean ? "clean" : "crash";
    }

    // The CREATE TABLE suffix that selects the storage mode.
    inline std::string storage_clause(storage_mode m) {
        return m == storage_mode::in_memory ? std::string{} : std::string{" WITH (storage = 'disk')"};
    }

    // ---------------------------------------------------------------------
    // Probes and groups
    // ---------------------------------------------------------------------

    // One observation. `writes` (optional) run first and are themselves observed
    // -- an INSERT that silently reports success with 0 rows is a divergence we
    // must see. `read` is the query whose canonicalized result is compared.
    //
    // Any occurrence of $K in either is replaced by a phase-unique key, so the
    // post-restart phase writes different rows than the pre-restart phase did
    // and the two can never collide on the same data directory.
    using named_sql = std::pair<std::string, std::string>; // observation name -> SQL

    struct probe_t {
        std::string name;                // prefixes every observation this probe makes
        std::vector<std::string> writes; // run first; each one is itself observed
        std::vector<named_sql> reads;    // the observations that are compared
        // Statements that undo `writes`, so the table is back in its setup state
        // when the next probe runs. Without this, a probe's pre-restart row is
        // still there in the post-restart phase and every later unkeyed read (a
        // COUNT, a full scan) sees one extra row -- a divergence the harness
        // itself manufactured. A probe that writes MUST clean up after itself.
        std::vector<std::string> cleanup;
    };

    // One table (or small set of tables) plus the probes over it. A group owns
    // its own data directory and its own process, so a group that bricks the
    // database cannot hide a divergence in any other group.
    struct group_t {
        std::string name;
        std::vector<std::string> setup; // runs once, pre-restart, on a fresh database
        std::vector<probe_t> probes;    // run identically in both phases
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

    // Render one cell as <TYPE>:<value>, or <TYPE>:NULL. Dispatches only on the
    // bare logical_type enum -- never on the type's extension, which is exactly
    // the thing a restart can leave in a type-confused state.
    inline std::string render_value(const components::types::logical_value_t& v) {
        using lt = components::types::logical_type;
        const auto t = v.type().type();
        const std::string tag = type_tag(t);
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
            // A DECIMAL renders as its raw scaled integer payload: reading its
            // scale would mean touching the type extension, which is precisely
            // what a v1 .otbx reload corrupts. The raw payload is enough to see
            // a divergence, and the value probes (WHERE price = 12.34) cover the
            // scale itself.
            case lt::DECIMAL:
            case lt::INTEGER_LITERAL:
                os << v.value<int64_t>();
                break;
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
        if (s.size() > 400) {
            s.resize(400);
            s += "...";
        }
        return s;
    }

    // Run one statement and canonicalize its outcome.
    //
    //   ERR|<message>                         a failing statement (text compared verbatim)
    //   OK|rows=N|cols=M|<row>|<row>|...      a succeeding one, rows sorted so that
    //                                         physical order -- which legitimately
    //                                         differs between a WAL-replayed and an
    //                                         .otbx-loaded table -- is not compared
    inline std::string observe_raw(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        if (!cur) {
            return "ERR|<null cursor>";
        }
        if (cur->is_error()) {
            return "ERR|" + sanitize(std::string(cur->get_error().what.c_str()));
        }
        const std::size_t rows = cur->size();
        const std::size_t cols = cur->column_count();
        std::vector<std::string> rendered;
        rendered.reserve(rows);
        for (std::size_t r = 0; r < rows; ++r) {
            std::string line;
            for (std::size_t c = 0; c < cols; ++c) {
                if (c) {
                    line += ',';
                }
                line += render_value(cur->value(c, r));
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

    inline std::string replace_all(std::string s, std::string_view token, const std::string& with) {
        for (std::size_t pos = s.find(token); pos != std::string::npos; pos = s.find(token, pos + with.size())) {
            s.replace(pos, token.size(), with);
        }
        return s;
    }

    // $K -> the phase-unique key; $S -> the storage clause for this mode. A group
    // is therefore written once and runs unchanged in every storage mode.
    inline std::string expand(const std::string& sql, int key, storage_mode mode) {
        return replace_all(replace_all(sql, "$S", storage_clause(mode)), "$K", std::to_string(key));
    }

    // The phase key is different in the two phases BY CONSTRUCTION -- that is how a
    // post-restart write is kept from colliding with the row its pre-restart twin
    // left behind. So a probe that selects the key column would "diverge" on every
    // run. Fold the key back out of the observation before comparing: a row keyed
    // $K+3 renders identically in both phases.
    inline std::string normalize_key(std::string observation, int key) {
        for (int offset = 0; offset < 100; ++offset) {
            observation =
                replace_all(std::move(observation), std::to_string(key + offset), "$K+" + std::to_string(offset));
        }
        return observation;
    }

    inline std::string
    observe(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, int key, storage_mode mode) {
        return normalize_key(observe_raw(dispatcher, expand(sql, key, mode)), key);
    }

    // ---------------------------------------------------------------------
    // The phase runner (child process)
    // ---------------------------------------------------------------------

    // Keys are phase-unique so a probe's post-restart write cannot collide with
    // the row its pre-restart write left behind.
    constexpr int key_pre = 700100;
    constexpr int key_post = 700200;

    struct phase_result {
        bool opened{false};
        std::string failure; // set when `opened` is false
        std::vector<std::pair<std::string, std::string>> observations;
    };

    namespace detail {

        inline void write_results(const std::string& path,
                                  const std::vector<std::pair<std::string, std::string>>& obs) {
            std::ofstream out(path, std::ios::trunc);
            for (const auto& [name, value] : obs) {
                out << name << '\t' << value << '\n';
            }
            out.flush();
        }

        inline void write_failure(const std::string& path, const std::string& what) {
            std::ofstream out(path, std::ios::trunc);
            out << "\t__OPEN_FAILED__\t" << sanitize(what) << '\n';
            out.flush();
        }

        // Runs inside the forked child. Never returns.
        [[noreturn]] inline void run_phase_child(const group_t& group,
                                                 const configuration::config& config,
                                                 storage_mode mode,
                                                 restart_flavor flavor,
                                                 bool is_setup_phase,
                                                 const std::string& out_path) {
            std::vector<std::pair<std::string, std::string>> obs;
            try {
                // Everything that talks to the database lives in this scope. On a
                // clean restart the scope exits normally and ~base_otterbrix_t runs
                // its checkpoint; on a crash restart we _exit() while still inside
                // it, so no destructor runs at all.
                {
                    test_spaces space(config);
                    auto* dispatcher = space.dispatcher();

                    const int key = is_setup_phase ? key_pre : key_post;

                    if (is_setup_phase) {
                        for (const auto& sql : group.setup) {
                            obs.emplace_back("setup: " + sanitize(expand(sql, key, mode)),
                                             observe(dispatcher, sql, key, mode));
                        }
                        if (mode == storage_mode::disk_checkpoint) {
                            obs.emplace_back("setup: CHECKPOINT", observe(dispatcher, "CHECKPOINT;", key, mode));
                        }
                    }

                    for (const auto& p : group.probes) {
                        for (std::size_t i = 0; i < p.writes.size(); ++i) {
                            obs.emplace_back(p.name + ".write" + std::to_string(i),
                                             observe(dispatcher, p.writes[i], key, mode));
                        }
                        for (const auto& [read_name, sql] : p.reads) {
                            obs.emplace_back(p.name + '.' + read_name, observe(dispatcher, sql, key, mode));
                        }
                        for (std::size_t i = 0; i < p.cleanup.size(); ++i) {
                            obs.emplace_back(p.name + ".cleanup" + std::to_string(i),
                                             observe(dispatcher, p.cleanup[i], key, mode));
                        }
                    }

                    write_results(out_path, obs);

                    if (is_setup_phase && flavor == restart_flavor::crash) {
                        // No unwinding: no checkpoint, no clean WAL seal. This is the
                        // state a `kill -9` or a power loss leaves behind.
                        _exit(0);
                    }
                }
            } catch (const std::exception& e) {
                write_failure(out_path, std::string("exception: ") + e.what());
                _exit(0);
            } catch (...) {
                write_failure(out_path, "exception: <unknown>");
                _exit(0);
            }
            _exit(0);
        }

        inline phase_result read_results(const std::string& path, int wait_status) {
            phase_result result;
            if (WIFSIGNALED(wait_status)) {
                result.failure = std::string("signal: ") + strsignal(WTERMSIG(wait_status));
                return result;
            }
            std::ifstream in(path);
            if (!in) {
                result.failure = "no results file (child produced nothing)";
                return result;
            }
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
            result.opened = true;
            return result;
        }

        // Fork, run one phase in the child, collect its observations.
        inline phase_result run_phase(const group_t& group,
                                      const configuration::config& config,
                                      storage_mode mode,
                                      restart_flavor flavor,
                                      bool is_setup_phase,
                                      const std::string& out_path) {
            std::remove(out_path.c_str());
            const pid_t pid = ::fork();
            if (pid == 0) {
                run_phase_child(group, config, mode, flavor, is_setup_phase, out_path);
            }
            int status = 0;
            ::waitpid(pid, &status, 0);
            return read_results(out_path, status);
        }

    } // namespace detail

    // ---------------------------------------------------------------------
    // The diff
    // ---------------------------------------------------------------------

    struct divergence_t {
        std::string probe;
        std::string before;
        std::string after;
    };

    struct rc_report {
        bool reopened{false};
        std::string reopen_failure;
        std::vector<divergence_t> divergences;
        std::size_t compared{0};

        bool consistent() const { return reopened && divergences.empty(); }

        std::string describe() const {
            std::ostringstream os;
            if (!reopened) {
                os << "DATABASE DID NOT REOPEN: " << reopen_failure;
                return os.str();
            }
            os << divergences.size() << " of " << compared << " probes diverged across the restart:\n";
            for (const auto& d : divergences) {
                os << "  " << d.probe << "\n    before: " << d.before << "\n    after : " << d.after << '\n';
            }
            return os.str();
        }
    };

    // Where a group's data directory lives. Deliberately NOT under /tmp: on this
    // machine /tmp is a small tmpfs shared with other work, and a restart battery
    // that fills it would take unrelated processes down with it.
    inline std::filesystem::path data_root() {
        if (const char* env = std::getenv("RC_DATA_ROOT")) {
            return std::filesystem::path(env);
        }
        return std::filesystem::path("/tmp/otterbrix/restart_consistency");
    }

    // Run one group under one (mode, flavor) and report every divergence.
    inline rc_report check_group(const group_t& group, storage_mode mode, restart_flavor flavor) {
        const auto dir = data_root() / group.name / to_string(mode) / to_string(flavor);
        auto config = test_create_config(dir);
        test_clear_directory(config);
        const std::string out_pre = (dir / "rc_pre.tsv").string();
        const std::string out_post = (dir / "rc_post.tsv").string();

        const auto before = detail::run_phase(group, config, mode, flavor, /*is_setup_phase=*/true, out_pre);
        rc_report report;
        if (!before.opened) {
            // The pre-restart phase itself failed. That is a bug, but not a RESTART
            // bug -- report it as such rather than blaming the restart.
            report.reopened = false;
            report.reopen_failure = "pre-restart phase failed: " + before.failure;
            return report;
        }

        const auto after = detail::run_phase(group, config, mode, flavor, /*is_setup_phase=*/false, out_post);
        if (!after.opened) {
            report.reopened = false;
            report.reopen_failure = after.failure;
            return report;
        }
        report.reopened = true;

        std::map<std::string, std::string> after_by_name;
        for (const auto& [name, value] : after.observations) {
            after_by_name.emplace(name, value);
        }
        for (const auto& [name, value] : before.observations) {
            if (name.rfind("setup: ", 0) == 0) {
                continue; // setup runs pre-restart only; it has no post-restart twin
            }
            ++report.compared;
            const auto it = after_by_name.find(name);
            if (it == after_by_name.end()) {
                report.divergences.push_back({name, value, "<probe missing after restart>"});
            } else if (it->second != value) {
                report.divergences.push_back({name, value, it->second});
            }
        }
        return report;
    }

} // namespace restart_rc
