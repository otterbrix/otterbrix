#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>
#include <services/index/disk_hash_table.hpp>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "index_fixture_path.hpp"

using services::index::tests::index_fixture_path;
using services::index::tests::index_fixture_root;

using services::index::disk_hash_table_t;

namespace {
    std::filesystem::path mk_path(const std::string& name) {
        const auto dir = std::filesystem::path(index_fixture_root());
        std::filesystem::create_directories(dir);
        return dir / name;
    }

    // The truncated-key question is asked through a TEMPLATE parameter now, not a virtual
    // hook, so a test lambda IS the loader and the adapter that used to wrap one is gone.
    // The question and every answer below are unchanged except for the `lock_bitcask`
    // flag, which every lambda here already ignored and which had no caller left once the
    // reads moved to the agent: the loader is chosen per call by the owning store, which
    // holds its own lock.
    //
    // A case with no truncated entry to resolve passes this one, and it now REFUSES rather
    // than answering false. The promise those cases rely on -- every key here is inside
    // inline_key_limit, so the loader is never consulted at all -- used to live in a
    // comment; as a refusal it is a CHECKED assertion, because a case that quietly grew a
    // long key would stop returning "no such row" for reasons unrelated to what it tests
    // and start failing at the walk, where it belongs. A value, not a Catch2 FAIL: rules 2
    // and 9 keep exceptions out, and the value form is loud for free.
    constexpr auto loader_must_not_be_consulted = [](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"the loader must not be consulted: every key in this case is inline",
                                              std::pmr::new_delete_resource()});
    };

    // The loader answers with a std::pmr::string now (rule 8, and it is the CONCEPT that
    // demands it). Every case here has a plain std::string to hand back, so the crossing is
    // spelled once, here, on an explicit resource -- never on the process default one.
    std::pmr::string as_loader_key(std::string_view key) {
        return std::pmr::string(key.data(), key.size(), std::pmr::new_delete_resource());
    }

    // THE WALKS ANSWER WITH A core::result_wrapper_t NOW: a chain they could not finish
    // is a value they hand back instead of a shortened list. must_read asserts the walk
    // reached the end and unwraps what it found, so the cases below stay about what they
    // were about -- and a case that is ABOUT the refusal checks has_error() itself.
    template<typename result_t>
    auto must_read(result_t&& walked) {
        REQUIRE_FALSE(walked.has_error());
        return std::move(walked.value());
    }

    // FNV-1a collision pair for truncated entries (same 32-byte prefix and encoded length).
    // AT NAMESPACE SCOPE because two cases need the same pair now: the one that resolves it
    // with a working loader, and the one that records what happens when the STRANGER of the
    // pair cannot be read. Same bytes, unchanged.
    static const unsigned char enc_a_bytes[] = {
        35,  200, 0,   0,   0,   97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,
        97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  9,   116, 135, 155, 250, 116, 9,   140, 227, 29,
        188, 54,  49,  139, 96,  164, 244, 19,  249, 118, 220, 255, 148, 220, 154, 28,  241, 216, 101, 91,  42,
        168, 242, 57,  62,  204, 83,  169, 47,  172, 148, 146, 211, 44,  178, 68,  202, 191, 171, 5,   69,  71,
        120, 74,  61,  120, 148, 11,  199, 187, 225, 101, 225, 164, 182, 68,  140, 150, 33,  215, 9,   12,  5,
        73,  92,  160, 147, 212, 150, 60,  92,  23,  165, 246, 199, 204, 52,  81,  209, 3,   39,  193, 82,  8,
        115, 21,  138, 68,  42,  7,   109, 19,  18,  220, 242, 193, 163, 118, 20,  9,   178, 204, 190, 70,  178,
        36,  177, 154, 201, 137, 158, 10,  92,  58,  81,  117, 170, 175, 9,   255, 203, 33,  205, 21,  157, 219,
        3,   208, 151, 119, 135, 125, 83,  141, 108, 68,  110, 205, 129, 211, 216, 70,  31,  86,  165, 11,  140,
        244, 78,  89,  216, 175, 81,  98,  151, 17,  46,  66,  24,  207, 219, 64,  203, 205};
    static const unsigned char enc_b_bytes[] = {
        35,  200, 0,   0,   0,   97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,
        97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  97,  126, 48,  173, 224, 150, 190, 22,  109, 132, 141,
        117, 4,   146, 254, 102, 53,  239, 54,  221, 225, 64,  61,  41,  164, 185, 142, 115, 85,  203, 158, 211,
        52,  221, 90,  4,   72,  254, 69,  71,  181, 204, 241, 230, 254, 1,   180, 253, 16,  49,  196, 230, 70,
        99,  29,  138, 164, 35,  206, 53,  53,  22,  52,  229, 141, 252, 108, 171, 189, 178, 58,  29,  44,  201,
        235, 88,  137, 102, 149, 69,  191, 51,  71,  158, 49,  119, 244, 227, 199, 41,  65,  233, 111, 253, 53,
        252, 85,  231, 211, 32,  172, 122, 99,  61,  32,  207, 24,  56,  209, 250, 208, 195, 54,  33,  212, 87,
        54,  203, 127, 180, 209, 40,  118, 118, 124, 112, 214, 35,  120, 149, 130, 214, 169, 59,  182, 224, 47,
        208, 12,  168, 49,  95,  174, 2,   225, 33,  5,   230, 190, 75,  223, 159, 194, 122, 246, 192, 57,  180,
        202, 72,  69,  22,  67,  149, 49,  195, 91,  7,   21,  177, 73,  137, 228, 127, 205};
    static const std::string enc_a(reinterpret_cast<const char*>(enc_a_bytes), sizeof(enc_a_bytes));
    static const std::string enc_b(reinterpret_cast<const char*>(enc_b_bytes), sizeof(enc_b_bytes));

    struct env_var_guard_t {
        std::string name;
        bool had_value{false};
        std::string prev;

        env_var_guard_t(std::string env_name, const std::string& value)
            : name(std::move(env_name)) {
            if (const char* current = std::getenv(name.c_str()); current != nullptr) {
                had_value = true;
                prev = current;
            }
            setenv(name.c_str(), value.c_str(), 1);
        }

        ~env_var_guard_t() {
            if (had_value) {
                setenv(name.c_str(), prev.c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }
    };
} // namespace

TEST_CASE("services::index::disk_hash_table::put_get_erase_roundtrip") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_roundtrip.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 64, &resource);
    REQUIRE_FALSE(table.put("alpha", 10, 1, 100).contains_error());
    REQUIRE_FALSE(table.put("beta", 20, 1, 200).contains_error());

    auto alpha = must_read(table.get("alpha", loader_must_not_be_consulted));
    REQUIRE(alpha.has_value());
    REQUIRE(alpha->value == 10);
    REQUIRE(alpha->log_file_id == 1);
    REQUIRE(alpha->log_offset == 100);

    auto beta = must_read(table.get("beta", loader_must_not_be_consulted));
    REQUIRE(beta.has_value());
    REQUIRE(beta->value == 20);

    REQUIRE(must_read(table.erase("alpha", loader_must_not_be_consulted)));
    REQUIRE_FALSE(must_read(table.get("alpha", loader_must_not_be_consulted)).has_value());
    REQUIRE(must_read(table.get("beta", loader_must_not_be_consulted)).has_value());
}

TEST_CASE("services::index::disk_hash_table::persist_reopen") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_persist.data");
    std::filesystem::remove(path);

    {
        disk_hash_table_t table(path, 32, &resource);
        REQUIRE_FALSE(table.put("k1", 111, 2, 1234).contains_error());
        REQUIRE_FALSE(table.put("k2", 222, 2, 5678).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    {
        disk_hash_table_t reopened(path, 32, &resource);
        auto v1 = must_read(reopened.get("k1", loader_must_not_be_consulted));
        REQUIRE(v1.has_value());
        REQUIRE(v1->value == 111);
        REQUIRE(v1->log_file_id == 2);
        REQUIRE(v1->log_offset == 1234);
        auto v2 = must_read(reopened.get("k2", loader_must_not_be_consulted));
        REQUIRE(v2.has_value());
        REQUIRE(v2->value == 222);
    }
}

TEST_CASE("services::index::disk_hash_table::multiple_values_per_key") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_multi_values.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 32, &resource);
    REQUIRE_FALSE(table.put("dup", 10, 1, 100).contains_error());
    REQUIRE_FALSE(table.put("dup", 20, 2, 200).contains_error());
    REQUIRE_FALSE(table.put("dup", 10, 3, 300).contains_error());

    const auto values = must_read(table.get_all("dup", loader_must_not_be_consulted));
    REQUIRE(values.size() == 3);
}

TEST_CASE("services::index::disk_hash_table::long_key_prefix_and_loader") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_long_key.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    const std::string other_key = long_key + "y";

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    const auto source_1 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        return as_loader_key(long_key);
    };
    auto with_loader = must_read(table.get(long_key, source_1));
    REQUIRE(with_loader.has_value());
    REQUIRE(with_loader->value == 777);

    const auto source_2 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(long_key);
    };
    auto mismatch = must_read(table.get(other_key, source_2));
    REQUIRE_FALSE(mismatch.has_value());
}

TEST_CASE("services::index::disk_hash_table::truncated_collision_requires_loader") {
    // enc_a/enc_b collide only for seed=0 (plain FNV-1a); table uses random seed by default.
    env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0");
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_truncated_collision.data");
    std::filesystem::remove(path);


    disk_hash_table_t table(path, 32, &resource);
    REQUIRE_FALSE(table.put(enc_a, 777, 1, 100).contains_error());

    size_t loader_calls = 0;
    const auto source_3 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key(enc_a);
    };

    REQUIRE(must_read(table.get_all(enc_b, source_3)).empty());
    REQUIRE(loader_calls >= 1);
}

// A CHANGED OUTCOME, WRITTEN DOWN RATHER THAN DISCOVERED LATER. The case above is the same
// collision pair with a loader that ANSWERS; this one is the pair with a loader that cannot.
//
// The entry that cannot be read belongs to a DIFFERENT key. It shares the probe's 32-bit
// key_hash and its 32-byte stored prefix -- that is what makes the pair a collision -- and
// the probe's own entry has ALREADY been matched and collected by the time the walk reaches
// it. Before the loader could refuse, keys_equal answered `false` here and the probe got its
// correct, complete answer. Now the walk refuses, and the probe gets nothing.
//
// IT IS ACCEPTED, and the reason is that "it belongs to someone else" is not something this
// walk knows: the only thing that could establish it is the record the loader could not
// read. Answering `false` for an entry it could not read is a GUESS, and it is the very
// guess this class was fixed to stop making -- in the other direction it drops a row of the
// probe's own key and reports success over it. There is no third answer available: the walk
// either reads the record or does not know.
//
// The price is real and it is exactly this: a probe can lose an answer it would have got had
// a STRANGER's record been readable. It is a refusal, not a wrong answer, and the next read
// after the device recovers is correct -- so it is bounded by the fault, and the fault is
// already loud.
TEST_CASE("services::index::disk_hash_table::a_colliding_stranger_that_cannot_be_read_refuses_the_whole_walk") {
    env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0");
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_unreadable_collision_stranger.data");
    std::filesystem::remove(path);
    // AND ITS OVERFLOW FILE. The table opens `path` and `path + ".ovf"` as one pair, so
    // removing only the first hands the fresh table a stranger's overflow chain from a
    // previous run -- and the header that would have said how long it is went with the file
    // that was removed.
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    disk_hash_table_t table(path, 32, &resource);
    // THE PROBE'S OWN ENTRY GOES IN FIRST, so the walk matches and collects it before it ever
    // reaches the stranger: slots are scanned in the order they were written. Without this
    // order the case would only show "a refusal short-circuits the walk", which is not the
    // outcome being recorded -- the outcome is that a COMPLETE answer already in hand is
    // thrown away.
    REQUIRE_FALSE(table.put(enc_b, 555, 2, 200).contains_error());
    REQUIRE_FALSE(table.put(enc_a, 777, 1, 100).contains_error());

    // The baseline, so the refusal below is a CHANGE of answer rather than an empty bucket:
    // with both records readable the probe gets its one row and the stranger is rejected.
    const auto both_readable = [&](uint32_t, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(offset == 200 ? enc_b : enc_a);
    };
    const auto complete = must_read(table.get_all(enc_b, both_readable));
    REQUIRE(complete.size() == 1);
    REQUIRE(complete.front().value == 555);

    std::vector<uint64_t> consulted;
    const auto stranger_unreadable = [&](uint32_t, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        consulted.push_back(offset);
        if (offset == 200) {
            return as_loader_key(enc_b);
        }
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"the stranger's record is unreadable",
                                              std::pmr::new_delete_resource()});
    };

    auto walked = table.get_all(enc_b, stranger_unreadable);
    REQUIRE(walked.has_error());
    REQUIRE(walked.error().type == core::error_code_t::io_error);
    // The order is the assertion: the probe's own entry was decided FIRST and was in the
    // result when the stranger's refusal discarded it.
    REQUIRE(consulted.size() == 2);
    REQUIRE(consulted[0] == 200);
    REQUIRE(consulted[1] == 100);
}

TEST_CASE("services::index::disk_hash_table::get_invokes_key_loader_for_truncated_entry") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_invoked_on_get.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    size_t loader_calls = 0;
    const auto source_4 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        REQUIRE(file_id == 7);
        REQUIRE(offset == 700);
        return as_loader_key(long_key);
    };

    const auto value = must_read(table.get(long_key, source_4));
    REQUIRE(value.has_value());
    REQUIRE(value->value == 777);
    REQUIRE(loader_calls == 1);
}

TEST_CASE("services::index::disk_hash_table::get_skips_key_loader_for_inline_entry") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_skipped_inline.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put("short-key", 5, 1, 100).contains_error());

    size_t loader_calls = 0;
    const auto source_5 = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key("short-key");
    };

    const auto value = must_read(table.get("short-key", source_5));
    REQUIRE(value.has_value());
    REQUIRE(value->value == 5);
    REQUIRE(loader_calls == 0);
}

TEST_CASE("services::index::disk_hash_table::erase_invokes_key_loader_for_truncated_entry") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_invoked_on_erase.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'y');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 909, 9, 900).contains_error());

    size_t loader_calls = 0;
    const auto source_6 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        REQUIRE(file_id == 9);
        REQUIRE(offset == 900);
        return as_loader_key(long_key);
    };

    REQUIRE(must_read(table.erase(long_key, source_6)));
    REQUIRE(loader_calls >= 1);
    REQUIRE_FALSE(must_read(table.get(long_key, source_6)).has_value());
}

// THE THIRD ANSWER, pinned at the layer that produces it -- and this IS the behavioural
// pin, not a compile-time one. The note that used to stand here said the red->green lived
// one layer up, in
// services/index/tests/test_bitcask_index_disk.cpp::find_refuses_when_a_long_keys_record_cannot_be_read,
// and that was wrong in a way worth writing down: find() has a SECOND reason to refuse on
// that fixture (its own read_rows_at over the same truncated segment), so it stays green
// even when keys_equal GUESSES "yes" on an entry nothing could decide. Measured, not
// argued: with `return true` substituted for the VALUE_OR_RETURN in keys_equal, that case
// passed all 19 of its assertions and THIS one failed on the REQUIRE below.
//
// The layer matters because only here is the guess visible. get_all is asked with a loader
// that REFUSES, so any decided answer -- a row, or an empty list -- is a decision nothing
// could have made: the record that alone could decide is the one the loader could not read.
// (The store-level case has since grown the same observation against its own keydir, so it
// is no longer blind either; the two now fail together.)
TEST_CASE("services::index::disk_hash_table::truncated_entry_refuses_when_the_record_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_refuses.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    // The entry is TRUNCATED, asserted rather than assumed -- and asserted through the one
    // reader that reports it WITHOUT a loader. get/get_all only put a value_ref_t in the
    // answer after keys_equal has already succeeded, i.e. after the step these cases are
    // about failing, so they cannot be used to establish the precondition.
    uint64_t truncated_entries = 0;
    REQUIRE_FALSE(table
                      .for_each([&](const disk_hash_table_t::value_ref_t& ref) {
                          if (ref.key_truncated) {
                              ++truncated_entries;
                          }
                      })
                      .contains_error());
    REQUIRE(truncated_entries == 1);

    const auto refuses = [](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"record unreadable", std::pmr::new_delete_resource()});
    };

    // R1a. Both readers of keys_equal hand the refusal on instead of reading it as
    // "continue, not your key". get_all throws away what it had collected: the refusal is
    // TOTAL, the same shape as the page it could not read.
    auto read = table.get_all(long_key, refuses);
    REQUIRE(read.has_error());
    REQUIRE(read.error().type == core::error_code_t::io_error);

    auto erased = table.erase(long_key, refuses);
    REQUIRE(erased.has_error());
    REQUIRE(erased.error().type == core::error_code_t::io_error);

    // AND NOTHING WAS REMOVED ON THE WAY OUT. try_erase_in_page mutates the page only on
    // the two lines before it reports success, so a refusal from the middle of a walk has
    // no half-done removal behind it.
    const auto source = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        return as_loader_key(long_key);
    };
    const auto still_there = must_read(table.get(long_key, source));
    REQUIRE(still_there.has_value());
    REQUIRE(still_there->value == 777);
}

// R1b. THE LEGAL "no" IS STILL A "no". The loader ANSWERED, and the whole key it produced
// is not the probe -- that is an answer, not a refusal, and the fix must not swallow it
// into the channel it just opened. Probing with the key that was PUT is what makes the
// loader reachable at all: get_all screens slots on the 32-bit key_hash first, so a probe
// for a different key never gets as far as keys_equal (which is why
// long_key_prefix_and_loader above can pass a loader that is never called).
TEST_CASE("services::index::disk_hash_table::truncated_entry_answers_no_when_the_full_key_differs") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_loader_says_different.data");
    std::filesystem::remove(path);

    const std::string long_key(200, 'x');
    // Same length, same 32-byte stored prefix: everything the entry itself holds matches,
    // so only the record the loader reads back can decide -- and it decides NO.
    const std::string different_key = std::string(199, 'x') + "y";

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put(long_key, 777, 7, 700).contains_error());

    size_t loader_calls = 0;
    const auto answers = [&](uint32_t, uint64_t) -> core::result_wrapper_t<std::pmr::string> {
        ++loader_calls;
        return as_loader_key(different_key);
    };

    const auto missing = must_read(table.get(long_key, answers));
    REQUIRE_FALSE(missing.has_value());
    REQUIRE(loader_calls == 1);
}

// R1c. AN INLINE KEY NEVER ASKS. loader_must_not_be_consulted refuses on sight, so this
// case is green only because the loader is not reached at all -- which is the property the
// twenty-odd other cases that pass it silently depend on.
TEST_CASE("services::index::disk_hash_table::inline_entry_never_reaches_a_refusing_loader") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_inline_skips_refusing_loader.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 8, &resource);
    REQUIRE_FALSE(table.put("short-key", 5, 1, 100).contains_error());

    const auto value = must_read(table.get("short-key", loader_must_not_be_consulted));
    REQUIRE(value.has_value());
    REQUIRE(value->value == 5);

    REQUIRE(must_read(table.erase("short-key", loader_must_not_be_consulted)));
    REQUIRE_FALSE(must_read(table.get("short-key", loader_must_not_be_consulted)).has_value());
}
TEST_CASE("services::index::disk_hash_table::rehash_preserves_entries") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, &resource);
    table.set_auto_rehash_suppressed(true);
    REQUIRE(table.bucket_count() == 4);

    for (int i = 0; i < 300; ++i) {
        const auto key = "k." + std::to_string(i);
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)).contains_error());
    }

    REQUIRE_FALSE(table.rehash(128).contains_error());
    REQUIRE(table.bucket_count() == 128);

    for (int i = 0; i < 300; ++i) {
        const auto key = "k." + std::to_string(i);
        auto v = must_read(table.get(key, loader_must_not_be_consulted));
        REQUIRE(v.has_value());
        REQUIRE(v->value == static_cast<int64_t>(i));
    }
}

TEST_CASE("services::index::disk_hash_table::rehash_truncated_keys_without_loader") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_rehash_truncated.data");
    std::filesystem::remove(path);

    const std::string key1(200, 'a');
    const std::string key2 = std::string(199, 'a') + "b";

    disk_hash_table_t table(path, 4, &resource);
    REQUIRE_FALSE(table.put(key1, 11, 5, 500).contains_error());
    REQUIRE_FALSE(table.put(key2, 22, 6, 600).contains_error());

    REQUIRE_FALSE(table.rehash(64).contains_error());

    // The unknown-location leg REFUSES rather than answering "not your key": a keydir
    // entry pointing at a record that is not there is corruption, not a mismatch. It is
    // unreachable by construction here (both puts use (5,500)/(6,600) and both probes are
    // for keys that were put), and stating it that way is what keeps it unreachable.
    const auto source_7 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
        if (file_id == 5 && offset == 500) {
            return as_loader_key(key1);
        }
        if (file_id == 6 && offset == 600) {
            return as_loader_key(key2);
        }
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"no record at this location", std::pmr::new_delete_resource()});
    };
    auto v1 = must_read(table.get(key1, source_7));
    REQUIRE(v1.has_value());
    REQUIRE(v1->value == 11);

    auto v2 = must_read(table.get(key2, source_7));
    REQUIRE(v2.has_value());
    REQUIRE(v2->value == 22);
}

TEST_CASE("services::index::disk_hash_table::linear_hashing_progression") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_linear_progression.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(120);
    for (int i = 0; i < 100; ++i) {
        keys.emplace_back("progress.k." + std::to_string(i));
    }
    for (int i = 0; i < 20; ++i) {
        keys.emplace_back(std::string(120, static_cast<char>('a' + (i % 10))) + ".long." + std::to_string(i));
    }

    std::unordered_map<uint64_t, std::string> full_key_by_offset;
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        REQUIRE(table.bucket_count() == 4);

        for (size_t i = 0; i < keys.size(); ++i) {
            const auto offset = static_cast<uint64_t>(10'000 + i);
            full_key_by_offset.emplace(offset, keys[i]);
            REQUIRE_FALSE(table.put(keys[i], static_cast<int64_t>(i), 42, offset).contains_error());
        }

        const auto source_8 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
            if (file_id != 42) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", std::pmr::new_delete_resource()});
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", std::pmr::new_delete_resource()});
            }
            return as_loader_key(it->second);
        };

        for (uint32_t target = 5; target <= 9; ++target) {
            REQUIRE_FALSE(table.rehash(target).contains_error());
            REQUIRE(table.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = must_read(table.get(keys[i], source_8));
                REQUIRE(v.has_value());
                REQUIRE(v->value == static_cast<int64_t>(i));
            }
        }
        REQUIRE_FALSE(table.sync().contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 9);

        const auto source_9 = [&](uint32_t file_id, uint64_t offset) -> core::result_wrapper_t<std::pmr::string> {
            if (file_id != 42) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", std::pmr::new_delete_resource()});
            }
            const auto it = full_key_by_offset.find(offset);
            if (it == full_key_by_offset.end()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"no record at this location", std::pmr::new_delete_resource()});
            }
            return as_loader_key(it->second);
        };

        for (uint32_t target = 10; target <= 12; ++target) {
            REQUIRE_FALSE(reopened.rehash(target).contains_error());
            REQUIRE(reopened.bucket_count() == target);
            for (size_t i = 0; i < keys.size(); ++i) {
                auto v = must_read(reopened.get(keys[i], source_9));
                REQUIRE(v.has_value());
                REQUIRE(v->value == static_cast<int64_t>(i));
            }
        }
    }
}

TEST_CASE("services::index::disk_hash_table::auto_rehash_by_load_factor") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_auto_rehash.data");
    std::filesystem::remove(path);

    disk_hash_table_t table(path, 4, &resource);
    const auto initial_buckets = table.bucket_count();
    REQUIRE(initial_buckets == 4);

    for (int i = 0; i < 20; ++i) {
        const auto key = "auto.k." + std::to_string(i);
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 10, static_cast<uint64_t>(i + 1)).contains_error());
    }

    REQUIRE(table.bucket_count() > initial_buckets);
    REQUIRE(table.load_factor() <= 0.75);
}

TEST_CASE("services::index::disk_hash_table::split_crash_after_copy_sync") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_split_crash_after_copy.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(300);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 300; ++i) {
            keys.emplace_back("crash.copy.k." + std::to_string(i));
            REQUIRE_FALSE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(1000 + i)).contains_error());
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_copy_sync");
        // The failpoint aborts the split; failure arrives by value, like the rest of this API
        // (put/erase/rehash return bool). The property under test is unchanged: the rehash does
        // not complete, and the reopened table below is still consistent.
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 4);
        for (int i = 0; i < 300; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

TEST_CASE("services::index::disk_hash_table::split_crash_after_header_sync") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_split_crash_after_header.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(300);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 300; ++i) {
            keys.emplace_back("crash.header.k." + std::to_string(i));
            REQUIRE_FALSE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(2000 + i)).contains_error());
        }
        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);
        for (int i = 0; i < 300; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

TEST_CASE("services::index::disk_hash_table::split_crash_recovery_continues_progression") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("disk_hash_table_split_crash_recovery_progression.data");
    std::filesystem::remove(path);

    std::vector<std::string> keys;
    keys.reserve(400);
    {
        disk_hash_table_t table(path, 4, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 400; ++i) {
            keys.emplace_back("crash.recover.k." + std::to_string(i));
            REQUIRE_FALSE(table.put(keys.back(), static_cast<int64_t>(i), 1, static_cast<uint64_t>(5000 + i)).contains_error());
        }

        env_var_guard_t guard("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT", "after_header_sync");
        REQUIRE(table.rehash(5).contains_error());
    }

    {
        disk_hash_table_t reopened(path, 4, &resource);
        REQUIRE(reopened.bucket_count() == 5);

        REQUIRE_FALSE(reopened.rehash(6).contains_error());
        REQUIRE(reopened.bucket_count() == 6);

        for (int i = 0; i < 400; ++i) {
            auto v = must_read(reopened.get(keys[static_cast<size_t>(i)], loader_must_not_be_consulted));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }
}

// create() is the production entry point: an unusable file must come back as a
// value, not as an exception and not as a half-open table. The direct ctor keeps
// aborting on the same input (same split as bitcask_index_disk_t), which is why
// callers that can report a failure — manager_index_t — must use create().
TEST_CASE("services::index::disk_hash_table::create_reports_unopenable_storage") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("create_unopenable.bin");
    std::filesystem::remove_all(path);
    // A directory where the file belongs: open(O_RDWR|O_CREAT) is EISDIR.
    std::filesystem::create_directories(path);

    auto result = disk_hash_table_t::create(path, disk_hash_table_t::default_bucket_count, &resource);
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::index_create_fail);

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::disk_hash_table::create_returns_a_usable_table") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("create_usable.bin");
    std::filesystem::remove_all(path);
    std::filesystem::remove_all(std::filesystem::path(path).concat(".ovf"));

    auto result = disk_hash_table_t::create(path, 8, &resource);
    REQUIRE_FALSE(result.has_error());
    // A unique_ptr now: the table stopped being reference-counted when the index facade
    // stopped holding a second reference to it across the actor boundary (C2c).
    auto table = std::move(result.value());
    REQUIRE(table);
    REQUIRE_FALSE(table->put("k", 42, 0, 0).contains_error());
    auto found = must_read(table->get("k", loader_must_not_be_consulted));
    REQUIRE(found.has_value());
    REQUIRE(found->value == 42);
}

// --- for_each: behavioural gate on walk order and capture lifetime -----------
// for_each's ORDER is observable, not an implementation detail: both production
// callers (bitcask_index_disk_t::load_entries and ::merge_immutable_segments)
// accumulate through a by-reference capture, so the sequence for_each hands out is
// the sequence they build -- load_entries' duplicate entries and the merge's ref
// list come out in exactly this order. The cases below pin that walk (buckets
// ascending, primary page before its overflow chain, slots 0..n-1 inside a page)
// so that a table which is merely "still complete" cannot pass.

TEST_CASE("services::index::disk_hash_table::for_each_walks_duplicates_in_insertion_order") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_duplicate_order.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    // ONE bucket: every entry lands on the same chain, so the whole for_each output
    // is the whole insertion order and no hash seed can perturb it. put() never
    // reuses a freed slot, it appends at slot index count(), so "insertion order"
    // is the ground truth the walk has to reproduce.
    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    constexpr int64_t duplicates = 300;
    for (int64_t i = 0; i < duplicates; ++i) {
        REQUIRE_FALSE(table.put("dup", i, static_cast<uint32_t>(i + 1), static_cast<uint64_t>(1000 + i)).contains_error());
    }
    REQUIRE_FALSE(table.sync().contains_error());

    // ~104 entries of this shape fit one 4096-byte page, so 300 duplicates PROVE the
    // walk crossed primary -> overflow instead of stopping at the first page. Asserted,
    // not assumed: without a real chain the order below would be a single-page order.
    REQUIRE(std::filesystem::exists(overflow_path));
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    std::pmr::vector<disk_hash_table_t::value_ref_t> seen(&resource);
    REQUIRE_FALSE(table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref); }).contains_error());

    REQUIRE(seen.size() == static_cast<size_t>(duplicates));
    for (int64_t i = 0; i < duplicates; ++i) {
        const auto& ref = seen[static_cast<size_t>(i)];
        REQUIRE(ref.value == i);
        // The whole value_ref_t rides along in step: a reordering that regenerated
        // values but shuffled the log coordinates would still be caught here.
        REQUIRE(ref.log_file_id == static_cast<uint32_t>(i + 1));
        REQUIRE(ref.log_offset == static_cast<uint64_t>(1000 + i));
        REQUIRE_FALSE(ref.key_truncated);
    }

    // Independent oracle: get_all walks the same chain by the same rule and is NOT
    // touched by this task, so the two orders must agree.
    auto all = must_read(table.get_all("dup", loader_must_not_be_consulted));
    REQUIRE(all.size() == seen.size());
    for (size_t i = 0; i < all.size(); ++i) {
        REQUIRE(all[i].value == seen[i].value);
        REQUIRE(all[i].log_offset == seen[i].log_offset);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_keeps_interleaved_keys_in_chain_order") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_interleaved_order.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    // One bucket again, but now with three keys interleaved: this pins that the walk
    // reports slot order and does NOT group or sort by key on the way out.
    disk_hash_table_t table(path, 1, &resource);
    REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

    struct put_t {
        std::string_view key;
        int64_t value;
    };
    const std::pmr::vector<put_t> script(
        {put_t{"alpha", 1},
         put_t{"beta", 2},
         put_t{"alpha", 3},
         put_t{"gamma", 4},
         put_t{"beta", 5},
         put_t{"alpha", 6},
         put_t{"gamma", 7},
         put_t{"beta", 8},
         put_t{"alpha", 9}},
        &resource);
    for (const auto& step : script) {
        REQUIRE_FALSE(table.put(step.key, step.value, 7, static_cast<uint64_t>(step.value) * 10).contains_error());
    }

    std::pmr::vector<int64_t> seen(&resource);
    REQUIRE_FALSE(table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { seen.push_back(ref.value); }).contains_error());

    REQUIRE(seen.size() == script.size());
    for (size_t i = 0; i < script.size(); ++i) {
        REQUIRE(seen[i] == script[i].value);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_multi_bucket_order_is_stable_and_per_key_ordered") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_multi_bucket_order.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    constexpr int key_count = 5;
    constexpr int64_t per_key = 20;
    std::pmr::vector<int64_t> first_pass(&resource);
    std::pmr::vector<int64_t> second_pass(&resource);

    {
        // Pin the hash seed so the bucket layout -- and therefore this case -- is the
        // same on every run instead of being reseeded per file.
        env_var_guard_t seed_guard("OTTERBRIX_DISK_HASH_SEED", "0x5eed1234");
        disk_hash_table_t table(path, 64, &resource);
        REQUIRE_FALSE(table.set_auto_rehash_suppressed(true));

        // Interleave the keys so that insertion order and key order disagree; the value
        // encodes which key produced it (key*1000 + n) so each key's subsequence is
        // recoverable from the value_ref_t alone.
        for (int64_t n = 0; n < per_key; ++n) {
            for (int k = 0; k < key_count; ++k) {
                const std::string key = "key_" + std::to_string(k);
                REQUIRE_FALSE(table.put(key, static_cast<int64_t>(k) * 1000 + n, 1, static_cast<uint64_t>(n)).contains_error());
            }
        }

        REQUIRE_FALSE(table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { first_pass.push_back(ref.value); }).contains_error());
        REQUIRE_FALSE(table.for_each([&](const disk_hash_table_t::value_ref_t& ref) { second_pass.push_back(ref.value); }).contains_error());
    }

    REQUIRE(first_pass.size() == static_cast<size_t>(key_count) * static_cast<size_t>(per_key));
    // Two consecutive walks over an unchanged table must produce the SAME order.
    REQUIRE(second_pass.size() == first_pass.size());
    for (size_t i = 0; i < first_pass.size(); ++i) {
        REQUIRE(second_pass[i] == first_pass[i]);
    }

    // The bucket loop runs ASCENDING, and with the seed pinned above every key lands
    // in a bucket of its own, so the whole sequence is fixed: fnv1a-32 seeded with
    // 0x5eed1234 mod 64 puts key_4 in bucket 3, key_0 in 15, key_3 in 34, key_2 in 53
    // and key_1 in 60. Walking the buckets the other way, or in any other order, moves
    // these five groups and is caught here -- the per-key check below cannot see it,
    // because reversing the bucket loop leaves every chain internally intact.
    const std::pmr::vector<int> expected_key_order({4, 0, 3, 2, 1}, &resource);
    for (size_t group = 0; group < expected_key_order.size(); ++group) {
        for (int64_t n = 0; n < per_key; ++n) {
            const auto position = group * static_cast<size_t>(per_key) + static_cast<size_t>(n);
            REQUIRE(first_pass[position] == static_cast<int64_t>(expected_key_order[group]) * 1000 + n);
        }
    }

    // Entries of one key share a bucket and a chain, so their relative order in the
    // output is their insertion order -- ascending n. This holds whatever bucket the
    // seed sends them to, and breaks the moment a chain is walked backwards or an
    // overflow page is visited before its primary.
    for (int k = 0; k < key_count; ++k) {
        int64_t expected_n = 0;
        for (auto value : first_pass) {
            if (value / 1000 != static_cast<int64_t>(k)) {
                continue;
            }
            REQUIRE(value % 1000 == expected_n);
            ++expected_n;
        }
        REQUIRE(expected_n == per_key);
    }
}

TEST_CASE("services::index::disk_hash_table::for_each_delivers_every_entry_before_it_returns") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("for_each_capture_lifetime.data");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    disk_hash_table_t table(path, 8, &resource);

    // An empty table must not invoke the callable at all -- no phantom entry, and
    // nothing queued to run later.
    std::pmr::vector<int64_t> collected(&resource);
    size_t calls = 0;
    auto empty_walk = table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });
    REQUIRE_FALSE(empty_walk.contains_error());
    REQUIRE(calls == 0);
    REQUIRE(collected.empty());

    constexpr int64_t total = 40;
    for (int64_t i = 0; i < total; ++i) {
        REQUIRE_FALSE(table.put("k" + std::to_string(i), i, 1, static_cast<uint64_t>(i)).contains_error());
    }

    auto full_walk = table.for_each([&](const disk_hash_table_t::value_ref_t& ref) {
        collected.push_back(ref.value);
        ++calls;
    });
    REQUIRE_FALSE(full_walk.contains_error());

    // Everything the callable did through its by-reference capture is visible to the
    // caller the instant for_each returns: the walk is synchronous, not deferred.
    const auto size_on_return = collected.size();
    REQUIRE(calls == size_on_return);
    REQUIRE(size_on_return == static_cast<size_t>(total));

    // ... and nothing keeps calling it afterwards: later work on the same table must
    // not append one more entry through the capture.
    REQUIRE(must_read(table.get("k0", loader_must_not_be_consulted)).has_value());
    REQUIRE_FALSE(table.sync().contains_error());
    REQUIRE(collected.size() == size_on_return);
    REQUIRE(calls == size_on_return);
}

// --- an unreadable page must REFUSE the walk, not shorten its answer ---------
//
// Every walk this class performs -- get_all, get, for_each -- follows a bucket's page
// chain, and every one of them used to `break` out of that chain when read_page said no,
// returning whatever it had collected so far with no way to say that it stopped early.
// The caller cannot tell "this key has three rows" from "the disk would not let me finish
// counting": a SUBSET dressed as the whole answer, which is a wrong answer rather than a
// slow one.
//
// THE INJECTION IS THE FILESYSTEM, not a seam: read_page calls an overflow page
// unreadable when it sits past the end of the overflow file, and file_size() is an fstat
// per call on a still-open descriptor, so truncating that file from underneath a live
// table produces exactly the refusal a short/rotten file produces in production. The
// bytes are read back and restored afterwards through the SAME inode (truncate does not
// replace it), so the table is provably intact on the other side and the case can pin
// that the refusal is temporary rather than destructive.
namespace {
    // Read a file whole, so the injection below can put it back exactly as it was.
    std::string read_file_bytes(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        REQUIRE(in.good());
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }

    // Write bytes back INTO the existing file rather than over the path: std::ios::in
    // keeps the stream from truncating and keeps the inode the live table has open.
    void restore_file_bytes(const std::filesystem::path& path, const std::string& bytes) {
        std::filesystem::resize_file(path, bytes.size());
        std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(out.good());
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.flush();
        REQUIRE(out.good());
    }

    // ONE bucket and no auto-rehash: every entry lands on bucket 0's chain, so the
    // overflow FILE is that chain's continuation and nothing else. The key sits exactly at
    // inline_key_limit (64 is stored whole, 65 would be truncated), which fixes the
    // per-entry cost at 91 payload bytes plus a 9-byte slot -- roughly 40 to a page.
    std::string chain_key() { return std::string(disk_hash_table_t::inline_key_limit, 'k'); }

    // The same fixed width, one per index. STILL AT the limit and not over it: a key of
    // 65 bytes would be stored as a 32-byte PREFIX plus a record location, and every
    // probe for it would then have to be resolved through a loader -- which the cases
    // below deliberately do not have (loader_must_not_be_consulted), so an over-long key would answer
    // "no such row" for reasons that have nothing to do with what they test.
    std::string chain_key(int index) {
        const auto suffix = "." + std::to_string(index);
        return std::string(disk_hash_table_t::inline_key_limit - suffix.size(), 'k') + suffix;
    }
} // namespace

TEST_CASE("services::index::disk_hash_table::reads_refuse_when_an_overflow_page_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_overflow_read_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    const auto key = chain_key();
    constexpr int64_t entry_count = 200;
    // MEASURED, not assumed: the entry that first grew the overflow file is the one the
    // primary page had no room for, so every entry after it can only live past that page.
    size_t entries_that_fit_the_primary_page = 0;
    for (int64_t i = 0; i < entry_count; ++i) {
        REQUIRE_FALSE(table.put(key, i, 1, static_cast<uint64_t>(1000 + i)).contains_error());
        if (entries_that_fit_the_primary_page == 0 && std::filesystem::exists(overflow_path) &&
            std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size) {
            entries_that_fit_the_primary_page = static_cast<size_t>(i);
        }
    }
    REQUIRE(entries_that_fit_the_primary_page > 0);
    REQUIRE(entries_that_fit_the_primary_page < static_cast<size_t>(entry_count));
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    REQUIRE_FALSE(table.sync().contains_error());
    const auto whole = must_read(table.get_all(key, loader_must_not_be_consulted));
    REQUIRE(whole.size() == static_cast<size_t>(entry_count));

    const auto overflow_bytes = read_file_bytes(overflow_path);
    std::filesystem::resize_file(overflow_path, 0);

    // THE PROPERTY. Not "answers fewer rows" -- answering fewer rows IS the defect: before
    // this was fixed the line below came back with 40 of the 200 rows and no way to say so.
    auto after = table.get_all(key, loader_must_not_be_consulted);
    INFO("get_all met an unreadable overflow page and must REFUSE, not answer with the primary page alone");
    REQUIRE(after.has_error());
    REQUIRE(after.error().type == core::error_code_t::io_error);

    // get() is get_all()'s front element, so it refuses through the same value rather
    // than reporting the key missing.
    auto single = table.get(key, loader_must_not_be_consulted);
    REQUIRE(single.has_error());

    size_t seen = 0;
    auto walk = table.for_each([&](const disk_hash_table_t::value_ref_t&) { ++seen; });
    INFO("for_each met the same unreadable page and must refuse the same way");
    REQUIRE(walk.contains_error());
    REQUIRE(seen < static_cast<size_t>(entry_count));

    // ... and so does the erase, whose false used to mean BOTH "no such key" and "the
    // chain ran out from under me" -- the second of which stopped
    // bitcask_index_disk_t::erase_all_refs_for_key's loop as if it were done. Probed with
    // a key that is NOT in the table, because that is the probe which has to walk the
    // chain all the way to the unreadable page before it can answer.
    auto erased = table.erase("absent-" + key, loader_must_not_be_consulted);
    REQUIRE(erased.has_error());

    // The damage was to the FILE, not to the table: put the bytes back and every row is
    // there again, which is what proves the refusal above was the injection biting.
    restore_file_bytes(overflow_path, overflow_bytes);
    const auto restored = must_read(table.get_all(key, loader_must_not_be_consulted));
    REQUIRE(restored.size() == static_cast<size_t>(entry_count));
}

// --- a split that could not copy an entry must NOT publish -------------------
//
// split_one_bucket_unlocked copied every move-candidate into the new bucket with the
// result of the copy DROPPED, and then advanced the addressing state unconditionally. An
// entry that failed to copy is therefore lost the instant the header moves: it is still
// physically in the source bucket, but the published state says its hash belongs to the
// new bucket, so no walk will ever look where it is.
TEST_CASE("services::index::disk_hash_table::split_refuses_when_an_entry_cannot_be_copied") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_split_copy_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    std::vector<std::string> keys;
    keys.reserve(400);
    {
        disk_hash_table_t table(path, 1, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int i = 0; i < 400; ++i) {
            auto key = chain_key(i);
            REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(3000 + i)).contains_error());
            keys.emplace_back(std::move(key));
        }
        REQUIRE(table.bucket_count() == 1);
        // The one bucket already spills, so the HALF of it the split moves cannot fit in
        // the new bucket's single primary page either -- which is what makes the copy
        // reach for an overflow page and meet the refusal below.
        REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

        {
            env_var_guard_t deny_overflow("OTTERBRIX_DISK_HASH_OVERFLOW_ALLOC_FAILPOINT", "1");
            // Sensitivity, proven on the spot: with the allocation refused, an ordinary
            // put that needs a fresh overflow page cannot succeed either.
            bool put_refused = false;
            for (int i = 0; i < 200 && !put_refused; ++i) {
                put_refused = table.put("sensitivity." + std::to_string(i), 0, 1, 0).contains_error();
            }
            REQUIRE(put_refused);

            INFO("a split that could not copy every entry must refuse, not publish a half-copied bucket");
            REQUIRE(table.rehash(2).contains_error());
        }
        REQUIRE(table.bucket_count() == 1);

        // STILL COMPLETE, and still addressed the old way: every key answers from the
        // source bucket, because the source bucket is still where the table looks.
        for (size_t i = 0; i < keys.size(); ++i) {
            auto v = must_read(table.get(keys[i], loader_must_not_be_consulted));
            REQUIRE(v.has_value());
            REQUIRE(v->value == static_cast<int64_t>(i));
        }
    }

    // Nothing half-published reached the disk either, and the split is still available:
    // a retry with the allocation working must complete and leave one row per key.
    disk_hash_table_t reopened(path, 1, &resource);
    reopened.set_auto_rehash_suppressed(true);
    REQUIRE(reopened.bucket_count() == 1);
    REQUIRE_FALSE(reopened.rehash(2).contains_error());
    REQUIRE(reopened.bucket_count() == 2);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(reopened.get_all(keys[i], loader_must_not_be_consulted));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
}

// The same refusal from the other side of the copy: the split cannot READ the rest of the
// source chain. Injected by the filesystem alone, and undone afterwards, so the case can
// also pin that a refused split leaves the table exactly as it found it.
TEST_CASE("services::index::disk_hash_table::split_refuses_when_a_source_page_cannot_be_read") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_split_source_read_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    std::vector<std::string> keys;
    keys.reserve(400);
    for (int i = 0; i < 400; ++i) {
        auto key = chain_key(i);
        REQUIRE_FALSE(table.put(key, static_cast<int64_t>(i), 1, static_cast<uint64_t>(4000 + i)).contains_error());
        keys.emplace_back(std::move(key));
    }
    REQUIRE_FALSE(table.sync().contains_error());
    REQUIRE(table.bucket_count() == 1);
    REQUIRE(std::filesystem::file_size(overflow_path) >= disk_hash_table_t::page_size);

    const auto overflow_bytes = read_file_bytes(overflow_path);
    std::filesystem::resize_file(overflow_path, 0);

    INFO("a split whose source chain could not be walked to the end must refuse, not publish");
    REQUIRE(table.rehash(2).contains_error());
    REQUIRE(table.bucket_count() == 1);

    restore_file_bytes(overflow_path, overflow_bytes);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(table.get_all(keys[i], loader_must_not_be_consulted));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
    // ... and the split it refused is still there to be done.
    REQUIRE_FALSE(table.rehash(2).contains_error());
    REQUIRE(table.bucket_count() == 2);
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto rows = must_read(table.get_all(keys[i], loader_must_not_be_consulted));
        REQUIRE(rows.size() == 1);
        REQUIRE(rows.front().value == static_cast<int64_t>(i));
    }
}

// ---------------------------------------------------------------------------------------
// THE OPEN PATH REFUSES WHAT IT CANNOT READ OR VERIFY (wave entries #30/#119/#234/#288).
//
// The helpers below tamper with the on-disk header the way a torn write or a rotten
// sector would, and then re-seal it the way the FIX seals every header it writes: an
// 8-byte magic at [0,8) and a CRC32C over the six header fields [12,40) stored at [8,12).
// A case that wants the seal VALID recomputes it after tampering, so the refusal it
// asserts can only come from the specific consistency check it targets, never from the
// checksum arm.

#include "absl/crc/crc32c.h"

namespace {
    constexpr char hash_header_magic[8] = {'o', 't', 'b', 'x', 'h', 'a', 's', 'h'};

    void write_le32_at(std::string& bytes, size_t offset, uint32_t v) {
        for (unsigned i = 0; i < 4; ++i) {
            bytes[offset + i] = static_cast<char>((v >> (8U * i)) & 0xFFU);
        }
    }

    void write_le64_at(std::string& bytes, size_t offset, uint64_t v) {
        for (unsigned i = 0; i < 8; ++i) {
            bytes[offset + i] = static_cast<char>((v >> (8U * i)) & 0xFFU);
        }
    }

    // Re-seal the header page the way persist_header does after the fix: magic at [0,8),
    // CRC32C of [12,40) at [8,12).
    void reseal_hash_header(std::string& bytes) {
        std::memcpy(bytes.data(), hash_header_magic, sizeof(hash_header_magic));
        const auto crc = static_cast<uint32_t>(absl::ComputeCrc32c(absl::string_view(bytes.data() + 12, 28)));
        write_le32_at(bytes, 8, crc);
    }
} // namespace

// Wave entry #30. count_entries_unlocked used to `break` out of a bucket chain whose page
// could not be read and hand open_or_create a COUNT OF THE READABLE PART -- so the table
// opened with an entry_count_ (and therefore a load factor) that silently understated the
// file. A walk that could not finish must refuse, and the open must hand that refusal on.
TEST_CASE("services::index::disk_hash_table::open_refuses_when_the_entry_count_cannot_be_counted") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_count_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 8, &resource);
        table.set_auto_rehash_suppressed(true);
        for (int64_t i = 0; i < 64; ++i) {
            REQUIRE_FALSE(table.put("count-key-" + std::to_string(i), i, 1, 100 + static_cast<uint64_t>(i))
                              .contains_error());
        }
        REQUIRE_FALSE(table.sync().contains_error());
    }

    // Cut the file mid-table: the header and the first four bucket pages survive, the last
    // four bucket pages do not. Before the fix this opened fine and counted only what the
    // readable half held.
    std::filesystem::resize_file(path, static_cast<uintmax_t>(5) * disk_hash_table_t::page_size);

    auto reopened = disk_hash_table_t::create(path, 8, &resource);
    INFO("an open that could not count its entries must refuse, not open over a partial count");
    REQUIRE(reopened.has_error());
    REQUIRE(reopened.error().type == core::error_code_t::io_error);
}

// Wave entry #119. load_existing_file used to meet an INCONSISTENT level/split_bucket pair
// -- a header where 2^level + split_bucket != bucket_count -- and silently REWRITE it from
// the bucket count. The header is re-sealed with a VALID checksum here, so the refusal
// this asserts can only come from the linear-state check itself.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_corrupt_linear_hash_state_instead_of_repairing_it") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_linear_state_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 32, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    // level 3 says base = 8; split stays 0; 8 + 0 != 32, so the pair no longer describes
    // the bucket count. Before the fix the open repaired this silently and reported
    // nothing.
    write_le32_at(bytes, 28, 3);
    reseal_hash_header(bytes);
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 32, &resource);
    INFO("a header whose linear-hash state does not describe its bucket count is corruption, not input");
    REQUIRE(reopened.has_error());
}

// Wave entry #288. The second silent auto-repair in the same function: a stored
// next_overflow_page BELOW the overflow id base was clamped up to the base and the open
// went on. Same shape as #119, same channel, same verdict. The seal is VALID here too.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_corrupt_overflow_cursor_instead_of_clamping_it") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_overflow_cursor_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    // 5 is far below the 2^40 overflow id base: no persist_header ever wrote it, so it is
    // a damaged file, not a value to clamp.
    write_le64_at(bytes, 20, 5);
    reseal_hash_header(bytes);
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("an overflow cursor below the overflow page id base is corruption, not a value to clamp");
    REQUIRE(reopened.has_error());
}

// Wave entry #234, the checksum arm. A flipped bit in the header used to pass whenever the
// value it produced still looked plausible: bucket_count 16 -> 17 keeps every structural
// check happy (base 16 + split 1 == 17) and silently re-addresses EVERY key in the file.
// The stale seal is left in place here, so only the checksum can catch it -- which is the
// point of having one.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_header_whose_checksum_does_not_match") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_checksum_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    write_le32_at(bytes, 16, 17); // one flipped count, structurally plausible
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("a header the checksum disowns must not be loaded, however plausible its fields look");
    REQUIRE(reopened.has_error());
}

// Wave entry #234, the magic arm. A file that is not a hash index at all -- or a header
// page that was never sealed by this codec -- must be refused by NAME, before any field of
// it is interpreted. The seal's checksum is made VALID over the tampered fields, so the
// refusal can only come from the magic check.
TEST_CASE("services::index::disk_hash_table::open_refuses_a_file_without_the_magic") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_open_magic_refusal.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    {
        disk_hash_table_t table(path, 16, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        REQUIRE_FALSE(table.sync().contains_error());
    }

    auto bytes = read_file_bytes(path);
    reseal_hash_header(bytes);
    std::memset(bytes.data(), 0, 8); // valid checksum, no name
    restore_file_bytes(path, bytes);

    auto reopened = disk_hash_table_t::create(path, 16, &resource);
    INFO("a header page that does not carry this table's magic is not this table's header");
    REQUIRE(reopened.has_error());
}

// Wave entry #234, the slot arm. An erased slot used to stay dead forever: every later
// insert appended a NEW slot and NEW payload bytes, so a workload that puts and erases the
// same key marched the page to exhaustion and then grew an overflow chain -- for a table
// whose LIVE contents never exceeded one entry.
TEST_CASE("services::index::disk_hash_table::an_erased_slot_is_reused_by_the_next_insert") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_slot_reuse.data");
    const auto overflow_path = std::filesystem::path(path).concat(".ovf");
    std::filesystem::remove(path);
    std::filesystem::remove(overflow_path);

    disk_hash_table_t table(path, 1, &resource);
    table.set_auto_rehash_suppressed(true);

    // 500 put/erase rounds of one identically-sized entry: live size never exceeds 1.
    for (int64_t i = 0; i < 500; ++i) {
        REQUIRE_FALSE(table.put("steady-key", i, 1, static_cast<uint64_t>(1000 + i)).contains_error());
        auto erased = table.erase("steady-key", loader_must_not_be_consulted);
        REQUIRE_FALSE(erased.has_error());
        REQUIRE(erased.value());
    }

    // One live entry's worth of state must not have grown an overflow chain.
    const bool overflow_grew =
        std::filesystem::exists(overflow_path) && std::filesystem::file_size(overflow_path) > 0;
    INFO("a page cycling ONE live entry must reuse its freed slot, not grow an overflow chain");
    REQUIRE_FALSE(overflow_grew);

    // And the table still answers correctly through the reused slot.
    REQUIRE_FALSE(table.put("steady-key", 42, 3, 4242).contains_error());
    auto found = must_read(table.get("steady-key", loader_must_not_be_consulted));
    REQUIRE(found.has_value());
    REQUIRE(found->value == 42);
    REQUIRE(found->log_file_id == 3);
    REQUIRE(found->log_offset == 4242);
}

// Wave entry #117. The destructor's closing header flush used to be checked only by
// assert(false), which -DNDEBUG compiles out -- so in release a failed closing persist was
// dropped in complete silence. The failure is staged through the close failpoint (the one
// seam this class has for a write the filesystem cannot be made to refuse from outside),
// and the property is that the destructor SAYS SO on stderr, in every build mode, instead
// of saying nothing.

#include <fcntl.h>
#include <unistd.h>

namespace {
    template<typename fn_t>
    std::string capture_stderr_of(const std::filesystem::path& capture_file, fn_t&& fn) {
        std::fflush(stderr);
        const int saved_stderr = ::dup(2);
        REQUIRE(saved_stderr >= 0);
        const int capture_fd =
            ::open(capture_file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
        REQUIRE(capture_fd >= 0);
        REQUIRE(::dup2(capture_fd, 2) == 2);
        ::close(capture_fd);
        fn();
        std::fflush(stderr);
        REQUIRE(::dup2(saved_stderr, 2) == 2);
        ::close(saved_stderr);
        return read_file_bytes(capture_file);
    }
} // namespace

TEST_CASE("services::index::disk_hash_table::a_failed_closing_flush_is_reported_loudly") {
    auto resource = core::pmr::otterbrix_resource();
    const auto path = mk_path("hash_close_flush_report.data");
    const auto capture_file = mk_path("hash_close_flush_report.stderr");
    std::filesystem::remove(path);
    std::filesystem::remove(std::filesystem::path(path).concat(".ovf"));

    const auto stderr_text = capture_stderr_of(capture_file, [&] {
        env_var_guard_t failpoint("OTTERBRIX_DISK_HASH_CLOSE_FAILPOINT", "1");
        disk_hash_table_t table(path, 8, &resource);
        REQUIRE_FALSE(table.put("k", 7, 1, 100).contains_error());
        // The destructor runs here, meets the staged persist failure, and must REPORT it.
    });

    INFO("a closing flush that failed must be named on stderr, not dropped in silence");
    REQUIRE(stderr_text.find("disk_hash_table") != std::string::npos);
    REQUIRE(stderr_text.find(path.string()) != std::string::npos);
}
