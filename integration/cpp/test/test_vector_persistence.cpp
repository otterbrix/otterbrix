// Storage persistence round-trip tests for typed columns — especially ARRAY
// (DOUBLE[N]) columns, which back the vector-search feature. These exercise the
// path the existing suite missed: write column DATA, restart, and verify the
// reloaded VALUES (not just kNN order, which the HNSW restart test covers via
// the graph snapshot). Regression guard for the array-checkpoint bug where the
// element child column was never persisted and reloaded as all-zeros.

#include "test_config.hpp"

#include <catch2/catch.hpp>

#include <cmath>
#include <sstream>
#include <string>
#include <vector>

using namespace components;

namespace {

    const std::string db = "PersistDb";
    const std::string tbl = "PersistDb.Docs";

    std::string arr(const std::vector<double>& v) {
        std::stringstream ss;
        ss << "ARRAY[";
        for (std::size_t i = 0; i < v.size(); ++i) {
            ss << (i ? ", " : "") << std::fixed << v[i];
        }
        ss << "]";
        return ss.str();
    }

    // Read the embedding (DOUBLE[N]) of the single row matching id; -1 col on miss.
    std::vector<double> read_embedding(otterbrix::wrapper_dispatcher_t* d, int id) {
        auto cur = d->execute_sql(otterbrix::session_id_t(),
                                  "SELECT id, embedding FROM " + tbl + " WHERE id == " + std::to_string(id) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto ec = cur->chunk_data().column_index("embedding");
        REQUIRE(ec != static_cast<size_t>(-1));
        auto val = cur->value(static_cast<uint64_t>(ec), 0);
        std::vector<double> out;
        for (const auto& child : val.children()) {
            out.push_back(child.value<double>());
        }
        return out;
    }

    void create_db_and_table(otterbrix::wrapper_dispatcher_t* d, std::size_t dim, const std::string& extra = "") {
        d->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE " + db + ";");
        auto cur = d->execute_sql(otterbrix::session_id_t(),
                                  "CREATE TABLE " + tbl + " (id BIGINT" + extra + ", embedding DOUBLE[" +
                                      std::to_string(dim) + "]);");
        REQUIRE(cur->is_success());
    }

} // namespace

// ---------------------------------------------------------------------------
// Group 1 — storage persistence round-trip
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::vector_persistence::scalar_baseline_survives_restart") {
    auto config = test_create_config("/tmp/test_vector_persistence/scalar_baseline");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        d->execute_sql(otterbrix::session_id_t(), "CREATE DATABASE " + db + ";");
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CREATE TABLE " + tbl + " (id BIGINT, category BIGINT);")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, category) VALUES (1, 11), (2, 22), (3, 33);")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        auto cur = d->execute_sql(otterbrix::session_id_t(), "SELECT id, category FROM " + tbl + " WHERE id == 2;");
        REQUIRE(cur->size() == 1);
        auto cc = cur->chunk_data().column_index("category");
        REQUIRE(cur->value(static_cast<uint64_t>(cc), 0).value<int64_t>() == 22);
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_survives_explicit_checkpoint_restart") {
    auto config = test_create_config("/tmp/test_vector_persistence/array_checkpoint");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (1, " + arr({1.5, 2.5}) + "), (2, " +
                                   arr({-3.0, 4.25}) + "), (3, " + arr({0.0, 0.0}) + ");")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
        // Same-session sanity: checkpoint must not corrupt the live values.
        REQUIRE(read_embedding(d, 1) == std::vector<double>({1.5, 2.5}));
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(read_embedding(d, 1) == std::vector<double>({1.5, 2.5}));
        REQUIRE(read_embedding(d, 2) == std::vector<double>({-3.0, 4.25}));
        REQUIRE(read_embedding(d, 3) == std::vector<double>({0.0, 0.0}));
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_survives_restart_without_explicit_checkpoint") {
    auto config = test_create_config("/tmp/test_vector_persistence/array_dtor_checkpoint");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config); // destructor checkpoints on shutdown
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (7, " + arr({9.0, 8.0}) + ");")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(read_embedding(d, 7) == std::vector<double>({9.0, 8.0}));
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_higher_dim_survives_restart") {
    auto config = test_create_config("/tmp/test_vector_persistence/array_dim8");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    const std::vector<double> v = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8};
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 8);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (1, " + arr(v) + ");")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        auto got = read_embedding(d, 1);
        REQUIRE(got.size() == 8);
        for (std::size_t i = 0; i < 8; ++i) {
            REQUIRE(got[i] == Approx(v[i]));
        }
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_multi_segment_survives_restart") {
    // > DEFAULT_VECTOR_CAPACITY (1024) rows so the element child column spans
    // multiple on-disk segments — exercises multi-segment checkpoint/restore.
    auto config = test_create_config("/tmp/test_vector_persistence/array_multiseg");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    constexpr int kRows = 2500;
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        std::stringstream q;
        q << "INSERT INTO " << tbl << " (id, embedding) VALUES ";
        for (int i = 0; i < kRows; ++i) {
            q << (i ? ", " : "") << "(" << i << ", ARRAY[" << std::fixed << static_cast<double>(i) << ", "
              << static_cast<double>(-i) << "])";
        }
        q << ";";
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), q.str())->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM " + tbl + ";")->size() ==
                static_cast<std::size_t>(kRows));
        for (int id : {0, 1, 1023, 1024, 1025, 2048, 2499}) {
            INFO("row id=" << id);
            REQUIRE(read_embedding(d, id) ==
                    std::vector<double>({static_cast<double>(id), static_cast<double>(-id)}));
        }
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_after_update_survives_restart") {
    auto config = test_create_config("/tmp/test_vector_persistence/array_update");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (1, " + arr({1.0, 1.0}) + "), (2, " +
                                   arr({2.0, 2.0}) + ");")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "UPDATE " + tbl + " SET embedding = " + arr({9.0, 9.0}) + " WHERE id == 1;")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(read_embedding(d, 1) == std::vector<double>({9.0, 9.0}));
        REQUIRE(read_embedding(d, 2) == std::vector<double>({2.0, 2.0}));
    }
}

TEST_CASE("integration::cpp::vector_persistence::array_after_delete_survives_restart") {
    auto config = test_create_config("/tmp/test_vector_persistence/array_delete");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (1, " + arr({1.0, 1.0}) + "), (2, " +
                                   arr({2.0, 2.0}) + "), (3, " + arr({3.0, 3.0}) + ");")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "DELETE FROM " + tbl + " WHERE id == 2;")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM " + tbl + ";")->size() == 2);
        REQUIRE(read_embedding(d, 1) == std::vector<double>({1.0, 1.0}));
        REQUIRE(read_embedding(d, 3) == std::vector<double>({3.0, 3.0}));
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM " + tbl + " WHERE id == 2;")->size() == 0);
    }
}

TEST_CASE("integration::cpp::vector_persistence::empty_array_table_survives_restart") {
    // An empty ARRAY-column table reloads and stays queryable (0-row array chunks
    // round-trip cleanly). NOTE: inserting into a freshly-reloaded *empty* table and
    // then querying it crashes for ANY table (scalar too) — a separate, pre-existing
    // engine bug unrelated to array persistence; deliberately not exercised here.
    auto config = test_create_config("/tmp/test_vector_persistence/array_empty");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "SELECT id FROM " + tbl + ";")->size() == 0);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "SELECT * FROM " + tbl + ";")->size() == 0);
    }
}

// ---------------------------------------------------------------------------
// Group 4 — demo scenario + restart regressions
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::vector_persistence::exact_knn_after_restart_no_index") {
    // The exact reproduction of the python demo: 3-column table, NO index, then
    // restart and re-run the kNN query. Must return correct order AND values.
    auto config = test_create_config("/tmp/test_vector_persistence/demo_exact");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    const std::string q = "SELECT id, category FROM " + tbl + " ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;";
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2, ", category BIGINT");
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl +
                                   " (id, category, embedding) VALUES "
                                   "(1, 10, " + arr({0.0, 0.0}) + "), (2, 20, " + arr({1.0, 0.0}) + "), "
                                   "(3, 30, " + arr({0.1, 0.1}) + "), (4, 30, " + arr({5.0, 5.0}) + "), "
                                   "(5, 20, " + arr({0.2, 0.0}) + ");")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        // embedding column itself survives:
        REQUIRE(read_embedding(d, 4) == std::vector<double>({5.0, 5.0}));
        // kNN order after restart (exact scan over reloaded storage):
        auto cur = d->execute_sql(otterbrix::session_id_t(), q);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto idc = cur->chunk_data().column_index("id");
        REQUIRE(cur->value(static_cast<uint64_t>(idc), 0).value<int64_t>() == 1); // [0,0] is nearest
        // projection holds after restart too — no phantom columns:
        REQUIRE(cur->chunk_data().column_index("embedding") == static_cast<size_t>(-1));
        REQUIRE(cur->chunk_data().column_index("vector_distance") == static_cast<size_t>(-1));
    }
}

TEST_CASE("integration::cpp::vector_persistence::exact_knn_after_restart_select_star") {
    auto config = test_create_config("/tmp/test_vector_persistence/demo_star");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        create_db_and_table(d, 2);
        REQUIRE(d->execute_sql(otterbrix::session_id_t(),
                               "INSERT INTO " + tbl + " (id, embedding) VALUES (1, " + arr({0.0, 0.0}) + "), (2, " +
                                   arr({3.0, 4.0}) + "), (3, " + arr({1.0, 0.0}) + ");")->is_success());
        REQUIRE(d->execute_sql(otterbrix::session_id_t(), "CHECKPOINT;")->is_success());
    }
    {
        test_spaces s(config);
        auto* d = s.dispatcher();
        auto cur = d->execute_sql(otterbrix::session_id_t(),
                                  "SELECT * FROM " + tbl + " ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        auto idc = cur->chunk_data().column_index("id");
        auto ec = cur->chunk_data().column_index("embedding");
        REQUIRE(ec != static_cast<size_t>(-1));
        // nearest first = id 1 [0,0], then id 3 [1,0], then id 2 [3,4]
        REQUIRE(cur->value(static_cast<uint64_t>(idc), 0).value<int64_t>() == 1);
        REQUIRE(cur->value(static_cast<uint64_t>(idc), 2).value<int64_t>() == 2);
        auto e2 = cur->value(static_cast<uint64_t>(ec), 2);
        REQUIRE(e2.children()[0].value<double>() == Approx(3.0));
        REQUIRE(e2.children()[1].value<double>() == Approx(4.0));
    }
}
