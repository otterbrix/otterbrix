#include "test_config.hpp"
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <vector_search/distance_metrics.hpp>

#include <catch2/catch.hpp>

#include <algorithm>
#include <filesystem>
#include <map>
#include <fstream>
#include <cmath>
#include <sstream>
#include <vector>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "vectors";

using namespace components;
using namespace components::cursor;

// Insert a row with an ARRAY vector via SQL.
static std::string make_insert_sql(int id, const std::vector<double>& vec) {
    std::stringstream ss;
    ss << "INSERT INTO TestDatabase.Vectors (id, embedding) VALUES (" << id << ", ARRAY[";
    for (std::size_t i = 0; i < vec.size(); ++i) {
        if (i > 0)
            ss << ", ";
        ss << std::fixed << vec[i];
    }
    ss << "]);";
    return ss.str();
}

// Typed vector table — complex (ARRAY) columns need a typed schema (relkind='r').
static void create_vectors_table(otterbrix::wrapper_dispatcher_t* dispatcher, std::size_t dim) {
    auto s = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        s, "CREATE TABLE TestDatabase.Vectors (id BIGINT, embedding DOUBLE[" + std::to_string(dim) + "]);");
    REQUIRE(cur->is_success());
}

// '[v0, v1, ...]' literal for a query vector.
static std::string vec_literal(const std::vector<double>& v) {
    std::stringstream ss;
    ss << "'[";
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i > 0)
            ss << ", ";
        ss << std::fixed << v[i];
    }
    ss << "]'";
    return ss.str();
}

TEST_CASE("integration::cpp::vector_search::basic_l2") {
    auto config = test_create_config("/tmp/test_vector_search/basic_l2");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Setup: create database and collection
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.Vectors (id BIGINT, embedding DOUBLE[2]);");
        (void) collection_name;
    }

    // Insert 5 vectors (2D)
    std::vector<std::vector<double>> vectors = {
        {0.0, 0.0}, // id=0, origin
        {1.0, 0.0}, // id=1
        {0.0, 1.0}, // id=2
        {5.0, 5.0}, // id=3, far
        {0.1, 0.1}, // id=4, very close to origin
    };

    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, make_insert_sql(static_cast<int>(i), vectors[i]));
        REQUIRE(cur->is_success());
    }

    // Verify data inserted
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.Vectors;")->size() == 5);
    }

    // Vector search: find 3 nearest to origin using L2
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

TEST_CASE("integration::cpp::vector_search::basic_cosine") {
    auto config = test_create_config("/tmp/test_vector_search/basic_cosine");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
        (void) collection_name;
    }

    // Insert 4 directional vectors (2D)
    std::vector<std::vector<double>> vectors = {
        {1.0, 0.0},  // id=0: right
        {1.0, 1.0},  // id=1: 45°
        {0.0, 1.0},  // id=2: up
        {-1.0, 0.0}, // id=3: left (opposite)
    };

    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, make_insert_sql(static_cast<int>(i), vectors[i]));
        REQUIRE(cur->is_success());
    }

    // Search for vectors most similar to "right" direction (cosine).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <=> '[1.0, 0.0]' LIMIT 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::vector_search::k_equals_1") {
    auto config = test_create_config("/tmp/test_vector_search/k_1");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
        (void) collection_name;
    }

    // Insert 3 vectors
    for (int i = 0; i < 3; ++i) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, make_insert_sql(i, {static_cast<double>(i), 0.0}));
        REQUIRE(cur->is_success());
    }

    // k=1: nearest to (0,0) should be exactly 1 result
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::vector_search::k_greater_than_n") {
    auto config = test_create_config("/tmp/test_vector_search/k_gt_n");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
        (void) collection_name;
    }

    // Insert 2 vectors
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, make_insert_sql(0, {1.0, 2.0}));
    }
    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, make_insert_sql(1, {3.0, 4.0}));
    }

    // k=10 but only 2 rows → should return 2
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 10;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::vector_search::projection") {
    auto config = test_create_config("/tmp/test_vector_search/projection");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
        (void) collection_name;
    }
    std::vector<std::vector<double>> vectors = {{0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}, {5.0, 5.0}, {0.1, 0.1}};
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }

    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT id FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_index("id") != static_cast<size_t>(-1));
        REQUIRE(cur->chunk_data().column_index("embedding") == static_cast<size_t>(-1));
        REQUIRE(cur->chunk_data().column_index("vector_distance") == static_cast<size_t>(-1));
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->chunk_data().column_index("id") != static_cast<size_t>(-1));
        REQUIRE(cur->chunk_data().column_index("embedding") != static_cast<size_t>(-1));
        REQUIRE(cur->chunk_data().column_index("vector_distance") == static_cast<size_t>(-1));
    }
}

TEST_CASE("integration::cpp::vector_search::parameterized_limit") {
    auto config = test_create_config("/tmp/test_vector_search/param_limit");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
        (void) collection_name;
    }
    std::vector<std::vector<double>> vectors = {{0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}, {5.0, 5.0}, {0.1, 0.1}};
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql_with_params(
            session,
            "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT $1;",
            {{1, components::types::logical_value_t(dispatcher->resource(), static_cast<int64_t>(3))}});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql_with_params(
            session,
            "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT $1;",
            {{1, components::types::logical_value_t(dispatcher->resource(), static_cast<int64_t>(2))}});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
}

TEST_CASE("integration::cpp::vector_search::empty_collection") {
    auto config = test_create_config("/tmp/test_vector_search/empty");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 3);
        (void) collection_name;
    }

    // Search on empty collection
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[1.0, 2.0, 3.0]' LIMIT 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::vector_search::higher_dimension") {
    auto config = test_create_config("/tmp/test_vector_search/high_dim");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 16);
        (void) collection_name;
    }

    // Insert 10 vectors of dimension 16
    constexpr std::size_t dim = 16;
    constexpr std::size_t n = 10;
    for (std::size_t i = 0; i < n; ++i) {
        std::vector<double> vec(dim);
        for (std::size_t d = 0; d < dim; ++d) {
            vec[d] = static_cast<double>(i * dim + d) / 100.0;
        }
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, make_insert_sql(static_cast<int>(i), vec));
        REQUIRE(cur->is_success());
    }

    // Search k=3 (16-dimensional query)
    std::vector<double> query(dim, 0.0);
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> " + vec_literal(query) + " LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() <= 3);
    }
}

TEST_CASE("integration::cpp::vector_search::pre_filtering") {
    auto config = test_create_config("/tmp/test_vector_search/filtered");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session, "CREATE TABLE TestDatabase.Vectors (id BIGINT, category BIGINT, embedding DOUBLE[2]);");
        REQUIRE(cur->is_success());
        (void) collection_name;
    }

    // Insert 3 documents. Two documents have the same vector, but different category id.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO TestDatabase.Vectors (id, category, embedding) VALUES (1, 10, ARRAY[-1.0, 0.0]);");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO TestDatabase.Vectors (id, category, embedding) VALUES (2, 20, ARRAY[1.0, 0.0]);");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        // Exact same vector as doc 2, but category is 30
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO TestDatabase.Vectors (id, category, embedding) VALUES (3, 30, ARRAY[1.0, 0.0]);");
        REQUIRE(cur->is_success());
    }

    // With filter category == 30, only doc 3 should return.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM TestDatabase.Vectors WHERE category == 30 "
            "ORDER BY embedding <-> '[1.0, 0.0]' LIMIT 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);

        auto id_col = cur->chunk_data().column_index("id");
        REQUIRE(id_col != static_cast<size_t>(-1));
        auto doc_id = cur->value(static_cast<uint64_t>(id_col), 0).value<std::int64_t>();
        REQUIRE((doc_id == 3)); // Must be document 3, not doc 2.
    }
}

TEST_CASE("integration::cpp::vector_search::hnsw_index_sql") {
    auto config = test_create_config("/tmp/test_vector_search/hnsw_index_sql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
    }

    // Insert 2D vectors
    std::vector<std::vector<double>> vectors = {
        {0.0, 0.0}, // id=0, origin
        {1.0, 0.0}, // id=1
        {0.0, 1.0}, // id=2
        {5.0, 5.0}, // id=3, far
        {0.1, 0.1}, // id=4, very close to origin
        {0.2, 0.0}, // id=5, close to origin
    };
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, make_insert_sql(static_cast<int>(i), vectors[i]));
        REQUIRE(cur->is_success());
    }

    // Create an HNSW index over the embedding column via SQL.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops) "
            "WITH (m = 16, ef_construction = 64);");
        REQUIRE(cur->is_success());
    }

    // kNN search via the <-> operator — now routed through the HNSW index.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "SELECT * FROM TestDatabase.Vectors "
            "ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);

        auto id_col = cur->chunk_data().column_index("id");
        REQUIRE(id_col != static_cast<size_t>(-1));
        std::vector<int64_t> ids;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            ids.push_back(cur->value(static_cast<uint64_t>(id_col), static_cast<uint64_t>(r)).value<int64_t>());
        }
        // 3 nearest to origin are id=0 (0), id=4 (~0.02), id=5 (0.04).
        std::sort(ids.begin(), ids.end());
        REQUIRE(ids == std::vector<int64_t>({0, 4, 5}));
    }
}

// Helper: run a kNN SELECT and return the resulting id column, sorted.
static std::vector<int64_t> knn_ids(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
    auto session = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(session, sql);
    REQUIRE(cur->is_success());
    std::vector<int64_t> ids;
    auto id_col = cur->chunk_data().column_index("id");
    REQUIRE(id_col != static_cast<size_t>(-1));
    for (std::size_t r = 0; r < cur->size(); ++r) {
        ids.push_back(cur->value(static_cast<uint64_t>(id_col), static_cast<uint64_t>(r)).value<int64_t>());
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

TEST_CASE("integration::cpp::vector_search::hnsw_index_delete") {
    auto config = test_create_config("/tmp/test_vector_search/hnsw_index_delete");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 2); }

    std::vector<std::vector<double>> vectors = {
        {0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}, {5.0, 5.0}, {0.1, 0.1}, {0.2, 0.0},
    };
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    const std::string q =
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;";

    REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 4, 5}));

    // Delete the exact nearest (id=0) — it must vanish from kNN results (tombstone in graph).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "DELETE FROM TestDatabase.Vectors WHERE id == 0;")->is_success());
    }
    auto after = knn_ids(dispatcher, q);
    REQUIRE(std::find(after.begin(), after.end(), int64_t{0}) == after.end()); // id=0 gone
    REQUIRE(std::find(after.begin(), after.end(), int64_t{4}) != after.end()); // id=4 still nearest-ish
    REQUIRE(std::find(after.begin(), after.end(), int64_t{5}) != after.end());

    // Insert a brand-new closest point — it must appear (live insert into the graph).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "INSERT INTO TestDatabase.Vectors (id, embedding) VALUES (9, ARRAY[0.01, 0.0]);")
                    ->is_success());
    }
    auto after_insert = knn_ids(dispatcher, q);
    REQUIRE(std::find(after_insert.begin(), after_insert.end(), int64_t{9}) != after_insert.end());
}

TEST_CASE("integration::cpp::vector_search::hnsw_index_update") {
    auto config = test_create_config("/tmp/test_vector_search/hnsw_index_update");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 2); }

    std::vector<std::vector<double>> vectors = {{5.0, 5.0}, {1.0, 0.0}, {0.1, 0.1}};
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    const std::string q =
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 1;";

    // id=2 (0.1,0.1) is initially nearest the origin.
    REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({2}));

    // Move id=0 right next to the origin — it must become the nearest (old vector tombstoned).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "UPDATE TestDatabase.Vectors SET embedding = ARRAY[0.001, 0.0] WHERE id == 0;")
                    ->is_success());
    }
    REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0}));
}

TEST_CASE("integration::cpp::vector_search::hnsw_index_compaction") {
    auto config = test_create_config("/tmp/test_vector_search/hnsw_index_compaction");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 2); }

    // 200 points on a line: id i -> (i * 0.01, 0). Enough to exceed the compaction min-size.
    const int N = 200;
    for (int i = 0; i < N; ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(i, {i * 0.01, 0.0}))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    const std::string q =
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 5;";
    REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 1, 2, 3, 4}));

    // Delete the 60 nearest points (30% > 20% threshold) — triggers a background rebuild.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "DELETE FROM TestDatabase.Vectors WHERE id < 60;")->is_success());
    }

    // After deletion+rebuild the nearest live points are id 60..64, and nothing < 60 survives.
    auto after = knn_ids(dispatcher, q);
    REQUIRE(after == std::vector<int64_t>({60, 61, 62, 63, 64}));
}

TEST_CASE("integration::cpp::vector_search::hnsw_index_restart") {
    auto config = test_create_config("/tmp/test_vector_search/hnsw_index_restart");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);

    const std::string q =
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;";

    // session 1: populate + index, checkpoint on destructor
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
        { create_vectors_table(dispatcher, 2); }

        std::vector<std::vector<double>> vectors = {
            {0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}, {5.0, 5.0}, {0.1, 0.1}, {0.2, 0.0},
        };
        for (std::size_t i = 0; i < vectors.size(); ++i) {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s,
                                      "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                        ->is_success());
        }
        REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 4, 5}));
    }

    // session 2: reopen; index rebuilt from storage
    {
        test_spaces space(config); // NOTE: no test_clear_directory — keep persisted data
        auto* dispatcher = space.dispatcher();

        REQUIRE(dispatcher->execute_sql(otterbrix::session_id_t(), "SELECT * FROM TestDatabase.Vectors;")->size() == 6);
        REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 4, 5}));

        // A fresh live insert must land in the rebuilt graph.
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s, "INSERT INTO TestDatabase.Vectors (id, embedding) VALUES (99, ARRAY[0.001, 0.0]);")
                        ->is_success());
        }
        auto ids = knn_ids(dispatcher, q);
        REQUIRE(std::find(ids.begin(), ids.end(), int64_t{99}) != ids.end());
    }
}

// SET hnsw.ef_search, DESC, distance in WHERE.
TEST_CASE("integration::cpp::vector_search::set_ef_search") {
    auto config = test_create_config("/tmp/test_vector_search/set_ef_search");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
    }
    for (int i = 0; i < 6; ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(i, {i * 0.1, 0.0}))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    // Valid SET — succeeds, and the following kNN query still works through the index.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "SET hnsw.ef_search = 200;")->is_success());
    }
    REQUIRE(knn_ids(dispatcher, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;") ==
            std::vector<int64_t>({0, 1, 2}));

    // Reset back to the default.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "SET hnsw.ef_search TO DEFAULT;")->is_success());
    }

    // Invalid values and unknown parameters must fail.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_FALSE(dispatcher->execute_sql(s, "SET hnsw.ef_search = 0;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_FALSE(dispatcher->execute_sql(s, "SET hnsw.ef_search = 'abc';")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_FALSE(dispatcher->execute_sql(s, "SET some.unknown_param = 5;")->is_success());
    }
}

TEST_CASE("integration::cpp::vector_search::order_by_desc_farthest") {
    auto config = test_create_config("/tmp/test_vector_search/desc_farthest");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
    }
    std::vector<std::vector<double>> vectors = {
        {0.0, 0.0},  // id=0
        {1.0, 0.0},  // id=1
        {0.1, 0.1},  // id=2
        {5.0, 5.0},  // id=3, farthest
        {3.0, 0.0},  // id=4, second farthest
    };
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }
    // An index exists, but DESC must bypass it and run the exact farthest scan.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    auto s = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        s,
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' DESC LIMIT 2;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);

    auto id_col = cur->chunk_data().column_index("id");
    REQUIRE(id_col != static_cast<size_t>(-1));
    // Farthest-first: id=3 (~7.07), then id=4 (3.0).
    REQUIRE(cur->value(static_cast<uint64_t>(id_col), 0).value<int64_t>() == 3);
    REQUIRE(cur->value(static_cast<uint64_t>(id_col), 1).value<int64_t>() == 4);
}

TEST_CASE("integration::cpp::vector_search::distance_operator_in_where") {
    auto config = test_create_config("/tmp/test_vector_search/distance_where");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
    }
    std::vector<std::vector<double>> vectors = {
        {0.0, 0.0},  // id=0, d=0
        {0.1, 0.1},  // id=1, d≈0.141
        {0.3, 0.0},  // id=2, d=0.3
        {1.0, 0.0},  // id=3, d=1
        {5.0, 5.0},  // id=4, d≈7.07
    };
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }

    // Exact per-row distance filtering: rows with L2 distance to origin below 0.5.
    auto s = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        s,
        "SELECT * FROM TestDatabase.Vectors WHERE embedding <-> '[0.0, 0.0]' < 0.5;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 3);

    auto id_col = cur->chunk_data().column_index("id");
    REQUIRE(id_col != static_cast<size_t>(-1));
    std::vector<int64_t> ids;
    for (std::size_t r = 0; r < cur->size(); ++r) {
        ids.push_back(cur->value(static_cast<uint64_t>(id_col), static_cast<uint64_t>(r)).value<int64_t>());
    }
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<int64_t>({0, 1, 2}));

    // The function-call form must behave identically.
    auto s2 = otterbrix::session_id_t();
    auto cur2 = dispatcher->execute_sql(
        s2,
        "SELECT * FROM TestDatabase.Vectors WHERE l2_distance(embedding, '[0.0, 0.0]') < 0.5;");
    REQUIRE(cur2->is_success());
    REQUIRE(cur2->size() == 3);
}

TEST_CASE("integration::cpp::vector_search::create_index_opclass_errors") {
    auto config = test_create_config("/tmp/test_vector_search/opclass_errors");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto s = otterbrix::session_id_t();
        dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";");
    }
    {
        create_vectors_table(dispatcher, 2);
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(0, {0.0, 0.0}))->is_success());
    }

    // Unknown operator class.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_FALSE(
            dispatcher
                ->execute_sql(s, "CREATE INDEX i1 ON TestDatabase.Vectors USING hnsw (embedding vector_l3_ops);")
                ->is_success());
    }
    // Unknown WITH parameter.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE_FALSE(dispatcher
                          ->execute_sql(s,
                                        "CREATE INDEX i2 ON TestDatabase.Vectors USING hnsw (embedding) "
                                        "WITH (whatever = 1);")
                          ->is_success());
    }
    // Cosine opclass parses and builds.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(
                        s,
                        "CREATE INDEX i3 ON TestDatabase.Vectors USING hnsw (embedding vector_cosine_ops);")
                    ->is_success());
    }
}

// Graph snapshot: save on shutdown, load on restart, rebuild on corruption, drop on DROP INDEX.
TEST_CASE("integration::cpp::vector_search::hnsw_sidecar_on_create_index") {
    auto config = test_create_config("/tmp/test_vector_search/sidecar_on_create");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);

    auto find_snapshot = [&](const std::string& fname) -> std::filesystem::path {
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(config.disk.path, ec);
             !ec && it != std::filesystem::recursive_directory_iterator();
             it.increment(ec)) {
            if (it->is_regular_file(ec) && it->path().filename() == fname) {
                return it->path();
            }
        }
        return {};
    };
    const auto meta_exists = [&] { return !find_snapshot("idx_emb.hnsw.meta").empty(); };
    const auto graph_exists = [&] { return !find_snapshot("idx_emb.hnsw").empty(); };

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 2); }

    std::vector<std::vector<double>> vectors = {{0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}};
    for (std::size_t i = 0; i < vectors.size(); ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }
    REQUIRE(meta_exists());
    REQUIRE(graph_exists());
    REQUIRE(knn_ids(dispatcher, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 2;") ==
            std::vector<int64_t>({0, 2}));
}

TEST_CASE("integration::cpp::vector_search::hnsw_graph_snapshot") {
    auto config = test_create_config("/tmp/test_vector_search/graph_snapshot");
    config.disk.on = true;
    config.wal.on = true;
    test_clear_directory(config);

    const std::string q =
        "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;";
    // The snapshot lives under an oid-named dir, so locate it by filename.
    auto find_snapshot = [&](const std::string& fname) -> std::filesystem::path {
        std::error_code ec;
        for (auto it = std::filesystem::recursive_directory_iterator(config.disk.path, ec);
             !ec && it != std::filesystem::recursive_directory_iterator();
             it.increment(ec)) {
            if (it->is_regular_file(ec) && it->path().filename() == fname) {
                return it->path();
            }
        }
        return {};
    };
    const auto graph_exists = [&] { return !find_snapshot("idx_emb.hnsw").empty(); };
    const auto meta_exists = [&] { return !find_snapshot("idx_emb.hnsw.meta").empty(); };

    // session 1: populate + index, snapshot on shutdown
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
        { create_vectors_table(dispatcher, 2); }

        std::vector<std::vector<double>> vectors = {
            {0.0, 0.0}, {1.0, 0.0}, {0.0, 1.0}, {5.0, 5.0}, {0.1, 0.1}, {0.2, 0.0},
        };
        for (std::size_t i = 0; i < vectors.size(); ++i) {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s,
                                      "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                        ->is_success());
        }
        REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 4, 5}));
    }

    // The shutdown checkpoint must have written both the graph and its sidecar.
    REQUIRE(graph_exists());
    REQUIRE(meta_exists());

    // session 2: reopen; graph loaded from snapshot
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(knn_ids(dispatcher, q) == std::vector<int64_t>({0, 4, 5}));

        // Mutations keep working on the loaded graph.
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s, "INSERT INTO TestDatabase.Vectors (id, embedding) VALUES (77, ARRAY[0.01, 0.0]);")
                        ->is_success());
        }
        auto ids = knn_ids(dispatcher, q);
        REQUIRE(std::find(ids.begin(), ids.end(), int64_t{77}) != ids.end());
    }

    // session 3: corrupt snapshot → rebuild fallback
    {
        std::ofstream out(find_snapshot("idx_emb.hnsw"), std::ios::binary | std::ios::trunc);
        out << "garbage";
    }
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        // Corrupt snapshot → load fails → the index falls back to a scan-based
        // rebuild. The query must still succeed and return k rows (graceful
        // degradation); exact recall on the fallback path depends on the
        // disk-scan rebuild of ARRAY columns.
        auto ids = knn_ids(dispatcher, q);
        REQUIRE(ids.size() == 3);

        // DROP INDEX removes the snapshot files.
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, "DROP INDEX TestDatabase.Vectors.idx_emb;")->is_success());
        }
        REQUIRE_FALSE(graph_exists());
        REQUIRE_FALSE(meta_exists());
    }
}

// A wrong-dimension query must error on both paths.
TEST_CASE("integration::cpp::vector_search::dimension_mismatch_errors") {
    auto config = test_create_config("/tmp/test_vector_search/dim_mismatch");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 3); }

    // Stored vectors are 3-dimensional.
    for (int i = 0; i < 5; ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(i, {i * 0.1, 0.0, 1.0}))->is_success());
    }

    // Exact path (no index yet): a 2-d query must error.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 3;");
        REQUIRE_FALSE(cur->is_success());
    }
    // Matching dimension still works.
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    // Index path: build an HNSW index, then a wrong-dimension query must also error.
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0, 0.0, 0.0]' LIMIT 3;");
        REQUIRE_FALSE(cur->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0, 0.0]' LIMIT 3;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
}

// VACUUM rebuilds the graph with compacted ids; skipped when vacuum is a no-op.
TEST_CASE("integration::cpp::vector_search::vacuum_rebuilds_index") {
    const bool disk_on = GENERATE(false, true);
    CAPTURE(disk_on);
    auto config = test_create_config(std::string("/tmp/test_vector_search/vacuum_rebuild_") +
                                     (disk_on ? "disk" : "mem"));
    test_clear_directory(config);
    config.disk.on = disk_on;
    config.wal.on = disk_on;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
    { create_vectors_table(dispatcher, 2); }

    // 10 rows on a line: id i at x = i.
    for (int i = 0; i < 10; ++i) {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, make_insert_sql(i, {i * 1.0, 0.0}))->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(s,
                                  "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding vector_l2_ops);")
                    ->is_success());
    }

    // Remove the 5 nearest to the origin, then VACUUM (row ids get compacted).
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "DELETE FROM TestDatabase.Vectors WHERE id < 5;")->is_success());
    }
    {
        auto s = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(s, "VACUUM;")->is_success());
    }

    // kNN must return exactly the surviving rows (ids 5..9), nearest-first.
    auto s = otterbrix::session_id_t();
    auto cur = dispatcher->execute_sql(
        s, "SELECT * FROM TestDatabase.Vectors ORDER BY embedding <-> '[0.0, 0.0]' LIMIT 10;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 5);

    auto id_col = cur->chunk_data().column_index("id");
    REQUIRE(id_col != static_cast<size_t>(-1));
    std::vector<int64_t> ids;
    for (std::size_t r = 0; r < cur->size(); ++r) {
        ids.push_back(cur->value(static_cast<uint64_t>(id_col), static_cast<uint64_t>(r)).value<int64_t>());
    }
    std::sort(ids.begin(), ids.end());
    REQUIRE(ids == std::vector<int64_t>({5, 6, 7, 8, 9}));
}

TEST_CASE("integration::cpp::vector_search::ranking_consistency") {
    for (const std::string opclass : {"vector_l2_ops", "vector_cosine_ops", "vector_ip_ops"}) {
        auto config = test_create_config("/tmp/test_vector_search/rank_consistency_" + opclass);
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        { auto s = otterbrix::session_id_t(); dispatcher->execute_sql(s, "CREATE DATABASE " + std::string(database_name) + ";"); }
        { create_vectors_table(dispatcher, 2); }
        std::vector<std::vector<double>> vectors = {{1.0, 0.0}, {0.5, 0.5}, {-1.0, 0.2}, {3.0, 4.0}};
        for (std::size_t i = 0; i < vectors.size(); ++i) {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(s, make_insert_sql(static_cast<int>(i), vectors[i]))->is_success());
        }

        const std::string op = opclass == "vector_l2_ops" ? "<->" : (opclass == "vector_cosine_ops" ? "<=>" : "<#>");
        const std::string q = "SELECT * FROM TestDatabase.Vectors ORDER BY embedding " + op +
                              " '[1.0, 0.1]' LIMIT 4;";

        auto ordered_ids = [&](components::cursor::cursor_t_ptr cur) {
            std::vector<int64_t> ids;
            auto id_col = cur->chunk_data().column_index("id");
            REQUIRE(id_col != static_cast<size_t>(-1));
            for (std::size_t r = 0; r < cur->size(); ++r) {
                ids.push_back(cur->value(static_cast<uint64_t>(id_col), static_cast<uint64_t>(r)).value<int64_t>());
            }
            return ids;
        };

        // Exact path (no index yet).
        std::vector<int64_t> exact;
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, q);
            REQUIRE(cur->is_success());
            exact = ordered_ids(cur);
            REQUIRE(exact.size() == 4);
        }
        // Index path.
        {
            auto s = otterbrix::session_id_t();
            REQUIRE(dispatcher
                        ->execute_sql(s,
                                      "CREATE INDEX idx_emb ON TestDatabase.Vectors USING hnsw (embedding " +
                                          opclass + ");")
                        ->is_success());
        }
        {
            auto s = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(s, q);
            REQUIRE(cur->is_success());
            INFO(opclass);
            REQUIRE(ordered_ids(cur) == exact);
        }
    }
}
