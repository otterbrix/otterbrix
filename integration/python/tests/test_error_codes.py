from otterbrix import Client


def test_a_cursor_names_the_postgresql_code_of_its_error(tmp_path):
    client = Client(str(tmp_path / "error_codes_db"))
    for statement in (
        "CREATE SCHEMA s;",
        "CREATE TABLE s.t (id BIGINT, v BIGINT);",
        "ALTER TABLE s.t ADD CONSTRAINT pk_t PRIMARY KEY (id);",
        "ALTER TABLE s.t ADD CONSTRAINT v_positive CHECK (v > 0);",
    ):
        assert not client.execute(statement).is_error(), statement

    assert client.execute("ALTER TABLE s.t ADD COLUMN v BIGINT;").get_error()[0] == "duplicate_column"
    assert client.execute("ALTER TABLE s.t DROP COLUMN nosuch;").get_error()[0] == "undefined_column"
    missing = client.execute("INSERT INTO s.t (id, v) VALUES (1, 1) ON CONFLICT ON CONSTRAINT missing DO NOTHING;")
    assert missing.get_error()[0] == "undefined_object"
    check = client.execute("INSERT INTO s.t (id, v) VALUES (1, 1) ON CONFLICT ON CONSTRAINT v_positive DO NOTHING;")
    assert check.get_error()[0] == "wrong_object_type"
    for statement, name in (
        ("SELECT 1 / 0;", "division_by_zero"),
        ("INSERT INTO s.t (id, v) VALUES (1, 1), (1, 2);", "unique_violation"),
        ("INSERT INTO s.t (id, v) VALUES (2, -1);", "check_violation"),
        ("SAVEPOINT a;", "no_active_sql_transaction"),
        ("SELECT nosuch(1);", "unrecognized_function"),
        ("SET autocommit = maybe;", "invalid_parameter"),
        ("EXECUTE nope;", "undefined_prepared_statement"),
    ):
        assert client.execute(statement).get_error()[0] == name, statement

    assert not client.execute("PREPARE q AS SELECT v FROM s.t WHERE id = $1;").is_error()
    for statement, name in (
        ("PREPARE q AS SELECT 1;", "duplicate_prepared_statement"),
        ("EXECUTE q($1);", "undefined_parameter"),
        ("EXECUTE q(row_number() OVER ());", "windowing_error"),
    ):
        assert client.execute(statement).get_error()[0] == name, statement

    assert client.execute("DECLARE c CURSOR FOR SELECT v FROM s.t;").get_error()[0] == "no_active_sql_transaction"
    for statement, name in (
        ("FETCH 1 FROM nope;", "undefined_cursor"),
        ("DECLARE c CURSOR FOR SELECT v FROM s.t;", "duplicate_cursor"),
        ("FETCH 0 FROM c;", "object_not_in_prerequisite_state"),
        ("DROP TABLE s.t;", "object_in_use"),
    ):
        assert not client.execute("BEGIN;").is_error()
        assert not client.execute("INSERT INTO s.t (id, v) VALUES (7, 7);").is_error()
        assert not client.execute("DECLARE c CURSOR FOR SELECT v FROM s.t;").is_error()
        assert not client.execute("FETCH 1 FROM c;").is_error()
        assert client.execute(statement).get_error()[0] == name, statement
        assert not client.execute("ROLLBACK;").is_error()


def test_a_closed_client_names_connection_closed(tmp_path):
    client = Client(str(tmp_path / "closed_db"))
    client.close()
    assert client.execute("SELECT 1;").get_error() == ("connection_closed", "the session is closed")
