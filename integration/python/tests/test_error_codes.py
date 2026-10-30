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
    ):
        assert client.execute(statement).get_error()[0] == name, statement


def test_a_closed_client_names_connection_closed(tmp_path):
    client = Client(str(tmp_path / "closed_db"))
    client.close()
    assert client.execute("SELECT 1;").get_error() == ("connection_closed", "the session is closed")
