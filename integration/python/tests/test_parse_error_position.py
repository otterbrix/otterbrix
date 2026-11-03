import pytest

import otterbrix
from otterbrix import Client


def test_a_parse_error_carries_its_position(tmp_path):
    connection = otterbrix.connect(str(tmp_path / "parse_error_position_db"))
    try:
        with pytest.raises(RuntimeError) as syntax_error:
            connection.execute("SELECT * FROM t WHERE a = = 1;")
        assert 'syntax error at or near "="' in str(syntax_error.value)
        assert syntax_error.value.position == 27

        with pytest.raises(RuntimeError) as unterminated:
            connection.execute("SELECT 'жж")
        assert unterminated.value.position == 8

        with pytest.raises(RuntimeError) as missing_table:
            connection.execute("SELECT * FROM no_such_db.no_such_table;")
        assert missing_table.value.position is None
    finally:
        connection.close()


def test_a_cursor_reports_its_error_position(tmp_path):
    client = Client(str(tmp_path / "cursor_error_position_db"))
    misspelled = client.execute("SELEC 1;")
    assert misspelled.is_error()
    assert misspelled.get_error() == ("sql_parse_error", 'syntax error at or near "SELEC"')
    assert misspelled.error_position == 1

    missing = client.execute("SELECT * FROM no_such_db.no_such_table;")
    assert missing.is_error()
    assert missing.error_position is None

    assert client.execute("SELECT 1;").error_position is None
