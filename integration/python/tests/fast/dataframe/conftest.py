import pytest

import otterbrix


@pytest.fixture(scope="session")
def conn():
    connection = otterbrix.connect("default")
    yield connection
    connection.close()
