import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.mongo)


@pytest.fixture(autouse=True)
def _db(clean_db):
    yield
