"""The runnable examples under docs/examples must keep working (they are what the docs show)."""

import importlib.util
import pathlib
import sys

import pytest

pytestmark = pytest.mark.mongo
EXAMPLES = pathlib.Path(__file__).resolve().parent.parent / "docs" / "examples"


def load(name):
    sys.path.insert(0, str(EXAMPLES))
    try:
        spec = importlib.util.spec_from_file_location(f"docs_example_{name}", EXAMPLES / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(EXAMPLES))


def test_quickstart_example(clean_db):
    assert load("quickstart").run()


def test_complex_queries_example(clean_db):
    assert load("complex_queries").run()
