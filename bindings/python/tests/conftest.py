"""Pytest configuration for anyio backend testing.

Configures tests to run on:
- asyncio (standard library)
- asyncio + uvloop (fast event loop, Unix only)
- trio (alternative async framework)
"""

import sys

import pytest


def pytest_addoption(parser):
    """Add --uvloop option to enable uvloop testing."""
    parser.addoption(
        "--uvloop",
        action="store_true",
        default=False,
        help="Include uvloop in asyncio backend tests",
    )


@pytest.fixture(
    params=[
        pytest.param("asyncio", id="asyncio"),
        pytest.param("trio", id="trio"),
    ]
    + ([pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")] if sys.platform != "win32" else [])
)
def anyio_backend(request):
    """Fixture that parameterizes tests across all anyio backends.

    This overrides the default anyio_backend fixture to include uvloop.
    """
    # Only include uvloop if requested and available
    backend = request.param
    if isinstance(backend, tuple) and backend[1].get("use_uvloop"):
        try:
            import uvloop  # noqa: F401
        except ImportError:
            pytest.skip("uvloop not available")
    return backend
