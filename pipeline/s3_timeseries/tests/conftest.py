import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--aws",
        action="store_true",
        default=False,
        help="run tests that require AWS S3 access",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "aws: mark test as requiring AWS S3 access",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--aws"):
        skip_aws = pytest.mark.skip(reason="need --aws option to run")
        for item in items:
            if "aws" in item.keywords:
                item.add_marker(skip_aws)
