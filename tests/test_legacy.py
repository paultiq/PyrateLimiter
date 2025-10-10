"""Test suite for legacy Limiter API compatibility"""

import warnings

import pytest

from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate


def test_new_api_no_warnings():
    bucket = InMemoryBucket([Rate(10, Duration.SECOND)])

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        limiter = Limiter(bucket)

        assert len(w) == 0


def test_new_decorator_syntax():
    bucket = InMemoryBucket([Rate(10, Duration.SECOND)])
    limiter = Limiter(bucket)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        @limiter.as_decorator(name="test", weight=1)
        def test_func():
            return "success"

        assert len(w) == 0

        result = test_func()
        assert result == "success"


def test_legacy_decorator_with_call_syntax():
    bucket = InMemoryBucket([Rate(10, Duration.SECOND)])
    limiter = Limiter(bucket)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        @limiter.as_decorator()(lambda: ("test", 1))
        def test_func():
            return "success"

        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "without arguments is deprecated" in str(w[0].message)

        result = test_func()
        assert result == "success"


def test_legacy_decorator_with_mapping_arg():
    bucket = InMemoryBucket([Rate(10, Duration.SECOND)])
    limiter = Limiter(bucket)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        @limiter.as_decorator(lambda: ("test", 1))
        def test_func():
            return "success"

        # Should produce one deprecation warning
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "mapping function directly" in str(w[0].message)

        # Should still work
        result = test_func()
        assert result == "success"


def test_decorator_with_invalid_positional_arg():
    bucket = InMemoryBucket([Rate(10, Duration.SECOND)])
    limiter = Limiter(bucket)

    with pytest.raises(TypeError, match="first positional argument for legacy mapping functions"):
        @limiter.as_decorator("test")
        def test_func():
            return "success"
