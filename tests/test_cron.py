"""Unit tests for cron parsing, matching, and next-fire computation."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ontologia.session import _compile_cron, _cron_matches, _next_fire, _parse_cron_field


class TestParseCronField:
    """Test _parse_cron_field for individual cron field parsing."""

    def test_wildcard(self):
        assert _parse_cron_field("*", 0, 59) == set(range(0, 60))

    def test_single_value(self):
        assert _parse_cron_field("5", 0, 59) == {5}

    def test_range(self):
        assert _parse_cron_field("1-5", 0, 59) == {1, 2, 3, 4, 5}

    def test_step(self):
        assert _parse_cron_field("*/15", 0, 59) == {0, 15, 30, 45}

    def test_comma_separated(self):
        assert _parse_cron_field("1,15,30", 0, 59) == {1, 15, 30}

    def test_range_and_single(self):
        assert _parse_cron_field("1-3,10", 0, 59) == {1, 2, 3, 10}

    def test_out_of_bounds_raises(self):
        with pytest.raises(ValueError, match="out of bounds"):
            _parse_cron_field("60", 0, 59)

    def test_out_of_bounds_low_raises(self):
        # "-1" is parsed as a range with empty lo, which raises ValueError
        with pytest.raises(ValueError):
            _parse_cron_field("-1", 0, 59)

    def test_invalid_range_raises(self):
        with pytest.raises(ValueError, match="invalid cron range"):
            _parse_cron_field("10-5", 0, 59)

    def test_zero_step_raises(self):
        with pytest.raises(ValueError, match="invalid cron step"):
            _parse_cron_field("*/0", 0, 59)

    def test_range_out_of_bounds_raises(self):
        with pytest.raises(ValueError, match="cron range out of bounds"):
            _parse_cron_field("0-32", 1, 31)


class TestCompileCron:
    """Test _compile_cron for full cron expression parsing."""

    def test_every_minute(self):
        spec = _compile_cron("* * * * *")
        assert spec.minutes == set(range(0, 60))
        assert spec.hours == set(range(0, 24))
        assert spec.days == set(range(1, 32))
        assert spec.months == set(range(1, 13))
        assert spec.weekdays == set(range(0, 8))  # 0-7

    def test_specific_time(self):
        spec = _compile_cron("30 14 * * *")
        assert spec.minutes == {30}
        assert spec.hours == {14}

    def test_wrong_field_count_raises(self):
        with pytest.raises(ValueError, match="must have 5 fields"):
            _compile_cron("* * *")

        with pytest.raises(ValueError, match="must have 5 fields"):
            _compile_cron("* * * * * *")

    def test_every_5_minutes(self):
        spec = _compile_cron("*/5 * * * *")
        assert spec.minutes == {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}


class TestCronMatches:
    """Test _cron_matches for cron schedule matching."""

    def test_every_minute_matches(self):
        spec = _compile_cron("* * * * *")
        dt = datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True

    def test_specific_minute_matches(self):
        spec = _compile_cron("30 * * * *")
        dt = datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True

    def test_specific_minute_no_match(self):
        spec = _compile_cron("15 * * * *")
        dt = datetime(2024, 6, 15, 10, 30, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is False

    def test_specific_hour_matches(self):
        spec = _compile_cron("0 14 * * *")
        dt = datetime(2024, 6, 15, 14, 0, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True

    def test_weekday_sunday(self):
        # 2024-06-16 is a Sunday
        spec = _compile_cron("0 0 * * 0")
        dt = datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True

    def test_weekday_sunday_as_7(self):
        # Cron allows 7 for Sunday too
        spec = _compile_cron("0 0 * * 7")
        dt = datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True

    def test_weekday_monday(self):
        # 2024-06-17 is a Monday
        spec = _compile_cron("0 0 * * 1")
        dt = datetime(2024, 6, 17, 0, 0, tzinfo=timezone.utc)
        assert _cron_matches(spec, dt) is True


class TestNextFire:
    """Test _next_fire for computing next cron trigger time."""

    def test_next_minute(self):
        spec = _compile_cron("* * * * *")
        after = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _next_fire(spec, after)
        assert result == datetime(2024, 6, 15, 10, 31, 0, tzinfo=timezone.utc)

    def test_next_specific_minute(self):
        spec = _compile_cron("45 * * * *")
        after = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _next_fire(spec, after)
        assert result == datetime(2024, 6, 15, 10, 45, 0, tzinfo=timezone.utc)

    def test_next_hour_rollover(self):
        spec = _compile_cron("0 * * * *")
        after = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _next_fire(spec, after)
        assert result == datetime(2024, 6, 15, 11, 0, 0, tzinfo=timezone.utc)

    def test_next_day_rollover(self):
        spec = _compile_cron("0 0 * * *")
        after = datetime(2024, 6, 15, 23, 30, 0, tzinfo=timezone.utc)
        result = _next_fire(spec, after)
        assert result == datetime(2024, 6, 16, 0, 0, 0, tzinfo=timezone.utc)

    def test_specific_day_of_week(self):
        # Find next Monday (weekday 1) after 2024-06-15 (Saturday)
        spec = _compile_cron("0 9 * * 1")
        after = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        result = _next_fire(spec, after)
        # 2024-06-17 is Monday
        assert result == datetime(2024, 6, 17, 9, 0, 0, tzinfo=timezone.utc)
