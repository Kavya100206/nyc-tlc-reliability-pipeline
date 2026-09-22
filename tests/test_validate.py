"""
tests/test_validate.py — Unit tests for src/validate.py

Strategy: one synthetic DataFrame per rule, containing exactly one violation.
Each test confirms that:
  - The expected flag is set to True on the violating row
  - All other rows in the DataFrame have that flag False
  - A clean baseline row passes all rules

No dependency on the real 3.5M-row Parquet — tests run fast and are fully
self-contained. This also makes schema drift immediately visible: if the
required columns change, these tests fail before any data is downloaded.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Allow imports from repo root when running pytest from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.validate import (
    DURATION_MAX_MINUTES,
    DURATION_MIN_MINUTES,
    DISTANCE_MAX_MILES,
    FARE_MAX,
    FARE_PER_MILE_MAX,
    FARE_PER_MILE_MIN,
    UNKNOWN_LOCATION_IDS,
    apply_validation_rules,
    build_report,
    profile_dataframe,
)

# ── Test fixtures ─────────────────────────────────────────────────────────────
MONTH = "2025-01"
# A representative set of valid zone IDs (1–263 minus 264, 265)
ZONE_IDS = set(range(1, 264))


def make_trip(**overrides) -> pd.DataFrame:
    """
    Return a single-row DataFrame of a clean, valid January 2025 trip.

    All values satisfy every hard and soft validation rule. Use **overrides
    to introduce exactly one violation per test.
    """
    base = {
        "VendorID": 2,
        "tpep_pickup_datetime": pd.Timestamp("2025-01-15 10:00:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2025-01-15 10:30:00"),  # 30 min trip
        "passenger_count": 2.0,
        "trip_distance": 5.0,        # 5 miles — well within bounds
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,          # Midtown Center — valid zone
        "DOLocationID": 132,          # JFK Airport — valid zone
        "payment_type": 1,            # Credit card
        "fare_amount": 20.00,         # $4/mile — within bounds
        "extra": 0.50,
        "mta_tax": 0.50,
        "tip_amount": 4.00,
        "tolls_amount": 0.00,
        "improvement_surcharge": 0.30,
        "total_amount": 25.30,
        "congestion_surcharge": 2.50,
        "airport_fee": 0.00,
    }
    base.update(overrides)
    return pd.DataFrame([base])


def validate(df: pd.DataFrame):
    """Helper: run apply_validation_rules and return (df_with_flags, hard_cols, soft_cols)."""
    return apply_validation_rules(df.copy(), ZONE_IDS, MONTH)


# ── Baseline: clean trip passes all rules ─────────────────────────────────────
class TestBaseline:
    def test_clean_trip_has_no_flags(self):
        df, hard, soft = validate(make_trip())
        assert df["validation_flags"].iloc[0] == "", (
            f"Expected no flags on a clean trip, got: '{df['validation_flags'].iloc[0]}'"
        )

    def test_all_hard_flags_false_on_clean_trip(self):
        df, hard, soft = validate(make_trip())
        for col in hard:
            assert not df[col].iloc[0], f"Hard flag {col} should be False on a clean trip"

    def test_all_soft_flags_false_on_clean_trip(self):
        df, hard, soft = validate(make_trip())
        for col in soft:
            assert not df[col].iloc[0], f"Soft flag {col} should be False on a clean trip"


# ── H1: Out of month ──────────────────────────────────────────────────────────
class TestH1OutOfMonth:
    def test_dec_trip_flagged(self):
        """Trip in December 2024 (day before Jan) must be flagged out_of_month."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2024-12-31 23:59:00"),
            tpep_dropoff_datetime=pd.Timestamp("2025-01-01 00:30:00"),
        ))
        assert df["flag_out_of_month"].iloc[0], "Dec 31 pickup should be flagged out_of_month"

    def test_feb_trip_flagged(self):
        """Trip in February 2025 (day after Jan) must be flagged out_of_month."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-02-01 00:00:01"),
            tpep_dropoff_datetime=pd.Timestamp("2025-02-01 00:30:00"),
        ))
        assert df["flag_out_of_month"].iloc[0], "Feb 1 pickup should be flagged out_of_month"

    def test_jan_1_midnight_not_flagged(self):
        """Exact start of January must NOT be flagged."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-01-01 00:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2025-01-01 00:30:00"),
        ))
        assert not df["flag_out_of_month"].iloc[0], "Jan 1 00:00:00 should be within month"

    def test_jan_31_last_second_not_flagged(self):
        """Last second of January must NOT be flagged."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-01-31 23:59:59"),
            tpep_dropoff_datetime=pd.Timestamp("2025-02-01 00:30:00"),
        ))
        assert not df["flag_out_of_month"].iloc[0], "Jan 31 23:59:59 should be within month"


# ── H2: Temporal invalid ──────────────────────────────────────────────────────
class TestH2TemporalInvalid:
    def test_dropoff_before_pickup_flagged(self):
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-01-15 10:30:00"),
            tpep_dropoff_datetime=pd.Timestamp("2025-01-15 10:00:00"),  # dropoff before pickup
        ))
        assert df["flag_temporal_invalid"].iloc[0]

    def test_equal_timestamps_flagged(self):
        """Pickup == dropoff is also invalid (zero-duration, not a genuine trip)."""
        ts = pd.Timestamp("2025-01-15 10:00:00")
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=ts,
            tpep_dropoff_datetime=ts,
        ))
        assert df["flag_temporal_invalid"].iloc[0]

    def test_valid_temporal_not_flagged(self):
        df, hard, _ = validate(make_trip())
        assert not df["flag_temporal_invalid"].iloc[0]


# ── H3: Duration invalid ──────────────────────────────────────────────────────
class TestH3DurationInvalid:
    def test_zero_duration_flagged(self):
        """Duration of 0 minutes should be flagged (also caught by H2, but independently valid)."""
        ts = pd.Timestamp("2025-01-15 10:00:00")
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=ts,
            tpep_dropoff_datetime=ts + pd.Timedelta(seconds=30),  # 0.5 min < 1 min
        ))
        assert df["flag_duration_invalid"].iloc[0]

    def test_too_long_flagged(self):
        """Trip longer than 6 hours (360 min) must be flagged."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-01-15 08:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2025-01-15 15:01:00"),  # 421 min
        ))
        assert df["flag_duration_invalid"].iloc[0]

    def test_exactly_360_minutes_is_valid(self):
        """Exactly 360 minutes (the boundary) should NOT be flagged."""
        df, hard, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2025-01-15 08:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2025-01-15 14:00:00"),  # exactly 360 min
        ))
        assert not df["flag_duration_invalid"].iloc[0]

    def test_normal_30min_trip_not_flagged(self):
        df, hard, _ = validate(make_trip())
        assert not df["flag_duration_invalid"].iloc[0]


# ── H4: Distance invalid ──────────────────────────────────────────────────────
class TestH4DistanceInvalid:
    def test_zero_distance_flagged(self):
        df, hard, _ = validate(make_trip(trip_distance=0.0))
        assert df["flag_distance_invalid"].iloc[0]

    def test_negative_distance_flagged(self):
        df, hard, _ = validate(make_trip(trip_distance=-1.0))
        assert df["flag_distance_invalid"].iloc[0]

    def test_over_100_miles_flagged(self):
        df, hard, _ = validate(make_trip(trip_distance=101.0))
        assert df["flag_distance_invalid"].iloc[0]

    def test_exactly_100_miles_is_valid(self):
        df, hard, _ = validate(make_trip(trip_distance=100.0))
        assert not df["flag_distance_invalid"].iloc[0]

    def test_normal_5_miles_not_flagged(self):
        df, hard, _ = validate(make_trip())
        assert not df["flag_distance_invalid"].iloc[0]


# ── H5: Fare invalid ─────────────────────────────────────────────────────────
class TestH5FareInvalid:
    def test_negative_fare_flagged(self):
        df, hard, _ = validate(make_trip(fare_amount=-5.0))
        assert df["flag_fare_invalid"].iloc[0]

    def test_zero_fare_with_cash_payment_flagged(self):
        """$0 fare on a cash trip (payment_type=2) is not a legitimate completed fare."""
        df, hard, _ = validate(make_trip(fare_amount=0.0, payment_type=2))
        assert df["flag_fare_invalid"].iloc[0]

    def test_zero_fare_with_credit_payment_flagged(self):
        """$0 fare on a credit card trip (payment_type=1) is invalid."""
        df, hard, _ = validate(make_trip(fare_amount=0.0, payment_type=1))
        assert df["flag_fare_invalid"].iloc[0]

    def test_zero_fare_with_no_charge_not_flagged(self):
        """$0 fare with payment_type=3 (No charge) is legitimate business logic."""
        df, hard, _ = validate(make_trip(fare_amount=0.0, payment_type=3))
        assert not df["flag_fare_invalid"].iloc[0]

    def test_zero_fare_with_voided_not_flagged(self):
        """$0 fare with payment_type=6 (Voided) is legitimate business logic."""
        df, hard, _ = validate(make_trip(fare_amount=0.0, payment_type=6))
        assert not df["flag_fare_invalid"].iloc[0]

    def test_fare_above_500_flagged(self):
        df, hard, _ = validate(make_trip(fare_amount=501.0))
        assert df["flag_fare_invalid"].iloc[0]

    def test_fare_exactly_500_not_flagged(self):
        df, hard, _ = validate(make_trip(fare_amount=500.0))
        assert not df["flag_fare_invalid"].iloc[0]

    def test_normal_fare_not_flagged(self):
        df, hard, _ = validate(make_trip())
        assert not df["flag_fare_invalid"].iloc[0]


# ── H6: Location ──────────────────────────────────────────────────────────────
class TestH6Location:
    def test_unknown_location_264_flagged(self):
        """LocationID 264 (Unknown) should be flagged location_unknown, not location_invalid."""
        df, hard, _ = validate(make_trip(PULocationID=264))
        assert df["flag_location_unknown"].iloc[0]
        assert not df["flag_location_invalid"].iloc[0]

    def test_unknown_location_265_flagged(self):
        df, hard, _ = validate(make_trip(DOLocationID=265))
        assert df["flag_location_unknown"].iloc[0]
        assert not df["flag_location_invalid"].iloc[0]

    def test_out_of_range_location_flagged_as_invalid(self):
        """A LocationID entirely outside the known range should be location_invalid."""
        df, hard, _ = validate(make_trip(PULocationID=999))
        assert df["flag_location_invalid"].iloc[0]
        assert not df["flag_location_unknown"].iloc[0]

    def test_valid_location_not_flagged(self):
        df, hard, _ = validate(make_trip())
        assert not df["flag_location_invalid"].iloc[0]
        assert not df["flag_location_unknown"].iloc[0]


# ── S1: Fare-per-mile outlier ─────────────────────────────────────────────────
class TestS1FarePerMileOutlier:
    def test_suspiciously_low_fare_per_mile_flagged(self):
        """$1/mile is below the lower bound ($1.50/mile)."""
        df, _, soft = validate(make_trip(trip_distance=10.0, fare_amount=10.0))  # $1/mile
        assert df["flag_fare_per_mile_outlier"].iloc[0]

    def test_suspiciously_high_fare_per_mile_flagged(self):
        """$60/mile is above the upper bound ($50/mile)."""
        df, _, soft = validate(make_trip(trip_distance=0.5, fare_amount=30.0))  # $60/mile
        assert df["flag_fare_per_mile_outlier"].iloc[0]

    def test_normal_fare_per_mile_not_flagged(self):
        """$4/mile (20 / 5) is within [$1.50, $50] — clean."""
        df, _, soft = validate(make_trip())  # $20 / 5 miles = $4/mile
        assert not df["flag_fare_per_mile_outlier"].iloc[0]

    def test_zero_distance_does_not_create_outlier_flag(self):
        """
        Zero-distance trips are already caught by H4 (distance_invalid).
        They must NOT also be flagged as fare_per_mile_outlier — that would
        conflate two separate issues and inflate the soft-flag count.
        """
        df, _, soft = validate(make_trip(trip_distance=0.0, fare_amount=10.0))
        assert not df["flag_fare_per_mile_outlier"].iloc[0]


# ── S2: Passenger count suspect ───────────────────────────────────────────────
class TestS2PassengerCount:
    def test_passenger_count_7_flagged(self):
        df, _, soft = validate(make_trip(passenger_count=7.0))
        assert df["flag_passenger_count_suspect"].iloc[0]

    def test_passenger_count_null_not_flagged(self):
        """Null passenger_count is explicitly allowed per TLC data dictionary."""
        import numpy as np
        df, _, soft = validate(make_trip(passenger_count=np.nan))
        assert not df["flag_passenger_count_suspect"].iloc[0]

    def test_passenger_count_6_not_flagged(self):
        """6 passengers is the max for accessible vehicles — valid."""
        df, _, soft = validate(make_trip(passenger_count=6.0))
        assert not df["flag_passenger_count_suspect"].iloc[0]


# ── S3: Zero distance with fare ───────────────────────────────────────────────
class TestS3ZeroDistanceWithFare:
    def test_zero_distance_with_fare_flagged(self):
        df, _, soft = validate(make_trip(trip_distance=0.0, fare_amount=5.0))
        assert df["flag_zero_distance_with_fare"].iloc[0]

    def test_zero_distance_zero_fare_not_flagged(self):
        """$0 fare and $0 distance — still caught by H4 (distance_invalid) but
        NOT flagged as zero_distance_with_fare (no fare discrepancy)."""
        df, _, soft = validate(make_trip(trip_distance=0.0, fare_amount=0.0))
        assert not df["flag_zero_distance_with_fare"].iloc[0]


# ── Validation flags string ───────────────────────────────────────────────────
class TestValidationFlagsString:
    def test_clean_trip_has_empty_flags_string(self):
        df, _, _ = validate(make_trip())
        assert df["validation_flags"].iloc[0] == ""

    def test_single_violation_produces_correct_flag_name(self):
        df, _, _ = validate(make_trip(trip_distance=0.0))
        flags = df["validation_flags"].iloc[0].split("|")
        assert "distance_invalid" in flags

    def test_multiple_violations_all_appear_in_flags_string(self):
        """A trip with both out_of_month and negative fare should have both flags."""
        df, _, _ = validate(make_trip(
            tpep_pickup_datetime=pd.Timestamp("2024-12-31 10:00:00"),
            tpep_dropoff_datetime=pd.Timestamp("2024-12-31 10:30:00"),
            fare_amount=-10.0,
        ))
        flags = set(df["validation_flags"].iloc[0].split("|"))
        assert "out_of_month" in flags
        assert "fare_invalid" in flags


# ── Profile schema ────────────────────────────────────────────────────────────
class TestProfileSchema:
    def test_profile_has_all_trip_columns(self):
        df = make_trip()
        profile = profile_dataframe(df)
        for col in df.columns:
            assert col in profile, f"Column '{col}' missing from profile"

    def test_profile_has_required_keys(self):
        df = make_trip()
        profile = profile_dataframe(df)
        for col, info in profile.items():
            assert "dtype" in info
            assert "null_count" in info
            assert "null_pct" in info


# ── Report schema ─────────────────────────────────────────────────────────────
class TestReportSchema:
    def test_report_has_required_top_level_keys(self):
        df, hard, soft = validate(make_trip())
        profile = profile_dataframe(df)
        report = build_report(df, profile, MONTH, hard, soft, elapsed=0.1)

        required_keys = {
            "run_at", "month", "elapsed_seconds",
            "counts", "flag_counts", "profile",
            "known_unknown_assumption_limitation",
        }
        assert required_keys.issubset(report.keys()), (
            f"Missing keys: {required_keys - set(report.keys())}"
        )

    def test_report_counts_are_consistent(self):
        """rows_hard_flagged + rows_valid_out must equal rows_in_raw."""
        df, hard, soft = validate(make_trip())
        profile = profile_dataframe(df)
        report = build_report(df, profile, MONTH, hard, soft, elapsed=0.1)
        c = report["counts"]
        assert c["rows_hard_flagged"] + c["rows_valid_out"] == c["rows_in_raw"]

    def test_kual_entries_have_required_keys(self):
        df, hard, soft = validate(make_trip())
        profile = profile_dataframe(df)
        report = build_report(df, profile, MONTH, hard, soft, elapsed=0.1)
        for entry in report["known_unknown_assumption_limitation"]:
            assert "category" in entry
            assert "entry" in entry
            assert entry["category"] in {"KNOWN", "UNKNOWN", "ASSUMPTION", "LIMITATION"}

    def test_all_hard_flags_appear_in_flag_counts(self):
        df, hard, soft = validate(make_trip())
        profile = profile_dataframe(df)
        report = build_report(df, profile, MONTH, hard, soft, elapsed=0.1)
        for col in hard:
            flag_name = col.replace("flag_", "")
            assert flag_name in report["flag_counts"], (
                f"Hard flag '{flag_name}' missing from report flag_counts"
            )
