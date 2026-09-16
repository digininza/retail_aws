from datetime import date

import pandas as pd

from src.dimensional_model.scd_type2 import apply_scd2, empty_dim_customer


def test_new_customer_inserted_as_current():
    dim = empty_dim_customer()
    incoming = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                               "segment": "LOYALTY", "city": "Delhi"}])
    result = apply_scd2(dim, incoming, as_of_date=date(2026, 1, 1))
    assert result.inserted_new == 1
    assert len(result.dim) == 1
    assert result.dim.iloc[0]["is_current"] == True  # noqa: E712


def test_no_change_is_a_noop():
    dim = empty_dim_customer()
    incoming = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                               "segment": "LOYALTY", "city": "Delhi"}])
    seeded = apply_scd2(dim, incoming, as_of_date=date(2026, 1, 1)).dim
    result = apply_scd2(seeded, incoming, as_of_date=date(2026, 2, 1))
    assert result.unchanged == 1
    assert result.inserted_changed == 0
    assert len(result.dim) == 1  # no new row


def test_attribute_change_expires_and_inserts_new_row():
    dim = empty_dim_customer()
    incoming = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                               "segment": "LOYALTY", "city": "Delhi"}])
    seeded = apply_scd2(dim, incoming, as_of_date=date(2026, 1, 1)).dim

    changed = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                              "segment": "LOYALTY", "city": "Gurgaon"}])
    result = apply_scd2(seeded, changed, as_of_date=date(2026, 3, 1))

    assert result.expired == 1
    assert result.inserted_changed == 1
    current_rows = result.dim[result.dim["is_current"] == True]  # noqa: E712
    assert len(current_rows) == 1
    assert current_rows.iloc[0]["city"] == "Gurgaon"

    expired_rows = result.dim[result.dim["is_current"] == False]  # noqa: E712
    assert len(expired_rows) == 1
    assert expired_rows.iloc[0]["effective_end_date"] == "2026-02-28"


def test_rerun_of_same_batch_is_idempotent():
    """Applying the exact same incoming batch twice in a row must not create
    a second history row — this is what makes SCD2 processing safe to retry."""
    dim = empty_dim_customer()
    incoming = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                               "segment": "LOYALTY", "city": "Gurgaon"}])
    seeded = apply_scd2(dim, incoming, as_of_date=date(2026, 1, 1)).dim
    rerun_result = apply_scd2(seeded, incoming, as_of_date=date(2026, 1, 1))
    assert rerun_result.unchanged == 1
    assert len(rerun_result.dim) == 1


def test_surrogate_key_differs_across_versions_of_same_business_key():
    dim = empty_dim_customer()
    v1 = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                         "segment": "LOYALTY", "city": "Delhi"}])
    seeded = apply_scd2(dim, v1, as_of_date=date(2026, 1, 1)).dim
    v2 = pd.DataFrame([{"customer_id": "C1", "customer_name": "Asha", "email": "a@x.com",
                         "segment": "LOYALTY", "city": "Gurgaon"}])
    result = apply_scd2(seeded, v2, as_of_date=date(2026, 3, 1))
    surrogate_keys = set(result.dim[result.dim["customer_id"] == "C1"]["customer_sk"])
    assert len(surrogate_keys) == 2  # two distinct versions, two distinct surrogate keys
