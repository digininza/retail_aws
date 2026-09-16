import pandas as pd

from src.cdc.merge import merge_upsert


def _empty(cols):
    return pd.DataFrame(columns=cols)


def test_insert_when_not_found():
    target = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}])
    inserts = pd.DataFrame([{"customer_id": "C2", "name": "Rohan"}])
    result = merge_upsert(target, inserts, _empty(["customer_id", "name"]), _empty(["customer_id"]), "customer_id")
    assert result.inserted == 1
    assert set(result.target["customer_id"]) == {"C1", "C2"}


def test_update_when_found():
    target = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}])
    updates = pd.DataFrame([{"customer_id": "C1", "name": "Asha Mehta"}])
    result = merge_upsert(target, _empty(["customer_id", "name"]), updates, _empty(["customer_id"]), "customer_id")
    assert result.updated == 1
    assert result.target.loc[result.target["customer_id"] == "C1", "name"].iloc[0] == "Asha Mehta"


def test_delete_removes_row():
    target = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}, {"customer_id": "C2", "name": "Rohan"}])
    deletes = pd.DataFrame([{"customer_id": "C1"}])
    result = merge_upsert(target, _empty(["customer_id", "name"]), _empty(["customer_id", "name"]), deletes, "customer_id")
    assert result.deleted == 1
    assert list(result.target["customer_id"]) == ["C2"]


def test_rerun_with_same_batch_is_idempotent_no_duplicates():
    target = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}])
    inserts = pd.DataFrame([{"customer_id": "C2", "name": "Rohan"}])
    first = merge_upsert(target, inserts, _empty(["customer_id", "name"]), _empty(["customer_id"]), "customer_id")
    second = merge_upsert(first.target, inserts, _empty(["customer_id", "name"]), _empty(["customer_id"]), "customer_id")
    # C2 already exists after the first run; rerunning the same insert batch
    # must not produce a duplicate row.
    assert len(second.target[second.target["customer_id"] == "C2"]) == 1


def test_update_no_change_counts_as_unchanged():
    target = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}])
    updates = pd.DataFrame([{"customer_id": "C1", "name": "Asha"}])
    result = merge_upsert(target, _empty(["customer_id", "name"]), updates, _empty(["customer_id"]), "customer_id")
    assert result.unchanged == 1
    assert result.updated == 0
