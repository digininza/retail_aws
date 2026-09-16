from src.data_quality.validators import classify_schema_change, has_breaking_change


ORDERS_SCHEMA = {
    "required": ["order_id", "customer_id", "order_date", "amount", "status"],
    "optional": ["store_id", "promotion_id", "channel"],
    "types": {"order_id": "string", "amount": "decimal", "order_date": "timestamp"},
}


def test_new_nullable_optional_column_is_compatible():
    findings = classify_schema_change(ORDERS_SCHEMA, incoming_columns=set(ORDERS_SCHEMA["required"]) | {"gift_wrap"}, incoming_types={})
    assert findings.get("gift_wrap") == "WARNING"  # unexpected_extra_column
    assert not has_breaking_change(findings)


def test_missing_required_column_is_breaking():
    incoming = set(ORDERS_SCHEMA["required"]) - {"amount"}
    findings = classify_schema_change(ORDERS_SCHEMA, incoming_columns=incoming, incoming_types={})
    assert findings["amount"] == "BREAKING"
    assert has_breaking_change(findings)


def test_type_widening_is_warning_not_breaking():
    incoming = set(ORDERS_SCHEMA["required"])
    findings = classify_schema_change(ORDERS_SCHEMA, incoming_columns=incoming,
                                       incoming_types={"amount": "string"})  # decimal -> string: widening
    assert findings["amount"] == "WARNING"
    assert not has_breaking_change(findings)


def test_type_narrowing_is_breaking():
    incoming = set(ORDERS_SCHEMA["required"])
    findings = classify_schema_change(ORDERS_SCHEMA, incoming_columns=incoming,
                                       incoming_types={"order_date": "string"})  # timestamp -> string not in widening map
    assert findings["order_date"] == "BREAKING"
    assert has_breaking_change(findings)


def test_fully_compatible_batch_has_no_findings():
    incoming = set(ORDERS_SCHEMA["required"]) | set(ORDERS_SCHEMA["optional"])
    findings = classify_schema_change(ORDERS_SCHEMA, incoming_columns=incoming, incoming_types={"amount": "decimal"})
    assert findings == {}
