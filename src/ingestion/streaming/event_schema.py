"""Shared event schema + validation for POS and e-commerce streaming events
(Section 16). Used by both the local Kinesis producer/consumer simulation
(`streaming/`) and the Lambda ingestion handlers (`lambda/`), so a single
definition governs what a "valid event" means everywhere it is checked.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from src.common.exceptions import SchemaValidationError

POS_REQUIRED_FIELDS = {"event_id", "store_id", "sku", "quantity", "amount", "event_timestamp"}
ECOMMERCE_REQUIRED_FIELDS = {"event_id", "event_type", "event_timestamp"}
VALID_ECOMMERCE_TYPES = {
    "ORDER_CREATED", "ORDER_UPDATED", "PAYMENT_COMPLETED", "CART_ACTIVITY", "PRODUCT_VIEWED",
}


@dataclass
class ValidationResult:
    valid: bool
    reason: Optional[str] = None


def validate_pos_event(event: dict[str, Any]) -> ValidationResult:
    missing = POS_REQUIRED_FIELDS - event.keys()
    if missing:
        return ValidationResult(False, f"missing fields: {missing}")
    if not isinstance(event.get("quantity"), (int, float)) or event["quantity"] <= 0:
        return ValidationResult(False, "quantity must be a positive number")
    if not isinstance(event.get("amount"), (int, float)) or event["amount"] < 0:
        return ValidationResult(False, "amount must be non-negative")
    return ValidationResult(True)


def validate_ecommerce_event(event: dict[str, Any]) -> ValidationResult:
    missing = ECOMMERCE_REQUIRED_FIELDS - event.keys()
    if missing:
        return ValidationResult(False, f"missing fields: {missing}")
    if event.get("event_type") not in VALID_ECOMMERCE_TYPES:
        return ValidationResult(False, f"invalid event_type: {event.get('event_type')}")
    return ValidationResult(True)


def require_valid_pos_event(event: dict[str, Any]) -> None:
    result = validate_pos_event(event)
    if not result.valid:
        raise SchemaValidationError(f"Invalid POS event {event.get('event_id')}: {result.reason}")


def require_valid_ecommerce_event(event: dict[str, Any]) -> None:
    result = validate_ecommerce_event(event)
    if not result.valid:
        raise SchemaValidationError(f"Invalid e-commerce event {event.get('event_id')}: {result.reason}")


def partition_key_for(event: dict[str, Any], key_field: str) -> str:
    """Kinesis partition key: store_id for POS (co-locate a store's events for
    ordering within a shard), customer_id for e-commerce. See Section 16."""
    return str(event.get(key_field, "unknown"))


def is_late_event(event_timestamp: str, watermark: Optional[str], grace_period_seconds: int = 300) -> bool:
    """An event is 'late' if its timestamp falls before the current watermark
    minus a grace period — still accepted, but flagged for the late-arrival
    handling path rather than the standard path."""
    if watermark is None:
        return False
    ts = datetime.fromisoformat(event_timestamp)
    wm = datetime.fromisoformat(watermark)
    return (wm - ts).total_seconds() > grace_period_seconds
