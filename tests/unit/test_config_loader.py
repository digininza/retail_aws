import pytest

from src.common.config_loader import ConfigLoader
from src.common.exceptions import ConfigurationError


def test_loads_pipeline_metadata():
    cfg = ConfigLoader(environment="dev")
    pipeline = cfg.get_pipeline("sqlserver_orders_incremental")
    assert pipeline.source_system == "SQLSERVER"
    assert pipeline.load_type == "INCREMENTAL"
    assert pipeline.watermark_column == "modified_date"
    assert pipeline.primary_key == "order_id"


def test_unknown_pipeline_raises_configuration_error():
    cfg = ConfigLoader(environment="dev")
    with pytest.raises(ConfigurationError):
        cfg.get_pipeline("does_not_exist")


def test_environment_overlay_is_environment_specific():
    dev = ConfigLoader(environment="dev").environment_config()
    prod = ConfigLoader(environment="prod").environment_config()
    assert dev["s3"]["bucket"] == "retail-data-dev"
    assert prod["s3"]["bucket"] == "retail-data-prod"
    assert dev["glue"]["number_of_workers"] != prod["glue"]["number_of_workers"]


def test_invalid_environment_rejected():
    with pytest.raises(ConfigurationError):
        ConfigLoader(environment="staging")


def test_s3_path_resolution():
    cfg = ConfigLoader(environment="dev")
    assert cfg.s3_path("bronze") == "s3://retail-data-dev/bronze/"


def test_dq_and_reconciliation_profiles_resolve_independently():
    cfg = ConfigLoader(environment="dev")
    dq = cfg.dq_profile("orders_standard")
    recon = cfg.reconciliation_profile("orders_daily")
    assert dq["table"] == "orders"
    assert recon["checks"][0]["type"] == "RECORD_COUNT"
