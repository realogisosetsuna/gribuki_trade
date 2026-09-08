from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from gribuki_trade.domain.paper_day import PaperDayRunManifest
from gribuki_trade.services.ashare.ashare_intraday_paper import IntradayPaperRiskConfig
from gribuki_trade.services.ashare.ashare_paper_day_config import (
    PAPER_RISK_POLICY_CHANGE_CONFIRMATION,
    ASharePaperDayConfig,
    _entry_policy_document,
    _risk_policy_manifest_binding,
    _runner_config_document,
)


def test_paper_day_config_audit_is_frozen_and_exposes_execution_policy() -> None:
    config = ASharePaperDayConfig(
        initial_cash=Decimal("123456"),
        fallback_price_tolerance=Decimal("0.02"),
    )

    document = config.audit_document()

    assert document["initial_cash"] == "123456"
    assert document["fallback_price_tolerance"] == "0.02"
    assert document["execution_mode"] == "PAPER_ONLY_NO_BROKER"
    assert document["exit_plan_enabled"] is True
    assert document["match_policy"] == "NEXT_FULLY_POST_SIGNAL_1M_INTERVAL_IOC"
    assert PAPER_RISK_POLICY_CHANGE_CONFIRMATION == "PAPER_RISK_POLICY_CHANGE"


def test_entry_and_runner_policy_documents_are_pure_projections() -> None:
    config = ASharePaperDayConfig()
    risk = IntradayPaperRiskConfig(initial_equity=config.initial_cash)

    entry = _entry_policy_document(config)
    runner = _runner_config_document(config, risk)

    assert entry["candidate_class_required"] == "MOMENTUM_EXPANSION"
    assert entry["complete_whole_market_scan_required"] is True
    assert entry["degraded_scan_authority"] is False
    assert entry["minute_freshness_required"] == "CURRENT"
    assert runner == {
        "paper_day": config.audit_document(),
        "intraday_risk": risk.audit_document(),
    }


def test_risk_policy_binding_distinguishes_manifest_upgrade() -> None:
    risk = IntradayPaperRiskConfig()
    policy = risk.audit_document()
    manifest = PaperDayRunManifest.create(
        session_date=date(2026, 8, 14),
        account_id="paper-day-config-test",
        config={"intraday_risk_policy": policy},
        created_at=datetime(2026, 8, 13, 8, tzinfo=UTC),
        target_hash="a" * 64,
        initial_cash=Decimal("200000"),
    )

    assert _risk_policy_manifest_binding(manifest, policy) == "MATCHES_MANIFEST"
    assert _risk_policy_manifest_binding(
        manifest,
        {**policy, "risk_per_trade_fraction": "0.01"},
    ) == "RUNTIME_APPEND_ONLY_LEGACY_MANIFEST"
