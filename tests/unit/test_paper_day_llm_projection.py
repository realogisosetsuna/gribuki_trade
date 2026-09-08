from datetime import UTC, datetime
from types import SimpleNamespace

from gribuki_trade.reporting.paper_day_llm_projection import project_llm_sidecars


def _event(sequence: int, event_type: str, payload: dict[str, object], symbol: str | None = None):
    return SimpleNamespace(
        sequence=sequence,
        event_type=event_type,
        payload=payload,
        symbol=symbol,
        known_at=datetime(2026, 8, 14, 1, 0, sequence, tzinfo=UTC),
    )


def test_llm_projection_extracts_dual_track_and_latency_metrics() -> None:
    identity = {"requested_model": "baseline-v1", "prompt_schema_sha256": "a" * 64}
    events = (
        _event(
            1,
            "LLM_INTRADAY_POLICY_CONFIGURED",
            {"manifest_binding": {"required_for_buy": True, "analyzer_identity": identity}},
        ),
        _event(
            2,
            "LLM_PREOPEN_CONTEXT_FROZEN",
            {
                "preopen_context": {
                    "context_id": "preopen-1",
                    "dual_track": {
                        "selected_track": "BASELINE",
                        "audit_record_sha256": "b" * 64,
                        "baseline": {
                            "decision": "PUBLISH",
                            "macro_impact": "0.2",
                            "model": "base",
                        },
                        "adversarial": {
                            "decision": "WATCH",
                            "macro_impact": "-0.1",
                            "model": "adv",
                        },
                    },
                }
            },
        ),
        _event(
            3,
            "LLM_CANDIDATE_REVIEW_COMPLETED",
            {
                "review": {
                    "symbol": "600000.SH",
                    "review_id": "r-1",
                    "latency_ms": 120,
                    "response_model": "base",
                    "analyzer_identity": identity,
                    "dual_track": {
                        "selected_track": "BASELINE",
                        "audit_record_sha256": "c" * 64,
                        "baseline_analysis": {
                            "decision": "PUBLISH",
                            "macro_impact": "0.1",
                            "regime": "RISK_ON",
                        },
                        "adversarial_analysis": {
                            "decision": "WATCH",
                            "macro_impact": "-0.2",
                            "regime": "RISK_OFF",
                        },
                    },
                }
            },
            symbol="600000.SH",
        ),
        _event(4, "LLM_SERVICE_STATE_CHANGED", {"state": "HEALTHY"}),
    )

    projection = project_llm_sidecars(events)

    assert projection.enabled is True
    assert projection.required_for_buy is True
    assert projection.preopen_status == "FROZEN"
    assert projection.preopen_dual_track is not None
    assert projection.preopen_dual_track.selected_track == "BASELINE"
    assert projection.review_batch_count == 0
    assert projection.reviews_completed == 1
    assert projection.latency_p50_ms == 120
    assert projection.latency_p95_ms == 120
    assert projection.requested_models == ("baseline-v1",)
    assert projection.response_models == ("base",)
    assert len(projection.dual_track_comparisons) == 1
    assert projection.service_state == "HEALTHY"
