"""A 股盘后研究的纯结果与档案转换。

这个模块只处理来自 PAPER 伴随事件或收盘研究结果的确定性投影：
不访问文件、网络、服务或通知端点。CLI facade 继续 re-export 历史私有
名称，以保持旧调用方和 monkeypatch 测试的兼容性。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from gribuki_trade.domain.instruments import ResearchInstrumentProfile

if TYPE_CHECKING:
    from gribuki_trade.reporting.paper_day_summary import PaperDayExecutiveProjection


def _paper_session_instrument_profiles(
    projection: PaperDayExecutiveProjection,
    instrument_types: Mapping[str, str],
) -> dict[str, ResearchInstrumentProfile]:
    """只构建在不可变 PAPER 伴随文件中明确归档的档案。"""

    names: dict[str, set[str]] = {}
    boards: dict[str, set[str]] = {}
    archived_instrument_types: dict[str, set[str]] = {}
    for event in projection.events:
        payload = event.payload
        collections: list[object] = []
        if event.event_type in {
            "PREOPEN_SCREEN_COMPLETED",
            "PREOPEN_SCREEN_RECOVERED",
            "SURVEILLANCE_SCAN_COMPLETED",
        }:
            collections.append(payload.get("candidates"))
        elif event.event_type == "WATCHLIST_UPDATED":
            collections.append(payload.get("watchlist"))
        elif event.event_type == "FILL_STARTED":
            raw_fill = payload.get("fill")
            if isinstance(raw_fill, dict):
                raw_symbol = raw_fill.get("symbol")
                raw_instrument_type = raw_fill.get("instrument_type")
                if isinstance(raw_symbol, str) and isinstance(raw_instrument_type, str):
                    canonical = raw_symbol.strip().upper()
                    archived_type = raw_instrument_type.strip().lower()
                    if canonical and archived_type in {"stock", "etf"}:
                        archived_instrument_types.setdefault(canonical, set()).add(archived_type)
        for collection in collections:
            if not isinstance(collection, list):
                continue
            for raw in collection:
                if not isinstance(raw, dict):
                    continue
                symbol = raw.get("symbol")
                if not isinstance(symbol, str):
                    continue
                canonical = symbol.strip().upper()
                if not canonical:
                    continue
                name = raw.get("name")
                board = raw.get("board")
                if isinstance(name, str) and name.strip():
                    names.setdefault(canonical, set()).add(name.strip())
                if isinstance(board, str) and board.strip():
                    boards.setdefault(canonical, set()).add(board.strip().upper())

    profiles: dict[str, ResearchInstrumentProfile] = {}
    board_values = {
        "SSE_MAIN": ("sse", "sse_main"),
        "SZSE_MAIN": ("szse", "szse_main"),
        "CHINEXT": ("szse", "chinext"),
        "STAR": ("sse", "star"),
        "BSE": ("bse", "bse"),
    }
    for symbol in sorted(set(names) | set(boards)):
        retained_names = names.get(symbol, set())
        retained_boards = boards.get(symbol, set())
        if len(retained_names) != 1 or len(retained_boards) != 1:
            continue
        board = next(iter(retained_boards))
        exchange_and_board = board_values.get(board)
        if exchange_and_board is None:
            continue
        asset_type = instrument_types.get(symbol)
        if asset_type not in {"stock", "etf"}:
            continue
        archived_types = archived_instrument_types.get(symbol, set())
        if len(archived_types) > 1 or (archived_types and archived_types != {asset_type}):
            continue
        profiles[symbol] = ResearchInstrumentProfile(
            symbol=symbol,
            name=next(iter(retained_names)),
            market="A-share",
            asset_type=asset_type,
            exchange=exchange_and_board[0],
            board=exchange_and_board[1],
            size_tier="unknown-not-provided",
            industry="unknown-not-provided",
            styles=("paper-session-archive", "degraded-profile"),
            research_role="held-position-post-close-review",
            risk_tags=(
                "DEGRADED_PROFILE",
                "INDUSTRY_NOT_PROVIDED",
                "SIZE_NOT_PROVIDED",
            ),
            source_id="PAPER_SESSION_ARCHIVE",
            verified_on=projection.session_date,
            background_facts=(
                f"PAPER archived name: {next(iter(retained_names))}",
                f"PAPER archived board: {board}",
                "Industry and size were not provided by the PAPER session archive.",
            ),
        )
    return profiles


def _close_batch_result_summary(
    symbol: str,
    result: Mapping[str, object],
) -> dict[str, object]:
    """限制批次标准输出大小，同时保留每个持久化结果标识。"""

    retained_fields = (
        "analysis_mode",
        "as_of",
        "combined_score",
        "decision",
        "error_code",
        "latest_completed_session",
        "macro_failure_code",
        "market_data_failure_code",
        "next_session",
        "notification_enqueued",
        "ok",
        "recommendation_id",
        "report_markdown",
        "stored_new",
        "technical_score",
    )
    summary = {key: result[key] for key in retained_fields if key in result}
    summary["symbol"] = str(result.get("symbol", symbol))
    return summary


def _instrument_profile_document(
    profile: ResearchInstrumentProfile | None,
) -> dict[str, object] | None:
    """将已核验的标的档案转换为不含运行时对象的 JSON 文档。"""

    if profile is None:
        return None
    return {
        "asset_type": profile.asset_type,
        "background_facts": list(profile.background_facts),
        "board": profile.board,
        "exchange": profile.exchange,
        "industry": profile.industry,
        "market": profile.market,
        "name": profile.name,
        "research_role": profile.research_role,
        "risk_tags": list(profile.risk_tags),
        "size_tier": profile.size_tier,
        "source_id": profile.source_id,
        "styles": list(profile.styles),
        "symbol": profile.symbol,
        "verified_on": profile.verified_on.isoformat(),
    }


def _daily_evidence_provider_id(source_name: str | None) -> str:
    """将已保留路由诊断映射为稳定且不含 URL 的来源标识。"""

    if source_name is None or source_name == "BaoStock":
        return "baostock.daily"
    if source_name.startswith("MIXED/TAIL_STITCH"):
        return "mixed.tail_stitch.daily"
    if source_name.startswith("AKShare"):
        return "akshare.daily"
    return "other.research.daily"
