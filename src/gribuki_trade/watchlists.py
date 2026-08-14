"""从 TOML 加载并校验带类型的研究关注名单。

关注名单是研究覆盖配置，不是投资组合或委托输入。加载器有意把研究元数据与券商、账户状态
分离，确保增加标的本身不会授予交易权限。
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, TypeVar

from gribuki_trade.domain.instruments import ResearchInstrumentProfile

EnumT = TypeVar("EnumT", bound=StrEnum)


class WatchlistConfigError(ValueError):
    """关注名单无法被无歧义解析时抛出。"""


class WatchlistAssetType(StrEnum):
    STOCK = "stock"
    ETF = "etf"


class WatchlistBoard(StrEnum):
    SSE_MAIN = "sse_main"
    SZSE_MAIN = "szse_main"
    CHINEXT = "chinext"
    STAR = "star"
    SSE_ETF = "sse_etf"
    SZSE_ETF = "szse_etf"


class WatchlistSizeTier(StrEnum):
    """用于研究的粗粒度分层，不是实时市值排名。"""

    MEGA = "mega"
    LARGE = "large"
    MID = "mid"
    SMALL = "small"
    CROSS_SIZE = "cross_size"


class WatchlistExchange(StrEnum):
    SSE = "sse"
    SZSE = "szse"


@dataclass(frozen=True, slots=True)
class WatchlistInstrument:
    symbol: str
    name: str
    asset_type: WatchlistAssetType
    board: WatchlistBoard
    size_tier: WatchlistSizeTier
    industry: str
    styles: tuple[str, ...]
    role: str
    risk_tags: tuple[str, ...]
    background_facts: tuple[str, ...] = ()

    @property
    def exchange(self) -> WatchlistExchange:
        return (
            WatchlistExchange.SSE
            if self.symbol.endswith(".SH")
            else WatchlistExchange.SZSE
        )


@dataclass(frozen=True, slots=True)
class ResearchWatchlist:
    schema_version: int
    watchlist_id: str
    title: str
    purpose: str
    verified_on: date
    verification_sources: tuple[str, ...]
    selection_rules: tuple[str, ...]
    default_symbols: tuple[str, ...]
    instruments: tuple[WatchlistInstrument, ...]

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.instruments)

    @property
    def stocks(self) -> tuple[WatchlistInstrument, ...]:
        return tuple(
            item for item in self.instruments if item.asset_type is WatchlistAssetType.STOCK
        )

    @property
    def etfs(self) -> tuple[WatchlistInstrument, ...]:
        return tuple(
            item
            for item in self.instruments
            if item.asset_type is WatchlistAssetType.ETF
        )

    def find_instrument(self, symbol: str) -> WatchlistInstrument | None:
        """返回规范格式或六位 A 股代码对应的元数据。"""

        canonical = _canonical_symbol(symbol)
        return next(
            (item for item in self.instruments if item.symbol == canonical),
            None,
        )

    def instrument_profile(self, symbol: str) -> ResearchInstrumentProfile | None:
        """从当前关注名单修订版构建可留存的领域快照。"""

        item = self.find_instrument(symbol)
        if item is None:
            return None
        return ResearchInstrumentProfile(
            symbol=item.symbol,
            name=item.name,
            market="A股",
            asset_type=item.asset_type.value,
            exchange=item.exchange.value,
            board=item.board.value,
            size_tier=item.size_tier.value,
            industry=item.industry,
            styles=item.styles,
            research_role=item.role,
            risk_tags=item.risk_tags,
            source_id=self.watchlist_id,
            verified_on=self.verified_on,
            background_facts=item.background_facts,
        )


_SYMBOL_PATTERN = re.compile(r"^(?P<code>\d{6})\.(?P<exchange>SH|SZ)$")


def _canonical_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if len(value) == 6 and value.isdigit():
        exchange = "SH" if value.startswith(("5", "6", "9")) else "SZ"
        return f"{value}.{exchange}"
    if _SYMBOL_PATTERN.fullmatch(value) is None:
        raise ValueError("symbol must look like 600000.SH or 000001.SZ")
    return value


def load_research_watchlist(path: str | Path) -> ResearchWatchlist:
    """加载 TOML 研究关注名单，并拒绝不一致的元数据。"""

    config_path = Path(path)
    try:
        with config_path.open("rb") as stream:
            payload = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise WatchlistConfigError(f"unable to load watchlist: {config_path}") from exc

    root = _mapping(payload, "root")
    metadata = _mapping(root.get("watchlist"), "watchlist")
    schema_version = _integer(metadata, "schema_version")
    if schema_version != 1:
        raise WatchlistConfigError("watchlist.schema_version must be 1")

    raw_instruments = root.get("instrument")
    if not isinstance(raw_instruments, list) or not raw_instruments:
        raise WatchlistConfigError("instrument must be a non-empty array of tables")

    instruments = tuple(
        _parse_instrument(_mapping(raw, f"instrument[{index}]"), index)
        for index, raw in enumerate(raw_instruments)
    )
    symbols = tuple(item.symbol for item in instruments)
    if len(symbols) != len(set(symbols)):
        raise WatchlistConfigError("instrument symbols must be unique")

    default_symbols = tuple(
        item.upper() for item in _text_list(metadata, "default_symbols")
    )
    unknown_defaults = set(default_symbols).difference(symbols)
    if unknown_defaults:
        raise WatchlistConfigError(
            "watchlist.default_symbols must reference configured instruments"
        )

    verified_on = metadata.get("verified_on")
    if not isinstance(verified_on, date):
        raise WatchlistConfigError("watchlist.verified_on must be a TOML local date")

    return ResearchWatchlist(
        schema_version=schema_version,
        watchlist_id=_text(metadata, "id"),
        title=_text(metadata, "title"),
        purpose=_text(metadata, "purpose"),
        verified_on=verified_on,
        verification_sources=_text_list(metadata, "verification_sources"),
        selection_rules=_text_list(metadata, "selection_rules"),
        default_symbols=default_symbols,
        instruments=instruments,
    )


def _parse_instrument(raw: dict[str, Any], index: int) -> WatchlistInstrument:
    prefix = f"instrument[{index}]"
    symbol = _text(raw, "symbol", prefix=prefix).upper()
    match = _SYMBOL_PATTERN.fullmatch(symbol)
    if match is None:
        raise WatchlistConfigError(f"{prefix}.symbol must look like 600000.SH or 000001.SZ")

    asset_type = _enum_value(WatchlistAssetType, raw, "asset_type", prefix)
    board = _enum_value(WatchlistBoard, raw, "board", prefix)
    size_tier = _enum_value(WatchlistSizeTier, raw, "size_tier", prefix)
    _validate_symbol_board(
        code=match.group("code"),
        exchange=match.group("exchange"),
        asset_type=asset_type,
        board=board,
        prefix=prefix,
    )
    return WatchlistInstrument(
        symbol=symbol,
        name=_text(raw, "name", prefix=prefix),
        asset_type=asset_type,
        board=board,
        size_tier=size_tier,
        industry=_text(raw, "industry", prefix=prefix),
        styles=_text_list(raw, "styles", prefix=prefix),
        role=_text(raw, "role", prefix=prefix),
        risk_tags=_text_list(raw, "risk_tags", prefix=prefix),
        background_facts=_optional_text_list(raw, "background", prefix=prefix),
    )


def _validate_symbol_board(
    *,
    code: str,
    exchange: str,
    asset_type: WatchlistAssetType,
    board: WatchlistBoard,
    prefix: str,
) -> None:
    stock_boards = {
        WatchlistBoard.SSE_MAIN,
        WatchlistBoard.SZSE_MAIN,
        WatchlistBoard.CHINEXT,
        WatchlistBoard.STAR,
    }
    etf_boards = {WatchlistBoard.SSE_ETF, WatchlistBoard.SZSE_ETF}
    if asset_type is WatchlistAssetType.STOCK and board not in stock_boards:
        raise WatchlistConfigError(f"{prefix}.board is not a stock board")
    if asset_type is WatchlistAssetType.ETF and board not in etf_boards:
        raise WatchlistConfigError(f"{prefix}.board is not an ETF board")

    valid = {
        WatchlistBoard.SSE_MAIN: exchange == "SH" and code.startswith(("600", "601", "603", "605")),
        WatchlistBoard.SZSE_MAIN: exchange == "SZ"
        and code.startswith(("000", "001", "002", "003")),
        WatchlistBoard.CHINEXT: exchange == "SZ" and code.startswith(("300", "301")),
        WatchlistBoard.STAR: exchange == "SH" and code.startswith("688"),
        WatchlistBoard.SSE_ETF: exchange == "SH",
        WatchlistBoard.SZSE_ETF: exchange == "SZ",
    }[board]
    if not valid:
        raise WatchlistConfigError(f"{prefix}.symbol is inconsistent with board")


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WatchlistConfigError(f"{field} must be a table")
    return value


def _text(
    payload: dict[str, Any],
    field: str,
    *,
    prefix: str = "watchlist",
) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise WatchlistConfigError(f"{prefix}.{field} must be non-empty text")
    return value.strip()


def _integer(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise WatchlistConfigError(f"watchlist.{field} must be an integer")
    return value


def _text_list(
    payload: dict[str, Any],
    field: str,
    *,
    prefix: str = "watchlist",
) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list) or not value:
        raise WatchlistConfigError(f"{prefix}.{field} must be a non-empty text array")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise WatchlistConfigError(f"{prefix}.{field} must contain non-empty text")
        normalized.append(item.strip())
    if len(normalized) != len(set(normalized)):
        raise WatchlistConfigError(f"{prefix}.{field} values must be unique")
    return tuple(normalized)


def _optional_text_list(
    payload: dict[str, Any],
    field: str,
    *,
    prefix: str,
) -> tuple[str, ...]:
    if field not in payload:
        return ()
    return _text_list(payload, field, prefix=prefix)


def _enum_value(
    enum_type: type[EnumT],
    payload: dict[str, Any],
    field: str,
    prefix: str,
) -> EnumT:
    value = _text(payload, field, prefix=prefix)
    try:
        return enum_type(value)
    except ValueError as exc:
        raise WatchlistConfigError(f"{prefix}.{field} has an unsupported value") from exc
