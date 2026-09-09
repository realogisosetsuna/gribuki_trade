from __future__ import annotations

import argparse

import pytest

from gribuki_trade.cli_commands.parser import build_parser
from gribuki_trade.cli_commands.parsers.ashare_common import (
    ALL_NEWS_FEEDS,
    GLOBAL_NEWS_FEEDS,
    add_optional_notify_target_arguments,
)


def test_shared_news_feed_contract_is_used_by_a_share_commands() -> None:
    parser = build_parser()

    news = parser.parse_args(["ashare-news", "--feed", "individual_eastmoney"])
    assert news.feed == "individual_eastmoney"
    watch = parser.parse_args(["ashare-news-watch", "--feed", "global_sina"])
    assert watch.feeds == ["global_sina"]
    close = parser.parse_args(["ashare-close-research-once", "--news-feed", "global_sina"])
    assert close.news_feeds == ["global_sina"]
    post_close = parser.parse_args(["ashare-post-close", "run", "--news-feed", "global_sina"])
    assert post_close.news_feeds == ["global_sina"]

    assert ("individual_eastmoney", *GLOBAL_NEWS_FEEDS) == ALL_NEWS_FEEDS
    for value in ("individual_eastmoney", *GLOBAL_NEWS_FEEDS):
        parser.parse_args(["ashare-news", "--feed", value])
    with pytest.raises(SystemExit):
        parser.parse_args(["ashare-news-watch", "--feed", "individual_eastmoney"])


def test_optional_notify_target_registration_has_stable_contract() -> None:
    parser = argparse.ArgumentParser()
    add_optional_notify_target_arguments(parser)

    parsed = parser.parse_args(["--notify-target-kind", "group", "--notify-target-id", "42"])
    assert parsed.notify_target_kind == "group"
    assert parsed.notify_target_id == "42"

    with pytest.raises(SystemExit):
        parser.parse_args(["--notify-target-kind", "channel"])
