#!/usr/bin/env python3
"""Production-style Farcaster daily scraper with engagement scoring.

What it does:
- Pulls Farcaster casts for one day or rolling window.
- Supports direct Snapchain/Hub ingestion (no API key) and Neynar API ingestion.
- Includes both posts and comments created on that target day.
- Computes a 1-10 algorithmic score per cast using likes-weighted engagement.
- Writes date-stamped .txt/.md outputs with token-estimate headers.

Notes:
- Farcaster posts/comments are protocol messages propagated by hubs.
- Snapchain mode reads raw hub events directly and derives cast/reaction counts.
- Neynar user score is an off-chain reputation metric, not an on-chain primitive.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import concurrent.futures
import json
import logging
import math
import os
import re
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CAST_SEARCH_URL = "https://api.neynar.com/v2/farcaster/cast/search"
CAST_REACTIONS_URL = "https://api.neynar.com/v2/farcaster/reactions/cast"
HUB_INFO_URL_PATH = "/v1/info"
HUB_EVENTS_URL_PATH = "/v1/events"
HUB_USER_DATA_BY_FID_URL_PATH = "/v1/userDataByFid"
HUB_USERNAME_PROOFS_BY_FID_URL_PATH = "/v1/userNameProofsByFid"
DEFAULT_HUB_URL = "http://54.157.62.17:3381"
FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
DEFAULT_MAIN_QUESTION = "What's the most amusing thing you saw on Farcaster today?"
KEYWORD_PATTERN = re.compile(r"[a-z][a-z0-9']{2,}")
KEYWORD_STOPWORDS = {
    "a",
    "about",
    "after",
    "all",
    "also",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "but",
    "by",
    "can",
    "could",
    "day",
    "days",
    "did",
    "do",
    "does",
    "don't",
    "for",
    "from",
    "get",
    "going",
    "had",
    "has",
    "have",
    "he",
    "her",
    "here",
    "him",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "just",
    "like",
    "me",
    "more",
    "my",
    "new",
    "no",
    "not",
    "now",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "people",
    "really",
    "so",
    "some",
    "that",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "to",
    "today",
    "too",
    "up",
    "us",
    "was",
    "we",
    "what",
    "when",
    "will",
    "with",
    "you",
    "your",
}

_thread_local = threading.local()


@dataclass(frozen=True)
class ScrapeConfig:
    source: str
    api_key: str | None
    hub_url: str
    snapchain_shards: tuple[int, ...]
    snapchain_event_id_span: int
    target_date: date
    timezone_name: str
    query: str
    collect_last_hours: float | None
    page_size: int
    max_pages: int | None
    like_page_size: int
    like_max_pages: int
    like_workers: int
    enrich_like_quality: bool
    ranking_max_age_hours: float
    likes_for_top_score: int
    output_path: Path
    instructions_path: Path
    readable_output_path: Path
    final_output_path: Path
    final_top_posts: int
    final_comments_per_post: int
    final_snippet_length: int
    focus_themes: tuple[str, ...]
    exclude_themes: tuple[str, ...]


def parse_date(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid date '{value}'. Use YYYY-MM-DD or M/D/YYYY."
    )


def resolve_timezone(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        pass

    # Accept fixed offsets such as +01:00, -0500, UTC+01:00, UTC-05:00.
    offset_match = re.fullmatch(r"(?:UTC)?([+-])(\d{2}):?(\d{2})", name.strip(), re.IGNORECASE)
    if offset_match:
        sign, hh, mm = offset_match.groups()
        hours = int(hh)
        minutes = int(mm)
        if hours > 23 or minutes > 59:
            raise argparse.ArgumentTypeError(
                f"Invalid timezone offset '{name}'."
            )
        delta = timedelta(hours=hours, minutes=minutes)
        if sign == "-":
            delta = -delta
        return timezone(delta)

    raise argparse.ArgumentTypeError(
        f"Unknown timezone '{name}'. Use IANA timezone like America/New_York or UTC offset like +00:00."
    )


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_plain_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"accept": "application/json"})

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def build_neynar_session(api_key: str) -> requests.Session:
    session = build_plain_session()
    session.headers.update(
        {
            "x-api-key": api_key,
            # Neynar docs key can reject default python-requests UA with HTTP 403.
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        }
    )
    return session


def get_thread_session(api_key: str) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = build_neynar_session(api_key)
        _thread_local.session = session
    return session


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: int = 40,
) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)
    if response.status_code >= 400:
        body = response.text.strip()
        body = body[:400] + ("..." if len(body) > 400 else "")
        raise RuntimeError(
            f"HTTP {response.status_code} calling {url} with params {params}. Body: {body}"
        )
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected response type from {url}: {type(payload)}")
    return payload


def parse_iso_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_iso_seconds(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.isoformat(timespec="seconds")


def cast_timestamp_to_local_day(cast: dict[str, Any], tz: tzinfo) -> date | None:
    dt = parse_iso_timestamp(cast.get("timestamp"))
    if dt is None:
        return None
    return dt.astimezone(tz).date()


def get_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def farcaster_time_to_utc(fc_seconds: int) -> datetime:
    return FARCASTER_EPOCH + timedelta(seconds=int(fc_seconds))


def utc_to_farcaster_time(dt_utc: datetime) -> int:
    normalized = dt_utc.astimezone(timezone.utc)
    return int((normalized - FARCASTER_EPOCH).total_seconds())


def event_timestamp_farcaster_seconds(event: dict[str, Any]) -> int | None:
    block_confirmed = event.get("blockConfirmedBody")
    if isinstance(block_confirmed, dict):
        ts_value = get_int(block_confirmed.get("timestamp"), default=-1)
        if ts_value >= 0:
            return ts_value

    for body_key in ("mergeMessageBody", "pruneMessageBody", "revokeMessageBody"):
        body = event.get(body_key)
        if not isinstance(body, dict):
            continue
        message = body.get("message")
        if not isinstance(message, dict):
            continue
        data = message.get("data")
        if not isinstance(data, dict):
            continue
        ts_value = get_int(data.get("timestamp"), default=-1)
        if ts_value >= 0:
            return ts_value
    return None


def normalize_hash(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip().lower()
    if not text:
        return None
    if text.startswith("0x") and len(text) > 2:
        return text
    if re.fullmatch(r"[0-9a-f]+", text):
        return f"0x{text}"
    return text


def parse_parent_cast_hash(cast_add_body: dict[str, Any]) -> str | None:
    parent_cast = cast_add_body.get("parentCastId")
    if not isinstance(parent_cast, dict):
        return None
    return normalize_hash(parent_cast.get("hash"))


def target_hash_from_reaction_body(reaction_body: dict[str, Any]) -> str | None:
    target_cast = reaction_body.get("targetCastId")
    if not isinstance(target_cast, dict):
        return None
    return normalize_hash(target_cast.get("hash"))


def cast_record_in_scope(
    cast_dt_utc: datetime,
    config: ScrapeConfig,
    tz: tzinfo,
    now_utc: datetime,
) -> bool:
    if config.collect_last_hours is not None:
        start_utc = now_utc - timedelta(hours=float(config.collect_last_hours))
        return start_utc <= cast_dt_utc <= now_utc
    return cast_dt_utc.astimezone(tz).date() == config.target_date


def get_hub_events_page(
    session: requests.Session,
    hub_url: str,
    shard_index: int,
    from_event_id: int,
    page_size: int,
) -> list[dict[str, Any]]:
    payload = request_json(
        session=session,
        url=f"{hub_url.rstrip('/')}{HUB_EVENTS_URL_PATH}",
        params={
            "shard_index": shard_index,
            "from_event_id": max(0, int(from_event_id)),
            "page_size": max(1, int(page_size)),
        },
        timeout=25,
    )
    events = payload.get("events") or []
    return [event for event in events if isinstance(event, dict)]


def get_hub_shard_max_heights(
    session: requests.Session,
    hub_url: str,
) -> dict[int, int]:
    payload = request_json(
        session=session,
        url=f"{hub_url.rstrip('/')}{HUB_INFO_URL_PATH}",
        params={},
        timeout=20,
    )
    shard_infos = payload.get("shardInfos") or []
    max_heights: dict[int, int] = {}
    for shard_info in shard_infos:
        if not isinstance(shard_info, dict):
            continue
        shard_id = get_int(shard_info.get("shardId"), default=-1)
        max_height = get_int(shard_info.get("maxHeight"), default=-1)
        if shard_id >= 0 and max_height >= 0:
            max_heights[shard_id] = max_height
    return max_heights


def find_hub_tip_event_id(
    session: requests.Session,
    hub_url: str,
    shard_index: int,
    shard_max_height: int | None = None,
) -> int | None:
    if shard_max_height is not None and shard_max_height >= 0:
        # Snapchain event id packs block height in the high bits (block << 14).
        probe_id = int(shard_max_height) << 14
        for _ in range(16):
            tail_page = get_hub_events_page(
                session=session,
                hub_url=hub_url,
                shard_index=shard_index,
                from_event_id=probe_id,
                page_size=2_000,
            )
            if tail_page:
                return get_int(tail_page[-1].get("id"), default=0)
            probe_id = max(0, probe_id - (1 << 14))

    def has_events(from_event_id: int) -> bool:
        page = get_hub_events_page(
            session=session,
            hub_url=hub_url,
            shard_index=shard_index,
            from_event_id=from_event_id,
            page_size=1,
        )
        return len(page) > 0

    lo = 0
    hi = 1
    while has_events(hi):
        lo = hi
        hi *= 2
        if hi > 2_000_000_000_000:
            break

    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if has_events(mid):
            lo = mid
        else:
            hi = mid

    tail_page = get_hub_events_page(
        session=session,
        hub_url=hub_url,
        shard_index=shard_index,
        from_event_id=lo,
        page_size=2_000,
    )
    if not tail_page:
        return None
    return get_int(tail_page[-1].get("id"), default=0)


def fetch_casts_for_day_from_snapchain(
    session: requests.Session,
    config: ScrapeConfig,
) -> list[dict[str, Any]]:
    tz = resolve_timezone(config.timezone_name)
    now_utc = datetime.now(timezone.utc)
    target_fc_start = utc_to_farcaster_time(now_utc - timedelta(hours=float(config.collect_last_hours or 24.0)))

    cast_records_by_hash: dict[str, dict[str, Any]] = {}
    likes_by_hash: Counter[str] = Counter()
    recasts_by_hash: Counter[str] = Counter()
    replies_by_hash: Counter[str] = Counter()
    removed_cast_hashes: set[str] = set()

    raw_events_processed = 0
    merge_events_processed = 0
    cast_messages_processed = 0
    reaction_messages_processed = 0

    shard_max_heights = get_hub_shard_max_heights(session=session, hub_url=config.hub_url)

    for shard_index in config.snapchain_shards:
        tip_event_id = find_hub_tip_event_id(
            session=session,
            hub_url=config.hub_url,
            shard_index=shard_index,
            shard_max_height=shard_max_heights.get(shard_index),
        )
        if tip_event_id is None:
            logging.warning("Shard %s has no readable events.", shard_index)
            continue

        span = int(config.snapchain_event_id_span)
        if config.collect_last_hours is not None:
            scaled_span = int(
                round(
                    float(config.snapchain_event_id_span)
                    * float(config.collect_last_hours)
                    / 24.0
                )
            )
            span = max(50_000_000, scaled_span)
        shard_start_id = max(0, tip_event_id - span)
        if config.collect_last_hours is not None and shard_start_id > 0:
            # Expand backwards if our initial span starts after the intended 24h window.
            while True:
                first_batch = get_hub_events_page(
                    session=session,
                    hub_url=config.hub_url,
                    shard_index=shard_index,
                    from_event_id=shard_start_id,
                    page_size=1,
                )
                if not first_batch:
                    break
                first_ts = event_timestamp_farcaster_seconds(first_batch[0])
                if first_ts is None or first_ts <= target_fc_start:
                    break
                span *= 2
                next_start_id = max(0, tip_event_id - span)
                if next_start_id == shard_start_id:
                    break
                logging.info(
                    "Expanding shard %s event-id span to %s (start_id=%s) to cover full rolling window",
                    shard_index,
                    span,
                    next_start_id,
                )
                shard_start_id = next_start_id
                if shard_start_id == 0:
                    break

        logging.info(
            "Reading hub shard %s events from id=%s (tip=%s)",
            shard_index,
            shard_start_id,
            tip_event_id,
        )

        cursor = shard_start_id
        shard_pages_consumed = 0
        while True:
            batch = get_hub_events_page(
                session=session,
                hub_url=config.hub_url,
                shard_index=shard_index,
                from_event_id=cursor,
                page_size=config.page_size,
            )
            if not batch:
                break

            shard_pages_consumed += 1
            raw_events_processed += len(batch)

            for event in batch:
                event_id = get_int(event.get("id"), default=0)
                event_type = str(event.get("type") or "")
                if event_type != "HUB_EVENT_TYPE_MERGE_MESSAGE":
                    continue
                merge_events_processed += 1

                merge_body = event.get("mergeMessageBody") or {}
                message = merge_body.get("message")
                if not isinstance(message, dict):
                    continue
                data = message.get("data")
                if not isinstance(data, dict):
                    continue

                message_ts = get_int(data.get("timestamp"), default=-1)
                if message_ts < 0:
                    continue
                cast_dt_utc = farcaster_time_to_utc(message_ts)
                if not cast_record_in_scope(cast_dt_utc, config, tz, now_utc):
                    continue

                message_type = str(data.get("type") or "")
                if message_type == "MESSAGE_TYPE_CAST_ADD":
                    cast_messages_processed += 1
                    cast_hash = normalize_hash(message.get("hash"))
                    if cast_hash is None:
                        continue

                    cast_add_body = data.get("castAddBody") or {}
                    if not isinstance(cast_add_body, dict):
                        cast_add_body = {}

                    text = str(cast_add_body.get("text") or "")
                    parent_hash = parse_parent_cast_hash(cast_add_body)
                    parent_url_value = cast_add_body.get("parentUrl")
                    parent_url = str(parent_url_value) if isinstance(parent_url_value, str) and parent_url_value else None

                    fid = get_int(data.get("fid"), default=-1)
                    if fid < 0:
                        continue

                    cast_records_by_hash[cast_hash] = {
                        "hash": cast_hash,
                        "timestamp": cast_dt_utc.isoformat(),
                        "author": {"fid": fid, "username": None},
                        "channel": {},
                        "text": text,
                        "parent_hash": parent_hash,
                        "parent_url": parent_url,
                        "reactions": {"likes_count": 0, "recasts_count": 0},
                        "replies": {"count": 0},
                        "_event_id": event_id,
                    }

                    if parent_hash:
                        replies_by_hash[parent_hash] += 1

                elif message_type in ("MESSAGE_TYPE_REACTION_ADD", "MESSAGE_TYPE_REACTION_REMOVE"):
                    reaction_messages_processed += 1
                    reaction_body = data.get("reactionBody") or {}
                    if not isinstance(reaction_body, dict):
                        continue
                    target_hash = target_hash_from_reaction_body(reaction_body)
                    if target_hash is None:
                        continue

                    reaction_type = str(reaction_body.get("type") or "")
                    delta = 1 if message_type.endswith("_ADD") else -1
                    if "LIKE" in reaction_type:
                        likes_by_hash[target_hash] += delta
                    elif "RECAST" in reaction_type:
                        recasts_by_hash[target_hash] += delta

                elif message_type == "MESSAGE_TYPE_CAST_REMOVE":
                    cast_remove_body = data.get("castRemoveBody") or {}
                    if not isinstance(cast_remove_body, dict):
                        continue
                    target_hash = normalize_hash(cast_remove_body.get("targetHash"))
                    if target_hash:
                        removed_cast_hashes.add(target_hash)

            next_cursor = get_int(batch[-1].get("id"), default=cursor) + 1
            if next_cursor <= cursor:
                logging.warning(
                    "Stopping shard %s due to non-increasing cursor (%s -> %s)",
                    shard_index,
                    cursor,
                    next_cursor,
                )
                break
            cursor = next_cursor

            if config.max_pages is not None:
                # In Snapchain mode, max_pages applies per shard.
                if shard_pages_consumed >= config.max_pages:
                    logging.info(
                        "Stopping shard %s due to --max-pages=%s",
                        shard_index,
                        config.max_pages,
                    )
                    break

        logging.info(
            "Shard %s done. cursor=%s tip=%s",
            shard_index,
            cursor,
            tip_event_id,
        )

    for removed_hash in removed_cast_hashes:
        cast_records_by_hash.pop(removed_hash, None)

    results: list[dict[str, Any]] = []
    for cast_hash, cast in cast_records_by_hash.items():
        reactions = cast.get("reactions") or {}
        replies = cast.get("replies") or {}
        reactions["likes_count"] = max(0, int(likes_by_hash.get(cast_hash, 0)))
        reactions["recasts_count"] = max(0, int(recasts_by_hash.get(cast_hash, 0)))
        replies["count"] = max(0, int(replies_by_hash.get(cast_hash, 0)))
        cast.pop("_event_id", None)
        results.append(cast)

    results.sort(
        key=lambda cast: (
            str(cast.get("timestamp") or ""),
            str(cast.get("hash") or ""),
        )
    )

    logging.info(
        "Snapchain fetch summary: raw_events=%s merge_events=%s cast_msgs=%s reaction_msgs=%s casts_kept=%s",
        raw_events_processed,
        merge_events_processed,
        cast_messages_processed,
        reaction_messages_processed,
        len(results),
    )
    return results


def fetch_casts_for_day_from_neynar(
    session: requests.Session,
    config: ScrapeConfig,
) -> list[dict[str, Any]]:
    tz = resolve_timezone(config.timezone_name)
    next_day = config.target_date + timedelta(days=1)

    now_utc = datetime.now(timezone.utc)
    rolling_start_utc: datetime | None = None
    if config.collect_last_hours is not None:
        rolling_start_utc = now_utc - timedelta(hours=float(config.collect_last_hours))
        # Neynar cast-search parser accepts YYYY-MM-DDTHH:MM:SS (without trailing Z).
        after_ts = rolling_start_utc.strftime("%Y-%m-%dT%H:%M:%S")
        before_ts = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
        query = f"{config.query} after:{after_ts} before:{before_ts}".strip()
        logging.info(
            "Rolling window enabled: last %.2f hours (%s to %s UTC)",
            float(config.collect_last_hours),
            rolling_start_utc.isoformat(),
            now_utc.isoformat(),
        )
    else:
        query = (
            f"{config.query} after:{config.target_date.isoformat()} before:{next_day.isoformat()}"
        ).strip()

    logging.info("Fetching casts for %s with query: %s", config.target_date, query)

    cursor: str | None = None
    seen_cursor: set[str] = set()
    seen_hashes: set[str] = set()
    results: list[dict[str, Any]] = []
    page = 0

    while True:
        params: dict[str, Any] = {
            "q": query,
            "limit": config.page_size,
            "sort_type": "desc_chron",
            "mode": "literal",
        }
        if cursor:
            params["cursor"] = cursor

        payload = request_json(session, CAST_SEARCH_URL, params)
        result = payload.get("result") or {}
        batch = result.get("casts") or []

        accepted_on_page = 0
        for cast in batch:
            if not isinstance(cast, dict):
                continue
            cast_hash = cast.get("hash")
            if not isinstance(cast_hash, str) or cast_hash in seen_hashes:
                continue
            if rolling_start_utc is not None:
                ts = cast.get("timestamp")
                if not isinstance(ts, str):
                    continue
                try:
                    cast_dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
                except ValueError:
                    continue
                if not (rolling_start_utc <= cast_dt_utc <= now_utc):
                    continue
            else:
                local_day = cast_timestamp_to_local_day(cast, tz)
                if local_day != config.target_date:
                    continue
            seen_hashes.add(cast_hash)
            results.append(cast)
            accepted_on_page += 1

        page += 1
        logging.info(
            "Page %s: fetched=%s accepted=%s total=%s",
            page,
            len(batch),
            accepted_on_page,
            len(results),
        )

        if rolling_start_utc is not None and batch:
            # Search results are requested in descending chronology. Once an entire
            # page is older than the rolling start, there is no need to continue.
            oldest_batch_dt: datetime | None = None
            for cast in batch:
                ts = cast.get("timestamp") if isinstance(cast, dict) else None
                if not isinstance(ts, str):
                    continue
                try:
                    cast_dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
                except ValueError:
                    continue
                if oldest_batch_dt is None or cast_dt < oldest_batch_dt:
                    oldest_batch_dt = cast_dt
            if oldest_batch_dt is not None and oldest_batch_dt < rolling_start_utc and accepted_on_page == 0:
                logging.info(
                    "Stopping: page is older than rolling window (oldest=%s < start=%s)",
                    oldest_batch_dt.isoformat(),
                    rolling_start_utc.isoformat(),
                )
                break

        if config.max_pages is not None and page >= config.max_pages:
            logging.info("Stopping due to --max-pages=%s", config.max_pages)
            break

        next_cursor = ((result.get("next") or {}).get("cursor"))
        if not next_cursor:
            break
        if next_cursor in seen_cursor:
            logging.warning("Detected repeated cursor; stopping to avoid loop.")
            break

        seen_cursor.add(next_cursor)
        cursor = next_cursor

    logging.info("Total casts collected from Neynar for %s: %s", config.target_date, len(results))
    return results


def fetch_casts_for_day(session: requests.Session, config: ScrapeConfig) -> list[dict[str, Any]]:
    if config.source == "snapchain":
        return fetch_casts_for_day_from_snapchain(session=session, config=config)
    if config.source == "neynar":
        return fetch_casts_for_day_from_neynar(session=session, config=config)
    raise RuntimeError(f"Unsupported source '{config.source}'. Use 'snapchain' or 'neynar'.")


def fetch_like_reactions(
    api_key: str,
    cast_hash: str,
    like_page_size: int,
    like_max_pages: int,
) -> list[dict[str, Any]]:
    session = get_thread_session(api_key)

    cursor: str | None = None
    seen_cursor: set[str] = set()
    all_reactions: list[dict[str, Any]] = []

    for _ in range(like_max_pages):
        params: dict[str, Any] = {
            "hash": cast_hash,
            "types": "likes",
            "limit": like_page_size,
        }
        if cursor:
            params["cursor"] = cursor

        payload = request_json(session, CAST_REACTIONS_URL, params, timeout=30)
        reactions = payload.get("reactions") or []
        for reaction in reactions:
            if isinstance(reaction, dict):
                all_reactions.append(reaction)

        next_cursor = ((payload.get("next") or {}).get("cursor"))
        if not next_cursor:
            break
        if next_cursor in seen_cursor:
            break

        seen_cursor.add(next_cursor)
        cursor = next_cursor

    return all_reactions


def extract_user_score_from_reaction(reaction: dict[str, Any]) -> float | None:
    user = reaction.get("user")
    if not isinstance(user, dict):
        return None

    score = user.get("score")
    if isinstance(score, (int, float)):
        return float(max(0.0, min(1.0, score)))

    experimental = user.get("experimental")
    if isinstance(experimental, dict):
        legacy_score = experimental.get("neynar_user_score")
        if isinstance(legacy_score, (int, float)):
            return float(max(0.0, min(1.0, legacy_score)))

    return None


def fallback_like_metrics(reported_like_count: int) -> dict[str, Any]:
    estimated_genuine_count = int(round(reported_like_count * 0.5))
    estimated_genuine_points = float(reported_like_count * 0.8)
    return {
        "likes_reported": reported_like_count,
        "likes_fetched": 0,
        "genuine_like_count": estimated_genuine_count,
        "genuine_like_points": estimated_genuine_points,
        "mean_liker_score": None,
        "like_data_source": "fallback_estimate",
    }


def compute_like_metrics(
    api_key: str,
    cast_hash: str,
    reported_like_count: int,
    like_page_size: int,
    like_max_pages: int,
) -> dict[str, Any]:
    if reported_like_count <= 0:
        return {
            "likes_reported": 0,
            "likes_fetched": 0,
            "genuine_like_count": 0,
            "genuine_like_points": 0.0,
            "mean_liker_score": None,
            "like_data_source": "none",
        }

    try:
        reactions = fetch_like_reactions(
            api_key=api_key,
            cast_hash=cast_hash,
            like_page_size=like_page_size,
            like_max_pages=like_max_pages,
        )
    except Exception:
        logging.exception("Failed fetching like reactions for cast %s", cast_hash)
        return fallback_like_metrics(reported_like_count)

    if not reactions:
        return {
            "likes_reported": reported_like_count,
            "likes_fetched": 0,
            "genuine_like_count": 0,
            "genuine_like_points": 0.0,
            "mean_liker_score": None,
            "like_data_source": "fetched_empty",
        }

    scores: list[float] = []
    genuine_like_count = 0
    genuine_like_points = 0.0

    for reaction in reactions:
        score = extract_user_score_from_reaction(reaction)
        if score is None:
            score = 0.35
        scores.append(score)

        if score >= 0.60:
            genuine_like_count += 1

        # Weighted like contribution: score in [0,1] maps to weight in [0.4, 1.6]
        genuine_like_points += 0.4 + (1.2 * score)

    mean_liker_score = sum(scores) / len(scores) if scores else None
    return {
        "likes_reported": reported_like_count,
        "likes_fetched": len(reactions),
        "genuine_like_count": genuine_like_count,
        "genuine_like_points": round(genuine_like_points, 4),
        "mean_liker_score": None if mean_liker_score is None else round(mean_liker_score, 4),
        "like_data_source": "fetched",
    }


def is_comment(cast: dict[str, Any]) -> bool:
    return bool(cast.get("parent_hash") or cast.get("parent_url"))


def build_cast_url(cast_hash: str) -> str:
    return f"https://warpcast.com/~/conversations/{cast_hash}"


def short_cast_hash(cast_hash: Any) -> str:
    cast_hash_text = normalize_text(cast_hash).lower()
    if re.fullmatch(r"0x[0-9a-f]{8,}", cast_hash_text):
        return cast_hash_text[:10]
    return cast_hash_text


def normalize_farcaster_handle(value: Any) -> str:
    handle = normalize_text(value).lower()
    if not handle:
        return ""
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,62}", handle):
        return handle
    return ""


def latest_username_from_user_data_payload(payload: dict[str, Any]) -> str | None:
    messages = payload.get("messages") or []
    best_username = ""
    best_timestamp = -1
    for message in messages:
        if not isinstance(message, dict):
            continue
        data = message.get("data")
        if not isinstance(data, dict):
            continue
        user_data_body = data.get("userDataBody")
        if not isinstance(user_data_body, dict):
            continue
        if str(user_data_body.get("type") or "") != "USER_DATA_TYPE_USERNAME":
            continue

        username = normalize_farcaster_handle(user_data_body.get("value"))
        if not username:
            continue

        timestamp = get_int(data.get("timestamp"), default=-1)
        if timestamp >= best_timestamp:
            best_timestamp = timestamp
            best_username = username

    return best_username or None


def latest_username_from_name_proofs_payload(payload: dict[str, Any]) -> str | None:
    proofs = payload.get("proofs") or []
    best_fname = ""
    best_fname_timestamp = -1
    best_any = ""
    best_any_timestamp = -1

    for proof in proofs:
        if not isinstance(proof, dict):
            continue
        username = normalize_farcaster_handle(proof.get("name"))
        if not username:
            continue

        timestamp = get_int(proof.get("timestamp"), default=-1)
        if timestamp >= best_any_timestamp:
            best_any_timestamp = timestamp
            best_any = username

        proof_type = str(proof.get("type") or "")
        if proof_type == "USERNAME_TYPE_FNAME" and timestamp >= best_fname_timestamp:
            best_fname_timestamp = timestamp
            best_fname = username

    if best_fname:
        return best_fname
    if best_any:
        return best_any
    return None


def fetch_username_by_fid_from_hub(
    session: requests.Session,
    hub_url: str,
    fid: int,
) -> str | None:
    username_from_user_data: str | None = None
    try:
        payload = request_json(
            session=session,
            url=f"{hub_url.rstrip('/')}{HUB_USER_DATA_BY_FID_URL_PATH}",
            params={"fid": int(fid), "pageSize": 100},
            timeout=20,
        )
        username_from_user_data = latest_username_from_user_data_payload(payload)
    except RuntimeError:
        username_from_user_data = None

    username_from_proofs: str | None = None
    try:
        payload = request_json(
            session=session,
            url=f"{hub_url.rstrip('/')}{HUB_USERNAME_PROOFS_BY_FID_URL_PATH}",
            params={"fid": int(fid)},
            timeout=20,
        )
        username_from_proofs = latest_username_from_name_proofs_payload(payload)
    except RuntimeError:
        username_from_proofs = None

    # Prefer fname-based proof names when available; they are canonical handles.
    if username_from_proofs:
        return username_from_proofs
    return username_from_user_data


def enrich_records_with_hub_usernames(
    records: list[dict[str, Any]],
    hub_url: str | None,
) -> None:
    if not hub_url:
        return

    records_by_fid: dict[int, list[dict[str, Any]]] = {}
    for record in records:
        existing_username = normalize_farcaster_handle(record.get("author_username"))
        fid = get_int(record.get("author_fid"), default=0)
        if fid <= 0:
            continue
        if existing_username:
            record["author_username"] = existing_username
            record["author_username_with_fid"] = f"{existing_username} ({fid})"
            continue
        records_by_fid.setdefault(fid, []).append(record)

    if not records_by_fid:
        return

    session = build_plain_session()
    resolved = 0
    for fid, fid_records in records_by_fid.items():
        try:
            username = fetch_username_by_fid_from_hub(
                session=session,
                hub_url=hub_url,
                fid=fid,
            )
        except RuntimeError:
            continue

        if not username:
            continue

        resolved += 1
        for record in fid_records:
            record["author_username"] = username
            record["author_username_with_fid"] = f"{username} ({fid})"

    if resolved > 0:
        logging.info(
            "Final context username enrichment: resolved %s of %s missing FIDs",
            resolved,
            len(records_by_fid),
        )


def build_farcaster_xyz_url(record: dict[str, Any]) -> str:
    cast_hash_short = short_cast_hash(record.get("hash"))
    if not cast_hash_short:
        return normalize_text(record.get("cast_url"))

    handle = normalize_farcaster_handle(record.get("author_username"))
    if handle:
        return f"https://farcaster.xyz/{handle}/{cast_hash_short}"
    return normalize_text(record.get("cast_url"))


def normalize_text(value: Any) -> str:
    text = str(value or "").replace("\x00", "")
    return " ".join(text.split())


def preview_text(value: Any, limit: int = 160) -> str:
    text = normalize_text(value)
    if not text:
        return "[no text]"
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def detect_content_label(text: str) -> str:
    t = text.lower()
    if not t or t == "[no text]":
        return "empty"
    promo_terms = (
        "$",
        "ca:",
        "ticker",
        "airdrop",
        "launched",
        "deployment",
        "deploy ",
        "new clanker",
    )
    if any(term in t for term in promo_terms):
        return "promo"
    if "?" in t:
        return "question"
    return "general"


def classify_visibility_tier(score: float, within_ranking_window: bool) -> str:
    if not within_ranking_window:
        return "STALE"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    return "LOW"


def build_scored_records(
    casts: list[dict[str, Any]],
    config: ScrapeConfig,
) -> list[dict[str, Any]]:
    logging.info("Scoring %s casts", len(casts))

    like_metrics_by_hash: dict[str, dict[str, Any]] = {}
    casts_needing_likes = []

    for cast in casts:
        cast_hash = cast.get("hash")
        if not isinstance(cast_hash, str):
            continue
        likes_count = get_int((cast.get("reactions") or {}).get("likes_count"), default=0)
        if likes_count > 0:
            casts_needing_likes.append((cast_hash, likes_count))
        else:
            like_metrics_by_hash[cast_hash] = {
                "likes_reported": 0,
                "likes_fetched": 0,
                "genuine_like_count": 0,
                "genuine_like_points": 0.0,
                "mean_liker_score": None,
                "like_data_source": "none",
            }

    can_enrich_like_quality = (
        bool(config.enrich_like_quality)
        and config.source == "neynar"
        and bool(config.api_key)
    )

    if casts_needing_likes and can_enrich_like_quality:
        logging.info(
            "Fetching like quality for %s casts with likes using %s workers",
            len(casts_needing_likes),
            config.like_workers,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=config.like_workers) as executor:
            future_map: dict[concurrent.futures.Future[dict[str, Any]], tuple[str, int]] = {}
            for cast_hash, likes_count in casts_needing_likes:
                future = executor.submit(
                    compute_like_metrics,
                    str(config.api_key),
                    cast_hash,
                    likes_count,
                    config.like_page_size,
                    config.like_max_pages,
                )
                future_map[future] = (cast_hash, likes_count)

            completed = 0
            for future in concurrent.futures.as_completed(future_map):
                cast_hash, likes_count = future_map[future]
                try:
                    like_metrics_by_hash[cast_hash] = future.result()
                except Exception:
                    logging.exception("Like scoring worker failed for cast %s", cast_hash)
                    like_metrics_by_hash[cast_hash] = fallback_like_metrics(likes_count)

                completed += 1
                if completed % 200 == 0 or completed == len(future_map):
                    logging.info("Like quality progress: %s/%s", completed, len(future_map))
    elif casts_needing_likes:
        if config.source != "neynar":
            logging.info(
                "Skipping liker-quality enrichment for %s casts (supported only with source=neynar)",
                len(casts_needing_likes),
            )
        elif not config.enrich_like_quality:
            logging.info(
                "Skipping liker-quality enrichment for %s casts (use --enrich-like-quality to enable)",
                len(casts_needing_likes),
            )
        else:
            logging.info(
                "Skipping liker-quality enrichment for %s casts (missing API key)",
                len(casts_needing_likes),
            )
        for cast_hash, likes_count in casts_needing_likes:
            like_metrics_by_hash[cast_hash] = {
                "likes_reported": likes_count,
                "likes_fetched": 0,
                "genuine_like_count": likes_count,
                "genuine_like_points": float(likes_count),
                "mean_liker_score": None,
                "like_data_source": "reported_only",
            }

    scored: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)

    for cast in casts:
        cast_hash = cast.get("hash")
        if not isinstance(cast_hash, str):
            continue

        reactions = cast.get("reactions") or {}
        replies = cast.get("replies") or {}

        likes_count = get_int(reactions.get("likes_count"), default=0)
        recasts_count = get_int(reactions.get("recasts_count"), default=0)
        replies_count = get_int(replies.get("count"), default=0)

        like_metrics = like_metrics_by_hash.get(cast_hash, fallback_like_metrics(likes_count))
        genuine_like_points = float(like_metrics.get("genuine_like_points") or 0.0)

        ts_value = cast.get("timestamp")
        cast_dt_utc: datetime | None = None
        age_hours: float | None = None
        within_like_ranking_window = False
        if isinstance(ts_value, str):
            try:
                cast_dt_utc = datetime.fromisoformat(ts_value.replace("Z", "+00:00")).astimezone(timezone.utc)
            except ValueError:
                cast_dt_utc = None
        if cast_dt_utc is not None:
            age = (now_utc - cast_dt_utc).total_seconds() / 3600.0
            age_hours = round(age, 4)
            within_like_ranking_window = 0.0 <= age <= float(config.ranking_max_age_hours)

        ranking_like_count = likes_count if within_like_ranking_window else -1

        cast_author = cast.get("author") or {}
        channel = cast.get("channel") or {}
        text_value = cast.get("text")
        text_preview = preview_text(text_value, limit=360)
        author_fid = cast_author.get("fid")
        author_username = cast_author.get("username")
        if author_username is not None and author_fid is not None:
            author_username_with_fid = f"{author_username} ({author_fid})"
        elif author_username is not None:
            author_username_with_fid = str(author_username)
        elif author_fid is not None:
            author_username_with_fid = f"fid:{author_fid}"
        else:
            author_username_with_fid = "unknown (unknown)"

        scored.append(
            {
                "hash": cast_hash,
                "timestamp": cast.get("timestamp"),
                "type": "comment" if is_comment(cast) else "post",
                "author_fid": author_fid,
                "author_username": author_username,
                "author_username_with_fid": author_username_with_fid,
                "channel_id": channel.get("id") if isinstance(channel, dict) else None,
                "text": text_value,
                "text_preview": text_preview,
                "parent_hash": cast.get("parent_hash"),
                "cast_url": build_cast_url(cast_hash),
                "content_label": detect_content_label(text_preview),
                "age_hours": age_hours,
                "within_like_ranking_window": within_like_ranking_window,
                "ranking_like_count": ranking_like_count,
                "engagement": {
                    "likes_count": likes_count,
                    "recasts_count": recasts_count,
                    "replies_count": replies_count,
                    "genuine_like_count": like_metrics.get("genuine_like_count", 0),
                    "genuine_like_points": round(genuine_like_points, 4),
                    "mean_liker_score": like_metrics.get("mean_liker_score"),
                    "like_data_source": like_metrics.get("like_data_source"),
                    "likes_fetched": like_metrics.get("likes_fetched", 0),
                },
                "engagement_raw": float(likes_count),
                "engagement_transformed": None,
                "algorithmic_score_1_to_10": None,
                "score_formula": (
                    f"rank by likes_count for casts <= {config.ranking_max_age_hours}h old; "
                    "stale casts ranked below fresh ones"
                ),
            }
        )

    if not scored:
        return []

    for record in scored:
        within_window = bool(record.get("within_like_ranking_window"))
        fresh_likes = int(record.get("ranking_like_count") or 0)
        if not within_window or fresh_likes <= 0:
            score = 1.0
        else:
            # Likes-only score curve with absolute scale: prevents tiny-like counts from
            # appearing as top-tier scores when overall engagement is low.
            denom = math.log1p(float(config.likes_for_top_score))
            normalized = math.log1p(float(fresh_likes)) / denom if denom > 0 else 0.0
            score = 1.0 + 9.0 * max(0.0, min(1.0, normalized))
        bounded_score = round(max(1.0, min(10.0, score)), 2)
        record["algorithmic_score_1_to_10"] = bounded_score
        record["visibility_tier"] = classify_visibility_tier(bounded_score, within_window)
        record["visibility_score_0_to_100"] = int(round(bounded_score * 10))

    scored.sort(
        key=lambda r: (
            1 if bool(r.get("within_like_ranking_window")) else 0,
            int((r.get("engagement") or {}).get("likes_count") or 0),
            str(r.get("timestamp") or ""),
        ),
        reverse=True,
    )

    for i, record in enumerate(scored, start=1):
        record["rank"] = i

    return scored


def estimate_llm_tokens(text: str) -> int:
    # Practical heuristic: GPT token count is often around chars / 4 for English-like text.
    if not text:
        return 0
    return math.ceil(len(text) / 4)


def write_scored_txt(
    records: list[dict[str, Any]],
    output_path: Path,
    target_date: date,
    timezone_name: str,
    query: str,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    json_lines = [json.dumps(item, ensure_ascii=False) for item in records]
    payload_text = "\n".join(json_lines)
    token_estimate = estimate_llm_tokens(payload_text)

    with output_path.open("w", encoding="utf-8") as f:
        f.write(
            f"ESTIMATED_LLM_TOKENS_FOR_SCRAPED_DATA: {token_estimate} "
            "(heuristic=ceil(char_count/4))\n"
        )
        f.write(f"DATE: {target_date.isoformat()}\n")
        f.write(f"TIMEZONE: {timezone_name}\n")
        f.write(f"QUERY: {query}\n")
        f.write(f"TOTAL_RECORDS: {len(records)}\n")
        for line in json_lines:
            f.write(line + "\n")

    return token_estimate


def write_llm_instructions_txt(
    instructions_path: Path,
    data_path: Path,
    target_date: date,
    token_estimate: int,
    main_question: str,
) -> None:
    instructions_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        f"ESTIMATED_LLM_TOKENS_FOR_SCRAPED_DATA: {token_estimate} (heuristic=ceil(char_count/4))",
        f"DATA_FILE: {data_path}",
        f"TARGET_DATE: {target_date.isoformat()}",
        f"MAIN_QUESTION: {main_question}",
        "",
        "You are analyzing Farcaster posts/comments for one day.",
        "",
        "Instructions:",
        "1. Read every JSON line in DATA_FILE after metadata header lines.",
        "2. Preserve the provided rank and algorithmic_score_1_to_10 values.",
        "3. Use author_username_with_fid for user-level references and cast_url for direct links.",
        "4. Produce summary sections:",
        "   - Volume: total posts vs comments.",
        "   - Top 20 by algorithmic score.",
        "   - Topics: recurring themes with examples.",
        "   - Engagement quality: patterns in mean_liker_score and genuine_like_count.",
        "5. Flag likely spam/manipulation signals:",
        "   - very low mean_liker_score with high like counts",
        "   - repeated near-duplicate text",
        "   - sudden engagement spikes from low-score liker pools",
        "6. Do not invent missing fields. If data is missing, state it explicitly.",
    ]

    instructions_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_readable_ranked_txt(
    records: list[dict[str, Any]],
    output_path: Path,
    target_date: date,
    timezone_name: str,
    token_estimate: int,
    ranking_max_age_hours: float,
) -> None:
    total = len(records)
    posts = sum(1 for r in records if r.get("type") == "post")
    comments = sum(1 for r in records if r.get("type") == "comment")
    high = sum(1 for r in records if r.get("visibility_tier") == "HIGH")
    medium = sum(1 for r in records if r.get("visibility_tier") == "MEDIUM")
    low = sum(1 for r in records if r.get("visibility_tier") == "LOW")
    stale = sum(1 for r in records if r.get("visibility_tier") == "STALE")
    fresh = sum(1 for r in records if bool(r.get("within_like_ranking_window")))
    unique_authors = len(
        {
            r.get("author_fid")
            for r in records
            if r.get("author_fid") is not None
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(
            f"ESTIMATED_LLM_TOKENS_FOR_SCRAPED_DATA: {token_estimate} "
            "(heuristic=ceil(char_count/4))\n"
        )
        f.write(f"DATE: {target_date.isoformat()}\n")
        f.write(f"TIMEZONE: {timezone_name}\n")
        f.write("FORMAT: one record per line with extra spacing between records\n")
        f.write(
            "RANKING_RULE: likes_count descending for casts <= "
            f"{ranking_max_age_hours}h old; older casts listed after fresh set\n"
        )
        f.write(
            f"SUMMARY: total={total} posts={posts} comments={comments} unique_authors={unique_authors} "
            f"tiers(H/M/L/STALE)={high}/{medium}/{low}/{stale} fresh={fresh}\n"
        )
        f.write("COLUMNS: rank | score | tier | fresh<=24h | type | user | fid | likes | recasts | replies | label | age_h | timestamp | channel | url | content\n")
        f.write("\n")
        f.write("TOP_25_BY_SCORE:\n")
        f.write("\n")

        top_records = records[:25]
        for r in top_records:
            e = r.get("engagement") or {}
            channel_id = r.get("channel_id") or "none"
            full_text = normalize_text(r.get("text"))
            if not full_text:
                full_text = "[no text]"
            line = (
                f"{int(r.get('rank') or 0):03d} | {float(r.get('algorithmic_score_1_to_10') or 0):>5.2f} "
                f"| {str(r.get('visibility_tier') or 'LOW'):>6} | {str(r.get('type') or 'unknown'):>7} "
                f"| fresh={bool(r.get('within_like_ranking_window'))} "
                f"| {r.get('author_username_with_fid')} | fid={r.get('author_fid')} "
                f"| likes={e.get('likes_count', 0)} | recasts={e.get('recasts_count', 0)} | replies={e.get('replies_count', 0)} "
                f"| {r.get('content_label')} | age_h={r.get('age_hours')} | {r.get('timestamp')} | {channel_id} | {r.get('cast_url')} | content={full_text}"
            )
            f.write(line + "\n\n")

        f.write("\n")
        f.write("ALL_RECORDS_BY_RANK:\n")
        f.write("\n")

        for r in records:
            e = r.get("engagement") or {}
            channel_id = r.get("channel_id") or "none"
            full_text = normalize_text(r.get("text"))
            if not full_text:
                full_text = "[no text]"
            line = (
                f"{int(r.get('rank') or 0):03d} | {float(r.get('algorithmic_score_1_to_10') or 0):>5.2f} "
                f"| {str(r.get('visibility_tier') or 'LOW'):>6} | {str(r.get('type') or 'unknown'):>7} "
                f"| fresh={bool(r.get('within_like_ranking_window'))} "
                f"| {r.get('author_username_with_fid')} | fid={r.get('author_fid')} "
                f"| likes={e.get('likes_count', 0)} | recasts={e.get('recasts_count', 0)} | replies={e.get('replies_count', 0)} "
                f"| {r.get('content_label')} | age_h={r.get('age_hours')} | {r.get('timestamp')} | {channel_id} "
                f"| hash={r.get('hash')} | url={r.get('cast_url')} | content={full_text}"
            )
            f.write(line + "\n\n")


def record_user_label(record: dict[str, Any]) -> str:
    user_value = normalize_text(record.get("author_username_with_fid"))
    if user_value and user_value != "[no text]":
        return user_value
    fid_value = record.get("author_fid")
    if fid_value is not None:
        return f"fid:{fid_value}"
    return "unknown"


def record_channel_label(record: dict[str, Any]) -> str:
    channel_value = normalize_text(record.get("channel_id"))
    if channel_value and channel_value != "[no text]":
        return channel_value
    return "none"


def record_likes_count(record: dict[str, Any]) -> int:
    return get_int((record.get("engagement") or {}).get("likes_count"), default=0)


def timestamp_utc_local(value: Any, tz: tzinfo) -> tuple[str, str]:
    dt = parse_iso_timestamp(value)
    if dt is None:
        return "unknown", "unknown"
    return (
        format_iso_seconds(dt.astimezone(timezone.utc)),
        format_iso_seconds(dt.astimezone(tz)),
    )


def extract_top_keywords(
    records: list[dict[str, Any]],
    limit_records: int | None = None,
    limit_keywords: int = 12,
) -> list[tuple[str, int]]:
    keyword_counts: Counter[str] = Counter()
    candidate_records = records if limit_records is None else records[:limit_records]
    for record in candidate_records:
        text = normalize_text(record.get("text")).lower()
        if not text or text == "[no text]":
            continue
        for token in KEYWORD_PATTERN.findall(text):
            if token in KEYWORD_STOPWORDS:
                continue
            if token.startswith("http"):
                continue
            keyword_counts[token] += 1
    return keyword_counts.most_common(limit_keywords)


def _mojibake_marker_score(value: str) -> int:
    return sum(value.count(marker) for marker in ("Ã", "â", "ð", "Â"))


def maybe_fix_mojibake(text: str) -> str:
    if not text or text == "[no text]":
        return text

    original_score = _mojibake_marker_score(text)
    if original_score == 0:
        return text

    best = text
    best_score = original_score

    def redecode_once(value: str) -> str:
        local_best = value
        local_score = _mojibake_marker_score(value)
        for codec in ("latin-1", "cp1252"):
            try:
                candidate = value.encode(codec).decode("utf-8")
            except (LookupError, UnicodeEncodeError, UnicodeDecodeError):
                continue
            candidate = normalize_text(candidate)
            if not candidate:
                continue
            candidate_score = _mojibake_marker_score(candidate)
            if candidate_score < local_score:
                local_best = candidate
                local_score = candidate_score
        return local_best

    for _ in range(2):
        candidate = redecode_once(best)
        candidate_score = _mojibake_marker_score(candidate)
        if candidate_score >= best_score:
            break
        best = candidate
        best_score = candidate_score
    return best


def percent(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return (float(numerator) / float(denominator)) * 100.0


def pipe_safe_text(value: Any) -> str:
    text = normalize_text(value).replace("\uFFFD", "'")
    if not text:
        return "[no text]"
    return text.replace("|", "/")


def compact_readable_text(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return "[no text]"

    common_replacements = {
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€\x9d": '"',
        "â€“": "-",
        "â€”": "-",
        "â€¦": "...",
        "Â": "",
    }
    for source, target in common_replacements.items():
        text = text.replace(source, target)

    text = maybe_fix_mojibake(text)
    mojibake_likely = _mojibake_marker_score(text) > 0 or any(
        marker in text for marker in ("ï¸", "Ÿ", "�")
    )
    if mojibake_likely:
        ascii_candidate = normalize_text(text.encode("ascii", errors="ignore").decode("ascii"))
        if ascii_candidate and len(ascii_candidate) >= max(12, int(len(text) * 0.4)):
            text = ascii_candidate
    return text


def final_preview_text(value: Any, limit: int) -> str:
    text = compact_readable_text(value)
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return pipe_safe_text(text)


def infer_daily_theme(value: Any) -> str:
    text = normalize_text(value).lower()
    if not text or text == "[no text]":
        return "empty"

    # Ordered by specificity so a cast maps to one primary theme.
    if (
        "quorum" in text
        or "decentralization" in text
        or "forking farcaster" in text
        or "farcaster is forking" in text
        or "fork the farcaster" in text
    ):
        return "protocol_fork"
    if (
        re.search(r"\$[a-z][a-z0-9]{1,}", text) is not None
        or " ticker" in text
        or "token" in text
        or "clanker" in text
        or "airdrop" in text
        or "deployed" in text
        or "deployment" in text
        or "forkcaster" in text
    ):
        return "token_promo"
    if (
        "1password" in text
        or "security" in text
        or "hacker" in text
        or "hack" in text
        or "revoke" in text
        or "permission" in text
        or "permissions" in text
        or "connected apps" in text
        or "key material" in text
    ):
        return "security_ops"
    if (
        "clicker" in text
        or "referral" in text
        or "framedl" in text
        or "mini app" in text
    ):
        return "apps_games"
    if "base" in text or "blue" in text:
        return "base_culture"
    if (
        "good morning" in text
        or "good night" in text
        or "good noon" in text
        or "good evening" in text
        or "good afternoon" in text
        or text.startswith("gm ")
        or text == "gm"
        or re.search(r"\bhappy\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|tree\s+tuesday)\b", text)
        or re.search(r"\bm\s*o\s*r\s*n\s*i\s*n\s*g\b", text)
        or re.search(r"\be\s*v\s*e\s*n\s*i\s*n\s*g\b", text)
    ):
        return "daily_greetings"
    return "general_chat"


def theme_display_name(theme: str) -> str:
    mapping = {
        "protocol_fork": "Protocol/Fork",
        "token_promo": "Token/Promo",
        "security_ops": "Security/Ops",
        "apps_games": "Apps/Games",
        "base_culture": "Base Culture",
        "daily_greetings": "Greetings",
        "general_chat": "General Chat",
        "empty": "Empty",
    }
    return mapping.get(theme, theme)


THEME_ALIASES = {
    "protocol": "protocol_fork",
    "protocol_fork": "protocol_fork",
    "fork": "protocol_fork",
    "forks": "protocol_fork",
    "token": "token_promo",
    "token_promo": "token_promo",
    "promo": "token_promo",
    "airdrop": "token_promo",
    "airdrops": "token_promo",
    "security": "security_ops",
    "security_ops": "security_ops",
    "ops": "security_ops",
    "app": "apps_games",
    "apps": "apps_games",
    "apps_games": "apps_games",
    "game": "apps_games",
    "base": "base_culture",
    "base_culture": "base_culture",
    "greeting": "daily_greetings",
    "greetings": "daily_greetings",
    "daily_greetings": "daily_greetings",
    "chat": "general_chat",
    "general": "general_chat",
    "general_chat": "general_chat",
    "empty": "empty",
}

KNOWN_THEMES = frozenset(THEME_ALIASES.values())


def parse_theme_filters(raw_value: str) -> tuple[str, ...]:
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return tuple()

    normalized: list[str] = []
    unknown: list[str] = []
    for item in raw_value.split(","):
        token = item.strip().lower()
        if not token:
            continue
        canonical = THEME_ALIASES.get(token.replace(" ", "_").replace("-", "_"), token)
        if canonical not in KNOWN_THEMES:
            unknown.append(token)
            continue
        if canonical not in normalized:
            normalized.append(canonical)

    if unknown:
        raise ValueError(f"Unknown theme value(s): {', '.join(sorted(unknown))}")
    return tuple(normalized)


FINAL_GREETING_PATTERN = re.compile(
    r"\b(?:gm|gn|bm|ge|ga|bn|good morning|good night|good evening|good afternoon|morning|evening|afternoon|night|morning fam|base morning|base noon|base evening|good noon|happy tree tuesday|tree tuesday|happy monday|happy tuesday|happy wednesday|happy thursday|happy friday|happy saturday|happy sunday)\b",
    re.IGNORECASE,
)
FINAL_GREETING_SQUEEZED_TERMS = {
    "goodmorning",
    "goodnight",
    "goodevening",
    "goodafternoon",
    "goodnoon",
    "morningfam",
    "basemorning",
    "baseevening",
    "basenoon",
    "happytreetuesday",
    "treetuesday",
}
FINAL_GREETING_STYLIZED_PATTERN = re.compile(
    r"(?:m\W*o\W*r\W*n\W*i\W*n\W*g|e\W*v\W*e\W*n\W*i\W*n\W*g|a\W*f\W*t\W*e\W*r\W*n\W*o\W*o\W*n|n\W*i\W*g\W*h\W*t|g\W*m|g\W*n)",
    re.IGNORECASE,
)
FINAL_PROMO_PATTERN = re.compile(
    r"(?:\$[a-z0-9]{2,}|\b(?:airdrop|airdrops|token|tokens|ticker|ca:|contract address|launch(?:ed|ing)?|deployed|deployment|mint(?:ed|ing)?|clanker(?:s)?|forkcaster|pump|moon|degen|nft|usdt|signal alert|leverage|stop loss|targets?|pair:|long signal|short signal|creator coins?)\b)",
    re.IGNORECASE,
)
FINAL_PROMO_SQUEEZED_TERMS = {
    "contractaddress",
    "signalalert",
    "longsignal",
    "shortsignal",
}
FINAL_SHORT_GENERIC_TEXTS = {
    "gm",
    "gn",
    "good morning",
    "good night",
    "good evening",
    "good noon",
    "nice",
    "great",
    "awesome",
    "amazing",
    "beautiful",
    "lfg",
    "thanks",
    "thank you",
    "do this",
}
FINAL_LOW_SIGNAL_SINGLE_WORDS = {
    "lol",
    "fr",
    "correct",
    "nice",
    "amazing",
    "beautiful",
}
FINAL_KEYWORD_STOPWORDS = KEYWORD_STOPWORDS.union(
    {
        "again",
        "beautiful",
        "every",
        "good",
        "happy",
        "hello",
        "hey",
        "itap",
        "keep",
        "let",
        "life",
        "look",
        "love",
        "make",
        "moment",
        "morning",
        "night",
        "other",
        "over",
        "own",
        "peace",
        "please",
        "see",
        "someone",
        "stay",
        "still",
        "strong",
        "sunset",
        "take",
        "thanks",
        "tomorrow",
        "where",
        "world",
        "youre",
        "even",
        "feel",
        "never",
        "don",
    }
)
FINAL_MAX_CASTS_PER_AUTHOR = 3
FINAL_QUOTE_SUGGESTION_COUNT = 10
FINAL_QUOTE_MAX_PER_AUTHOR = 2
FINAL_QUOTE_SPAM_PATTERN = re.compile(
    r"(?:\b(?:join now|reward pool|campaign|quests?|taskpay|waitlist|drop a reply|bullish on|check your score|get roasted|follow for|dm me|invite|referral code|airdrop)\b)",
    re.IGNORECASE,
)
FINAL_QUOTE_SPAM_TEXT_MARKERS = (
    "day ",
    "baseposting",
    "stay base",
    "good morning",
    "good night",
    "good evening",
)


def final_tokenize_words(value: Any) -> list[str]:
    return re.findall(r"[a-z0-9']+", compact_readable_text(value).lower())


def final_text_fingerprint(value: Any) -> str:
    words = [
        token
        for token in final_tokenize_words(value)
        if len(token) >= 3 and token not in FINAL_KEYWORD_STOPWORDS
    ]
    if not words:
        words = [token for token in final_tokenize_words(value) if len(token) >= 2]
    return " ".join(words[:14])


def extract_top_keywords_for_final(
    records: list[dict[str, Any]],
    limit_keywords: int = 20,
) -> list[tuple[str, int]]:
    counts: Counter[str] = Counter()
    for record in records:
        for token in final_tokenize_words(record.get("text")):
            if len(token) < 3:
                continue
            if token in FINAL_KEYWORD_STOPWORDS:
                continue
            if token.startswith("http"):
                continue
            if token.isdigit():
                continue
            if len(set(token)) <= 1:
                continue
            counts[token] += 1
    return counts.most_common(limit_keywords)


def final_median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def final_percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    bounded_pct = max(0.0, min(100.0, pct))
    if len(ordered) == 1:
        return ordered[0]
    rank = int(round((bounded_pct / 100.0) * (len(ordered) - 1)))
    rank = max(0, min(rank, len(ordered) - 1))
    return ordered[rank]


def final_quote_spam_penalty(record: dict[str, Any]) -> int:
    text = compact_readable_text(record.get("text"))
    lower = text.lower()
    words = final_tokenize_words(text)
    unique_words = len({word for word in words if len(word) >= 2})
    theme = infer_daily_theme(text)
    engagement = record.get("engagement") or {}
    replies = get_int(engagement.get("replies_count"), default=0)
    likes = get_int(engagement.get("likes_count"), default=0)
    recasts = get_int(engagement.get("recasts_count"), default=0)

    penalty = 0
    if record.get("content_label") == "promo":
        penalty += 5
    if theme in {"token_promo", "daily_greetings"}:
        penalty += 5
    if FINAL_PROMO_PATTERN.search(lower):
        penalty += 4
    if FINAL_QUOTE_SPAM_PATTERN.search(lower):
        penalty += 3
    if re.search(r"\bday\s+\d+\b", lower) and ("base" in lower or "posting" in lower):
        penalty += 4
    if "stay base" in lower:
        penalty += 4
    if "base app" in lower and len(words) < 10:
        penalty += 3
    hype_emoji_count = sum(text.count(symbol) for symbol in ("🟦", "🚀", "💯", "🔥", "✨", "📈"))
    if hype_emoji_count >= 4:
        penalty += 2
    if "http://" in lower or "https://" in lower:
        penalty += 1
    if lower.count("#") >= 2:
        penalty += 1
    if len(words) < 7:
        penalty += 2
    if unique_words <= 4:
        penalty += 2
    if sum(1 for token in words if token in {"gm", "gn", "base", "fud", "moon"}) >= 4:
        penalty += 1
    if any(marker in lower for marker in FINAL_QUOTE_SPAM_TEXT_MARKERS):
        penalty += 1
    if sum(1 for token in words if token in {"base", "blue", "based"}) >= 4 and unique_words <= 10:
        penalty += 2
    if "fud" in words and ("base" in words or "blue" in words):
        penalty += 2
    if replies <= 1 and likes <= 6 and recasts <= 1:
        penalty += 2
    return penalty


def final_quote_score(
    record: dict[str, Any],
    *,
    relative_engagement: float,
    spam_penalty: int,
) -> float:
    engagement = record.get("engagement") or {}
    likes = get_int(engagement.get("likes_count"), default=0)
    recasts = get_int(engagement.get("recasts_count"), default=0)
    replies = get_int(engagement.get("replies_count"), default=0)
    engagement_score = final_engagement_score(record)
    score = (
        (engagement_score * 0.70)
        + (replies * 1.80)
        + (recasts * 1.25)
        + math.log1p(likes) * 6.5
        + (min(relative_engagement, 4.0) * 16.0)
        - (float(spam_penalty) * 22.0)
    )
    return round(score, 3)


def final_exclusion_reason(record: dict[str, Any]) -> str | None:
    text = compact_readable_text(record.get("text")).lower()
    if not text or text == "[no text]":
        return "empty_text"

    normalized = re.sub(r"[^a-z0-9 ]+", " ", text)
    normalized = " ".join(normalized.split())
    if normalized in FINAL_SHORT_GENERIC_TEXTS:
        return "short_generic"

    words = re.findall(r"[a-z0-9']+", normalized)
    alpha_words = [word for word in words if re.search(r"[a-z]", word)]
    unique_alpha_words = {word for word in alpha_words if len(word) >= 2}
    squeezed = re.sub(r"[^a-z]+", "", text)

    if (
        FINAL_PROMO_PATTERN.search(text)
        or any(token in squeezed for token in FINAL_PROMO_SQUEEZED_TERMS)
        or (
            "http" in text
            and any(
                phrase in text
                for phrase in ("join here", "new quest", "rewards:", "alpha signal", "alpha signals")
            )
        )
    ):
        return "token_airdrop_promo"

    if (
        FINAL_GREETING_PATTERN.search(text)
        or FINAL_GREETING_STYLIZED_PATTERN.search(text)
        or any(token in squeezed for token in FINAL_GREETING_SQUEEZED_TERMS)
        or any(token in squeezed for token in ("morning", "evening", "afternoon", "night"))
        or squeezed.startswith("bm")
        or (
            normalized.startswith("happy ")
            and any(
                token in normalized
                for token in (" monday", " tuesday", " wednesday", " thursday", " friday", " saturday", " sunday", " day")
            )
        )
        or any(token in {"gm", "gn", "bm", "ge", "ga", "bn"} for token in alpha_words[:3])
    ):
        return "greeting"

    engagement = record.get("engagement") or {}
    likes = get_int(engagement.get("likes_count"), default=0)
    replies = get_int(engagement.get("replies_count"), default=0)

    if not re.search(r"[a-z0-9]", normalized):
        return "symbolic_text"
    if not alpha_words:
        return "symbolic_text"
    if len(alpha_words) == 1 and alpha_words[0] in FINAL_LOW_SIGNAL_SINGLE_WORDS and replies < 20:
        return "low_signal_short"
    if len(alpha_words) <= 2 and len(squeezed) < 18 and replies < 30:
        return "low_signal_short"
    if len(alpha_words) <= 3 and len(unique_alpha_words) <= 3 and replies < 50:
        return "low_signal_short"
    if len(alpha_words) <= 3 and len(unique_alpha_words) <= 2 and replies < 30:
        return "low_signal_short"
    if len(unique_alpha_words) <= 1 and likes < 60 and replies < 35:
        return "low_signal_short"

    return None


def final_engagement_score(record: dict[str, Any]) -> float:
    engagement = record.get("engagement") or {}
    likes = get_int(engagement.get("likes_count"), default=0)
    recasts = get_int(engagement.get("recasts_count"), default=0)
    replies = get_int(engagement.get("replies_count"), default=0)
    informative_tokens = sum(
        1 for token in final_tokenize_words(record.get("text")) if len(token) >= 4
    )
    content_bonus = min(informative_tokens, 25) * 0.2
    return round(
        float(likes)
        + (1.5 * float(recasts))
        + (2.5 * float(replies))
        + content_bonus,
        3,
    )


def write_final_context_txt(
    records: list[dict[str, Any]],
    output_path: Path,
    target_date: date,
    timezone_name: str,
    final_top_posts: int,
    final_comments_per_post: int,
    final_snippet_length: int,
    hub_url: str | None = None,
    focus_themes: tuple[str, ...] = tuple(),
    exclude_themes: tuple[str, ...] = tuple(),
) -> int:
    tz = resolve_timezone(timezone_name)
    top_cast_limit = max(200, get_int(final_top_posts, default=200))
    thread_comments_per_post = max(0, min(final_comments_per_post, 5))
    snippet_len = max(100, min(final_snippet_length, 200))
    focus_theme_set = set(focus_themes or ())
    exclude_theme_set = set(exclude_themes or ())

    total = len(records)
    posts = [record for record in records if record.get("type") == "post"]
    comments = [record for record in records if record.get("type") == "comment"]
    unique_authors = len(
        {
            record.get("author_fid")
            for record in records
            if record.get("author_fid") is not None
        }
    )

    timestamps = [
        dt.astimezone(timezone.utc)
        for dt in (parse_iso_timestamp(record.get("timestamp")) for record in records)
        if dt is not None
    ]
    window_start_utc = format_iso_seconds(min(timestamps)) if timestamps else "unknown"
    window_end_utc = format_iso_seconds(max(timestamps)) if timestamps else "unknown"

    hour_counts: Counter[str] = Counter()
    for dt in timestamps:
        hour_counts[dt.astimezone(tz).strftime("%H:00")] += 1
    hot_hours = " ; ".join(
        f"{hour}({count})"
        for hour, count in hour_counts.most_common(8)
    ) or "none"

    exclusion_counts: Counter[str] = Counter()
    scored_candidates: list[tuple[float, int, int, int, int, dict[str, Any]]] = []
    for record in records:
        theme = infer_daily_theme(record.get("text"))
        if focus_theme_set and theme not in focus_theme_set:
            exclusion_counts["excluded_by_theme_focus"] += 1
            continue
        if theme in exclude_theme_set:
            exclusion_counts["excluded_by_theme"] += 1
            continue

        exclusion_reason = final_exclusion_reason(record)
        if exclusion_reason is not None:
            exclusion_counts[exclusion_reason] += 1
            continue

        engagement = record.get("engagement") or {}
        likes = get_int(engagement.get("likes_count"), default=0)
        recasts = get_int(engagement.get("recasts_count"), default=0)
        replies = get_int(engagement.get("replies_count"), default=0)
        if likes <= 0 and recasts <= 0 and replies <= 0:
            exclusion_counts["no_engagement"] += 1
            continue

        engagement_score = final_engagement_score(record)
        if engagement_score <= 0:
            exclusion_counts["no_engagement"] += 1
            continue

        scored_candidates.append(
            (
                engagement_score,
                replies,
                likes,
                recasts,
                -get_int(record.get("rank"), default=10_000_000),
                record,
            )
        )

    scored_candidates.sort(reverse=True)
    filtered_records = [candidate[-1] for candidate in scored_candidates]

    selected_records: list[dict[str, Any]] = []
    selected_record_keys: set[str] = set()
    seen_fingerprints: set[str] = set()
    author_counts: Counter[int] = Counter()
    overflow_candidates: list[tuple[dict[str, Any], str, str, int | None]] = []

    for _, _, _, _, _, record in scored_candidates:
        record_key = pipe_safe_text(record.get("hash"))
        if record_key == "[no text]":
            record_key = (
                f"{get_int(record.get('author_fid'), default=0)}::"
                f"{pipe_safe_text(record.get('timestamp'))}::"
                f"{get_int(record.get('rank'), default=0)}"
            )
        if record_key in selected_record_keys:
            continue

        fingerprint = final_text_fingerprint(record.get("text"))
        if fingerprint and fingerprint in seen_fingerprints:
            exclusion_counts["near_duplicate"] += 1
            continue

        fid = record.get("author_fid")
        if isinstance(fid, int) and author_counts[fid] >= FINAL_MAX_CASTS_PER_AUTHOR:
            exclusion_counts["author_cap"] += 1
            overflow_candidates.append((record, record_key, fingerprint, fid))
            continue

        selected_records.append(record)
        selected_record_keys.add(record_key)
        if fingerprint:
            seen_fingerprints.add(fingerprint)
        if isinstance(fid, int):
            author_counts[fid] += 1
        if len(selected_records) >= top_cast_limit:
            break

    if len(selected_records) < top_cast_limit:
        for record, record_key, fingerprint, fid in overflow_candidates:
            if record_key in selected_record_keys:
                continue
            if fingerprint and fingerprint in seen_fingerprints:
                continue
            selected_records.append(record)
            selected_record_keys.add(record_key)
            if fingerprint:
                seen_fingerprints.add(fingerprint)
            if isinstance(fid, int):
                author_counts[fid] += 1
            if len(selected_records) >= top_cast_limit:
                break

    if not selected_records:
        selected_records = [
            candidate[-1]
            for candidate in sorted(
                (
                    (
                        final_engagement_score(record),
                        get_int((record.get("engagement") or {}).get("replies_count"), default=0),
                        record_likes_count(record),
                        get_int((record.get("engagement") or {}).get("recasts_count"), default=0),
                        -get_int(record.get("rank"), default=10_000_000),
                        record,
                    )
                    for record in records
                    if final_engagement_score(record) > 0
                ),
                reverse=True,
            )[:top_cast_limit]
        ]

    selected_posts = [record for record in selected_records if record.get("type") == "post"]
    selected_comments = [record for record in selected_records if record.get("type") == "comment"]

    selected_post_engagements = [
        final_engagement_score(record)
        for record in selected_posts
    ]
    selected_engagements = [
        final_engagement_score(record)
        for record in selected_records
    ]
    global_engagement_baseline = max(final_median(selected_engagements), 1.0)
    quote_high_engagement_threshold = max(
        40.0,
        final_percentile(selected_post_engagements, 82.0),
    )
    quote_fallback_engagement_threshold = max(
        24.0,
        final_percentile(selected_post_engagements, 60.0),
    )
    quote_relative_floor = max(global_engagement_baseline, quote_high_engagement_threshold * 0.50, 1.0)

    author_engagement_values: defaultdict[int, list[float]] = defaultdict(list)
    for record in filtered_records:
        fid = record.get("author_fid")
        if isinstance(fid, int):
            author_engagement_values[fid].append(final_engagement_score(record))
    author_engagement_baseline: dict[int, float] = {
        fid: max(final_median(values), quote_relative_floor, 1.0)
        for fid, values in author_engagement_values.items()
        if values
    }

    quote_primary_candidates: list[dict[str, Any]] = []
    quote_fallback_candidates: list[dict[str, Any]] = []
    quote_relaxed_candidates: list[dict[str, Any]] = []
    for record in selected_posts:
        text_tokens = final_tokenize_words(record.get("text"))
        if len(text_tokens) < 7:
            continue

        engagement = record.get("engagement") or {}
        likes = get_int(engagement.get("likes_count"), default=0)
        recasts = get_int(engagement.get("recasts_count"), default=0)
        replies = get_int(engagement.get("replies_count"), default=0)
        total_social = likes + recasts + replies
        engagement_score = final_engagement_score(record)
        if engagement_score <= 0:
            continue

        fid = record.get("author_fid")
        baseline = quote_relative_floor
        if isinstance(fid, int):
            baseline = author_engagement_baseline.get(fid, baseline)
        relative_engagement = engagement_score / max(baseline, 1.0)

        spam_penalty = final_quote_spam_penalty(record)
        if spam_penalty >= 6:
            continue

        quote_score = final_quote_score(
            record,
            relative_engagement=relative_engagement,
            spam_penalty=spam_penalty,
        )
        candidate_row = {
            "record": record,
            "quote_score": quote_score,
            "relative_engagement": round(relative_engagement, 3),
            "spam_penalty": spam_penalty,
            "engagement_score": engagement_score,
            "likes": likes,
            "recasts": recasts,
            "replies": replies,
            "why": (
                f"eng={engagement_score:.1f}; rel={relative_engagement:.2f}x baseline; "
                f"rp={replies}; spam_penalty={spam_penalty}"
            ),
        }

        if (
            engagement_score >= quote_high_engagement_threshold
            and relative_engagement >= 1.05
            and spam_penalty <= 1
            and total_social >= 25
        ):
            quote_primary_candidates.append(candidate_row)
            continue

        if (
            engagement_score >= quote_fallback_engagement_threshold
            and relative_engagement >= 0.90
            and spam_penalty <= 2
            and total_social >= 16
        ):
            quote_fallback_candidates.append(candidate_row)
            continue

        if (
            engagement_score >= max(18.0, quote_fallback_engagement_threshold * 0.70)
            and spam_penalty <= 3
            and total_social >= 12
        ):
            quote_relaxed_candidates.append(candidate_row)

    def sort_quote_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            candidates,
            key=lambda item: (
                float(item.get("quote_score") or 0.0),
                float(item.get("engagement_score") or 0.0),
                int(item.get("replies") or 0),
                int(item.get("likes") or 0),
            ),
            reverse=True,
        )

    quote_primary_candidates = sort_quote_candidates(quote_primary_candidates)
    quote_fallback_candidates = sort_quote_candidates(quote_fallback_candidates)
    quote_relaxed_candidates = sort_quote_candidates(quote_relaxed_candidates)

    quote_suggestions: list[dict[str, Any]] = []
    quote_seen_hashes: set[str] = set()
    quote_author_counts: Counter[int] = Counter()

    def append_quote_suggestions(candidates: list[dict[str, Any]]) -> None:
        for candidate in candidates:
            record = candidate.get("record")
            if not isinstance(record, dict):
                continue
            cast_hash = normalize_text(record.get("hash"))
            if not cast_hash or cast_hash in quote_seen_hashes:
                continue

            fid = record.get("author_fid")
            if isinstance(fid, int) and quote_author_counts[fid] >= FINAL_QUOTE_MAX_PER_AUTHOR:
                continue

            quote_suggestions.append(candidate)
            quote_seen_hashes.add(cast_hash)
            if isinstance(fid, int):
                quote_author_counts[fid] += 1
            if len(quote_suggestions) >= FINAL_QUOTE_SUGGESTION_COUNT:
                return

    append_quote_suggestions(quote_primary_candidates)
    if len(quote_suggestions) < FINAL_QUOTE_SUGGESTION_COUNT:
        append_quote_suggestions(quote_fallback_candidates)
    if len(quote_suggestions) < FINAL_QUOTE_SUGGESTION_COUNT:
        append_quote_suggestions(quote_relaxed_candidates)

    if len(quote_suggestions) < FINAL_QUOTE_SUGGESTION_COUNT:
        for candidate in sort_quote_candidates(
            [
                {
                    "record": record,
                    "quote_score": final_quote_score(
                        record,
                        relative_engagement=(
                            final_engagement_score(record)
                            / max(
                                author_engagement_baseline.get(
                                    get_int(record.get("author_fid"), default=0),
                                    quote_relative_floor,
                                ),
                                1.0,
                            )
                        ),
                        spam_penalty=final_quote_spam_penalty(record),
                    ),
                    "relative_engagement": round(
                        final_engagement_score(record)
                        / max(
                            author_engagement_baseline.get(
                                get_int(record.get("author_fid"), default=0),
                                quote_relative_floor,
                            ),
                            1.0,
                        ),
                        3,
                    ),
                    "spam_penalty": final_quote_spam_penalty(record),
                    "engagement_score": final_engagement_score(record),
                    "likes": get_int((record.get("engagement") or {}).get("likes_count"), default=0),
                    "recasts": get_int((record.get("engagement") or {}).get("recasts_count"), default=0),
                    "replies": get_int((record.get("engagement") or {}).get("replies_count"), default=0),
                    "why": "fallback from high-engagement posts after strict filtering",
                }
                for record in selected_posts
                if final_quote_spam_penalty(record) <= 3
            ]
        ):
            append_quote_suggestions([candidate])
            if len(quote_suggestions) >= FINAL_QUOTE_SUGGESTION_COUNT:
                break

    quote_records = [
        candidate.get("record")
        for candidate in quote_suggestions
        if isinstance(candidate, dict) and isinstance(candidate.get("record"), dict)
    ]

    theme_counts: Counter[str] = Counter(
        infer_daily_theme(record.get("text"))
        for record in selected_records
    )
    top_themes = sorted(
        theme_counts,
        key=lambda theme: (theme_counts[theme], theme),
        reverse=True,
    )[:8]
    top_themes_line = " ; ".join(
        (
            f"{theme_display_name(theme)}={theme_counts[theme]}"
            f"({percent(theme_counts[theme], len(selected_records)):.1f}%)"
        )
        for theme in top_themes
    ) or "none"

    most_replied_posts = sorted(
        selected_posts,
        key=lambda record: (
            get_int((record.get("engagement") or {}).get("replies_count"), default=0),
            final_engagement_score(record),
            -get_int(record.get("rank"), default=10_000_000),
        ),
        reverse=True,
    )[:30]
    keyword_source_records = most_replied_posts[:80]
    if len(keyword_source_records) < 40:
        keyword_source_records = selected_records
    selected_keywords = extract_top_keywords_for_final(
        keyword_source_records,
        limit_keywords=20,
    )
    selected_keywords_line = " ; ".join(
        f"{keyword}={count}"
        for keyword, count in selected_keywords
    ) or "none"

    max_what_happened = 12
    eligible_thread_comments = [
        record for record in filtered_records if record.get("type") == "comment"
    ]
    comments_by_parent: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for comment in eligible_thread_comments:
        comment_words = final_tokenize_words(comment.get("text"))
        comment_engagement = comment.get("engagement") or {}
        comment_likes = get_int(comment_engagement.get("likes_count"), default=0)
        comment_replies = get_int(comment_engagement.get("replies_count"), default=0)
        if len(comment_words) < 5 and comment_likes <= 1 and comment_replies <= 0:
            continue
        parent_hash = comment.get("parent_hash")
        if isinstance(parent_hash, str) and parent_hash:
            comments_by_parent[parent_hash].append(comment)
    for group in comments_by_parent.values():
        group.sort(
            key=lambda record: (
                final_engagement_score(record),
                get_int((record.get("engagement") or {}).get("replies_count"), default=0),
                -get_int(record.get("rank"), default=10_000_000),
            ),
            reverse=True,
        )

    output_records_for_username_enrichment: list[dict[str, Any]] = []
    output_records_for_username_enrichment.extend(
        record for record in selected_records if isinstance(record, dict)
    )
    output_records_for_username_enrichment.extend(
        record for record in quote_records if isinstance(record, dict)
    )
    output_records_for_username_enrichment.extend(
        record for record in most_replied_posts if isinstance(record, dict)
    )
    if thread_comments_per_post > 0:
        for post in most_replied_posts[:max_what_happened]:
            post_hash = post.get("hash")
            if not isinstance(post_hash, str) or not post_hash:
                continue
            output_records_for_username_enrichment.extend(
                record
                for record in comments_by_parent.get(post_hash, [])[:thread_comments_per_post]
                if isinstance(record, dict)
            )

    enrich_records_with_hub_usernames(
        records=output_records_for_username_enrichment,
        hub_url=hub_url,
    )

    discussion_cluster_rows: list[tuple[float, int, int, int, str, dict[str, Any], str]] = []
    records_by_theme: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in selected_records:
        records_by_theme[infer_daily_theme(record.get("text"))].append(record)
    for theme, items in records_by_theme.items():
        if not items:
            continue
        eng_sum = round(sum(final_engagement_score(item) for item in items), 2)
        reply_sum = sum(
            get_int((item.get("engagement") or {}).get("replies_count"), default=0)
            for item in items
        )
        unique_theme_authors = len(
            {
                item.get("author_fid")
                for item in items
                if item.get("author_fid") is not None
            }
        )
        lead_record = max(
            items,
            key=lambda item: (
                get_int((item.get("engagement") or {}).get("replies_count"), default=0),
                final_engagement_score(item),
                -get_int(item.get("rank"), default=10_000_000),
            ),
        )
        lead_keywords = extract_top_keywords_for_final(items, limit_keywords=4)
        keyword_line = ",".join(keyword for keyword, _ in lead_keywords) or "none"
        discussion_cluster_rows.append(
            (
                eng_sum,
                reply_sum,
                len(items),
                unique_theme_authors,
                theme,
                lead_record,
                keyword_line,
            )
        )
    discussion_cluster_rows.sort(
        key=lambda item: (item[0], item[1], item[2], item[3]),
        reverse=True,
    )

    excluded_line = " ; ".join(
        f"{reason}={count}"
        for reason, count in exclusion_counts.most_common()
    ) or "none"
    top_hour, top_hour_count = hour_counts.most_common(1)[0] if hour_counts else ("unknown", 0)
    avg_engagement = (
        round(
            sum(final_engagement_score(record) for record in selected_records) / len(selected_records),
            2,
        )
        if selected_records
        else 0.0
    )
    discussion_focus = " ; ".join(
        (
            f"{pipe_safe_text(post.get('hash'))}:"
            f"{get_int((post.get('engagement') or {}).get('replies_count'), default=0)}r/"
            f"{len(comments_by_parent.get(post.get('hash') or '', []))}c"
        )
        for post in most_replied_posts[:5]
        if isinstance(post.get("hash"), str) and post.get("hash")
    ) or "none"

    summary_lines = [
        (
            "S00 Theme filter active: "
            f"include={','.join(sorted(focus_theme_set)) if focus_theme_set else 'all'} | "
            f"exclude={','.join(sorted(exclude_theme_set)) if exclude_theme_set else 'none'}"
        ),
        (
            f"S01 Captured {total} casts ({len(posts)} posts, {len(comments)} comments) "
            f"from {unique_authors} authors."
        ),
        (
            f"S02 Filter removed {sum(exclusion_counts.values())} casts ({excluded_line})."
        ),
        (
            f"S03 Ranked {len(filtered_records)} eligible casts and kept top {len(selected_records)} "
            f"({len(selected_posts)} posts, {len(selected_comments)} comments)."
        ),
        (
            f"S04 Mean engagement score in selected set: {avg_engagement} "
            "(score = likes + 1.5*recasts + 2.5*replies + text bonus)."
        ),
        (
            f"S05 Dominant themes in selected set: {top_themes_line}."
        ),
        (
            f"S06 Most discussed thread hashes (replies/high-signal comments_available): {discussion_focus}."
        ),
        (
            f"S07 Peak hour ({timezone_name}) was {top_hour} with {top_hour_count} casts."
        ),
    ]

    narrative_lines: list[str] = []
    for index, (
        eng_sum,
        reply_sum,
        cast_count,
        author_count,
        theme,
        lead_record,
        keyword_line,
    ) in enumerate(discussion_cluster_rows[:6], start=1):
        lead_engagement = lead_record.get("engagement") or {}
        narrative_lines.append(
            (
                f"N{index:02d} {theme_display_name(theme)}: {cast_count} casts from {author_count} authors "
                f"(eng_sum={eng_sum:.2f}, replies={reply_sum}) led by "
                f"{pipe_safe_text(lead_record.get('hash'))} "
                f"(rp={get_int(lead_engagement.get('replies_count'), default=0)}, "
                f"l={get_int(lead_engagement.get('likes_count'), default=0)}) "
                f"keywords={pipe_safe_text(keyword_line)}."
            )
        )
    if not narrative_lines:
        narrative_lines.append("N00 no dominant discussion clusters after filtering.")

    what_happened_lines: list[str] = []
    for index, post in enumerate(most_replied_posts[:max_what_happened], start=1):
        post_engagement = post.get("engagement") or {}
        post_hash = post.get("hash")
        post_hash_str = post_hash if isinstance(post_hash, str) else ""
        thread_comments = comments_by_parent.get(post_hash_str, [])
        top_comment_preview = (
            final_preview_text(thread_comments[0].get("text"), limit=90)
            if thread_comments
            else "none"
        )
        what_happened_lines.append(
            (
                f"W{index:02d} {theme_display_name(infer_daily_theme(post.get('text')))} | "
                f"rp={get_int(post_engagement.get('replies_count'), default=0)} "
                f"l={get_int(post_engagement.get('likes_count'), default=0)} "
                f"rc={get_int(post_engagement.get('recasts_count'), default=0)} "
                f"top_comments_available={len(thread_comments)} "
                f"user={record_user_label(post)} "
                f"hash={pipe_safe_text(post_hash)} "
                f"text={final_preview_text(post.get('text'), limit=120)} "
                f"top_comment={top_comment_preview}"
            )
        )
    if not what_happened_lines:
        what_happened_lines.append("W00 no discussion-heavy posts after filtering.")

    generated_at_utc = format_iso_seconds(datetime.now(timezone.utc))
    body_lines = [
        "FORMAT_VERSION: FINAL_CONTEXT_V5",
        "PARSING_HINT: metadata uses KEY: VALUE, tables are pipe-delimited.",
        "URL_TEMPLATE: https://farcaster.xyz/{username}/{hash8} (fallback: cast_url)",
        "QUOTE_URL_TEMPLATE: https://farcaster.xyz/{username}/{hash8} (fallback: cast_url)",
        f"GENERATED_AT_UTC: {generated_at_utc}",
        f"DATE: {target_date.isoformat()}",
        f"TIMEZONE: {timezone_name}",
        f"WINDOW_UTC: {window_start_utc} -> {window_end_utc}",
        "RANKING_FORMULA: engagement_score=likes + 1.5*recasts + 2.5*replies + 0.2*min(informative_tokens,25)",
        (
            "FILTER_RULES: remove greeting chatter (gm/gn/good morning-like), "
            "token/airdrop/ticker promo language, symbolic or ultra-short low-signal casts, "
            "near-duplicates, and over-represented authors."
        ),
        (
            f"RAW_COUNTS: total={total} posts={len(posts)} comments={len(comments)} "
            f"unique_authors={unique_authors}"
        ),
        f"FILTERED_OUT: {excluded_line}",
        (
            f"SELECTED_TOP_SET: size={len(selected_records)} target_limit={top_cast_limit} "
            f"posts={len(selected_posts)} comments={len(selected_comments)} "
            f"max_casts_per_author={FINAL_MAX_CASTS_PER_AUTHOR}"
        ),
        f"HOT_HOURS_LOCAL_TOP8: {hot_hours}",
        f"TOP_THEMES_FILTERED: {top_themes_line}",
        f"TOP_KEYWORDS_FILTERED: {selected_keywords_line}",
        (
            "QUOTE_SELECTION_RULES: top 10 posts by quote_score where quote_score="
            "0.70*engagement_score + 1.80*replies + 1.25*recasts + 6.5*ln(1+likes)"
            " + 16*relative_author_engagement - 22*spam_penalty; strict anti-spam gating applied."
        ),
        "DAILY_SUMMARY_LINES:",
    ]
    body_lines.extend(summary_lines)
    body_lines.append("NARRATIVE_SUMMARY_LINES:")
    body_lines.extend(narrative_lines)
    body_lines.append(
        "DISCUSSION_CLUSTER_COLUMNS: idx|theme|casts|authors|eng_sum|reply_sum|lead_hash|lead_user|keywords|lead_txt"
    )
    if discussion_cluster_rows:
        for idx, (
            eng_sum,
            reply_sum,
            cast_count,
            author_count,
            theme,
            lead_record,
            keyword_line,
        ) in enumerate(discussion_cluster_rows[:8], start=1):
            body_lines.append(
                (
                    f"D{idx:02d}|{pipe_safe_text(theme_display_name(theme))}|"
                    f"{cast_count}|{author_count}|{eng_sum:.2f}|{reply_sum}|"
                    f"{pipe_safe_text(lead_record.get('hash'))}|"
                    f"{pipe_safe_text(record_user_label(lead_record))}|"
                    f"{pipe_safe_text(keyword_line)}|"
                    f"{final_preview_text(lead_record.get('text'), limit=110)}"
                )
            )
    else:
        body_lines.append("D00|none")

    body_lines.append("WHAT_HAPPENED_TODAY_LINES:")
    body_lines.extend(what_happened_lines)

    body_lines.append(
        "TOP_10_QUOTE_SUGGESTIONS_COLUMNS: idx|quote_score|rel_eng|spam|eng|l|rc|rp|theme|user|hash|quote_url|warpcast_url|why|txt"
    )
    if quote_suggestions:
        for index, suggestion in enumerate(quote_suggestions[:FINAL_QUOTE_SUGGESTION_COUNT], start=1):
            record = suggestion.get("record") if isinstance(suggestion, dict) else None
            if not isinstance(record, dict):
                continue
            body_lines.append(
                (
                    f"Q{index:02d}|"
                    f"{float(suggestion.get('quote_score') or 0.0):.2f}|"
                    f"{float(suggestion.get('relative_engagement') or 0.0):.3f}|"
                    f"{get_int(suggestion.get('spam_penalty'), default=0)}|"
                    f"{float(suggestion.get('engagement_score') or 0.0):.2f}|"
                    f"{get_int(suggestion.get('likes'), default=0)}|"
                    f"{get_int(suggestion.get('recasts'), default=0)}|"
                    f"{get_int(suggestion.get('replies'), default=0)}|"
                    f"{pipe_safe_text(theme_display_name(infer_daily_theme(record.get('text'))))}|"
                    f"{pipe_safe_text(record_user_label(record))}|"
                    f"{pipe_safe_text(record.get('hash'))}|"
                    f"{pipe_safe_text(build_farcaster_xyz_url(record))}|"
                    f"{pipe_safe_text(record.get('cast_url'))}|"
                    f"{pipe_safe_text(suggestion.get('why'))}|"
                    f"{final_preview_text(record.get('text'), limit=snippet_len)}"
                )
            )
    else:
        body_lines.append("Q00|none")

    body_lines.append(
        "TOP_200_ENGAGEMENT_CASTS_COLUMNS: idx|type|eng|l|rc|rp|rank|fid|user|theme|label|ts_local|hash|txt"
    )
    if not selected_records:
        body_lines.append("E000|none")
    for index, record in enumerate(selected_records, start=1):
        engagement = record.get("engagement") or {}
        _, ts_local = timestamp_utc_local(record.get("timestamp"), tz)
        body_lines.append(
            (
                f"E{index:03d}|{pipe_safe_text(record.get('type') or 'unknown')}|"
                f"{final_engagement_score(record):.2f}|"
                f"{get_int(engagement.get('likes_count'), default=0)}|"
                f"{get_int(engagement.get('recasts_count'), default=0)}|"
                f"{get_int(engagement.get('replies_count'), default=0)}|"
                f"{get_int(record.get('rank'), default=0)}|"
                f"{get_int(record.get('author_fid'), default=0)}|"
                f"{pipe_safe_text(record_user_label(record))}|"
                f"{pipe_safe_text(theme_display_name(infer_daily_theme(record.get('text'))))}|"
                f"{pipe_safe_text(record.get('content_label') or 'unknown')}|"
                f"{ts_local}|"
                f"{pipe_safe_text(record.get('hash'))}|"
                f"{final_preview_text(record.get('text'), limit=snippet_len)}"
            )
        )

    body_lines.append(
        "THREAD_CONTEXT_COLUMNS: tid|cid|parent_hash|eng|rp|l|rc|rank|fid|user|ts_local|hash|txt"
    )
    thread_rows = 0
    if thread_comments_per_post > 0:
        for tid, post in enumerate(most_replied_posts[:max_what_happened], start=1):
            post_hash = post.get("hash")
            if not isinstance(post_hash, str) or not post_hash:
                continue
            thread_comments = comments_by_parent.get(post_hash, [])
            if not thread_comments:
                continue
            for cid, comment in enumerate(thread_comments[:thread_comments_per_post], start=1):
                comment_engagement = comment.get("engagement") or {}
                _, ts_local = timestamp_utc_local(comment.get("timestamp"), tz)
                body_lines.append(
                    (
                        f"T{tid:02d}|C{cid:02d}|{post_hash}|"
                        f"{final_engagement_score(comment):.2f}|"
                        f"{get_int(comment_engagement.get('replies_count'), default=0)}|"
                        f"{get_int(comment_engagement.get('likes_count'), default=0)}|"
                        f"{get_int(comment_engagement.get('recasts_count'), default=0)}|"
                        f"{get_int(comment.get('rank'), default=0)}|"
                        f"{get_int(comment.get('author_fid'), default=0)}|"
                        f"{pipe_safe_text(record_user_label(comment))}|"
                        f"{ts_local}|"
                        f"{pipe_safe_text(comment.get('hash'))}|"
                        f"{final_preview_text(comment.get('text'), limit=snippet_len)}"
                    )
                )
                thread_rows += 1
    if thread_rows == 0:
        body_lines.append("T00|C00|none")

    payload_text = "\n".join(body_lines)
    token_estimate = estimate_llm_tokens(payload_text)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(
            f"ESTIMATED_LLM_TOKENS_FOR_FINAL_CONTEXT: {token_estimate} "
            "(heuristic=ceil(char_count/4))\n"
        )
        for line in body_lines:
            f.write(line + "\n")

    return token_estimate

def parse_args() -> argparse.Namespace:
    local_tz = datetime.now().astimezone().tzinfo
    local_offset = datetime.now().astimezone().strftime("%z")
    local_offset_label = f"{local_offset[:3]}:{local_offset[3:]}" if local_offset else "+00:00"
    local_tz_name = getattr(local_tz, "key", None) or local_offset_label
    default_date = datetime.now().astimezone().date().isoformat()

    parser = argparse.ArgumentParser(
        description=(
            "Scrape Farcaster posts/comments for a day and score each cast from 1-10 "
            "using genuine-like-weighted engagement."
        )
    )

    parser.add_argument(
        "--source",
        choices=("snapchain", "neynar"),
        default="snapchain",
        help="Data source. Default is direct hub data via snapchain.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("NEYNAR_API_KEY") or os.getenv("FARCASTER_API_KEY"),
        help="Neynar API key (required only when --source neynar).",
    )
    parser.add_argument(
        "--hub-url",
        default=os.getenv("SNAPCHAIN_HUB_URL") or DEFAULT_HUB_URL,
        help=f"Snapchain hub base URL. Default: {DEFAULT_HUB_URL}",
    )
    parser.add_argument(
        "--snapchain-shards",
        default="2",
        help="Comma-separated shard indices to read in snapchain mode (e.g. 2 or 0,2).",
    )
    parser.add_argument(
        "--snapchain-event-id-span",
        type=int,
        default=3_000_000_000,
        help="How far back to start from shard tip by event-id in snapchain mode.",
    )
    parser.add_argument(
        "--date",
        type=parse_date,
        default=default_date,
        help="Target date: YYYY-MM-DD or M/D/YYYY. Default is local today.",
    )
    parser.add_argument(
        "--timezone",
        default=local_tz_name,
        help=f"Timezone for date filtering. Default: local timezone ({local_tz_name}).",
    )
    parser.add_argument(
        "--query",
        default="*",
        help="Search term. Use '*' to include broad Farcaster activity.",
    )
    parser.add_argument(
        "--collect-last-hours",
        type=float,
        default=None,
        help="Collect a rolling time window ending now (UTC), e.g. 24 for last 24h.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Page size (Neynar cast page or Snapchain event page).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional cap for cast-search pages.",
    )
    parser.add_argument(
        "--like-page-size",
        type=int,
        default=100,
        help="Like-reaction page size for each cast.",
    )
    parser.add_argument(
        "--like-max-pages",
        type=int,
        default=20,
        help="Max like-reaction pages per cast.",
    )
    parser.add_argument(
        "--like-workers",
        type=int,
        default=8,
        help="Concurrent workers for like-quality enrichment.",
    )
    parser.add_argument(
        "--enrich-like-quality",
        action="store_true",
        help="Fetch liker-level quality data. Leave off for faster full-window crawls.",
    )
    parser.add_argument(
        "--ranking-max-age-hours",
        type=float,
        default=24.0,
        help="Only casts newer than this age are ranked by likes (default: 24).",
    )
    parser.add_argument(
        "--likes-for-top-score",
        type=int,
        default=50,
        help="Likes needed to approach score 10 in the likes-only scoring curve.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output .txt path. Default: data/farcaster_YYYY-MM-DD.txt",
    )
    parser.add_argument(
        "--instructions-output",
        default=None,
        help="Instructions .txt path. Default: data/llm_instructions_YYYY-MM-DD.txt",
    )
    parser.add_argument(
        "--readable-output",
        default=None,
        help="Readable parsed .txt path. Default: data/farcaster_YYYY-MM-DD_readable.txt",
    )
    parser.add_argument(
        "--final-output",
        default=None,
        help="Compact final context .txt path. Default: data/farcaster_YYYY-MM-DD_final.txt",
    )
    parser.add_argument(
        "--final-top-posts",
        type=int,
        default=12,
        help="How many top posts to include in final context output.",
    )
    parser.add_argument(
        "--final-comments-per-post",
        type=int,
        default=3,
        help="How many top comments to include under each selected top post.",
    )
    parser.add_argument(
        "--final-snippet-length",
        type=int,
        default=180,
        help="Max text length per post/comment snippet in final context output.",
    )
    parser.add_argument(
        "--focus-themes",
        default="",
        help="Comma-separated themes to keep (e.g. protocol_fork,security_ops,base_culture). Supported themes: protocol_fork,token_promo,security_ops,apps_games,base_culture,daily_greetings,general_chat,empty.",
    )
    parser.add_argument(
        "--exclude-themes",
        default="",
        help="Comma-separated themes to remove. Same values as --focus-themes.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level logs.",
    )
    parser.add_argument(
        "--main-question",
        default=DEFAULT_MAIN_QUESTION,
        help="Primary analysis question injected into generated instruction text.",
    )

    args = parser.parse_args()

    if args.source == "neynar" and not args.api_key:
        parser.error("Missing API key for source=neynar. Set --api-key or NEYNAR_API_KEY.")

    if args.page_size <= 0:
        parser.error("--page-size must be > 0")
    if args.source == "neynar" and args.page_size > 100:
        parser.error("--page-size must be <= 100 when --source neynar")
    if args.collect_last_hours is not None and args.collect_last_hours <= 0:
        parser.error("--collect-last-hours must be > 0 when provided")
    if args.like_page_size <= 0:
        parser.error("--like-page-size must be > 0")
    if args.max_pages is not None and args.max_pages <= 0:
        parser.error("--max-pages must be > 0 when provided")
    if args.like_max_pages <= 0:
        parser.error("--like-max-pages must be > 0")
    if args.like_workers <= 0:
        parser.error("--like-workers must be > 0")
    if args.ranking_max_age_hours <= 0:
        parser.error("--ranking-max-age-hours must be > 0")
    if args.likes_for_top_score <= 0:
        parser.error("--likes-for-top-score must be > 0")
    if args.final_top_posts <= 0:
        parser.error("--final-top-posts must be > 0")
    if args.final_comments_per_post < 0:
        parser.error("--final-comments-per-post must be >= 0")
    if args.final_snippet_length <= 0:
        parser.error("--final-snippet-length must be > 0")
    if args.snapchain_event_id_span <= 0:
        parser.error("--snapchain-event-id-span must be > 0")

    try:
        args.focus_themes = parse_theme_filters(args.focus_themes)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        args.exclude_themes = parse_theme_filters(args.exclude_themes)
    except ValueError as exc:
        parser.error(str(exc))

    overlap = set(args.focus_themes) & set(args.exclude_themes)
    if overlap:
        parser.error(f"Theme filters overlap between --focus-themes and --exclude-themes: {', '.join(sorted(overlap))}")

    try:
        shard_values = tuple(
            sorted(
                {
                    int(part.strip())
                    for part in str(args.snapchain_shards).split(",")
                    if part.strip()
                }
            )
        )
    except ValueError as exc:
        parser.error(f"Invalid --snapchain-shards value: {exc}")
    if not shard_values:
        parser.error("--snapchain-shards must include at least one shard index")
    if any(shard < 0 for shard in shard_values):
        parser.error("--snapchain-shards values must be >= 0")
    args.snapchain_shards = shard_values

    _ = resolve_timezone(args.timezone)
    return args


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    target_date = args.date if isinstance(args.date, date) else parse_date(args.date)
    output_path = (
        Path(args.output)
        if args.output
        else Path("data") / f"farcaster_{target_date.isoformat()}.txt"
    )
    instructions_path = (
        Path(args.instructions_output)
        if args.instructions_output
        else Path("data") / f"llm_instructions_{target_date.isoformat()}.txt"
    )
    readable_output_path = (
        Path(args.readable_output)
        if args.readable_output
        else Path("data") / f"farcaster_{target_date.isoformat()}_readable.txt"
    )
    final_output_path = (
        Path(args.final_output)
        if args.final_output
        else Path("data") / f"farcaster_{target_date.isoformat()}_final.txt"
    )

    config = ScrapeConfig(
        source=args.source,
        api_key=args.api_key if args.source == "neynar" else None,
        hub_url=args.hub_url,
        snapchain_shards=tuple(args.snapchain_shards),
        snapchain_event_id_span=args.snapchain_event_id_span,
        target_date=target_date,
        timezone_name=args.timezone,
        query=(args.query.strip() or "*"),
        collect_last_hours=args.collect_last_hours,
        page_size=args.page_size,
        max_pages=args.max_pages,
        like_page_size=args.like_page_size,
        like_max_pages=args.like_max_pages,
        like_workers=args.like_workers,
        enrich_like_quality=args.enrich_like_quality,
        ranking_max_age_hours=args.ranking_max_age_hours,
        likes_for_top_score=args.likes_for_top_score,
        output_path=output_path,
        instructions_path=instructions_path,
        readable_output_path=readable_output_path,
        final_output_path=final_output_path,
        final_top_posts=args.final_top_posts,
        final_comments_per_post=args.final_comments_per_post,
        final_snippet_length=args.final_snippet_length,
        focus_themes=args.focus_themes,
        exclude_themes=args.exclude_themes,
    )

    session = (
        build_neynar_session(str(config.api_key))
        if config.source == "neynar"
        else build_plain_session()
    )
    try:
        casts = fetch_casts_for_day(session, config)
    except RuntimeError as exc:
        message = str(exc)
        if config.source == "neynar" and "HTTP 403" in message:
            raise SystemExit(
                "API access forbidden (HTTP 403). Use a valid NEYNAR_API_KEY with cast search access."
            ) from exc
        raise
    scored_records = build_scored_records(casts, config)

    token_estimate = write_scored_txt(
        records=scored_records,
        output_path=config.output_path,
        target_date=config.target_date,
        timezone_name=config.timezone_name,
        query=config.query,
    )
    write_llm_instructions_txt(
        instructions_path=config.instructions_path,
        data_path=config.output_path,
        target_date=config.target_date,
        token_estimate=token_estimate,
        main_question=args.main_question,
    )
    write_readable_ranked_txt(
        records=scored_records,
        output_path=config.readable_output_path,
        target_date=config.target_date,
        timezone_name=config.timezone_name,
        token_estimate=token_estimate,
        ranking_max_age_hours=config.ranking_max_age_hours,
    )
    final_token_estimate = write_final_context_txt(
        records=scored_records,
        output_path=config.final_output_path,
        target_date=config.target_date,
        timezone_name=config.timezone_name,
        final_top_posts=config.final_top_posts,
        final_comments_per_post=config.final_comments_per_post,
        final_snippet_length=config.final_snippet_length,
        hub_url=config.hub_url if config.source == "snapchain" else None,
        focus_themes=config.focus_themes,
        exclude_themes=config.exclude_themes,
    )

    posts = sum(1 for r in scored_records if r.get("type") == "post")
    comments = sum(1 for r in scored_records if r.get("type") == "comment")

    print(f"Saved: {config.output_path}")
    print(f"Saved: {config.instructions_path}")
    print(f"Saved: {config.readable_output_path}")
    print(f"Saved: {config.final_output_path}")
    print(f"Source: {config.source}")
    if config.source == "snapchain":
        print(f"Hub: {config.hub_url} | shards={','.join(str(s) for s in config.snapchain_shards)}")
    print(f"Date: {config.target_date.isoformat()} ({config.timezone_name})")
    print(f"Records: {len(scored_records)} | posts={posts} comments={comments}")
    print(f"Estimated LLM tokens for scraped data: {token_estimate}")
    print(f"Estimated LLM tokens for final context: {final_token_estimate}")


if __name__ == "__main__":
    main()

