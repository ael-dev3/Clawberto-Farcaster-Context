#!/usr/bin/env python3
from __future__ import annotations

import csv
import html
import json
import math
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
GENERATED = ROOT / "generated"
PUBLIC_GENERATED = ROOT / "public" / "generated"
SITE_URL = "https://ael-dev3.github.io/Clawberto-Farcaster-Context/"
REPO_URL = "https://github.com/ael-dev3/Clawberto-Farcaster-Context"

STOPWORDS = {
    "about", "after", "again", "all", "also", "amp", "and", "any", "are", "because", "been", "being", "but",
    "can", "com", "could", "created", "day", "did", "does", "don", "each", "every", "for", "from",
    "get", "got", "has", "have", "here", "how", "https", "into", "its", "just", "like", "made", "make",
    "more", "not", "now", "one", "out", "our", "post", "really", "see", "should", "some", "that", "the",
    "their", "them", "then", "there", "they", "this", "today", "too", "use", "using", "via", "was", "way",
    "what", "when", "where", "who", "will", "with", "would", "you", "your",
}
TOKEN_RE = re.compile(r"[a-z][a-z0-9]{2,}", re.IGNORECASE)


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def clamp_text(value: Any, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def read_latest_raw() -> tuple[Path, dict[str, str], list[dict[str, Any]]]:
    if not DATA.exists():
        raise SystemExit("No data/ directory found. Run `bash scripts/farcaster_context_24h.sh` first.")
    candidates = [
        path
        for path in DATA.glob("farcaster_24h_*.txt")
        if "_final_" not in path.name and "_readable_" not in path.name
    ]
    if not candidates:
        candidates = [
            path
            for path in DATA.glob("farcaster_*.txt")
            if "_final_" not in path.name and "_readable_" not in path.name
        ]
    if not candidates:
        raise SystemExit("No raw Farcaster output found. Run `bash scripts/farcaster_context_24h.sh` first.")

    raw_path = max(candidates, key=lambda p: p.stat().st_mtime)
    metadata: dict[str, str] = {}
    records: list[dict[str, Any]] = []
    with raw_path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            if text.startswith("{"):
                try:
                    records.append(json.loads(text))
                except json.JSONDecodeError as exc:
                    raise SystemExit(f"Invalid JSON line in {raw_path}: {exc}") from exc
            elif ":" in text:
                key, value = text.split(":", 1)
                metadata[key.strip()] = value.strip()
    return raw_path, metadata, records


def latest_final_path(raw_path: Path) -> Path | None:
    final_name = raw_path.name.replace("farcaster_24h_", "farcaster_24h_final_")
    direct = raw_path.with_name(final_name)
    if direct.exists():
        return direct
    candidates = sorted(DATA.glob("farcaster_*_final_*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def parse_final_user(value: str) -> tuple[str, str]:
    text = value.strip()
    match = re.match(r"(.+?)\s*\((\d+)\)$", text)
    if match:
        username = match.group(1).strip().lstrip("@").lower()
        fid = match.group(2)
        if username.startswith("fid:") or username in {"unknown", "none"}:
            username = ""
        return username, fid
    fid_match = re.search(r"fid:(\d+)", text)
    return "", fid_match.group(1) if fid_match else ""


def parse_final_context(raw_path: Path, records: list[dict[str, Any]]) -> tuple[Path | None, list[dict[str, Any]], list[str]]:
    final_path = latest_final_path(raw_path)
    if final_path is None or not final_path.exists():
        return None, [], []
    raw_by_hash = {str(record.get("hash") or "").lower(): record for record in records if record.get("hash")}
    enriched: list[dict[str, Any]] = []
    summary_lines: list[str] = []
    with final_path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            text = line.strip()
            if re.match(r"^[SN]\d{2}\s+", text):
                summary_lines.append(re.sub(r"^[SN]\d{2}\s+", "", text))
                continue
            if not re.match(r"^E\d{3}\|", text):
                continue
            parts = text.split("|", 13)
            if len(parts) < 14:
                continue
            idx, cast_type, eng, likes, recasts, replies, rank, fid, user, theme, label, ts_local, hash_value, cast_text = parts
            username, parsed_fid = parse_final_user(user)
            fid = fid or parsed_fid
            hash_value = hash_value.strip()
            base = dict(raw_by_hash.get(hash_value.lower(), {}))
            if username:
                profile_name = username
                display_with_fid = f"{username} ({fid})" if fid else username
                final_cast_url = f"https://farcaster.xyz/{username}/{hash_value[:10]}" if hash_value.startswith("0x") else ""
            else:
                profile_name = ""
                display_with_fid = f"fid:{fid}" if fid else user.strip()
                final_cast_url = f"https://warpcast.com/~/conversations/{hash_value}" if hash_value.startswith("0x") else ""
            base.update(
                {
                    "type": cast_type.strip(),
                    "hash": hash_value,
                    "text": cast_text.strip(),
                    "timestamp": ts_local.strip(),
                    "author_username": profile_name,
                    "author_fid": fid,
                    "author_username_with_fid": display_with_fid,
                    "content_label": theme.strip(),
                    "visibility_tier": label.strip(),
                    "cast_url": final_cast_url or base.get("cast_url") or "",
                    "engagement": {
                        "likes_count": intish(likes),
                        "recasts_count": intish(recasts),
                        "replies_count": intish(replies),
                    },
                    "ranking_like_count": intish(likes),
                    "_engagement_score": float(eng or 0),
                    "_site_order": len(enriched) + 1,
                    "_final_rank": intish(rank),
                }
            )
            enriched.append(base)
    return final_path, enriched, summary_lines[:10]


def parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def iso_z(dt: datetime | None) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def score(record: dict[str, Any]) -> float:
    try:
        return float(record.get("algorithmic_score_1_to_10") or 0)
    except (TypeError, ValueError):
        return 0.0


def intish(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def engagement(record: dict[str, Any], key: str) -> int:
    raw_data = record.get("engagement")
    data: dict[str, Any] = raw_data if isinstance(raw_data, dict) else {}
    return intish(data.get(key))


def author_key(record: dict[str, Any]) -> str:
    username = str(record.get("author_username") or "").strip().lstrip("@")
    if username:
        return username.lower()
    fid = str(record.get("author_username_with_fid") or "").strip()
    if fid:
        return fid.lower()
    fid_value = record.get("author_fid")
    return f"fid:{fid_value}" if fid_value not in (None, "") else "unknown"


def author_display(record: dict[str, Any]) -> str:
    username = str(record.get("author_username") or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    fid = str(record.get("author_username_with_fid") or "").strip()
    if fid:
        return fid
    fid_value = record.get("author_fid")
    return f"fid:{fid_value}" if fid_value not in (None, "") else "unknown"


def author_url(record_or_name: dict[str, Any] | str) -> str:
    if isinstance(record_or_name, dict):
        username = str(record_or_name.get("author_username") or "").strip().lstrip("@")
        fid = record_or_name.get("author_fid")
        display = author_display(record_or_name)
    else:
        username = str(record_or_name or "").strip().lstrip("@")
        fid = None
        display = str(record_or_name or "")
    if username and not username.lower().startswith("fid:"):
        return f"https://farcaster.xyz/{username}"
    fid_match = re.search(r"fid:(\d+)", display)
    if fid_match:
        return f"https://warpcast.com/~/profiles/{fid_match.group(1)}"
    if fid not in (None, ""):
        return f"https://warpcast.com/~/profiles/{fid}"
    return ""


def cast_url(record: dict[str, Any]) -> str:
    url = str(record.get("cast_url") or "").strip()
    if url:
        return url
    username = str(record.get("author_username") or "").strip().lstrip("@")
    hash_value = str(record.get("hash") or "").strip()
    if username and hash_value.startswith("0x"):
        return f"https://farcaster.xyz/{username}/{hash_value[2:10]}"
    return ""


def row_for_record(record: dict[str, Any], rank: int) -> dict[str, Any]:
    ts = parse_dt(record.get("timestamp"))
    text = str(record.get("text") or record.get("text_preview") or "")
    eng_score = record.get("_engagement_score")
    if eng_score in (None, ""):
        eng_score = engagement(record, "likes_count") + 1.5 * engagement(record, "recasts_count") + 2.5 * engagement(record, "replies_count")
    return {
        "rank": intish(record.get("_site_order")) or rank,
        "timestamp_utc": iso_z(ts),
        "type": record.get("type") or "cast",
        "author": author_display(record),
        "author_url": author_url(record),
        "score": f"{score(record):.2f}",
        "engagement": f"{float(eng_score):.2f}",
        "likes": engagement(record, "likes_count"),
        "recasts": engagement(record, "recasts_count"),
        "replies": engagement(record, "replies_count"),
        "theme": record.get("content_label") or "general",
        "visibility": record.get("visibility_tier") or "",
        "hash": record.get("hash") or "",
        "cast_url": cast_url(record),
        "parent_hash": record.get("parent_hash") or "",
        "preview": clamp_text(text, 260),
        "text": text,
    }


def sorted_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda r: (
            score(r),
            intish(r.get("ranking_like_count")),
            engagement(r, "likes_count"),
            parse_dt(r.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )


def build_tables(records: list[dict[str, Any]], metadata: dict[str, str], site_records: list[dict[str, Any]] | None = None) -> dict[str, list[dict[str, Any]]]:
    ordered = sorted_records(records)
    visible_records = list(site_records or ordered)
    top_casts = [row_for_record(record, i + 1) for i, record in enumerate(visible_records[:150])]
    posts = [row_for_record(record, i + 1) for i, record in enumerate([r for r in visible_records if r.get("type") == "post"][:150])]
    comments = [row_for_record(record, i + 1) for i, record in enumerate([r for r in visible_records if r.get("type") == "comment"][:150])]

    by_author: dict[str, dict[str, Any]] = {}
    for record in visible_records:
        key = author_key(record)
        item = by_author.setdefault(
            key,
            {
                "author": author_display(record),
                "author_url": author_url(record),
                "casts": 0,
                "posts": 0,
                "comments": 0,
                "likes": 0,
                "recasts": 0,
                "replies": 0,
                "score_total": 0.0,
                "best_score": 0.0,
                "top_cast_url": "",
            },
        )
        item["casts"] += 1
        if record.get("type") == "post":
            item["posts"] += 1
        if record.get("type") == "comment":
            item["comments"] += 1
        item["likes"] += engagement(record, "likes_count")
        item["recasts"] += engagement(record, "recasts_count")
        item["replies"] += engagement(record, "replies_count")
        item["score_total"] += score(record)
        if score(record) >= item["best_score"]:
            item["best_score"] = score(record)
            item["top_cast_url"] = cast_url(record)
    authors = []
    for item in by_author.values():
        casts = max(1, int(item["casts"]))
        item["avg_score"] = f"{float(item['score_total']) / casts:.2f}"
        item["best_score"] = f"{float(item['best_score']):.2f}"
        del item["score_total"]
        authors.append(item)
    authors.sort(key=lambda x: (int(x["likes"]), float(x["best_score"]), int(x["casts"])), reverse=True)
    authors = authors[:100]

    theme_rows = []
    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in visible_records:
        by_theme[str(record.get("content_label") or "general")].append(record)
    for theme, group in sorted(by_theme.items(), key=lambda kv: len(kv[1]), reverse=True):
        top = sorted_records(group)[0] if group else {}
        theme_rows.append(
            {
                "theme": theme,
                "casts": len(group),
                "posts": sum(1 for r in group if r.get("type") == "post"),
                "comments": sum(1 for r in group if r.get("type") == "comment"),
                "likes": sum(engagement(r, "likes_count") for r in group),
                "avg_score": f"{sum(score(r) for r in group) / max(1, len(group)):.2f}",
                "top_author": author_display(top) if top else "",
                "top_cast_url": cast_url(top) if top else "",
            }
        )

    timestamps = [dt for dt in (parse_dt(r.get("timestamp")) for r in records) if dt]
    window_start = min(timestamps) if timestamps else None
    window_end = max(timestamps) if timestamps else None
    total_likes = sum(engagement(r, "likes_count") for r in records)
    total_recasts = sum(engagement(r, "recasts_count") for r in records)
    total_replies = sum(engagement(r, "replies_count") for r in records)
    unique_authors = len({author_key(r) for r in records})
    top_record = visible_records[0] if visible_records else (ordered[0] if ordered else {})
    metrics = [
        {"metric": "site_url", "value": SITE_URL},
        {"metric": "repo_url", "value": REPO_URL},
        {"metric": "generated_at_utc", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")},
        {"metric": "source", "value": metadata.get("SOURCE", "")},
        {"metric": "hub_url", "value": metadata.get("HUB_URL", "")},
        {"metric": "snapchain_shards", "value": metadata.get("SNAPCHAIN_SHARDS", "")},
        {"metric": "collect_last_hours", "value": metadata.get("COLLECT_LAST_HOURS", "")},
        {"metric": "window_start_utc", "value": iso_z(window_start)},
        {"metric": "window_end_utc", "value": iso_z(window_end)},
        {"metric": "total_records", "value": len(records)},
        {"metric": "posts", "value": sum(1 for r in records if r.get("type") == "post")},
        {"metric": "comments", "value": sum(1 for r in records if r.get("type") == "comment")},
        {"metric": "selected_records", "value": len(visible_records)},
        {"metric": "unique_authors", "value": unique_authors},
        {"metric": "total_likes", "value": total_likes},
        {"metric": "total_recasts", "value": total_recasts},
        {"metric": "total_replies", "value": total_replies},
        {"metric": "top_cast_score", "value": f"{score(top_record):.2f}" if top_record else ""},
        {"metric": "top_cast_author", "value": author_display(top_record) if top_record else ""},
        {"metric": "top_cast_url", "value": cast_url(top_record) if top_record else ""},
    ]
    return {
        "summary_metrics": metrics,
        "theme_summary": theme_rows,
        "authors": authors,
        "top_casts": top_casts,
        "posts": posts,
        "comments": comments,
    }


def keyword_summary(records: list[dict[str, Any]], limit: int = 18) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for record in records:
        text = str(record.get("text") or record.get("text_preview") or "")
        for token in TOKEN_RE.findall(text.lower()):
            if token not in STOPWORDS and not token.startswith("http") and not token.isdigit():
                counter[token] += 1
    return counter.most_common(limit)


def write_table(name: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    GENERATED.mkdir(parents=True, exist_ok=True)
    PUBLIC_GENERATED.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["empty"]
    csv_path = GENERATED / f"{name}.csv"
    json_path = GENERATED / f"{name}.json"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
        f.write("\n")
    shutil.copy2(csv_path, PUBLIC_GENERATED / csv_path.name)
    shutil.copy2(json_path, PUBLIC_GENERATED / json_path.name)
    return {"table": name, "file": f"generated/{name}.csv", "rows": len(rows)}


def write_generated(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    for path in [GENERATED, PUBLIC_GENERATED]:
        path.mkdir(parents=True, exist_ok=True)
    manifest = []
    for name, rows in tables.items():
        manifest.append(write_table(name, rows))
    write_table("manifest", manifest)
    return manifest


def cell_html(key: str, value: Any, row: dict[str, Any]) -> str:
    text = str(value if value is not None else "")
    if key in {"author", "top_author"} and row.get("author_url"):
        return f'<a class="chip-link profile" href="{esc(row["author_url"])}" target="_blank" rel="noopener noreferrer">{esc(text)}</a>'
    if key == "top_author" and row.get("top_cast_url"):
        return f'<a class="chip-link" href="{esc(row["top_cast_url"])}" target="_blank" rel="noopener noreferrer">{esc(text)}</a>'
    if key in {"cast_url", "top_cast_url"}:
        return f'<a class="chip-link cast" href="{esc(text)}" target="_blank" rel="noopener noreferrer">open cast</a>' if text else ""
    if key == "author_url":
        return f'<a class="chip-link profile" href="{esc(text)}" target="_blank" rel="noopener noreferrer">profile</a>' if text else ""
    if key == "preview":
        return f'<span class="preview-text">{esc(text)}</span>'
    if key == "theme":
        return f'<span class="theme-pill">{esc(text)}</span>'
    return esc(text)


def render_table(name: str, rows: list[dict[str, Any]], columns: list[str], title: str, featured: bool = False) -> str:
    head = "".join(
        f'<th scope="col" aria-sort="none"><button type="button" data-col="{idx}">{esc(col.replace("_", " "))}</button></th>'
        for idx, col in enumerate(columns)
    )
    body_parts = []
    for row in rows:
        body_parts.append("<tr>" + "".join(f"<td>{cell_html(col, row.get(col, ''), row)}</td>" for col in columns) + "</tr>")
    cls = "table-card featured-table" if featured else "table-card"
    return (
        f'<section class="{cls}" data-name="{esc(name)}">'
        f'<div class="table-scroll"><table data-table="{esc(name)}">'
        f'<caption><span>{esc(title)}</span><span data-total="{len(rows)}">{len(rows)} rows</span></caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body_parts)}</tbody></table></div></section>"
    )


def metric_value(metrics: list[dict[str, Any]], key: str) -> str:
    for row in metrics:
        if row.get("metric") == key:
            return str(row.get("value", ""))
    return ""


def pct(value: Any, total: Any) -> str:
    try:
        numerator = float(value or 0)
        denominator = float(total or 0)
    except (TypeError, ValueError):
        return "0%"
    if denominator <= 0:
        return "0%"
    return f"{(numerator / denominator) * 100:.0f}%"


def build_agent_take(tables: dict[str, list[dict[str, Any]]], keywords: list[tuple[str, int]], summary_lines: list[str]) -> list[dict[str, str]]:
    """Create a concise data-backed editorial readout for the public site.

    This intentionally stays deterministic so scheduled refreshes keep the
    website useful without needing live LLM credentials in CI.
    """
    metrics = tables.get("summary_metrics", [])
    theme_rows = tables.get("theme_summary", [])
    top_casts = tables.get("top_casts", [])
    authors = tables.get("authors", [])
    selected_total = sum(intish(row.get("casts")) for row in theme_rows) or intish(metric_value(metrics, "selected_records"))
    top_theme = theme_rows[0] if theme_rows else {"theme": "the dominant theme", "casts": 0, "top_author": ""}
    second_theme = theme_rows[1] if len(theme_rows) > 1 else {"theme": "secondary chatter", "casts": 0}
    security = next((row for row in theme_rows if str(row.get("theme", "")).lower() == "security/ops"), {})
    protocol = next((row for row in theme_rows if str(row.get("theme", "")).lower() == "protocol/fork"), {})
    token = next((row for row in theme_rows if str(row.get("theme", "")).lower() == "token/promo"), {})
    top_author = authors[0].get("author", "") if authors else metric_value(metrics, "top_cast_author")
    second_author = authors[1].get("author", "") if len(authors) > 1 else ""
    top_one = top_casts[0] if top_casts else {}
    top_two = top_casts[1] if len(top_casts) > 1 else {}
    engagement_total = sum(float(row.get("engagement") or 0) for row in top_casts)
    top_two_eng = sum(float(row.get("engagement") or 0) for row in top_casts[:2])
    low_signal_terms = {"podium", "good", "caster", "score", "name", "something", "thing", "back", "real"}
    keyword_terms = ", ".join([word for word, _ in keywords if word not in low_signal_terms][:5])
    total_records = metric_value(metrics, "total_records")
    unique_authors = metric_value(metrics, "unique_authors")
    selected_records = metric_value(metrics, "selected_records")
    base_share = pct(top_theme.get("casts"), selected_total)
    top_two_share = pct(top_two_eng, engagement_total)
    protocol_note = ""
    if protocol:
        protocol_note = f" {protocol.get('theme')} is only {pct(protocol.get('casts'), selected_total)} of selected casts, but it carries the {top_two.get('author', 'second-ranked')} miniapp/quorum thread, so it is high leverage despite low volume."
    security_note = ""
    if security:
        security_note = f" {security.get('theme')} is {pct(security.get('casts'), selected_total)} and is mostly product friction, safety, and community-tool complaints."
    token_note = ""
    if token and intish(token.get("casts")):
        token_note = f" Token/promo stayed small at {pct(token.get('casts'), selected_total)}, so I would not let it drive the narrative unless a single cast breaks out."

    return [
        {
            "label": "My read",
            "take": f"This is a Base-first snapshot, not a broad Farcaster one. {top_theme.get('theme')} is {base_share} of the selected signal, keywords cluster around {keyword_terms or 'Base, apps, builders, and onchain'}, and the feed is mostly community/status chatter with a few practical builder moments.",
        },
        {
            "label": "What matters",
            "take": f"Attention is concentrated. The first two casts account for {top_two_share} of top-table engagement: {top_one.get('author', top_author)} on Basebuzz analytics and {top_two.get('author', second_author)} on miniapp/quorum momentum. I would treat those as the anchor threads before scanning the long tail.",
        },
        {
            "label": "Watch next",
            "take": f"The useful tension is Base app growth versus Farcaster community identity.{protocol_note}{security_note} If that friction keeps showing up, it is where good commentary and support content will land.",
        },
        {
            "label": "Clawberto angle",
            "take": f"Best move: play curator, not hype account. Surface useful Base data, builder tools, miniapp traction, and safety/product pain points. Ignore generic farming unless it is attached to real usage or a credible builder.{token_note}",
        },
    ]


def render_index(raw_path: Path, final_path: Path | None, tables: dict[str, list[dict[str, Any]]], manifest: list[dict[str, Any]], keywords: list[tuple[str, int]], summary_lines: list[str]) -> None:
    metrics = tables["summary_metrics"]
    top_casts = tables["top_casts"]
    theme_rows = tables["theme_summary"]
    authors = tables["authors"]
    agent_take = tables.get("agent_take") or build_agent_take(tables, keywords, summary_lines)
    window = f"{metric_value(metrics, 'window_start_utc')} → {metric_value(metrics, 'window_end_utc')}"
    top_cards = []
    for row in top_casts[:3]:
        top_cards.append(
            '<article class="cast-card">'
            f'<div class="cast-meta"><span>#{esc(row["rank"])}</span><span>{esc(row["type"])}</span><span>eng {esc(row["engagement"])}</span><span>score {esc(row["score"])}</span></div>'
            f'<h3>{cell_html("author", row["author"], row)}</h3>'
            f'<p>{esc(row["preview"])}</p>'
            f'<div class="cast-actions"><a href="{esc(row["cast_url"])}" target="_blank" rel="noopener noreferrer">Open post</a><span>{esc(row["likes"])} likes</span><span>{esc(row["theme"])} </span></div>'
            '</article>'
        )
    keyword_html = "".join(f"<span>{esc(word)} <b>{count}</b></span>" for word, count in keywords)
    take_cards = "".join(
        f'<article><b>{esc(row.get("label", "Take"))}</b><p>{esc(row.get("take", ""))}</p></article>'
        for row in agent_take
    )
    key_summary_lines = [
        line
        for line in summary_lines
        if not line.lower().startswith(("theme filter active", "filter removed", "mean engagement score", "most discussed thread hashes"))
    ]
    summary_html = "".join(f"<li>{esc(line)}</li>" for line in key_summary_lines[:4]) or "<li>Summary lines unavailable for this run.</li>"
    technical_summary_html = "".join(f"<li>{esc(line)}</li>" for line in summary_lines) or "<li>Run notes unavailable for this snapshot.</li>"
    technical_rows = "".join(
        f"<div><dt>{esc(label)}</dt><dd>{esc(value)}</dd></div>"
        for label, value in [
            ("Window UTC", window),
            ("Generated UTC", metric_value(metrics, "generated_at_utc")),
            ("Source", metric_value(metrics, "source")),
            ("Hub", metric_value(metrics, "hub_url")),
            ("Readable shards", metric_value(metrics, "snapchain_shards")),
            ("Raw file", raw_path.name),
            ("Final context", final_path.name if final_path else "not found"),
        ]
    )
    export_rows = "".join(
        f'<tr><td>{esc(item["table"].replace("_", " "))}</td><td>{esc(item["rows"])}</td>'
        f'<td><a href="{esc(item["file"])}" download>CSV</a><a href="{esc(item["file"].replace(".csv", ".json"))}" download>JSON</a></td></tr>'
        for item in manifest
        if item["table"] != "manifest"
    )
    primary_table = render_table(
        "top_casts",
        top_casts,
        ["rank", "author", "type", "engagement", "score", "likes", "recasts", "replies", "theme", "preview", "cast_url", "timestamp_utc"],
        "top casts",
        featured=True,
    )
    secondary_tables = "".join(
        [
            render_table("theme_summary", theme_rows, ["theme", "casts", "posts", "comments", "likes", "avg_score", "top_author", "top_cast_url"], "theme summary"),
            render_table("authors", authors, ["author", "casts", "posts", "comments", "likes", "recasts", "replies", "avg_score", "best_score", "top_cast_url"], "top authors"),
            render_table("posts", tables["posts"], ["rank", "author", "score", "likes", "replies", "theme", "preview", "cast_url", "timestamp_utc"], "posts"),
            render_table("comments", tables["comments"], ["rank", "author", "score", "likes", "replies", "theme", "preview", "cast_url", "timestamp_utc"], "comments"),
        ]
    )
    css = """
:root{color-scheme:dark;--bg:#0b0a10;--surface:#17131f;--surface-2:#201a2c;--ink:#eeeaf6;--muted:#a9a0b8;--line:#332a45;--accent:#5f35a8;--accent-soft:rgba(95,53,168,.18);--shadow:0 18px 48px rgba(0,0,0,.34);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}*{box-sizing:border-box}html{background:var(--bg)}body{margin:0;min-width:320px;background:var(--bg);color:var(--ink)}a{color:var(--ink);text-decoration:none}.shell{width:min(1320px,calc(100% - 28px));margin:0 auto;padding:16px 0 40px}.hero,.table-card,.exports,details,.insight-card,.cast-card,.technical-card{background:var(--surface);border:1px solid var(--line);box-shadow:var(--shadow)}.hero{border-radius:20px;padding:18px;display:grid;grid-template-columns:minmax(260px,.78fr) minmax(460px,1.22fr);gap:14px;align-items:stretch}.eyebrow{display:flex;gap:9px;align-items:center;color:var(--muted);font-size:11px;font-weight:900;letter-spacing:.12em;text-transform:uppercase}.orb{width:10px;height:10px;border-radius:999px;background:var(--accent)}h1{font-size:clamp(34px,4.2vw,56px);line-height:.94;margin:8px 0 8px;letter-spacing:-.05em;max-width:12ch}.subtitle{color:var(--muted);font-weight:650;line-height:1.35;max-width:680px}.hero-actions,.cast-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.hero-actions a,.cast-actions a,.exports a,.chip-link{display:inline-flex;align-items:center;gap:6px;border:1px solid var(--line);background:var(--accent-soft);border-radius:999px;padding:7px 10px;font-weight:800;color:#f4effc}.hero-actions a:hover,.cast-actions a:hover,.exports a:hover,.chip-link:hover{border-color:var(--accent);background:rgba(95,53,168,.28)}.stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.stat{background:var(--surface-2);border:1px solid var(--line);border-radius:14px;padding:10px;min-height:68px}.stat b{display:block;color:var(--muted);font-size:10px;letter-spacing:.09em;text-transform:uppercase}.stat span{display:block;margin-top:6px;font-size:clamp(18px,2.5vw,28px);font-weight:900;letter-spacing:-.04em;overflow-wrap:anywhere}.toolbar{margin:12px 0 10px}.toolbar input{width:100%;border:1px solid var(--line);background:var(--surface);border-radius:16px;color:var(--ink);font-size:15px;font-weight:650;padding:14px 16px;outline:none}.toolbar input:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(95,53,168,.2)}.insights{display:grid;grid-template-columns:minmax(0,1.2fr) minmax(260px,.8fr);gap:10px;margin:10px 0}.agent-take{margin:10px 0}.insight-card,.cast-card,.table-card,.exports,details,.technical-card{border-radius:20px}.insight-card{padding:14px}.take-card{padding:14px}.take-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}.take-grid article{background:var(--surface-2);border:1px solid var(--line);border-radius:14px;padding:10px}.take-grid b{display:block;color:#f4effc;font-size:12px;letter-spacing:.06em;text-transform:uppercase}.take-grid p{margin:6px 0 0;color:var(--muted);line-height:1.38;font-size:13px}.insight-card h2,.exports h2,.technical-card h2{margin:0 0 8px;font-size:16px;letter-spacing:-.02em}.summary-list{margin:0;padding-left:18px;color:var(--muted);line-height:1.35}.summary-list li{margin:3px 0}.keyword-cloud{display:flex;flex-wrap:wrap;gap:8px}.keyword-cloud span,.theme-pill,.cast-meta span{border:1px solid var(--line);background:var(--surface-2);border-radius:999px;padding:5px 8px;color:var(--muted);font-size:11px;font-weight:800}.keyword-cloud b{color:var(--ink)}.cast-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin:10px 0 14px}.cast-card{padding:13px}.cast-card h3{margin:8px 0 6px;font-size:17px}.cast-card p{margin:0;color:var(--muted);line-height:1.35;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}.cast-meta{display:flex;gap:7px;flex-wrap:wrap}.cast-actions span{color:var(--muted);font-size:12px;font-weight:800;padding:7px 0}.table-scroll{overflow:auto;max-height:72vh}table{width:100%;border-collapse:separate;border-spacing:0}caption{text-align:left;padding:14px 16px;font-size:18px;font-weight:900;display:flex;justify-content:space-between;gap:16px;position:sticky;left:0;background:var(--surface);z-index:1}th,td{border-bottom:1px solid var(--line);padding:10px 12px;vertical-align:top;text-align:left}th{position:sticky;top:0;background:var(--surface-2);z-index:2}th button{all:unset;cursor:pointer;color:var(--ink);font-size:12px;font-weight:900;text-transform:uppercase;letter-spacing:.08em}td{color:var(--muted);font-size:13px;line-height:1.4}tbody tr:hover td{background:rgba(95,53,168,.08)}.featured-table .preview-text{display:inline-block;min-width:360px;max-width:620px;color:var(--ink)}.technical{margin-top:22px}.technical-grid{display:grid;grid-template-columns:minmax(0,.95fr) minmax(300px,.65fr);gap:14px}.technical-card,.exports{padding:18px}.technical-card dl{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:0}.technical-card dl div{border:1px solid var(--line);background:var(--surface-2);border-radius:14px;padding:10px}.technical-card dt{color:var(--muted);font-size:11px;font-weight:900;letter-spacing:.08em;text-transform:uppercase}.technical-card dd{margin:6px 0 0;color:var(--ink);font-weight:750;overflow-wrap:anywhere}.exports table td,.exports table th{padding:9px}.exports a{margin-right:7px}details{margin-top:14px;padding:0;overflow:hidden}summary{cursor:pointer;padding:16px 18px;font-size:16px;font-weight:900;color:var(--ink);list-style:none}summary::-webkit-details-marker{display:none}.inner{display:grid;gap:14px;padding:0 14px 14px}.footer{color:var(--muted);text-align:center;margin:24px 0 0;font-size:13px}@media(max-width:980px){.hero,.insights,.technical-grid{grid-template-columns:1fr}.take-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.stats,.cast-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.technical-card dl{grid-template-columns:1fr}}@media(max-width:640px){.shell{width:min(100% - 18px,1320px);padding-top:12px}.hero{padding:18px}.stats,.cast-grid,.take-grid{grid-template-columns:1fr}th,td{padding:9px}.featured-table .preview-text{min-width:260px}}
"""
    js = """
const filter=document.getElementById('filter');
const key=v=>{const s=v.trim().replaceAll(',','').replace(/[()$]/g,'');const n=Number(s.split(' ')[0]);return s!==''&&Number.isFinite(n)?n:v.trim().toLowerCase();};
const updateCounts=()=>{document.querySelectorAll('table').forEach(table=>{const rows=[...table.tBodies[0].rows];const visible=rows.filter(row=>!row.hidden).length;const total=table.caption?.querySelector('[data-total]');if(total){const base=Number(total.dataset.total);total.textContent=visible===base?`${base} rows`:`${visible} / ${base} rows`;}});};
filter.addEventListener('input',()=>{const q=filter.value.trim().toLowerCase();document.querySelectorAll('tbody tr').forEach(tr=>{tr.hidden=q!==''&&!tr.textContent.toLowerCase().includes(q);});updateCounts();});
document.querySelectorAll('th button').forEach(button=>{button.addEventListener('click',()=>{const table=button.closest('table');const tbody=table.tBodies[0];const col=Number(button.dataset.col);const next=button.dataset.dir==='asc'?'desc':'asc';table.querySelectorAll('th').forEach(th=>{const b=th.querySelector('button');if(b)delete b.dataset.dir;th.setAttribute('aria-sort','none');});button.dataset.dir=next;button.closest('th').setAttribute('aria-sort',next==='asc'?'ascending':'descending');const rows=[...tbody.rows].sort((a,b)=>{const av=key(a.cells[col]?.textContent||'');const bv=key(b.cells[col]?.textContent||'');const cmp=typeof av==='number'&&typeof bv==='number'?av-bv:String(av).localeCompare(String(bv));return next==='asc'?cmp:-cmp;});rows.forEach(row=>tbody.appendChild(row));});});
updateCounts();
"""
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0b0a10">
<title>Clawberto Farcaster Context</title>
<style>{css}</style>
</head>
<body>
<div class="shell">
  <section class="hero" aria-label="Farcaster context overview">
    <div>
      <div class="eyebrow"><span class="orb"></span>Farcaster signal board</div>
      <h1>Clawberto context</h1>
      <p class="subtitle">Latest 24-hour Farcaster activity, filtered into the casts, authors, and themes worth checking first.</p>
      <div class="hero-actions"><a href="{esc(REPO_URL)}" target="_blank" rel="noopener noreferrer">GitHub repo</a></div>
    </div>
    <div class="stats">
      <div class="stat"><b>Total casts</b><span>{esc(metric_value(metrics, 'total_records'))}</span></div>
      <div class="stat"><b>Unique authors</b><span>{esc(metric_value(metrics, 'unique_authors'))}</span></div>
      <div class="stat"><b>Selected signal</b><span>{esc(metric_value(metrics, 'selected_records'))}</span></div>
      <div class="stat"><b>Posts / comments</b><span>{esc(metric_value(metrics, 'posts'))} / {esc(metric_value(metrics, 'comments'))}</span></div>
      <div class="stat"><b>Total likes</b><span>{esc(metric_value(metrics, 'total_likes'))}</span></div>
      <div class="stat"><b>Top author</b><span>{esc(metric_value(metrics, 'top_cast_author'))}</span></div>
    </div>
  </section>
  <section class="insights" aria-label="Key context summary">
    <article class="insight-card"><h2>Key readout</h2><ul class="summary-list">{summary_html}</ul></article>
    <article class="insight-card"><h2>Recurring terms</h2><div class="keyword-cloud">{keyword_html}</div></article>
  </section>
  <section class="agent-take" aria-label="Clawberto analysis">
    <article class="insight-card take-card"><h2>Clawberto take</h2><div class="take-grid">{take_cards}</div></article>
  </section>
  <section class="cast-grid" aria-label="Featured casts">{''.join(top_cards)}</section>
  <div class="toolbar"><input id="filter" type="search" aria-label="filter visible tables" placeholder="Search usernames, posts, themes, hashes" autocomplete="off"></div>
  <main>{primary_table}</main>
  <section class="technical" aria-label="Technical context">
    <div class="technical-grid">
      <article class="technical-card"><h2>Technical context</h2><dl>{technical_rows}</dl></article>
      <section class="exports"><h2>Cached data exports</h2><table><thead><tr><th>table</th><th>rows</th><th>download</th></tr></thead><tbody>{export_rows}</tbody></table></section>
    </div>
  </section>
  <details class="advanced"><summary>Technical parsed tables</summary><div class="inner"><article class="technical-card"><h2>Full run notes</h2><ul class="summary-list">{technical_summary_html}</ul></article>{secondary_tables}</div></details>
  <p class="footer">Generated from Hypersnap/Snapchain node data. Profile and post links open Farcaster/Warpcast in a new tab.</p>
</div>
<script>{js}</script>
</body>
</html>
"""
    (ROOT / "index.html").write_text(html_doc, encoding="utf-8")


def render_readme(tables: dict[str, list[dict[str, Any]]], manifest: list[dict[str, Any]]) -> None:
    metrics = tables["summary_metrics"]
    rows = {row["metric"]: row["value"] for row in metrics}
    manifest_lines = "\n".join(
        f"| {item['table']} | `{item['file']}` | {item['rows']} | [CSV]({item['file']}) / [JSON]({item['file'].replace('.csv', '.json')}) |"
        for item in manifest
        if item["table"] != "manifest"
    )
    readme = f"""# Clawberto Farcaster Context

Static, cached Farcaster context from direct Hypersnap/Snapchain node scraping. The public site serves parsed 24-hour context tables, a compact agent-written take, linkable usernames, cast URLs, and downloadable CSV/JSON exports.

## Links

- Live context site: [{SITE_URL}]({SITE_URL})
- Repository: [{REPO_URL}]({REPO_URL})
- Generated exports: [`generated/`](generated/)

## Current snapshot

| Field | Value |
| --- | --- |
| Source | {rows.get('source', '')} |
| Hub URL | {rows.get('hub_url', '')} |
| Readable shards | {rows.get('snapchain_shards', '')} |
| Window UTC | {rows.get('window_start_utc', '')} → {rows.get('window_end_utc', '')} |
| Total casts | {rows.get('total_records', '')} |
| Posts / comments | {rows.get('posts', '')} / {rows.get('comments', '')} |
| Unique authors | {rows.get('unique_authors', '')} |
| Total likes / recasts / replies | {rows.get('total_likes', '')} / {rows.get('total_recasts', '')} / {rows.get('total_replies', '')} |
| Top cast author | {rows.get('top_cast_author', '')} |
| Top cast | {rows.get('top_cast_url', '')} |

## Published datasets

| Table | CSV path | Rows | Downloads |
| --- | --- | --- | --- |
{manifest_lines}

## Data pipeline

1. `scripts/farcaster_daily_scraper.py` talks directly to Hypersnap/Snapchain HTTP nodes (`/v1/info`, `/v1/events`, and username lookup endpoints).
2. `scripts/farcaster_context_24h.sh` collects the latest rolling 24-hour context and writes raw/readable/final text outputs under ignored `data/`.
3. `scripts/build_site.py` parses the latest raw output into cached CSV/JSON datasets under `generated/` and `public/generated/`.
4. `index.html` renders the dark-purple searchable context site with linkable profiles, casts, summaries, and exports.
5. GitHub Pages deploys the static site from the Vite build artifact.

## Local development

```bash
python3 -m pip install -r requirements.txt
npm ci
npm run refresh      # scrape latest 24h + rebuild generated site artifacts
npm run build        # Vite build for GitHub Pages
npm run preview -- --host 127.0.0.1 --port 4188 --strictPort
```

For scraper-only validation:

```bash
python3 -m compileall scripts tests
python3 -m unittest discover -s tests -v
python3 scripts/farcaster_daily_scraper.py --help
```

## Node configuration

```bash
export HYPERSNAP_NODE_URLS="http://node-a:3381,http://node-b:3381"
export FC_CONTEXT_SNAPCHAIN_SHARDS="auto"
```

`--source hypersnap` is the default. `snapchain` remains a backwards-compatible alias.

## Deployment notes

If the first GitHub Pages deploy fails in `actions/configure-pages` with `Resource not accessible by integration`, Pages has not been enabled for the repo yet. Enable workflow-based Pages once, then rerun the deploy workflow:

```bash
gh api repos/ael-dev3/Clawberto-Farcaster-Context/pages -X POST -f build_type=workflow
gh run rerun <deploy-run-id>
```
"""
    (ROOT / "README.md").write_text(readme, encoding="utf-8")


def main() -> None:
    raw_path, metadata, records = read_latest_raw()
    if not records:
        raise SystemExit(f"No records found in {raw_path}")
    final_path, site_records, summary_lines = parse_final_context(raw_path, records)
    tables = build_tables(records, metadata, site_records=site_records or None)
    keywords = keyword_summary(site_records or records)
    tables["agent_take"] = build_agent_take(tables, keywords, summary_lines)
    manifest = write_generated(tables)
    render_index(raw_path, final_path, tables, manifest, keywords, summary_lines)
    render_readme(tables, manifest)
    print(f"Built site from {raw_path}")
    print(f"Records: {len(records)} | Generated tables: {len(manifest)} | Site: {SITE_URL}")


if __name__ == "__main__":
    main()
