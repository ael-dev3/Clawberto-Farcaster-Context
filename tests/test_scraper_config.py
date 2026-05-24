import importlib.util
import sys
from datetime import date
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_PATH = ROOT / "scripts" / "farcaster_daily_scraper.py"
spec = importlib.util.spec_from_file_location("farcaster_daily_scraper", SCRAPER_PATH)
assert spec is not None
scraper = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = scraper
assert spec.loader is not None
spec.loader.exec_module(scraper)


def make_config(**overrides):
    values = dict(
        source="hypersnap",
        api_key=None,
        hub_url="http://primary:3381",
        hub_urls=("http://primary:3381",),
        snapchain_shards=tuple(),
        snapchain_event_id_span=3_000_000_000,
        target_date=date(2026, 5, 24),
        timezone_name="UTC",
        query="*",
        collect_last_hours=24.0,
        page_size=1000,
        max_pages=1,
        like_page_size=100,
        like_max_pages=20,
        like_workers=8,
        enrich_like_quality=False,
        ranking_max_age_hours=24.0,
        likes_for_top_score=50,
        output_path=Path("data/raw.txt"),
        instructions_path=Path("data/instructions.txt"),
        readable_output_path=Path("data/readable.txt"),
        final_output_path=Path("data/final.txt"),
        final_top_posts=12,
        final_comments_per_post=3,
        final_snippet_length=180,
        focus_themes=tuple(),
        exclude_themes=tuple(),
        filter_empty_records=True,
        filter_promo_records=True,
        filter_gm_gn_records=True,
    )
    values.update(overrides)
    return scraper.ScrapeConfig(**values)


class ScraperConfigTests(TestCase):
    def test_parse_hub_urls_dedupes_and_adds_scheme(self):
        self.assertEqual(
            scraper.parse_hub_urls("node-a:3381,https://node-b:3381 node-a:3381"),
            ("http://node-a:3381", "https://node-b:3381"),
        )

    def test_parse_snapchain_shards_supports_auto_and_sorted_values(self):
        self.assertEqual(scraper.parse_snapchain_shards("auto"), tuple())
        self.assertEqual(scraper.parse_snapchain_shards("2,0,2,1"), (0, 1, 2))

    def test_runtime_config_fails_over_and_auto_discovers_shards(self):
        config = make_config(
            hub_urls=("http://down:3381", "http://up:3381"),
            snapchain_shards=tuple(),
        )
        info_payload = {
            "shardInfos": [
                {"shardId": 2, "maxHeight": 30},
                {"shardId": 0, "maxHeight": 10},
                {"shardId": 1, "maxHeight": 20},
            ]
        }
        with patch.object(
            scraper,
            "fetch_hub_info",
            side_effect=[RuntimeError("down"), info_payload],
        ), patch.object(scraper, "get_hub_events_page", return_value=[{"id": 1}]):
            resolved = scraper.resolve_snapchain_runtime_config(object(), config)

        self.assertEqual(resolved.hub_url, "http://up:3381")
        self.assertEqual(resolved.snapchain_shards, (0, 1, 2))

    def test_runtime_config_preserves_explicit_shards(self):
        config = make_config(snapchain_shards=(2,))
        with patch.object(
            scraper,
            "fetch_hub_info",
            return_value={"shardInfos": [{"shardId": 0, "maxHeight": 10}]},
        ):
            resolved = scraper.resolve_snapchain_runtime_config(object(), config)

        self.assertEqual(resolved.snapchain_shards, (2,))

    def test_runtime_config_filters_unreadable_auto_shards(self):
        config = make_config(snapchain_shards=tuple())
        info_payload = {
            "shardInfos": [
                {"shardId": 0, "maxHeight": 10},
                {"shardId": 1, "maxHeight": 20},
                {"shardId": 2, "maxHeight": 30},
            ]
        }

        def fake_events_page(*, shard_index, **_kwargs):
            if shard_index == 0:
                raise RuntimeError("Shard not found")
            return [{"id": 1}]

        with patch.object(scraper, "fetch_hub_info", return_value=info_payload), patch.object(
            scraper,
            "get_hub_events_page",
            side_effect=fake_events_page,
        ):
            resolved = scraper.resolve_snapchain_runtime_config(object(), config)

        self.assertEqual(resolved.snapchain_shards, (1, 2))


if __name__ == "__main__":
    main()
