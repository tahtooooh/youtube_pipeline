"""Collect public YouTube video snapshots and optionally publish them to Kafka."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API_ROOT = "https://www.googleapis.com/youtube/v3"


class YouTubeAPIError(RuntimeError):
    """An API failure whose message never contains the credential or URL."""


def load_local_env(path: Path = Path(".env")) -> None:
    """Read this project's simple .env file without overriding shell variables."""
    if not path.is_file():
        return
    allowed = {"YOUTUBE_API_KEY", "YOUTUBE_QUERY", "YOUTUBE_QUERIES", "YOUTUBE_MAX_PAGES", "YOUTUBE_MAX_VIDEOS",
               "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC"}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        key = key.strip()
        if separator and key in allowed:
            os.environ.setdefault(key, value.strip().strip('"\''))


def api_get(session, resource: str, params: dict, retries: int = 3) -> dict:
    for attempt in range(retries + 1):
        response = None
        try:
            response = session.get(f"{API_ROOT}/{resource}", params=params, timeout=30)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            status = response.status_code if response is not None else None
            if attempt == retries or (status is not None and status not in (429, 500, 502, 503, 504)):
                raise YouTubeAPIError(f"YouTube {resource} request failed (HTTP {status or 'network error'})") from None
            time.sleep(2 ** attempt)
    raise RuntimeError("API request failed")


def collect(session, api_key: str, query: str, max_pages: int, max_videos: int = 1000) -> list[dict]:
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY is required for live collection")
    if not 1 <= max_pages <= 30:
        raise ValueError("max_pages must be between 1 and 30")
    if not 1 <= max_videos <= 1000:
        raise ValueError("max_videos must be between 1 and 1000")
    video_ids: list[str] = []
    page_token = None
    for _ in range(max_pages):
        params = {
            "key": api_key, "part": "id", "type": "video", "q": query,
            "maxResults": 50, "order": "date",
        }
        if page_token:
            params["pageToken"] = page_token
        page = api_get(session, "search", params)
        video_ids.extend(item["id"]["videoId"] for item in page.get("items", [])
                         if item.get("id", {}).get("videoId"))
        # Allow a small buffer for videos that disappear between search and enrichment.
        if len(set(video_ids)) >= max_videos + 100:
            break
        page_token = page.get("nextPageToken")
        if not page_token:
            break
    unique_ids = list(dict.fromkeys(video_ids))
    collected_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []
    for start in range(0, len(unique_ids), 50):
        page = api_get(session, "videos", {
            "key": api_key, "part": "snippet,statistics,contentDetails",
            "id": ",".join(unique_ids[start:start + 50]), "maxResults": 50,
        })
        for item in page.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            details = item.get("contentDetails", {})
            records.append({
                "video_id": item["id"],
                "collected_at": collected_at,
                "published_at": snippet.get("publishedAt"),
                "title": snippet.get("title"),
                "channel_id": snippet.get("channelId"),
                "channel_title": snippet.get("channelTitle"),
                "category_id": snippet.get("categoryId"),
                "duration_iso8601": details.get("duration"),
                "view_count": _integer(stats.get("viewCount")),
                "like_count": _integer(stats.get("likeCount")),
                "comment_count": _integer(stats.get("commentCount")),
                "query": query,
            })
            if len(records) >= max_videos:
                return records
    return records


def collect_queries(session, api_key: str, queries: list[str], max_pages: int,
                    max_videos: int) -> list[dict]:
    seen = set()
    combined = []
    for query in queries:
        for record in collect(session, api_key, query, max_pages, max_videos):
            video_id = record["video_id"]
            if video_id not in seen:
                seen.add(video_id)
                combined.append(record)
            if len(combined) >= max_videos:
                return combined
    return combined


def _integer(value):
    return int(value) if value is not None else None


def write_jsonl(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def publish(records: list[dict], bootstrap_servers: str, topic: str) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        retries=5,
    )
    try:
        futures = [producer.send(topic, key=r["video_id"], value=r) for r in records]
        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)
    finally:
        producer.close()


def main(argv=None) -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", help="One search phrase; overrides YOUTUBE_QUERIES")
    parser.add_argument("--queries", help="Comma-separated search phrases")
    parser.add_argument("--max-pages", type=int, default=int(os.getenv("YOUTUBE_MAX_PAGES", "30")))
    parser.add_argument("--max-videos", type=int, default=int(os.getenv("YOUTUBE_MAX_VIDEOS", "1000")))
    parser.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"))
    parser.add_argument("--fixture", type=Path, help="Use local JSONL records instead of the API")
    parser.add_argument("--output", type=Path, default=Path("data/youtube_videos.jsonl"))
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "youtube.videos.raw"))
    args = parser.parse_args(argv)
    try:
        if args.fixture:
            records = [json.loads(line) for line in args.fixture.read_text(encoding="utf-8").splitlines() if line.strip()]
        else:
            phrases = args.queries or args.query or os.getenv("YOUTUBE_QUERIES") or os.getenv("YOUTUBE_QUERY", "big data maroc")
            queries = [phrase.strip() for phrase in phrases.split(",") if phrase.strip()]
            if not queries:
                raise ValueError("At least one search phrase is required")
            records = collect_queries(requests.Session(), args.api_key, queries, args.max_pages, args.max_videos)
        write_jsonl(records, args.output)
        if args.publish and records:
            publish(records, args.bootstrap_servers, args.topic)
        print(f"Collected {len(records)} records; wrote {args.output}; published={args.publish and bool(records)}")
        return 0
    except (ValueError, YouTubeAPIError, OSError) as exc:
        print(f"Collection failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
