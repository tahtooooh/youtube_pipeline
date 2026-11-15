import json
import tempfile
import unittest
from pathlib import Path

from ingestion.youtube_batch import YouTubeAPIError, collect, collect_queries, load_local_env, write_jsonl


class Response:
    status_code = 200

    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params))
        if url.endswith("/search"):
            if params.get("pageToken"):
                return Response({"items": [{"id": {"videoId": "b"}}]})
            return Response({"items": [
                {"id": {"videoId": "a"}}, {"id": {"videoId": "b"}}],
                "nextPageToken": "next"})
        return Response({"items": [
            {"id": "a", "snippet": {"title": "A", "channelId": "x"},
             "statistics": {"viewCount": "12"}, "contentDetails": {"duration": "PT2M"}},
            {"id": "b", "snippet": {"title": "B", "channelId": "y"},
             "statistics": {}, "contentDetails": {"duration": "PT3M"}},
        ]})


class CollectorTests(unittest.TestCase):
    def test_reads_local_api_key_without_overriding_environment(self):
        import os
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("YOUTUBE_API_KEY=from-file\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                load_local_env(env_file)
                self.assertEqual(os.environ["YOUTUBE_API_KEY"], "from-file")
            with patch.dict(os.environ, {"YOUTUBE_API_KEY": "from-shell"}, clear=True):
                load_local_env(env_file)
                self.assertEqual(os.environ["YOUTUBE_API_KEY"], "from-shell")

    def test_pagination_deduplication_and_missing_statistics(self):
        session = Session()
        records = collect(session, "fake-key", "big data", 2)
        self.assertEqual([r["video_id"] for r in records], ["a", "b"])
        self.assertEqual(records[0]["view_count"], 12)
        self.assertIsNone(records[1]["view_count"])
        self.assertEqual(session.calls[-1][1]["id"], "a,b")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            write_jsonl(records, path)
            self.assertEqual(len([json.loads(x) for x in path.read_text().splitlines()]), 2)

    def test_rejects_unbounded_collection(self):
        with self.assertRaises(ValueError):
            collect(Session(), "fake-key", "query", 31)

    def test_limits_number_of_returned_records(self):
        records = collect(Session(), "fake-key", "query", 2, max_videos=1)
        self.assertEqual(len(records), 1)

    def test_combines_queries_without_duplicate_videos(self):
        records = collect_queries(Session(), "fake-key", ["big data", "apache spark"], 2, 2)
        self.assertEqual([record["video_id"] for record in records], ["a", "b"])

    def test_api_error_does_not_print_key(self):
        import requests

        class FailedResponse:
            status_code = 403

            def raise_for_status(self):
                raise requests.HTTPError("https://api.example/?key=secret-key")

        class FailedSession:
            def get(self, url, params, timeout):
                return FailedResponse()

        with self.assertRaises(YouTubeAPIError) as raised:
            collect(FailedSession(), "secret-key", "query", 1)
        self.assertIn("HTTP 403", str(raised.exception))
        self.assertNotIn("secret-key", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
