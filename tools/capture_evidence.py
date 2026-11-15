"""Save short, faithful terminal-output captures for the weekly LaTeX reports."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
FIGURES = ROOT / "docs" / "figures"
FIGURES.mkdir(parents=True, exist_ok=True)
FONT_PATH = Path("C:/Windows/Fonts/consola.ttf")


def run(*args: str) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=True)
    return re.sub(r"\x1b\[[0-9;]*m", "", result.stdout).strip()


def draw(name: str, command: str, output: str) -> None:
    lines = [f"PS> {command}", ""] + output.splitlines()
    font = ImageFont.truetype(str(FONT_PATH), 20)
    line_height = 29
    width = 1320
    height = 96 + len(lines) * line_height + 34
    image = Image.new("RGB", (width, height), "#101820")
    ink = ImageDraw.Draw(image)
    ink.rounded_rectangle((1, 1, width - 2, height - 2), radius=13,
                          outline="#5e7689", width=2)
    ink.rectangle((2, 2, width - 3, 55), fill="#243645")
    ink.text((24, 16), "PowerShell  |  elghali project", font=font, fill="#e4edf3")
    for index, line in enumerate(lines):
        ink.text((27, 75 + index * line_height), line, font=font,
                 fill="#8be0a7" if index == 0 else "#edf4f7")
    image.save(FIGURES / f"{name}.png")


def main() -> None:
    statuses = run("docker", "compose", "ps", "--format", "table {{.Service}}\t{{.Status}}")
    draw("terminal_docker_ps", "docker compose ps --format ...", statuses)

    offsets = run("docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                  "--bootstrap-server", "kafka:29092", "--topic", "youtube.videos.raw")
    draw("terminal_kafka_offsets", "kafka-get-offsets.sh --topic youtube.videos.raw", offsets)

    report = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfsadmin", "-report")
    important = [line for line in report.splitlines() if
                 line.startswith(("Live datanodes", "Name:", "Configured Capacity:"))]
    replication = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "getconf",
                      "-confKey", "dfs.replication")
    draw("terminal_hdfs_nodes", "hdfs dfsadmin -report  [extrait]", "\n".join(important) +
         f"\n\nReplication factor: {replication}")

    bronze = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-ls",
                 "/data/youtube/bronze")
    gold = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-ls",
               "/data/youtube/gold")
    draw("terminal_hdfs_files", "hdfs dfs -ls /data/youtube/bronze ; hdfs dfs -ls /data/youtube/gold",
         "\n".join(line for line in (bronze + "\n" + gold).splitlines()
                   if "/data/youtube/" in line))

    quality = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-cat",
                  "/data/youtube/gold/quality/part-*.json")
    quality = "\n".join(line for line in quality.splitlines() if line.startswith("{"))
    draw("terminal_quality", "hdfs dfs -cat /data/youtube/gold/quality/part-*.json", quality)

    real_offsets = run("docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                       "--bootstrap-server", "kafka:29092", "--topic", "youtube.videos.real")
    draw("terminal_real_kafka", "kafka-get-offsets.sh --topic youtube.videos.real", real_offsets)

    real_quality = run("docker", "compose", "exec", "-T", "namenode", "hdfs", "dfs", "-cat",
                       "/data/youtube_real/gold/quality/part-*.json")
    real_quality = "\n".join(line for line in real_quality.splitlines() if line.startswith("{"))
    draw("terminal_real_quality", "hdfs dfs -cat /data/youtube_real/gold/quality/part-*.json",
         real_quality)

    for file in FIGURES.glob("terminal_*.png"):
        print(f"{file.name}: {file.stat().st_size} bytes")


if __name__ == "__main__":
    main()
