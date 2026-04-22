#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files in data/."""

import os
import re
import time
import glob
import json
import requests
import yaml

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "7698095566")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def load_frontmatter(content: str):
    """Parse YAML frontmatter and return (meta_dict, body)."""
    if not content.startswith("---"):
        return {}, content
    end = content.find("\n---", 3)
    if end == -1:
        return {}, content
    yaml_text = content[3:end].strip()
    body = content[end + 4:]
    try:
        meta = yaml.safe_load(yaml_text) or {}
    except Exception:
        meta = {}
    return meta, body


def set_telegram_sent(filepath: str):
    """Flip telegram_sent: false → true in the file."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    new_content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)


def build_message(meta: dict, filepath: str) -> str:
    channel = meta.get("channel", meta.get("source", os.path.dirname(filepath).split("/")[-1]))
    title = meta.get("title", os.path.basename(filepath))
    date = str(meta.get("date", ""))
    url = meta.get("url", "")
    sectors = meta.get("sectors") or []
    tickers = meta.get("tickers") or []
    themes = meta.get("themes") or []
    sentiment = meta.get("sentiment", "")

    sectors_str = ", ".join(str(s) for s in sectors) if sectors else "—"
    themes_str = ", ".join(str(t) for t in themes) if themes else "—"

    lines = [
        f"<b>{channel}</b> {title}",
        f"📅 {date}",
    ]
    if url:
        lines.append(f'🔗 <a href="{url}">영상 보기</a>')
    lines.append(f"📊 섹터: {sectors_str}")
    if tickers:
        tickers_str = ", ".join(str(t) for t in tickers)
        lines.append(f"💹 종목: {tickers_str}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)


def find_unsent_files(data_dir: str):
    files = []
    for md in glob.glob(os.path.join(data_dir, "**", "*.md"), recursive=True):
        with open(md, "r", encoding="utf-8") as f:
            content = f.read()
        # Skip files with telegram_sent: true
        if "telegram_sent: true" in content:
            continue
        # Include files with telegram_sent: false OR missing telegram_sent
        files.append(md)
    # Sort by date in filename for chronological order
    files.sort()
    return files


def main():
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    unsent = find_unsent_files(data_dir)
    print(f"Found {len(unsent)} unsent files")

    sent = 0
    failed = 0
    skipped = 0

    for i, filepath in enumerate(unsent):
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        meta, _ = load_frontmatter(content)

        # Skip files that have no meaningful enrichment yet
        if not meta.get("enriched") and meta.get("source") not in ("YouTube", "BOK경제연구", "X"):
            print(f"[{i+1}/{len(unsent)}] SKIP (no enrichment): {os.path.basename(filepath)}")
            skipped += 1
            continue

        msg = build_message(meta, filepath)

        try:
            resp = requests.post(
                API_URL,
                json={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": msg},
                timeout=15,
            )
            data = resp.json()
            if resp.status_code == 200 and data.get("ok"):
                set_telegram_sent(filepath)
                sent += 1
                print(f"[{i+1}/{len(unsent)}] OK: {os.path.basename(filepath)}")
            else:
                failed += 1
                print(f"[{i+1}/{len(unsent)}] FAIL ({resp.status_code}): {data} | {os.path.basename(filepath)}")
        except Exception as e:
            failed += 1
            print(f"[{i+1}/{len(unsent)}] ERROR: {e} | {os.path.basename(filepath)}")

        time.sleep(1)

    print(f"\n=== Done: {sent} sent, {failed} failed, {skipped} skipped ===")


if __name__ == "__main__":
    main()
