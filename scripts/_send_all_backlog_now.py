#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files under data/."""

import os
import re
import time
import requests

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TG_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)


def parse_frontmatter(text):
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}
    block = m.group(1)
    result = {}
    for line in block.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        # handle YAML lists like [a, b, c]
        if val.startswith("[") and val.endswith("]"):
            inner = val[1:-1]
            if inner.strip():
                items = [x.strip().strip('"').strip("'") for x in inner.split(",")]
            else:
                items = []
            result[key] = items
        else:
            result[key] = val.strip('"').strip("'")
    return result


def set_telegram_sent(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()
    text = text.replace("telegram_sent: false", "telegram_sent: true", 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)


def build_message(fm, filepath):
    channel = fm.get("channel") or fm.get("source") or os.path.basename(os.path.dirname(filepath))
    title = fm.get("title", os.path.basename(filepath))
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    if isinstance(sectors, list):
        sectors_str = ", ".join(sectors)
    else:
        sectors_str = str(sectors)

    if isinstance(tickers, list):
        tickers_str = ", ".join(tickers)
    else:
        tickers_str = str(tickers)

    if isinstance(themes, list):
        themes_str = ", ".join(themes)
    else:
        themes_str = str(themes)

    lines = []
    lines.append(f"<b>[{channel}]</b> {title}")
    lines.append(f"📅 {date}")
    if url:
        lines.append(f'🔗 <a href="{url}">영상 보기</a>')
    if sectors_str:
        lines.append(f"📊 섹터: {sectors_str}")
    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")
    if themes_str:
        lines.append(f"🎯 테마: {themes_str}")
    if sentiment:
        lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def find_unsent_files():
    unsent = []
    for root, dirs, files in os.walk(DATA_DIR):
        # skip hidden dirs
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in sorted(files):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    text = f.read()
            except Exception:
                continue
            # include if telegram_sent is false or missing
            if "telegram_sent: true" in text:
                continue
            unsent.append(fpath)
    return sorted(unsent)


def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files")

    sent_count = 0
    failed_count = 0

    for i, fpath in enumerate(unsent):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                text = f.read()
        except Exception as e:
            print(f"[{i+1}/{len(unsent)}] READ ERROR {fpath}: {e}")
            failed_count += 1
            continue

        fm = parse_frontmatter(text)
        msg = build_message(fm, fpath)

        try:
            resp = requests.post(TG_URL, json={
                "chat_id": CHAT_ID,
                "parse_mode": "HTML",
                "text": msg,
            }, timeout=15)
            data = resp.json()
            if resp.status_code == 200 and data.get("ok"):
                set_telegram_sent(fpath)
                sent_count += 1
                print(f"[{i+1}/{len(unsent)}] SENT: {os.path.relpath(fpath, DATA_DIR)}")
            else:
                failed_count += 1
                print(f"[{i+1}/{len(unsent)}] FAILED ({resp.status_code}): {data} — {os.path.relpath(fpath, DATA_DIR)}")
        except Exception as e:
            failed_count += 1
            print(f"[{i+1}/{len(unsent)}] ERROR: {e} — {os.path.relpath(fpath, DATA_DIR)}")

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone. Sent: {sent_count}, Failed: {failed_count}")


if __name__ == "__main__":
    main()
