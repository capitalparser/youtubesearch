#!/usr/bin/env python3
"""
Send Telegram alerts for all unsent markdown files in data/.
Updates telegram_sent: true in frontmatter after each successful send.
"""

import os
import re
import sys
import time
import json
import requests
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def find_unsent_files():
    files = []
    for md in sorted(DATA_DIR.rglob("*.md")):
        if "links/" in str(md):
            continue
        text = md.read_text(encoding="utf-8")
        # Skip if telegram_sent is true
        if re.search(r"telegram_sent:\s*true", text):
            continue
        files.append(md)
    return files


def parse_frontmatter(text):
    m = re.match(r"^---\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return {}
    fm_text = m.group(1)
    data = {}

    def get(key):
        pat = rf"^{key}:\s*(.+)$"
        match = re.search(pat, fm_text, re.MULTILINE)
        return match.group(1).strip() if match else ""

    def get_list(key):
        pat = rf"^{key}:\s*\[([^\]]*)\]"
        match = re.search(pat, fm_text, re.MULTILINE)
        if not match:
            return []
        raw = match.group(1)
        items = [x.strip().strip('"').strip("'") for x in raw.split(",") if x.strip()]
        return items

    data["title"] = get("title").strip('"')
    data["channel"] = get("channel").strip('"')
    data["date"] = get("date")
    data["url"] = get("url").strip('"')
    data["sectors"] = get_list("sectors")
    data["tickers"] = get_list("tickers")
    data["themes"] = get_list("themes")
    data["sentiment"] = get("sentiment")
    data["source"] = get("source")
    return data


def build_message(fm):
    channel = fm.get("channel") or fm.get("source") or "알 수 없음"
    title = fm.get("title", "")
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = ", ".join(fm.get("sectors", [])) or "-"
    tickers = fm.get("tickers", [])
    themes = ", ".join(fm.get("themes", [])) or "-"
    sentiment = fm.get("sentiment", "-")

    lines = [
        f"<b>{channel}</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {themes}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)


def send_telegram(message):
    payload = {"chat_id": CHAT_ID, "parse_mode": "HTML", "text": message}
    resp = requests.post(TELEGRAM_URL, json=payload, timeout=15)
    return resp.status_code == 200 and resp.json().get("ok", False)


def mark_sent(filepath):
    text = filepath.read_text(encoding="utf-8")
    # Replace telegram_sent: false with telegram_sent: true
    new_text = re.sub(r"(telegram_sent:\s*)false", r"\1true", text)
    if new_text == text:
        # Field might be missing — insert after sentiment line
        new_text = re.sub(
            r"(sentiment:\s*\S+)",
            r"\1\ntelegram_sent: true",
            text,
            count=1,
        )
    filepath.write_text(new_text, encoding="utf-8")


def main():
    files = find_unsent_files()
    print(f"Found {len(files)} unsent files")

    sent = 0
    failed = 0
    for i, fp in enumerate(files, 1):
        text = fp.read_text(encoding="utf-8")
        fm = parse_frontmatter(text)
        msg = build_message(fm)

        print(f"[{i}/{len(files)}] Sending: {fp.name[:60]}")
        ok = send_telegram(msg)
        if ok:
            mark_sent(fp)
            sent += 1
            print(f"  ✓ sent")
        else:
            failed += 1
            print(f"  ✗ FAILED")

        if i < len(files):
            time.sleep(1)

    print(f"\nDone: {sent} sent, {failed} failed")
    result = {"sent": sent, "failed": failed, "total": len(files)}
    print(f"__RESULT_JSON__:{json.dumps(result)}")


if __name__ == "__main__":
    main()
