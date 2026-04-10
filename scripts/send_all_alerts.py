#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files and mark them sent."""
import os
import re
import time
import glob
import urllib.request
import urllib.parse
import json

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def parse_frontmatter(text):
    """Extract YAML frontmatter as a dict (simple key: value parser)."""
    m = re.match(r"^---\n(.*?)\n---", text, re.DOTALL)
    if not m:
        return {}
    fm = {}
    for line in m.group(1).split("\n"):
        kv = re.match(r"^(\w+):\s*(.*)", line)
        if kv:
            key, val = kv.group(1), kv.group(2).strip()
            # Parse lists like [a, b, c]
            lm = re.match(r"^\[(.*)\]$", val)
            if lm:
                items = [x.strip() for x in lm.group(1).split(",") if x.strip()]
                fm[key] = items
            else:
                fm[key] = val.strip('"').strip("'")
    return fm


def build_message(fm):
    channel = fm.get("channel", "")
    title = fm.get("title", "")
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    lines = []
    lines.append(f"<b>{channel}</b> {title}")
    lines.append(f"📅 {date}")
    lines.append(f'🔗 <a href="{url}">영상 보기</a>')
    lines.append(f"📊 섹터: {', '.join(sectors) if sectors else '-'}")
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {', '.join(themes) if themes else '-'}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)


def send_message(text):
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text
    }).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read())
        return resp.status == 200 and body.get("ok", False)


def mark_sent(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "**/*.md"), recursive=True))
    unsent = [f for f in files if "telegram_sent: true" not in open(f, encoding="utf-8").read()]
    print(f"Found {len(unsent)} unsent files")

    sent = 0
    failed = 0
    for i, filepath in enumerate(unsent):
        content = open(filepath, encoding="utf-8").read()
        fm = parse_frontmatter(content)
        msg = build_message(fm)
        try:
            ok = send_message(msg)
            if ok:
                mark_sent(filepath)
                print(f"[{i+1}/{len(unsent)}] SENT: {os.path.basename(filepath)}")
                sent += 1
            else:
                print(f"[{i+1}/{len(unsent)}] FAIL (ok=false): {os.path.basename(filepath)}")
                failed += 1
        except Exception as e:
            print(f"[{i+1}/{len(unsent)}] ERROR: {os.path.basename(filepath)}: {e}")
            failed += 1
        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone. Sent: {sent}, Failed: {failed}")


if __name__ == "__main__":
    main()
