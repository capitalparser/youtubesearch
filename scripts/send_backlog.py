#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files in data/."""

import os
import re
import time
import urllib.request
import urllib.parse
import json
import sys

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
DATA_DIR = "/home/user/youtubesearch/data"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

SKIP_DIRS = {"analysis", "links"}


def parse_frontmatter(content):
    """Extract YAML frontmatter as a dict (simple key: value parser)."""
    if not content.startswith("---"):
        return {}
    end = content.find("\n---", 3)
    if end == -1:
        return {}
    fm_text = content[4:end]
    result = {}
    for line in fm_text.splitlines():
        if ": " in line:
            key, _, val = line.partition(": ")
            key = key.strip()
            val = val.strip()
            # parse lists like [a, b, c]
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                if inner:
                    items = [x.strip().strip('"').strip("'") for x in inner.split(",")]
                else:
                    items = []
                result[key] = items
            else:
                result[key] = val.strip('"').strip("'")
    return result


def find_unsent_files():
    files = []
    for root, dirs, filenames in os.walk(DATA_DIR):
        # skip unwanted dirs
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fn in sorted(filenames):
            if not fn.endswith(".md"):
                continue
            path = os.path.join(root, fn)
            with open(path, encoding="utf-8") as f:
                content = f.read()
            fm = parse_frontmatter(content)
            sent = fm.get("telegram_sent", "false")
            if sent == "true":
                continue
            files.append((path, content, fm))
    return files


def build_message(fm):
    channel = fm.get("channel", "")
    title = fm.get("title", "")
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    sectors_str = ", ".join(sectors) if sectors else "-"
    themes_str = ", ".join(themes) if themes else "-"
    sentiment_str = sentiment if sentiment else "-"

    lines = [
        f"<b>{channel}</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors_str}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment_str}")

    return "\n".join(lines)


def send_telegram(text):
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }).encode("utf-8")
    req = urllib.request.Request(
        TELEGRAM_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            return resp.status == 200 and body.get("ok") is True, body
    except Exception as e:
        return False, str(e)


def mark_sent(path, content):
    new_content = re.sub(
        r"^(telegram_sent:\s*)false\s*$",
        "telegram_sent: true",
        content,
        flags=re.MULTILINE,
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)


def main():
    files = find_unsent_files()
    print(f"Found {len(files)} unsent files.")
    sent_count = 0
    fail_count = 0

    for i, (path, content, fm) in enumerate(files):
        msg = build_message(fm)
        ok, resp = send_telegram(msg)
        short_path = os.path.relpath(path, DATA_DIR)
        if ok:
            mark_sent(path, content)
            sent_count += 1
            print(f"[{i+1}/{len(files)}] SENT: {short_path}")
        else:
            fail_count += 1
            print(f"[{i+1}/{len(files)}] FAIL: {short_path} -> {resp}")
        if i < len(files) - 1:
            time.sleep(1)

    print(f"\nDone. Sent: {sent_count}, Failed: {fail_count}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
