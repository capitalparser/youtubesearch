#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files in data/."""

import os
import re
import time
import json
import urllib.request
import urllib.error

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

EXCLUDE_DIRS = set()  # process all directories


def parse_frontmatter(content):
    """Parse YAML frontmatter from markdown content."""
    if not content.startswith("---"):
        return {}, content
    end = content.find("\n---", 3)
    if end == -1:
        return {}, content
    fm_text = content[4:end]
    body = content[end + 4:]

    result = {}
    for line in fm_text.splitlines():
        # skip indented lines (malformed)
        if line.startswith("  "):
            line = line.strip()
        m = re.match(r'^(\w+):\s*(.*)', line)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        # handle list values like [a, b, c] or ["a", "b"]
        if val.startswith('[') and val.endswith(']'):
            inner = val[1:-1]
            if inner.strip() == '':
                result[key] = []
            else:
                items = [x.strip().strip('"').strip("'") for x in inner.split(',') if x.strip()]
                result[key] = items
        else:
            result[key] = val.strip('"').strip("'")
    return result, body


def send_telegram(text):
    """Send a Telegram message. Returns True on success."""
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text
    }).encode("utf-8")
    req = urllib.request.Request(
        TELEGRAM_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            return data.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP error {e.code}: {body[:200]}")
        return False
    except Exception as e:
        print(f"  Request error: {e}")
        return False


def mark_sent(filepath, content):
    """Replace telegram_sent: false with telegram_sent: true in the file."""
    new_content = re.sub(
        r'(telegram_sent:\s*)false',
        r'\1true',
        content,
        count=1
    )
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)


def build_message(fm, filepath):
    """Build Telegram HTML message from frontmatter dict."""
    # Determine channel name from the file path
    parts = filepath.split(os.sep)
    # Find data dir position
    try:
        data_idx = parts.index("data")
        channel_dir = parts[data_idx + 1] if data_idx + 1 < len(parts) else "unknown"
    except ValueError:
        channel_dir = "unknown"

    # Prefer channel > source > author > directory name
    channel = fm.get("channel") or fm.get("source") or fm.get("author") or channel_dir

    # Prefer title; fall back to type or filename
    title = fm.get("title", "")
    if not title:
        doc_type = fm.get("type", "")
        if doc_type == "cross_channel_consensus":
            title = "크로스채널 컨센서스 분석"
        elif doc_type == "bok_cross_reference":
            title = "BOK 연구 교차검증"
        elif doc_type:
            title = doc_type
        else:
            title = os.path.basename(filepath).replace(".md", "")
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

    # Escape HTML special chars in title
    title_escaped = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    channel_escaped = channel.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    lines = [
        f"<b>{channel_escaped}</b> {title_escaped}",
        f"📅 {date}",
    ]
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


def collect_unsent_files():
    """Find all .md files with telegram_sent: false or missing."""
    unsent = []
    for root, dirs, files in os.walk(DATA_DIR):
        # Skip excluded dirs
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for fname in sorted(files):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(root, fname)
            with open(fpath, 'r', encoding='utf-8') as f:
                content = f.read()
            fm, _ = parse_frontmatter(content)
            sent = fm.get("telegram_sent", "false")
            if sent == "false" or sent is False or sent == "":
                unsent.append((fpath, content, fm))
    return unsent


def main():
    unsent = collect_unsent_files()
    print(f"Found {len(unsent)} unsent files")

    sent_count = 0
    failed_count = 0

    for i, (fpath, content, fm) in enumerate(unsent):
        short = os.path.relpath(fpath, DATA_DIR)
        print(f"[{i+1}/{len(unsent)}] {short}")

        msg = build_message(fm, fpath)
        success = send_telegram(msg)

        if success:
            mark_sent(fpath, content)
            sent_count += 1
            print(f"  ✓ sent")
        else:
            failed_count += 1
            print(f"  ✗ failed")

        # 1 second delay between messages
        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone: {sent_count} sent, {failed_count} failed")


if __name__ == "__main__":
    main()
