#!/usr/bin/env python3
"""Send Telegram alerts for all unsent files and mark them sent."""

import os
import re
import time
import json
import urllib.request
import urllib.error

DATA_DIR = "/home/user/youtubesearch/data"
BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---", re.DOTALL)


def parse_frontmatter(content):
    """Parse YAML frontmatter without external deps."""
    m = FRONTMATTER_RE.match(content)
    if not m:
        return {}
    fm = {}
    for line in m.group(1).splitlines():
        if ": " in line:
            key, _, val = line.partition(": ")
            key = key.strip()
            val = val.strip()
            # Parse list values like [a, b, c] or ["a", "b"]
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                if not inner:
                    fm[key] = []
                else:
                    items = [x.strip().strip('"').strip("'") for x in inner.split(",")]
                    fm[key] = [x for x in items if x]
            elif val in ("true", "false"):
                fm[key] = val == "true"
            else:
                fm[key] = val.strip('"').strip("'")
    return fm


def build_message(fm, filepath):
    """Build Telegram HTML message from frontmatter."""
    source = fm.get("source", "")
    file_type = fm.get("type", "")

    title = fm.get("title", os.path.basename(filepath).replace(".md", ""))
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    # Determine channel/source label
    if source == "YouTube":
        channel = fm.get("channel", "YouTube")
        header = f"<b>[{channel}]</b> {title}"
    elif source == "BOK경제연구":
        header = f"<b>[BOK경제연구]</b> {title}"
    elif source == "X":
        author = fm.get("author", "@unknown")
        header = f"<b>[X / {author}]</b> {title}"
    elif file_type == "cross_channel_consensus":
        header = f"<b>[크로스채널 분석]</b> 컨센서스 리포트 {date}"
    elif file_type:
        header = f"<b>[분석]</b> {title or file_type} {date}"
    else:
        header = f"<b>[수집]</b> {title}"

    lines = [header]
    if date:
        lines.append(f"📅 {date}")
    if url:
        lines.append(f'🔗 <a href="{url}">영상 보기</a>')
    if sectors:
        lines.append(f"📊 섹터: {', '.join(sectors)}")
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    if themes:
        lines.append(f"🎯 테마: {', '.join(themes)}")
    if sentiment:
        lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def send_telegram(text):
    """Send message via Telegram Bot API. Returns True on success."""
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    }).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  HTTP error {e.code}: {body[:200]}")
        return False
    except Exception as e:
        print(f"  Network error: {e}")
        return False


def mark_sent(filepath, content):
    """Replace telegram_sent: false with telegram_sent: true in file."""
    new_content = content.replace("telegram_sent: false", "telegram_sent: true", 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)


def collect_unsent_files():
    """Walk data dir and collect all .md files with telegram_sent: false."""
    unsent = []
    for root, dirs, files in os.walk(DATA_DIR):
        # Sort for deterministic order
        dirs.sort()
        for fname in sorted(files):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(root, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    content = f.read()
                if "telegram_sent: false" in content:
                    unsent.append((fpath, content))
            except Exception as e:
                print(f"  Warning: could not read {fpath}: {e}")
    return unsent


def main():
    unsent = collect_unsent_files()
    print(f"Found {len(unsent)} unsent files.")

    sent_count = 0
    failed_count = 0

    for i, (fpath, content) in enumerate(unsent, 1):
        rel = os.path.relpath(fpath, DATA_DIR)
        fm = parse_frontmatter(content)
        msg = build_message(fm, fpath)

        print(f"[{i}/{len(unsent)}] Sending: {rel}")
        ok = send_telegram(msg)

        if ok:
            mark_sent(fpath, content)
            sent_count += 1
            print(f"  ✓ sent")
        else:
            failed_count += 1
            print(f"  ✗ FAILED")

        if i < len(unsent):
            time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {failed_count} failed ===")


if __name__ == "__main__":
    main()
