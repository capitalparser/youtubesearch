#!/usr/bin/env python3
"""Send Telegram alerts for all files where telegram_sent is false, then update frontmatter."""

import os
import re
import time
import glob
import json
import urllib.request
import urllib.error

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

FRONTMATTER_RE = re.compile(r"^---\n(.*?\n)---\n", re.DOTALL)


def parse_yaml_field(text, field):
    """Extract a simple YAML field value from frontmatter text."""
    pattern = re.compile(rf"^{field}:\s*(.+)$", re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return None
    return m.group(1).strip()


def parse_yaml_list(text, field):
    """Extract a YAML list field (inline bracket form) from frontmatter text."""
    pattern = re.compile(rf"^{field}:\s*\[([^\]]*)\]", re.MULTILINE)
    m = pattern.search(text)
    if not m:
        return []
    inner = m.group(1).strip()
    if not inner:
        return []
    items = [i.strip().strip('"').strip("'") for i in inner.split(",")]
    return [i for i in items if i]


def build_message(fm_text, title):
    """Build Telegram HTML message from frontmatter."""
    channel = parse_yaml_field(fm_text, "channel")
    if not channel:
        channel = parse_yaml_field(fm_text, "source") or "Unknown"
    channel = channel.strip('"').strip("'")

    date = parse_yaml_field(fm_text, "date") or ""
    url = parse_yaml_field(fm_text, "url") or ""
    url = url.strip('"').strip("'")

    sectors = parse_yaml_list(fm_text, "sectors")
    tickers = parse_yaml_list(fm_text, "tickers")
    themes = parse_yaml_list(fm_text, "themes")
    sentiment = parse_yaml_field(fm_text, "sentiment") or ""

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {', '.join(sectors) if sectors else '-'}",
    ]
    if tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    lines.append(f"🎯 테마: {', '.join(themes) if themes else '-'}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def send_telegram(text):
    """Send a Telegram message. Returns True on success."""
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
        print(f"  [HTTP ERROR] {e.code}: {body[:200]}")
        return False
    except Exception as e:
        print(f"  [ERROR] {e}")
        return False


def mark_sent(filepath, content):
    """Replace telegram_sent: false with telegram_sent: true in file."""
    new_content = re.sub(
        r"(^telegram_sent:\s*)false",
        r"\1true",
        content,
        flags=re.MULTILINE,
    )
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(new_content)


def find_unsent_files():
    pattern = os.path.join(DATA_DIR, "**", "*.md")
    files = glob.glob(pattern, recursive=True)
    unsent = []
    for fp in sorted(files):
        with open(fp, encoding="utf-8") as f:
            content = f.read()
        if re.search(r"^telegram_sent:\s*false", content, re.MULTILINE):
            unsent.append((fp, content))
    return unsent


def extract_title(fm_text, filepath):
    title = parse_yaml_field(fm_text, "title")
    if title:
        return title.strip('"').strip("'")
    return os.path.basename(filepath).replace(".md", "")


def main():
    unsent = find_unsent_files()
    total = len(unsent)
    print(f"Found {total} unsent files.")

    sent_count = 0
    failed_count = 0

    for i, (filepath, content) in enumerate(unsent, 1):
        m = FRONTMATTER_RE.match(content)
        if not m:
            print(f"[{i}/{total}] SKIP (no frontmatter): {os.path.basename(filepath)}")
            continue

        fm_text = m.group(1)
        title = extract_title(fm_text, filepath)
        msg = build_message(fm_text, title)

        print(f"[{i}/{total}] Sending: {os.path.basename(filepath)[:60]}")
        ok = send_telegram(msg)
        if ok:
            mark_sent(filepath, content)
            sent_count += 1
            print(f"  -> OK")
        else:
            failed_count += 1
            print(f"  -> FAILED")

        if i < total:
            time.sleep(1)

    print(f"\nDone. Sent: {sent_count}, Failed: {failed_count}, Total: {total}")


if __name__ == "__main__":
    main()
