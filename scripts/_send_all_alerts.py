#!/usr/bin/env python3
"""
Send Telegram alerts for all unsent markdown files in data/
Updates telegram_sent: false -> true after successful send.
"""

import os
import re
import time
import json
import urllib.request
import urllib.error
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
DATA_DIR = REPO_ROOT / "data"

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def parse_frontmatter(text: str) -> dict:
    """Extract YAML frontmatter fields as a dict (simple line-by-line parser)."""
    if not text.startswith("---"):
        return {}
    lines = text.split("\n")
    fm_lines = []
    in_fm = False
    for i, line in enumerate(lines):
        if i == 0 and line.strip() == "---":
            in_fm = True
            continue
        if in_fm:
            if line.strip() == "---":
                break
            fm_lines.append(line)

    data = {}
    for line in fm_lines:
        m = re.match(r'^(\w+):\s*(.*)', line)
        if m:
            key = m.group(1)
            val = m.group(2).strip()
            # Parse lists
            if val.startswith("[") and val.endswith("]"):
                inner = val[1:-1]
                if inner.strip() == "":
                    data[key] = []
                else:
                    items = [x.strip().strip('"').strip("'") for x in inner.split(",")]
                    data[key] = [x for x in items if x]
            else:
                # Strip surrounding quotes
                data[key] = val.strip('"').strip("'")
    return data


def build_message(fm: dict) -> str:
    channel = fm.get("channel", "")
    title = fm.get("title", "")
    date = fm.get("date", "")
    url = fm.get("url", "")
    sectors = fm.get("sectors", [])
    tickers = fm.get("tickers", [])
    themes = fm.get("themes", [])
    sentiment = fm.get("sentiment", "")

    if isinstance(sectors, list):
        sectors_str = ", ".join(sectors) if sectors else "-"
    else:
        sectors_str = sectors or "-"

    if isinstance(themes, list):
        themes_str = ", ".join(themes) if themes else "-"
    else:
        themes_str = themes or "-"

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors_str}",
    ]

    if isinstance(tickers, list) and tickers:
        lines.append(f"💹 종목: {', '.join(tickers)}")
    elif isinstance(tickers, str) and tickers:
        lines.append(f"💹 종목: {tickers}")

    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return "\n".join(lines)


def send_telegram(message: str) -> bool:
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": message,
    }).encode("utf-8")
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("ok", False)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  HTTP {e.code}: {body[:200]}")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def mark_sent(filepath: Path):
    text = filepath.read_text(encoding="utf-8")
    updated = text.replace("telegram_sent: false", "telegram_sent: true", 1)
    filepath.write_text(updated, encoding="utf-8")


def main():
    # Find all markdown files where telegram_sent is false or missing
    all_md = list(DATA_DIR.rglob("*.md"))
    unsent = []
    for f in all_md:
        content = f.read_text(encoding="utf-8")
        if "telegram_sent: true" in content:
            continue
        unsent.append(f)

    print(f"Found {len(unsent)} unsent files")
    sent_count = 0
    fail_count = 0

    for i, filepath in enumerate(sorted(unsent)):
        content = filepath.read_text(encoding="utf-8")
        fm = parse_frontmatter(content)

        if not fm.get("url"):
            print(f"  [{i+1}/{len(unsent)}] SKIP (no url): {filepath.name}")
            continue

        msg = build_message(fm)
        print(f"  [{i+1}/{len(unsent)}] Sending: {filepath.name[:60]}")

        ok = send_telegram(msg)
        if ok:
            mark_sent(filepath)
            sent_count += 1
            print(f"    OK -> telegram_sent: true")
        else:
            fail_count += 1
            print(f"    FAILED")

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone: {sent_count} sent, {fail_count} failed")


if __name__ == "__main__":
    main()
