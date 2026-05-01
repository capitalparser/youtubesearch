#!/usr/bin/env python3
"""Send Telegram alerts for all files with telegram_sent: false."""

import os
import re
import time
import glob
import requests
import yaml

TELEGRAM_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
DATA_DIR = "/home/user/youtubesearch/data"
DELAY = 1.0

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text}
    r = requests.post(url, json=payload, timeout=15)
    return r.status_code == 200 and r.json().get("ok", False)

def parse_frontmatter(content):
    m = re.match(r'^---\n(.*?)\n---\n', content, re.DOTALL)
    if not m:
        return None, content
    fm_raw = m.group(1)
    try:
        fm = yaml.safe_load(fm_raw)
    except Exception:
        return None, content
    return fm, content

def set_telegram_sent(filepath, content):
    """Replace telegram_sent: false with telegram_sent: true in the file."""
    new_content = re.sub(
        r'^(telegram_sent:\s*)false\s*$',
        r'\1true',
        content,
        flags=re.MULTILINE
    )
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    return new_content

def build_message(fm):
    title = fm.get("title", "제목 없음")
    date = str(fm.get("date", ""))
    url = fm.get("url", "")
    channel = fm.get("channel", fm.get("source", ""))

    sectors = fm.get("sectors", []) or []
    tickers = fm.get("tickers", []) or []
    themes = fm.get("themes", []) or []
    sentiment = fm.get("sentiment", "")

    sectors_str = ", ".join(str(s) for s in sectors) if sectors else "-"
    tickers_str = ", ".join(str(t) for t in tickers) if tickers else ""
    themes_str = ", ".join(str(t) for t in themes) if themes else "-"

    lines = [
        f"<b>[{channel}]</b> {title}",
        f"📅 {date}",
        f'🔗 <a href="{url}">영상 보기</a>',
        f"📊 섹터: {sectors_str}",
    ]
    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")
    return "\n".join(lines)

def main():
    # Find all markdown files
    patterns = [
        os.path.join(DATA_DIR, "**", "*.md"),
        os.path.join(DATA_DIR, "*.md"),
    ]
    all_files = []
    for pat in patterns:
        all_files.extend(glob.glob(pat, recursive=True))
    all_files = sorted(set(all_files))

    print(f"Total markdown files found: {len(all_files)}")

    # Filter to unsent files
    unsent = []
    for fp in all_files:
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        # Check telegram_sent field
        m = re.search(r'^telegram_sent:\s*(.+)$', content, re.MULTILINE)
        if m:
            val = m.group(1).strip().lower()
            if val == "false":
                unsent.append(fp)
        else:
            # Missing telegram_sent — treat as unsent
            unsent.append(fp)

    print(f"Unsent files: {len(unsent)}")

    sent_count = 0
    failed_count = 0

    for i, fp in enumerate(unsent):
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()

        fm, _ = parse_frontmatter(content)
        if fm is None:
            print(f"[{i+1}/{len(unsent)}] SKIP (no frontmatter): {os.path.basename(fp)}")
            continue

        # Check enriched
        if not fm.get("enriched", False):
            print(f"[{i+1}/{len(unsent)}] SKIP (not enriched): {os.path.basename(fp)}")
            continue

        msg = build_message(fm)
        ok = send_telegram(msg)
        if ok:
            set_telegram_sent(fp, content)
            sent_count += 1
            print(f"[{i+1}/{len(unsent)}] SENT: {os.path.basename(fp)}")
        else:
            failed_count += 1
            print(f"[{i+1}/{len(unsent)}] FAILED: {os.path.basename(fp)}")

        time.sleep(DELAY)

    print(f"\n=== Done: {sent_count} sent, {failed_count} failed ===")

if __name__ == "__main__":
    main()
