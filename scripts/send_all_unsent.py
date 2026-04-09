#!/usr/bin/env python3
"""Send Telegram alerts for all unsent markdown files in data/."""

import os
import re
import time
import urllib.request
import urllib.parse
import json
import yaml

BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
CHAT_ID = "7698095566"
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
DATA_DIR = "/home/user/youtubesearch/data"

FRONTMATTER_RE = re.compile(r'^---\n(.*?)\n---', re.DOTALL)


def parse_frontmatter(text):
    m = FRONTMATTER_RE.match(text)
    if not m:
        return None, text
    try:
        fm = yaml.safe_load(m.group(1))
    except Exception:
        return None, text
    return fm, text


def set_telegram_sent(filepath, content):
    """Set telegram_sent: false -> true in file."""
    new_content = re.sub(r'telegram_sent: false', 'telegram_sent: true', content, count=1)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)


def build_message(fm):
    channel = fm.get('channel', fm.get('source', ''))
    title = fm.get('title', '')
    date = str(fm.get('date', ''))
    url = fm.get('url', '')
    sectors = fm.get('sectors', []) or []
    tickers = fm.get('tickers', []) or []
    themes = fm.get('themes', []) or []
    sentiment = fm.get('sentiment', '')

    # Escape HTML special chars in title
    def esc(s):
        return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    lines = []
    lines.append(f"<b>{esc(channel)}</b> {esc(title)}")
    lines.append(f"📅 {esc(date)}")
    if url:
        lines.append(f'🔗 <a href="{esc(url)}">영상 보기</a>')
    if sectors:
        lines.append(f"📊 섹터: {esc(', '.join(str(s) for s in sectors))}")
    if tickers:
        lines.append(f"💹 종목: {esc(', '.join(str(t) for t in tickers))}")
    if themes:
        lines.append(f"🎯 테마: {esc(', '.join(str(t) for t in themes))}")
    if sentiment:
        lines.append(f"📈 센티먼트: {esc(sentiment)}")

    return '\n'.join(lines)


def send_telegram(text):
    payload = json.dumps({
        "chat_id": CHAT_ID,
        "parse_mode": "HTML",
        "text": text
    }).encode('utf-8')
    req = urllib.request.Request(
        API_URL,
        data=payload,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode('utf-8'))
            return body.get('ok', False)
    except Exception as e:
        print(f"  ERROR sending: {e}")
        return False


def find_unsent_files():
    unsent = []
    for root, dirs, files in os.walk(DATA_DIR):
        # Skip analysis and links directories
        rel = os.path.relpath(root, DATA_DIR)
        if rel.startswith('analysis') or rel.startswith('links'):
            continue
        for fname in sorted(files):
            if not fname.endswith('.md'):
                continue
            fpath = os.path.join(root, fname)
            with open(fpath, 'r', encoding='utf-8') as f:
                content = f.read()
            if 'telegram_sent: true' in content:
                continue
            unsent.append((fpath, content))
    return unsent


def main():
    unsent = find_unsent_files()
    print(f"Found {len(unsent)} unsent files")

    sent_count = 0
    fail_count = 0

    for i, (fpath, content) in enumerate(unsent):
        fm, _ = parse_frontmatter(content)
        if fm is None:
            print(f"  [{i+1}] SKIP (no frontmatter): {os.path.basename(fpath)}")
            continue

        # Check if enriched, if not skip (shouldn't happen per our check)
        if not fm.get('enriched'):
            print(f"  [{i+1}] SKIP (not enriched): {os.path.basename(fpath)}")
            continue

        msg = build_message(fm)
        fname_short = os.path.basename(fpath)[:60]
        print(f"  [{i+1}/{len(unsent)}] Sending: {fname_short}")

        ok = send_telegram(msg)
        if ok:
            set_telegram_sent(fpath, content)
            sent_count += 1
            print(f"    ✓ Sent & marked")
        else:
            fail_count += 1
            print(f"    ✗ Failed")

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\n=== Done: {sent_count} sent, {fail_count} failed ===")


if __name__ == '__main__':
    main()
