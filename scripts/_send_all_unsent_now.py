#!/usr/bin/env python3
"""Send Telegram alerts for all unsent files and mark them as sent."""

import os
import re
import time
import glob
import requests
import yaml

TELEGRAM_BOT_TOKEN = "8324061381:AAH5AWkw0Fiw66oem1DM2VgbY2-Bqs9fsrU"
TELEGRAM_CHAT_ID = "7698095566"
TELEGRAM_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
DATA_DIR = "/home/user/youtubesearch/data"


def parse_frontmatter(content):
    """Extract YAML frontmatter from markdown content."""
    match = re.match(r'^---\n(.*?)\n---\n', content, re.DOTALL)
    if not match:
        return None, content
    try:
        fm = yaml.safe_load(match.group(1))
        return fm, content
    except Exception:
        return None, content


def set_telegram_sent(filepath, content):
    """Set telegram_sent: true in the file's frontmatter."""
    new_content = re.sub(
        r'(telegram_sent:\s*)false',
        r'\1true',
        content,
        count=1
    )
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)


def build_message(fm):
    """Build Telegram HTML message from frontmatter."""
    channel = fm.get('channel') or fm.get('source') or 'Unknown'
    title = fm.get('title', '제목 없음')
    date = fm.get('date', '')
    url = fm.get('url', '')
    sectors = fm.get('sectors') or []
    tickers = fm.get('tickers') or []
    themes = fm.get('themes') or []
    sentiment = fm.get('sentiment', '')

    sectors_str = ', '.join(str(s) for s in sectors) if sectors else '-'
    tickers_str = ', '.join(str(t) for t in tickers) if tickers else ''
    themes_str = ', '.join(str(t) for t in themes) if themes else '-'

    # Escape HTML special chars in title
    title_safe = title.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    channel_safe = str(channel).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    lines = [
        f"<b>[{channel_safe}]</b> {title_safe}",
        f"📅 {date}",
    ]
    if url:
        lines.append(f'🔗 <a href="{url}">영상 보기</a>')
    lines.append(f"📊 섹터: {sectors_str}")
    if tickers_str:
        lines.append(f"💹 종목: {tickers_str}")
    lines.append(f"🎯 테마: {themes_str}")
    lines.append(f"📈 센티먼트: {sentiment}")

    return '\n'.join(lines)


def send_telegram(message):
    """Send a message via Telegram Bot API. Returns True on success."""
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "parse_mode": "HTML",
        "text": message
    }
    try:
        resp = requests.post(TELEGRAM_URL, json=payload, timeout=15)
        data = resp.json()
        if resp.status_code == 200 and data.get('ok'):
            return True
        else:
            print(f"  [WARN] Telegram error: {resp.status_code} {data}")
            return False
    except Exception as e:
        print(f"  [ERROR] Request failed: {e}")
        return False


def main():
    all_files = sorted(glob.glob(os.path.join(DATA_DIR, '**', '*.md'), recursive=True))
    unsent = []
    for fp in all_files:
        with open(fp, 'r', encoding='utf-8') as f:
            content = f.read()
        if 'telegram_sent: true' not in content:
            unsent.append((fp, content))

    print(f"Found {len(unsent)} unsent files.")
    sent_count = 0
    failed_count = 0

    for i, (fp, content) in enumerate(unsent):
        fm, _ = parse_frontmatter(content)
        if fm is None:
            print(f"  [{i+1}/{len(unsent)}] SKIP (no frontmatter): {os.path.basename(fp)}")
            continue

        msg = build_message(fm)
        print(f"  [{i+1}/{len(unsent)}] Sending: {os.path.basename(fp)[:60]}")

        if send_telegram(msg):
            set_telegram_sent(fp, content)
            sent_count += 1
        else:
            failed_count += 1

        if i < len(unsent) - 1:
            time.sleep(1)

    print(f"\nDone. Sent: {sent_count}, Failed: {failed_count}")


if __name__ == '__main__':
    main()
